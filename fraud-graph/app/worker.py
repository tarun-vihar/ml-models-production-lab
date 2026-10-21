import logging
import json

from confluent_kafka import KafkaError, TopicPartition

from .config import Settings
from .observability import CONSUMER_LAG, DEAD_LETTERS, INVALID_EVENTS, MESSAGES_CONSUMED, PUBLISH_FAILURES, RETRIES
from .pipeline import GraphPipeline, InvalidEvent

logger = logging.getLogger(__name__)


class GraphWorker:
    def __init__(self, settings: Settings, consumer, producer, pipeline: GraphPipeline):
        self.settings, self.consumer, self.producer, self.pipeline = settings, consumer, producer, pipeline

    def _publish(self, topic: str, key: bytes | None, value: bytes) -> None:
        failures: list[Exception] = []
        def delivered(error, _message):
            if error is not None:
                failures.append(error)
        self.producer.produce(topic, key=key, value=value, on_delivery=delivered)
        remaining = self.producer.flush(10)
        if remaining or failures:
            PUBLISH_FAILURES.inc()
            raise RuntimeError(f"Kafka delivery failed for {topic}: {failures[0] if failures else remaining}")

    def process(self, message) -> None:
        raw = message.value() if message.value() is not None else b""
        try:
            event, output = self.pipeline.analyze(raw)
        except InvalidEvent as exc:
            INVALID_EVENTS.inc()
            self._publish(self.settings.dlq_topic, message.key(), self.pipeline.dead_letter(raw, exc))
            DEAD_LETTERS.inc()
            self.consumer.commit(message=message, asynchronous=False)
            logger.warning("Invalid scored event sent to graph DLQ error_type=%s", type(exc).__name__)
            return
        # Neptune and Kafka have no shared transaction: MERGE makes graph replay safe,
        # and graphEventId lets downstream consumers deduplicate repeated output.
        self._publish(self.settings.output_topic, output["graphEventId"].encode(),
                      json.dumps(output, separators=(",", ":")).encode())
        self.consumer.commit(message=message, asynchronous=False)
        logger.info("Published graph event event_id=%s transaction_id=%s",
                    event.eventId, event.transactionId)

    def run(self, stop_event) -> None:
        self.consumer.subscribe([self.settings.input_topic])
        try:
            while not stop_event.is_set():
                message = self.consumer.poll(1.0)
                if message is None:
                    continue
                error = message.error()
                if error is not None:
                    if error.code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Kafka consumer error: %s", error)
                    continue
                MESSAGES_CONSUMED.inc()
                try:
                    _low, high = self.consumer.get_watermark_offsets(
                        TopicPartition(message.topic(), message.partition()), timeout=0.1, cached=True)
                    CONSUMER_LAG.set(max(0, high - message.offset() - 1))
                except Exception:
                    logger.debug("Unable to sample Kafka consumer lag", exc_info=True)
                for attempt in range(self.settings.max_processing_attempts):
                    try:
                        self.process(message)
                        break
                    except Exception:
                        logger.exception("Graph record processing failed attempt=%s", attempt + 1)
                        if attempt + 1 == self.settings.max_processing_attempts:
                            try:
                                self._publish(self.settings.dlq_topic, message.key(), self.pipeline.dead_letter(
                                    message.value() or b"", RuntimeError("processing attempts exhausted")))
                                DEAD_LETTERS.inc()
                                self.consumer.commit(message=message, asynchronous=False)
                            except Exception:
                                logger.exception("Unable to dead-letter; stopping for safe replay")
                                raise
                        else:
                            RETRIES.inc()
                            stop_event.wait(min(0.25 * (2 ** attempt), 2.0))
        finally:
            self.consumer.close()
