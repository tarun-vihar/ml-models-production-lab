import logging
import time

from confluent_kafka import KafkaError, TopicPartition

from .config import Settings
from .observability import (
    CONSUMER_LAG,
    DEAD_LETTERS,
    INVALID_EVENTS,
    MESSAGES_CONSUMED,
    PUBLISH_FAILURES,
    RETRIES,
)
from .pipeline import InvalidEvent, ScoringPipeline

logger = logging.getLogger(__name__)


class ScoringWorker:
    def __init__(self, settings: Settings, consumer, producer, pipeline: ScoringPipeline):
        self.settings = settings
        self.consumer = consumer
        self.producer = producer
        self.pipeline = pipeline

    def _publish(self, topic: str, key: bytes | None, value: bytes) -> None:
        delivery_errors: list[Exception] = []

        def delivered(error, _message):
            if error is not None:
                delivery_errors.append(error)

        self.producer.produce(topic, key=key, value=value, on_delivery=delivered)
        remaining = self.producer.flush(10)
        if remaining or delivery_errors:
            PUBLISH_FAILURES.inc()
            reason = delivery_errors[0] if delivery_errors else f"{remaining} messages still pending"
            raise RuntimeError(f"Kafka delivery failed for {topic}: {reason}")

    def process(self, message) -> None:
        raw = message.value() if message.value() is not None else b""
        key = message.key()
        try:
            event, scored = self.pipeline.score(raw)
        except InvalidEvent as exc:
            INVALID_EVENTS.inc()
            self._publish(self.settings.dlq_topic, key, self.pipeline.dead_letter(raw, exc))
            DEAD_LETTERS.inc()
            self.consumer.commit(message=message, asynchronous=False)
            logger.warning("Invalid event sent to DLQ error_type=%s", type(exc).__name__)
            return

        # Commit the input only after Kafka confirms the score publication.
        self._publish(self.settings.output_topic, scored.scoreId.encode(), scored.model_dump_json().encode())
        self.consumer.commit(message=message, asynchronous=False)
        logger.info("Scored event_id=%s transaction_id=%s model_version=%s risk=%s",
                    event.eventId, event.transactionId, scored.modelVersion, scored.riskCategory)

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
                        TopicPartition(message.topic(), message.partition()), timeout=0.1, cached=True
                    )
                    CONSUMER_LAG.set(max(0, high - message.offset() - 1))
                except Exception:
                    logger.debug("Unable to sample Kafka consumer lag", exc_info=True)
                try:
                    self.process(message)
                except Exception:
                    logger.exception("Record processing failed; input offset remains uncommitted")
                    # Retry this same record synchronously so a later offset cannot
                    # accidentally advance the partition past the failed record.
                    for attempt in range(1, self.settings.max_processing_attempts):
                        if stop_event.wait(min(0.25 * (2 ** (attempt - 1)), 2.0)):
                            break
                        RETRIES.inc()
                        try:
                            self.process(message)
                            break
                        except Exception:
                            logger.exception("Retry %s/%s failed", attempt, self.settings.max_processing_attempts - 1)
                    else:
                        try:
                            self._publish(self.settings.dlq_topic, message.key(),
                                          self.pipeline.dead_letter(message.value(), RuntimeError("processing attempts exhausted")))
                            DEAD_LETTERS.inc()
                            self.consumer.commit(message=message, asynchronous=False)
                        except Exception:
                            logger.exception("Unable to dead-letter failed record; stopping for safe replay")
                            raise
        finally:
            self.consumer.close()
