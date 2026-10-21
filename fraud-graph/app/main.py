import logging
import os
import signal
from threading import Event

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .config import Settings
from .graph_repository import create_repository
from .kafka_client import client_config
from .observability import set_ready, start_http_server
from .pipeline import GraphPipeline
from .worker import GraphWorker


def ensure_topics(settings: Settings, kafka_config: dict) -> None:
    if not settings.create_topics:
        return
    admin = AdminClient(kafka_config)
    for topic, future in admin.create_topics([
        NewTopic(name, num_partitions=3, replication_factor=int(os.getenv("KAFKA_TOPIC_REPLICATION_FACTOR", "1")))
        for name in (settings.output_topic, settings.dlq_topic)
    ]).items():
        try:
            future.result()
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise RuntimeError(f"Unable to create Kafka topic {topic}: {exc}") from exc


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logger = logging.getLogger(__name__)
    settings = Settings()
    settings.validate()
    metrics_server = start_http_server(settings.metrics_port)
    repository = create_repository(settings)
    stop_event = Event()

    def stop(signum, _frame) -> None:
        logger.info("Shutdown requested signal=%s", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        # Wait for Neptune (or the explicit in-memory local mock) before consuming.
        while not stop_event.is_set():
            try:
                repository.verify()
                repository.ensure_schema()
                break
            except Exception:
                logger.exception("Graph repository is not ready; retrying startup connection")
                stop_event.wait(2)
        if stop_event.is_set():
            return

        kafka_config = client_config(settings)
        ensure_topics(settings, kafka_config)
        consumer = Consumer({
            **kafka_config,
            "group.id": settings.consumer_group,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 45000,
        })
        producer = Producer({
            **kafka_config,
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 10000,
        })
        set_ready(True)
        logger.info("Starting fraud graph mode=%s input=%s output=%s",
                    settings.graph_mode, settings.input_topic, settings.output_topic)
        try:
            GraphWorker(settings, consumer, producer,
                        GraphPipeline(settings, repository)).run(stop_event)
        finally:
            producer.flush(10)
    finally:
        set_ready(False)
        repository.close()
        metrics_server.shutdown()


if __name__ == "__main__":
    main()
