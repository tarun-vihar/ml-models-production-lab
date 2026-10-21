import logging
import os
import signal
from threading import Event

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .config import Settings
from .kafka_client import client_config
from .observability import start_http_server
from .pipeline import ScoringPipeline
from .scoring import Scorer
from .worker import ScoringWorker


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def ensure_topics(settings: Settings) -> None:
    if os.getenv("KAFKA_CREATE_TOPICS", "true").lower() not in {"true", "1", "yes"}:
        return
    replication_factor = int(os.getenv("KAFKA_TOPIC_REPLICATION_FACTOR", "1"))
    admin = AdminClient(client_config(settings))
    topics = [NewTopic(topic, num_partitions=3, replication_factor=replication_factor)
              for topic in (settings.output_topic, settings.dlq_topic)]
    for topic, future in admin.create_topics(topics).items():
        try:
            future.result()
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise RuntimeError(f"Unable to create Kafka topic {topic}: {exc}") from exc


def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    settings = Settings()
    settings.validate()

    # Fail fast for an absent or incompatible production model.
    scorer = Scorer(settings.model_mode, settings.model_dir, settings.model_version, settings.feature_manifest)
    metrics_server = start_http_server(settings.metrics_port)
    ensure_topics(settings)

    kafka_config = client_config(settings)
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
    stop_event = Event()

    def stop(signum, _frame) -> None:
        logger.info("Shutdown requested signal=%s", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    worker = ScoringWorker(settings, consumer, producer, ScoringPipeline(settings, scorer))
    logger.info("Starting fraud scoring mode=%s model_version=%s input=%s output=%s",
                settings.model_mode, scorer.model_version, settings.input_topic, settings.output_topic)
    try:
        worker.run(stop_event)
    finally:
        producer.flush(10)
        metrics_server.shutdown()


if __name__ == "__main__":
    main()
