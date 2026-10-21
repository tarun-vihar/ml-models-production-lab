from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    msk_auth_mode: str = os.getenv("MSK_AUTH_MODE", "none")
    aws_region: str = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", ""))
    aws_profile: str = os.getenv("AWS_PROFILE", "")
    input_topic: str = os.getenv("KAFKA_INPUT_TOPIC", "scored-transactions")
    output_topic: str = os.getenv("KAFKA_OUTPUT_TOPIC", "graph-scored-transactions")
    dlq_topic: str = os.getenv("KAFKA_DLQ_TOPIC", "graph-dead-letter-transactions")
    consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "fraud-graph-v1")
    graph_mode: str = os.getenv("GRAPH_MODE", "mock")
    neptune_endpoint: str = os.getenv("NEPTUNE_ENDPOINT", "")
    neptune_auth_mode: str = os.getenv("NEPTUNE_AUTH_MODE", "iam")
    neptune_connect_timeout: int = int(os.getenv("NEPTUNE_CONNECT_TIMEOUT_SECONDS", "5"))
    neptune_read_timeout: int = int(os.getenv("NEPTUNE_READ_TIMEOUT_SECONDS", "20"))
    shared_device_account_threshold: int = int(os.getenv("GRAPH_SHARED_DEVICE_ACCOUNT_THRESHOLD", "3"))
    medium_threshold: float = float(os.getenv("GRAPH_MEDIUM_THRESHOLD", "0.30"))
    high_threshold: float = float(os.getenv("GRAPH_HIGH_THRESHOLD", "0.70"))
    max_processing_attempts: int = int(os.getenv("MAX_PROCESSING_ATTEMPTS", "3"))
    metrics_port: int = int(os.getenv("METRICS_PORT", "8001"))
    create_topics: bool = os.getenv("KAFKA_CREATE_TOPICS", "true").lower() in {"true", "1", "yes"}

    def validate(self) -> None:
        if self.msk_auth_mode not in {"none", "iam"}:
            raise ValueError("MSK_AUTH_MODE must be none or iam")
        if self.msk_auth_mode == "iam" and not self.aws_region:
            raise ValueError("AWS_REGION is required when MSK_AUTH_MODE=iam")
        if self.graph_mode not in {"aws", "mock"}:
            raise ValueError("GRAPH_MODE must be aws or mock")
        if self.graph_mode == "aws":
            if not self.neptune_endpoint:
                raise ValueError("NEPTUNE_ENDPOINT is required when GRAPH_MODE=aws")
            if not self.aws_region:
                raise ValueError("AWS_REGION is required when GRAPH_MODE=aws")
            if self.neptune_auth_mode not in {"iam", "none"}:
                raise ValueError("NEPTUNE_AUTH_MODE must be iam or none")
        if self.shared_device_account_threshold < 2:
            raise ValueError("GRAPH_SHARED_DEVICE_ACCOUNT_THRESHOLD must be at least 2")
        if not 0 <= self.medium_threshold < self.high_threshold <= 1:
            raise ValueError("risk thresholds must satisfy 0 <= medium < high <= 1")
        if self.max_processing_attempts < 1:
            raise ValueError("MAX_PROCESSING_ATTEMPTS must be at least 1")
        if self.neptune_connect_timeout < 1 or self.neptune_read_timeout < 1:
            raise ValueError("Neptune timeouts must be positive")
