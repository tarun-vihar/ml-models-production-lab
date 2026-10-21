from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    msk_auth_mode: str = os.getenv("MSK_AUTH_MODE", "none")
    aws_region: str = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", ""))
    aws_profile: str = os.getenv("AWS_PROFILE", "")
    input_topic: str = os.getenv("KAFKA_INPUT_TOPIC", "enriched-transactions")
    output_topic: str = os.getenv("KAFKA_OUTPUT_TOPIC", "scored-transactions")
    dlq_topic: str = os.getenv("KAFKA_DLQ_TOPIC", "scoring-dead-letter-transactions")
    consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "fraud-scoring-v1")
    model_mode: str = os.getenv("MODEL_MODE", "xgboost")
    model_dir: str = os.getenv("MODEL_DIR", "artifacts")
    model_version: str = os.getenv("MODEL_VERSION", "")
    feature_manifest: str = os.getenv("MODEL_FEATURE_MANIFEST", "artifacts/feature-manifest.json")
    medium_threshold: float = float(os.getenv("SCORING_THRESHOLD_MEDIUM", "0.30"))
    high_threshold: float = float(os.getenv("SCORING_THRESHOLD_HIGH", "0.70"))
    max_processing_attempts: int = int(os.getenv("MAX_PROCESSING_ATTEMPTS", "3"))
    metrics_port: int = int(os.getenv("METRICS_PORT", "8000"))

    def validate(self) -> None:
        if self.model_mode not in {"xgboost", "development-baseline"}:
            raise ValueError("MODEL_MODE must be xgboost or development-baseline")
        if self.msk_auth_mode not in {"none", "iam"}:
            raise ValueError("MSK_AUTH_MODE must be none or iam")
        if self.msk_auth_mode == "iam" and not self.aws_region:
            raise ValueError("AWS_REGION is required when MSK_AUTH_MODE=iam")
        if not 0 <= self.medium_threshold < self.high_threshold <= 1:
            raise ValueError("risk thresholds must satisfy 0 <= medium < high <= 1")
        if self.max_processing_attempts < 1:
            raise ValueError("MAX_PROCESSING_ATTEMPTS must be at least 1")
