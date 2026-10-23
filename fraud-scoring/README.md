# Fraud Scoring (Layer 2)

Standalone Python service that consumes Layer 1's enriched Kafka events, validates them, scores them, and publishes a versioned result for future downstream consumers. It reuses the root Compose Kafka broker; it does not connect to Layer 1 PostgreSQL or Redis and does not implement Layer 3.

```text
raw-transactions -> Layer 1 (Java) -> enriched-transactions
                                      -> Layer 2 (Python/XGBoost)
                                           -> scored-transactions
                                           -> scoring-dead-letter-transactions
```

## Discovered Layer 1 contract

Layer 1 uses the root Compose broker at `kafka:9092` internally and publishes UTF-8 JSON to `enriched-transactions`. Its schema version 1 includes `eventId`, `transactionId`, `eventTimestamp`, `accountId`, numeric `amount`, `currency`, `merchantId`, `deviceId`, `paymentInstrumentId`, `priorTransactionCount`, `priorAmountTotal`, `suspicious`, `matchedRules`, and `enrichedAt`. Layer 1 writes the original enriched transaction, audit data, and outbox to PostgreSQL, and uses Redis for online account/currency aggregates. Layer 2 reads only the Kafka event.

The candidate numeric feature mapping is in [artifacts/feature-manifest.example.json](artifacts/feature-manifest.example.json). The independent [training pipeline](training/README.md) now trains this exact four-feature contract from a labeled CSV and writes a versioned model and manifest. No labeled training data is included, so this repository still has no trained fraud model.

## Run with Docker Compose

From the repository root:

```sh
docker compose up --build -d
docker compose ps
docker compose logs -f fraud-scoring
```

Compose starts the existing Layer 1 dependencies and waits for Layer 1 to become healthy before starting Layer 2. Local mode explicitly uses `MODEL_MODE=development-baseline`. Send an input event through Layer 1 using its console producer (see [fraud detection guide](../fraud-detection/README.md#start-locally)); Layer 2 will consume it automatically.

Read scores and dead letters in separate terminals:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic scored-transactions --from-beginning

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic scoring-dead-letter-transactions --from-beginning
```

Health and Prometheus metrics are exposed at <http://localhost:8000/health> and <http://localhost:8000/metrics>. Useful checks: `docker compose logs -f fraud-scoring`, `docker compose ps`, and `curl --fail http://localhost:8000/health`.

## Run locally without Compose

Use Python 3.12 or later, install dependencies, and point at the Kafka broker exposed on localhost by Compose:

```sh
cd fraud-scoring
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 MODEL_MODE=development-baseline python -m app.main
```

For XGBoost inference, set `FRAUD_SCORING_MODEL_MODE=xgboost` and mount the generated model artifacts at `MODEL_DIR` (Compose uses `/models`). Startup fails if model or manifest is missing or inconsistent. See [artifact notes](artifacts/README.md) and the [training instructions](training/README.md).

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap brokers |
| `MSK_AUTH_MODE` | `none` | Use `iam` for Amazon MSK IAM authentication over TLS |
| `AWS_REGION` | empty | Required for `MSK_AUTH_MODE=iam` |
| `AWS_PROFILE` | empty | Optional local named AWS profile; leave unset on AWS to use the ECS task role |
| `KAFKA_INPUT_TOPIC` | `enriched-transactions` | Layer 1 output topic |
| `KAFKA_OUTPUT_TOPIC` | `scored-transactions` | Scoring output topic |
| `KAFKA_DLQ_TOPIC` | `scoring-dead-letter-transactions` | Invalid or exhausted event topic |
| `KAFKA_CONSUMER_GROUP` | `fraud-scoring-v1` | Consumer group |
| `KAFKA_CREATE_TOPICS` | `true` | Create output and DLQ topics on startup; production may pre-create and disable this |
| `KAFKA_TOPIC_REPLICATION_FACTOR` | `1` | Replication factor for Layer 2-created topics |
| `MODEL_MODE` | `xgboost` | `xgboost` or explicit local-only `development-baseline` |
| `MODEL_DIR` | `artifacts` | Directory containing versioned `model.json` |
| `MODEL_FEATURE_MANIFEST` | `artifacts/feature-manifest.json` | Feature contract loaded with the model |
| `FRAUD_SCORING_MODEL_ARTIFACTS_DIR` | `./fraud-scoring/artifacts` in Compose | Host directory mounted read-only at `/models` for deployment-supplied artifacts |
| `MODEL_VERSION` | empty | Optional deployment override; otherwise read from the manifest |
| `SCORING_THRESHOLD_MEDIUM` / `SCORING_THRESHOLD_HIGH` | `0.30` / `0.70` | Probability boundaries for risk category |
| `MAX_PROCESSING_ATTEMPTS` | `3` | Attempts before dead-lettering a processing failure |
| `METRICS_PORT` | `8000` | Health and Prometheus HTTP port |

## Model and feature contract

The XGBoost artifact is loaded once at process startup and reused. The manifest pins `modelVersion`, `featureSchemaVersion`, feature names and order, source fields, float type, and currently supported identity preprocessing. The loaded model's feature names/order are checked against the manifest. The present event-to-feature adapter supports the four Layer 1 numeric/bool inputs listed in the example; any different training features or transforms require a matching, tested adapter change. Do not train inside the consumer.

The development baseline is a transparent rule heuristic to verify Kafka wiring and output shape. Its probability is not calibrated, not a learned model prediction, and must not be used for operational fraud decisions. Training holdout metrics are written with each training run; they are only an initial check and do not establish production performance or calibration.

## Output event contract

`scored-transactions` contains JSON schema version 1 with source event/transaction IDs, transaction fields useful to downstream consumers, Layer 1 rule signals, `fraudProbability`, `riskCategory` (`LOW`, `MEDIUM`, or `HIGH`), `modelVersion`, `featureSchemaVersion`, `scoringMode`, `scoredAt`, and deterministic `scoreId` derived from event ID plus model version. Layer 3 should deduplicate by `scoreId` (or `(eventId, modelVersion)`) because at-least-once delivery may produce duplicates.

Malformed JSON or schema-invalid events are sent with their original payload and error details to `scoring-dead-letter-transactions`; malformed payment fields are not written to logs. Transient processing failures retry the same record with bounded backoff. Input offsets are committed synchronously only after output/DLQ broker acknowledgement. If publication succeeds but offset commit fails, a repeat can produce a duplicate; this service does not claim cross-system exactly-once delivery. If Kafka remains unavailable, failed records are left uncommitted and the container stops so Compose/orchestrator restart can replay them.

## Tests

From this directory:

```sh
python3 -m pip install -r requirements.txt
pytest -q
```

Tests use synthetic fixtures only. They validate schema and feature mapping, XGBoost artifact loading/inference, category mapping, publish-before-commit behavior, invalid-event DLQ routing, and offset retention on broker publication failure. They do not evaluate fraud accuracy or replace a full deployed Kafka integration test.

## Amazon MSK

Set `MSK_AUTH_MODE=iam`, `AWS_REGION`, and the IAM bootstrap broker endpoints to use MSK IAM from Python. The client uses SASL/OAUTHBEARER with AWS's MSK token signer. For local host runs, provide credentials through a named `AWS_PROFILE` (including an active SSO login); for ECS, leave `AWS_PROFILE` unset and attach the least-privilege task role. The task role needs cluster connect and topic/group permissions. The broker endpoints must be reachable over the VPC network. Compose keeps `MSK_AUTH_MODE=none` for the local Kafka broker. See [AWS MSK IAM setup](https://docs.aws.amazon.com/msk/latest/developerguide/configure-clients-for-iam-access-control.html) and the [AWS Python signer](https://github.com/aws/aws-msk-iam-sasl-signer-python).
