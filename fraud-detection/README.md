# Fraud Detection Layer 1: Ingestion and Enrichment

Single Spring Boot service for validating raw transaction events, applying deterministic rules, enriching from online account history, persisting to PostgreSQL, and publishing versioned events for later scoring services.

## Architecture and data flow

```text
raw-transactions
  -> manual-ack Kafka listener (group fraud-ingestion-v1)
  -> JSON + Jakarta validation -> Redis historical features -> configurable rules
  -> PostgreSQL transaction + audit record + outbox (one database transaction)
  -> outbox publisher -> enriched-transactions
  -> invalid / exhausted failure -> dead-letter-transactions
```

The packages separate consumer, validation, rules, features, persistence, producer, deadletter, config, and observability. The enriched event is a stable contract for future model and graph services; this project does not run inference.

| Topic | Purpose | Local layout |
| --- | --- | --- |
| `raw-transactions` | Input transaction events | 3 partitions, 1 replica |
| `enriched-transactions` | Validated, enriched version 1 events | 3 partitions, 1 replica |
| `dead-letter-transactions` | Original payload and failure envelope | 3 partitions, 1 replica |

The service creates these topics on startup. Replication factor one is only suitable for the local demonstration.

## Prerequisites and versions

- Java 25 LTS and Maven 3.9+ for local builds, or Docker Compose v2 for the container workflow.
- Spring Boot 4.1.1, Spring Kafka 4.1.x / Kafka client versions managed by Boot, Jakarta Validation, Spring Data Redis/JDBC, Actuator, and Micrometer are dependency-managed by the Boot parent. Boot 4.1.1 supports Java 17–26; Java 25 is the selected LTS baseline. Spring Kafka recommends using Boot's Kafka starter to select a compatible version.

Boot 4 was selected because its system requirements cover Java 25 and it provides the Kafka starter alongside the Spring Framework 7 line used by Spring Kafka 4.1. [Boot requirements](https://docs.spring.io/spring-boot/system-requirements.html), [Spring Kafka quick tour](https://docs.spring.io/spring-kafka/reference/quick-tour.html), [Boot starters](https://docs.spring.io/spring-boot/reference/using/build-systems.html).

## Start locally

Run these commands from the repository root. The application code and Maven project are in `fraud-detection/`; Compose remains at the root.

Start Kafka, Redis, PostgreSQL, and the application:

```sh
docker compose up --build -d
docker compose ps
```

Wait for the app's health check at <http://localhost:8080/actuator/health/readiness>. Stop services with `docker compose down`; persistent volumes are retained. To remove local data, use `docker compose down -v`.

Submit an event using Kafka's console producer:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic raw-transactions
```

Paste this single-line JSON and press Enter:

```json
{"eventId":"evt-1001","transactionId":"txn-1001","schemaVersion":1,"eventType":"TRANSACTION_AUTHORIZED","eventTimestamp":"2026-01-15T12:30:00Z","accountId":"acct-42","amount":125.50,"currency":"USD","merchantId":"merchant-9","deviceId":"device-7","paymentInstrumentId":"card-3"}
```

Watch enriched events and dead letters in separate terminals:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic enriched-transactions --from-beginning
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic dead-letter-transactions --from-beginning
```

Inspect persisted data:

```sh
docker compose exec postgres psql -U fraud -d fraud -c 'SELECT event_id, transaction_id, status, enriched_json FROM transactions ORDER BY created_at DESC;'
```

Invalid example (unsupported schema and missing required fields; produces a DLT envelope):

```json
{"eventId":"evt-invalid","transactionId":"txn-invalid","schemaVersion":99,"eventType":"UNKNOWN"}
```

## Latest local run

The Compose stack was built and started with `docker compose up --build -d`. The build used Maven 3.9 on Eclipse Temurin 25 and completed `mvn package -DskipTests`. Kafka, Redis, PostgreSQL, and the app reported healthy; `/actuator/health/readiness` returned `{"status":"UP"}`.

A sample event (`demo-20261021-001` / `demo-txn-001`) was submitted to `raw-transactions`. Verification found it in `enriched-transactions` with `priorTransactionCount: 0` and `suspicious: false`, and in PostgreSQL with status `ENRICHED`. This confirms a local happy-path run. The automated test suite has not been run.

The Compose stack was left running after that check. Use `docker compose ps` to check its current status.

## Manual run and debugging

Useful commands while the Compose stack is running:

```sh
docker compose ps
docker compose logs -f app
docker compose logs -f kafka
curl --fail http://localhost:8080/actuator/health/readiness
docker compose exec postgres psql -U fraud -d fraud -c 'SELECT event_id, transaction_id, status FROM transactions ORDER BY created_at DESC;'
```

After a code change, rebuild and restart the app with `docker compose up --build -d app`. To restart without rebuilding, use `docker compose restart app`.

For a source-level IDE debugger, the current Compose setup does not expose a JVM debug port. Running the app directly on the host with `mvn spring-boot:run` also needs a Kafka listener advertised as `localhost:9092`; this Compose file currently advertises `kafka:9092` for containers. Until an external Kafka listener is configured, run/debug the service in Compose and use its logs, or add a host listener before starting the JVM from the IDE.

The full automated suite from the repository root is `mvn -pl fraud-detection -am test`; it requires Maven 3.9+, Java 25, and a working Docker daemon for Testcontainers. The app image build skips test execution (`-DskipTests`), so its successful build does not mean the tests passed.

## Event contract

Raw JSON schema version `1` requires every field: `eventId`, `transactionId`, `schemaVersion`, `eventType`, `eventTimestamp`, `accountId`, `amount`, `currency`, `merchantId`, `deviceId`, and `paymentInstrumentId`. IDs are non-empty and bounded to 100/150 chars, amount is positive with up to four decimal places, currency is an uppercase ISO-style three-letter code, event timestamp cannot be more than five minutes in the future, and the only accepted type is `TRANSACTION_AUTHORIZED`. Timestamps use ISO-8601 UTC. `eventId` and `transactionId` are unique in storage; `transactionId` is the Redis deduplication key.

Enriched JSON schema version `1` carries the original IDs and transaction fields, `priorTransactionCount`, `priorAmountTotal`, `suspicious`, `matchedRules`, and `enrichedAt`. Historical aggregates are scoped by account and currency so unlike currencies are never summed.

## Rules and configuration

The `HIGH_AMOUNT` rule matches amounts greater than or equal to `FRAUD_RULES_HIGH_AMOUNT_THRESHOLD` (default 5000). `HIGH_VELOCITY` matches when the number of earlier accepted events for that account and currency is at least `FRAUD_RULES_VELOCITY_THRESHOLD` (default 8). The current transaction's amount and count are not included in its own snapshot. Rule names are returned in deterministic order: `HIGH_AMOUNT`, then `HIGH_VELOCITY`.

Use environment variables for broker, database, Redis, server port, and rule thresholds. The Compose defaults are intended for local demonstration only.

## AWS connection profile

The `aws` Spring profile is a connection configuration for the application; it does not create AWS resources. It configures Kafka clients for MSK IAM authentication over TLS, requires RDS connection settings from environment variables, and enables TLS for Redis. Enable it with `SPRING_PROFILES_ACTIVE=aws` and provide:

| Variable | Value |
| --- | --- |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | MSK IAM bootstrap broker addresses, including port (typically `9098`) |
| `SPRING_DATASOURCE_URL` | `jdbc:postgresql://<RDS-endpoint>:5432/<database>?sslmode=verify-full` (configure the RDS CA trust as required by the runtime) |
| `SPRING_DATASOURCE_USERNAME` / `SPRING_DATASOURCE_PASSWORD` | RDS application database user credentials |
| `SPRING_DATA_REDIS_HOST` / `SPRING_DATA_REDIS_PORT` | ElastiCache primary/cluster endpoint and TLS port |
| `SPRING_DATA_REDIS_USERNAME` / `SPRING_DATA_REDIS_PASSWORD` | ElastiCache user credentials, when authentication is enabled |

The AWS profile adds the AWS MSK IAM client library. The application still needs network access to the private broker, database, and cache. Configure VPC subnets and security groups so inbound service ports accept traffic only from the application's security group. Topic creation uses three partitions and a configurable replication factor: one locally, three with the AWS profile by default. Set `FRAUD_TOPICS_REPLICATION_FACTOR` to a value supported by your broker count. The app role needs topic-creation permissions if the app is responsible for creating topics; alternatively, pre-create topics and restrict the app role to consuming and producing.

### AWS credentials: local development and deployment

If VS Code's AWS profile is a named profile in the shared AWS config/credentials files, verify it from a terminal with `aws sts get-caller-identity --profile <profile-name>`. If it uses IAM Identity Center (SSO), sign in with `aws sso login --profile <profile-name>` first. Set `AWS_PROFILE=<profile-name>` when running the app directly on your host with the `aws` profile. The MSK IAM client library uses AWS credentials from its provider chain; the selected VS Code account alone does not make AWS credentials available inside a Docker container.

For AWS deployment, run the container as an ECS task and attach a least-privilege task role. Use that role for MSK IAM access; do not copy IAM access keys or mount your developer's `~/.aws` directory into a deployed container. Supply the database and cache credentials through ECS references to AWS Secrets Manager, and keep endpoints and non-secret values in the task configuration. Secret values injected at startup require a new deployment after rotation. Restrict Secrets Manager access to only the secrets this task needs.

For local access to AWS-hosted private endpoints, your laptop must have a private network path (for example, VPN or a controlled tunnel) and the service security groups must allow it. Do not make MSK, RDS, or ElastiCache public solely for local development. See the AWS guides for [MSK IAM client configuration](https://docs.aws.amazon.com/msk/latest/developerguide/configure-clients-for-iam-access-control.html), [ECS task secrets](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/secrets-envvar-secrets-manager.html), and [ECS task roles](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/security-iam-roles.html).

## Reliability and recovery

- Kafka auto-commit is disabled and listener acknowledgment mode is `MANUAL_IMMEDIATE`. A successful transaction requests an offset commit after validation, feature read/update, PostgreSQL persistence, and outbox insertion. Invalid or exhausted messages request a commit only after the dead-letter send completes. If dead-letter publication fails, the listener container stops without acknowledging; the record remains eligible for redelivery when the service restarts.
- Transient processing failures receive three bounded attempts with short increasing delays. Validation failures bypass retries. There is no infinite retry loop.
- PostgreSQL uses unique constraints on `event_id` and `transaction_id`; insertion uses `ON CONFLICT DO NOTHING`. The enriched payload, audit record, and outbox record commit together. Outbox publication is retried by a scheduled poller; a crash after Kafka accepts an event but before `published=true` may publish it again. Consumers should deduplicate by event/transaction ID.
- Redis reads prior values and adds the transaction ID to a seen set in one Lua script. Replays do not increment the counters twice. The seen set and feature hash expire after 30 days. Redis and PostgreSQL do not share a transaction: a crash after the Redis update but before PostgreSQL commits can leave an aggregate for an unpersisted event. The Kafka retry will not double increment it, but this prototype has no reconciliation job for that orphaned feature update. A durable feature-update outbox/rebuild should be added before production use.
- These boundaries do not provide distributed exactly-once processing across Kafka, PostgreSQL, and Redis.

## Health and metrics

Actuator exposes `/actuator/health`, `/actuator/health/liveness`, `/actuator/health/readiness`, `/actuator/metrics`, and `/actuator/prometheus`. Metrics cover consumed, processed, and published messages, processing latency, validation failures, rule matches, retries, dead letters, and persistence/outbox failures. Labels are fixed categories/rule names only; event, account, and transaction IDs are never labels.

## Tests

Run all unit and Testcontainers integration tests from the repository root with:

```sh
mvn -pl fraud-detection -am test
```

Integration tests require a working Docker daemon and pull Kafka, Redis, and PostgreSQL images. They exercise the actual service path, including pre-event feature values, duplicate delivery, PostgreSQL storage, enriched Kafka output, and invalid-message dead-letter routing. The same suite also checks bounded retry behavior.

## Known limitations

- There is no schema registry or compatibility negotiation; consumers must honor the explicit `schemaVersion`.
- Redis uses a 30-day account-and-currency aggregate, with no event-time windowing or out-of-order correction.
- Local Compose uses one Kafka broker and replication factor one; it is not production HA configuration.
- Authentication, encryption, and AWS connection settings are available through the `aws` profile, but no AWS infrastructure, ECS task definition, IAM policies, or deployment pipeline is included. PII masking and rate limiting are also not implemented.
- Outbox delivery is at-least-once and Redis/PostgreSQL recovery requires reconciliation before production use.
