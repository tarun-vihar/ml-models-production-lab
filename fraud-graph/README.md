# Fraud Graph (Layer 3)

Standalone Python graph-risk service. It consumes Layer 2 scores, persists transaction/entity relationships in Amazon Neptune using parameterized openCypher, calculates explainable graph indicators, and publishes graph-enriched events. It does not access Layer 1 PostgreSQL or Redis and does not replace transaction-level model inference.

```text
raw-transactions -> fraud-detection -> enriched-transactions
                  -> fraud-scoring -> scored-transactions
                  -> fraud-graph -> graph-scored-transactions
                              -> graph-dead-letter-transactions
```

## Confirmed input contract and Neptune graph schema

Input is UTF-8 JSON on `scored-transactions`, schema version 1, keyed by Layer 2's `scoreId`. Required fields include `eventId`, `transactionId`, `eventTimestamp`, `accountId`, `amount`, `currency`, `merchantId`, `deviceId`, `paymentInstrumentId`, prior aggregates, Layer 1 indicators, `fraudProbability`, `riskCategory`, `modelVersion`, `featureSchemaVersion`, `scoringMode`, `scoredAt`, and `scoreId`. Unknown fields are preserved through the output for forward compatibility. Local Compose currently uses Layer 2's development baseline, not a trained/calibrated model.

The confirmed identifiers support `Account`, `Device`, `PaymentInstrument`, `Merchant`, and `Transaction` nodes. Proposed edges are `(Transaction)-[:BY_ACCOUNT]->(Account)`, `-[:ON_DEVICE]->(Device)`, `-[:USES_PAYMENT_INSTRUMENT]->(PaymentInstrument)`, `-[:AT_MERCHANT]->(Merchant)`, plus `(Account)-[:USES_DEVICE]->(Device)` and `-[:USES_PAYMENT_INSTRUMENT]->(PaymentInstrument)`. The source schema has no IP address, confirmed fraud label, or explicit card ID. We therefore do not create IP nodes, known-fraud links, or label the payment instrument as a card.

Each node and edge receives a deterministic namespaced Neptune custom `~id`; parameterized openCypher `MERGE` uses those IDs for replay safety. Neptune enforces custom ID uniqueness. Neptune does not provide Neo4j-style uniqueness constraints. The `~id` openCypher extension requires Neptune Database engine 1.2.0.2 or newer. Do not store raw card data; send stable tokenized payment-instrument identifiers.

## Graph features and output

After each idempotent graph upsert, targeted one-hop queries calculate:

| Feature | Meaning |
| --- | --- |
| `accountsPerDevice` | Distinct accounts connected to this transaction's device |
| `devicesPerAccount` | Distinct devices connected to the transaction's account |
| `paymentInstrumentsPerAccount` | Distinct payment instrument identifiers connected to the account |

The initial heuristic adds up to 0.70 for multiple accounts sharing the event's device (`0.20` per additional account), 0.15 when an account has at least five devices, and 0.15 when it has at least five payment instruments. A shared-device evidence string is emitted when configurable `GRAPH_SHARED_DEVICE_ACCOUNT_THRESHOLD` is reached (default 3); two accounts still contribute a weaker score. Configurable `GRAPH_MEDIUM_THRESHOLD` and `GRAPH_HIGH_THRESHOLD` map the score to `LOW`, `MEDIUM`, or `HIGH`. These values are uncalibrated indicators, not fraud labels or allow/block/challenge decisions. Shared infrastructure can be legitimate.

Output topic `graph-scored-transactions` preserves Layer 2 fields and adds `graphSchemaVersion`, deterministic `graphEventId`, `graphAnalysisVersion`, `analyzedAt`, `graphRiskScore`, `graphRiskCategory`, `graphFeatures`, and `graphEvidence`. `graphEventId` is derived from `scoreId` plus the graph analysis version; downstream consumers should deduplicate by it.

## Local development mode

Neptune is AWS-managed; this project does not ship a local Neptune container. Root Compose explicitly sets `GRAPH_MODE=mock`, which uses an in-memory adapter to exercise the Kafka flow. Mock graph data disappears on restart and does not validate Neptune queries, IAM, VPC connectivity, or Neptune behavior.

Start the local stack and watch graph processing:

```sh
docker compose up --build -d
docker compose ps
docker compose logs -f fraud-graph
```

Submit synthetic events through Layer 1 using the [Layer 1 sample instructions](../fraud-detection/README.md#start-locally). To trigger a shared-device indicator, send at least three events with different account IDs and the same device ID. Check health at <http://localhost:8001/health/ready> and Prometheus metrics at <http://localhost:8001/metrics>.

Read graph-enriched output and invalid/permanently failed records:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic graph-scored-transactions --from-beginning

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic graph-dead-letter-transactions --from-beginning
```

Stop the local stack with `docker compose down`. No graph database volume is created in mock mode.

## Automated tests

Tests use synthetic events, a fake Neptune Data API client, and the explicit in-memory adapter. They verify query parameterization/custom IDs, idempotent mock updates, feature calculations, Layer 2 field preservation, DLQ routing, and publish-before-commit ordering. They do not establish real Neptune query compatibility or connectivity.

```sh
cd fraud-graph
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -q
```

## AWS integration mode

Set `GRAPH_MODE=aws`, `NEPTUNE_ENDPOINT`, `AWS_REGION`, and `NEPTUNE_AUTH_MODE=iam`. The endpoint is the cluster endpoint with port 8182, for example `https://cluster-name.cluster-id.us-east-1.neptune.amazonaws.com:8182`. The Python client uses Boto3's `neptunedata` API; Boto3 signs requests with SigV4 and resolves credentials through the standard AWS chain. For local development, `AWS_PROFILE` can select an already authenticated profile. In ECS, leave `AWS_PROFILE` unset and attach a task role. Do not pass long-lived AWS keys in application configuration.

The service image can be built from the repository root:

```sh
docker build -f fraud-graph/Dockerfile -t fraud-graph:local fraud-graph
```

Run the container as an ECS task (or another compute environment) with VPC reachability to both MSK and Neptune. Set these task environment variables:

```text
GRAPH_MODE=aws
NEPTUNE_ENDPOINT=https://<cluster-endpoint>:8182
AWS_REGION=<cluster-region>
NEPTUNE_AUTH_MODE=iam
KAFKA_BOOTSTRAP_SERVERS=<MSK-bootstrap-brokers>
MSK_AUTH_MODE=iam
KAFKA_INPUT_TOPIC=scored-transactions
KAFKA_OUTPUT_TOPIC=graph-scored-transactions
KAFKA_DLQ_TOPIC=graph-dead-letter-transactions
KAFKA_CONSUMER_GROUP=fraud-graph-v1
```

Networking requirements: place the ECS task and Neptune in the same VPC or connected VPCs; allow inbound TCP 8182 on the Neptune security group from the task security group; allow the task to reach Neptune and the MSK broker ports; and ensure VPC DNS resolution works. Neptune IAM DB authentication must be enabled for `NEPTUNE_AUTH_MODE=iam`.

Grant the task role Neptune data access scoped to the cluster's **Neptune data resource ARN** (which differs from its RDS cluster ARN). `MERGE`/`SET` operations can require all three query actions:

```json
{
  "Effect": "Allow",
  "Action": [
    "neptune-db:ReadDataViaQuery",
    "neptune-db:WriteDataViaQuery",
    "neptune-db:DeleteDataViaQuery"
  ],
  "Resource": "arn:aws:neptune-db:<region>:<account-id>:<cluster-resource-id>/*",
  "Condition": {"StringEquals": {"neptune-db:QueryLanguage": "OpenCypher"}}
}
```

For MSK IAM mode, grant the same task role the required Kafka cluster/topic/group permissions. The service only needs topic/group access for the configured input, output, and DLQ topics.

Submit a raw synthetic event using the Layer 1 Kafka producer instructions. Wait for Layer 1 to publish `enriched-transactions`, Layer 2 to publish `scored-transactions`, then inspect `graph-scored-transactions` with the Kafka console consumer command above. From a host inside the VPC with an authorized AWS profile, verify stored graph records using the Neptune Data API:

```sh
aws neptunedata execute-open-cypher-query \
  --region "$AWS_REGION" \
  --endpoint-url "$NEPTUNE_ENDPOINT" \
  --open-cypher-query 'MATCH (t:Transaction)-[:BY_ACCOUNT]->(a:Account) OPTIONAL MATCH (t)-[:ON_DEVICE]->(d:Device) RETURN t.eventId, t.transactionId, a.accountId, d.deviceId LIMIT 25'
```

Alternatively, query the shared device counts:

```sh
aws neptunedata execute-open-cypher-query \
  --region "$AWS_REGION" \
  --endpoint-url "$NEPTUNE_ENDPOINT" \
  --open-cypher-query 'MATCH (d:Device)<-[:USES_DEVICE]-(a:Account) WITH d, count(DISTINCT a) AS accountCount WHERE accountCount > 1 RETURN d.deviceId, accountCount LIMIT 25'
```

Neptune requires an in-VPC network path unless you deliberately configure a supported access path. For AWS-managed endpoint, SigV4, parameterized openCypher, and IAM action details, see [Neptune Python IAM connection guide](https://docs.aws.amazon.com/neptune/latest/userguide/iam-auth-connecting-python.html), [parameterized openCypher examples](https://docs.aws.amazon.com/neptune/latest/userguide/opencypher-parameterized-queries.html), [Neptune IAM data policies](https://docs.aws.amazon.com/neptune/latest/userguide/iam-data-access-policies.html), and [ECS/EKS networking guidance](https://docs.aws.amazon.com/neptune/latest/userguide/best-practices-ecs-eks-networking-iam.html).

## Reliability and limits

Kafka auto-commit is disabled. The service upserts Neptune and publishes output before synchronously committing the input offset; invalid events are committed only after DLQ acknowledgement. Transient errors receive bounded synchronous retries. If Neptune succeeds and Kafka publication or offset commit fails, replay is possible: deterministic node/edge IDs make graph upserts idempotent, while output can be duplicated. Kafka and Neptune have no automatic shared atomic transaction, so this service does not claim exactly-once processing. If exhausted failures cannot be written to the DLQ, the input remains uncommitted for replay.

Queries stay on the current account/device and one-hop edges. The real-time path does not load the full graph or perform connected-component traversal. Metrics expose consumed/analyzed counts, invalid/DLQ events, observed Kafka lag, publish failures, retries, and graph query latency. No throughput/latency benchmark or fraud-accuracy claim is made. IP features, confirmed-fraud connections, cluster analysis, and graph embeddings can be added when upstream data contracts and workloads support them.
