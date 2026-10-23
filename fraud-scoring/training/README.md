# Fraud model training

This is a separate, batch training image. It reads a labeled CSV, performs a stratified holdout split, trains XGBoost, evaluates ROC-AUC and average precision, and writes the inference artifacts consumed by `fraud-scoring`:

```text
model.json
feature-manifest.json
training-metrics.json
```

The training feature names and order match the Layer 1 event adapter. Dataset fields must be `amount`, `prior_transaction_count`, `prior_amount_total`, and `layer1_suspicious`; the label defaults to `is_fraud`. Labels must be confirmed outcomes. The model package does not include training data.

## Run training with Docker Compose

Prepare a labeled CSV at `fraud-scoring/training-data/transactions.csv` (see its [data contract](../training-data/README.md)), then run from the repository root:

```sh
docker compose --profile training run --build --rm fraud-model-training
```

The training container gets the CSV from a read-only mount and writes artifacts to a new versioned directory under `fraud-scoring/artifacts/`. It refuses to overwrite an existing version directory. To point at another host directory or filename:

```sh
FRAUD_TRAINING_DATA_DIR=/absolute/path/to/data \
TRAINING_DATA_PATH=/data/my-labeled-transactions.csv \
TRAINING_MODEL_VERSION=fraud-xgb-2026-10-22-001 \
docker compose --profile training run --build --rm fraud-model-training
```

Other settings include `TRAINING_LABEL_COLUMN`, `TRAINING_TEST_SIZE`, `TRAINING_RANDOM_SEED`, `TRAINING_ROUNDS`, `TRAINING_MAX_DEPTH`, `TRAINING_LEARNING_RATE`, `TRAINING_SUBSAMPLE`, and `TRAINING_COLSAMPLE_BYTREE`.

## Use the artifact for local inference

After reviewing the metrics and manifest, choose that model version and run the scoring service in XGBoost mode. For example, if training produced version `fraud-xgb-2026-10-22-001`:

```sh
FRAUD_SCORING_MODEL_MODE=xgboost \
FRAUD_SCORING_MODEL_DIR=/models/fraud-xgb-2026-10-22-001 \
docker compose up --build -d fraud-scoring fraud-graph
docker compose logs -f fraud-scoring
```

The training and inference images are independent. Training does not run inside the streaming consumer. The Compose model mount is convenient for local development; production deployments should publish the three output files as an immutable, versioned artifact and build or deploy an inference image pinned to that artifact. Retain prior versions so a deployment can be rolled back.

## Data and model limits

The first version supports only identity-mapped numeric features from the current event contract. It does not discover or download datasets, infer labels, run preprocessing encoders, tune a decision threshold, calibrate probabilities, or train a graph model. Do not use Layer 1's `suspicious` flag as the target label; it is only one input feature. The holdout metrics are not a substitute for time-based validation, leakage review, calibration checks, or a deployment evaluation using representative confirmed outcomes.
