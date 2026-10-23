"""Train a versioned XGBoost artifact compatible with the fraud-scoring service."""

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
import re

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split


# Output order and source fields must stay aligned with fraud-scoring/app/scoring.py.
FEATURES = [
    ("amount", "amount"),
    ("prior_transaction_count", "priorTransactionCount"),
    ("prior_amount_total", "priorAmountTotal"),
    ("layer1_suspicious", "suspicious"),
]


def main() -> None:
    data_path = Path(os.getenv("TRAINING_DATA_PATH", "/data/transactions.csv"))
    label_column = os.getenv("TRAINING_LABEL_COLUMN", "is_fraud")
    output_root = Path(os.getenv("TRAINING_OUTPUT_DIR", "/models"))
    test_size = float(os.getenv("TRAINING_TEST_SIZE", "0.2"))
    random_seed = int(os.getenv("TRAINING_RANDOM_SEED", "42"))
    model_version = os.getenv("TRAINING_MODEL_VERSION") or datetime.now(timezone.utc).strftime("fraud-xgb-%Y%m%dT%H%M%SZ")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", model_version):
        raise ValueError("TRAINING_MODEL_VERSION may contain only letters, numbers, dots, underscores, and hyphens")

    if not 0 < test_size < 1:
        raise ValueError("TRAINING_TEST_SIZE must be between 0 and 1")
    if not data_path.is_file():
        raise FileNotFoundError(f"Training CSV not found: {data_path}")

    frame = pd.read_csv(data_path)
    required = [column for column, _ in FEATURES] + [label_column]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"CSV is missing required columns: {missing}")
    if frame.empty:
        raise ValueError("Training CSV has no rows")

    x = frame[[column for column, _ in FEATURES]].apply(pd.to_numeric, errors="raise")
    y = pd.to_numeric(frame[label_column], errors="raise")
    if x.isna().any().any() or y.isna().any():
        raise ValueError("Training features and labels must not contain missing values")
    if not np.isfinite(x.to_numpy(dtype=float)).all():
        raise ValueError("Training features must be finite numeric values")
    if set(y.unique()) != {0, 1}:
        raise ValueError(f"Label column {label_column!r} must contain both binary classes 0 and 1")
    if y.value_counts().min() < 2:
        raise ValueError("Need at least two examples of each label for a stratified holdout")

    x_train, x_test, y_train, y_test = train_test_split(
        x, y.astype(int), test_size=test_size, random_state=random_seed, stratify=y
    )
    train_matrix = xgb.DMatrix(x_train, label=y_train, feature_names=[name for name, _ in FEATURES])
    test_matrix = xgb.DMatrix(x_test, label=y_test, feature_names=[name for name, _ in FEATURES])
    model = xgb.train(
        {
            "objective": "binary:logistic",
            "eval_metric": "aucpr",
            "tree_method": "hist",
            "max_depth": int(os.getenv("TRAINING_MAX_DEPTH", "5")),
            "eta": float(os.getenv("TRAINING_LEARNING_RATE", "0.08")),
            "subsample": float(os.getenv("TRAINING_SUBSAMPLE", "0.8")),
            "colsample_bytree": float(os.getenv("TRAINING_COLSAMPLE_BYTREE", "0.8")),
            "seed": random_seed,
        },
        train_matrix,
        num_boost_round=int(os.getenv("TRAINING_ROUNDS", "250")),
        evals=[(test_matrix, "holdout")],
        verbose_eval=False,
    )

    probabilities = model.predict(test_matrix)
    metrics = {
        "holdoutRows": int(len(y_test)),
        "positiveRate": float(y_test.mean()),
        "rocAuc": float(roc_auc_score(y_test, probabilities)),
        "averagePrecision": float(average_precision_score(y_test, probabilities)),
    }
    if not all(math.isfinite(value) for value in metrics.values()):
        raise ValueError("Training produced non-finite evaluation metrics")

    output_dir = output_root / model_version
    output_dir.mkdir(parents=True, exist_ok=False)
    model.save_model(str(output_dir / "model.json"))
    manifest = {
        "modelVersion": model_version,
        "featureSchemaVersion": "fraud-features-v1",
        "featureNames": [name for name, _ in FEATURES],
        "features": [
            {"name": name, "type": "float32", "source": source, "transform": "identity"}
            for name, source in FEATURES
        ],
        "training": {
            "algorithm": "xgboost-binary-logistic",
            "labelColumn": label_column,
            "trainingRows": int(len(y_train)),
            "holdoutRows": int(len(y_test)),
            "randomSeed": random_seed,
            "evaluationMetrics": metrics,
            "dataFile": data_path.name,
        },
    }
    (output_dir / "feature-manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    (output_dir / "training-metrics.json").write_text(
        json.dumps(metrics, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"modelVersion": model_version, "outputDir": str(output_dir), "metrics": metrics}, indent=2))


if __name__ == "__main__":
    main()
