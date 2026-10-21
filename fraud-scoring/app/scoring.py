import json
import math
from dataclasses import dataclass
from pathlib import Path

from .features import DEV_FEATURE_NAMES, DEV_FEATURE_SCHEMA_VERSION, development_features
from .schemas import EnrichedTransaction


@dataclass(frozen=True)
class Prediction:
    probability: float
    model_version: str
    feature_schema_version: str
    scoring_mode: str


class Scorer:
    def __init__(self, mode: str, model_dir: str, model_version: str, manifest_path: str):
        self.mode = mode
        self.model = None
        self.xgb = None
        self.model_version = model_version
        self.feature_schema_version = DEV_FEATURE_SCHEMA_VERSION
        self.feature_names = DEV_FEATURE_NAMES
        if mode == "development-baseline":
            self.model_version = model_version or "development-baseline-v1"
            return
        if mode != "xgboost":
            raise ValueError(f"Unsupported model mode: {mode}")

        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        names = manifest.get("featureNames")
        specs = manifest.get("features")
        version = manifest.get("modelVersion")
        schema_version = manifest.get("featureSchemaVersion")
        if not isinstance(names, list) or not names or not all(isinstance(name, str) for name in names):
            raise ValueError("Feature manifest must define a non-empty featureNames list")
        if not version or not schema_version:
            raise ValueError("Feature manifest must define modelVersion and featureSchemaVersion")
        if not isinstance(specs, list) or [item.get("name") for item in specs if isinstance(item, dict)] != names:
            raise ValueError("Feature manifest features must match featureNames in exact order")
        supported_sources = {"amount", "priorTransactionCount", "priorAmountTotal", "suspicious"}
        for spec in specs:
            if (spec.get("type") != "float32" or spec.get("transform") != "identity"
                    or spec.get("source") not in supported_sources):
                raise ValueError("Only declared float32 identity mappings from Layer 1 fields are supported")
        if len({spec["source"] for spec in specs}) != len(specs):
            raise ValueError("Feature manifest must not map the same source field more than once")
        self.feature_names = tuple(names)
        self.feature_sources = tuple(spec["source"] for spec in specs)
        self.model_version = model_version or version
        self.feature_schema_version = schema_version

        import xgboost as xgb

        self.xgb = xgb
        self.model = xgb.Booster()
        self.model.load_model(str(Path(model_dir) / "model.json"))
        model_feature_names = self.model.feature_names
        if model_feature_names and list(model_feature_names) != names:
            raise ValueError("Model feature names/order do not match the feature manifest")

    def predict(self, event: EnrichedTransaction) -> Prediction:
        if self.mode == "development-baseline":
            # A transparent demo heuristic, not a trained or calibrated fraud model.
            amount, prior_count, prior_total, layer1_flag = development_features(event)
            probability = 0.82 if layer1_flag else 0.10
            if amount >= 5000:
                probability = max(probability, 0.78)
            if prior_count >= 8:
                probability = max(probability, 0.72)
            if prior_total >= 10000:
                probability = max(probability, 0.70)
        else:
            values = {
                "amount": float(event.amount),
                "priorTransactionCount": float(event.priorTransactionCount),
                "priorAmountTotal": float(event.priorAmountTotal),
                "suspicious": float(event.suspicious),
            }
            matrix = self.xgb.DMatrix([[values[source] for source in self.feature_sources]], feature_names=list(self.feature_names))
            probability = float(self.model.predict(matrix)[0])
        if not math.isfinite(probability) or not 0 <= probability <= 1:
            raise ValueError("Model returned a probability outside [0, 1]")
        return Prediction(probability, self.model_version, self.feature_schema_version, self.mode)
