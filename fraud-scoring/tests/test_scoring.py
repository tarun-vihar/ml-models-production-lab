import json

import pytest

from app.config import Settings
from app.features import DEV_FEATURE_NAMES, development_features
from app.pipeline import InvalidEvent, ScoringPipeline
from app.scoring import Scorer


def test_event_schema_features_and_development_score(enriched_event):
    settings = Settings(model_mode="development-baseline", medium_threshold=0.3, high_threshold=0.7)
    scorer = Scorer("development-baseline", "artifacts", "development-baseline-v1", "unused")
    pipeline = ScoringPipeline(settings, scorer)
    event, scored = pipeline.score(json.dumps(enriched_event).encode())

    assert event.transactionId == "txn-test-1"
    assert DEV_FEATURE_NAMES == (
        "amount", "prior_transaction_count", "prior_amount_total", "layer1_suspicious"
    )
    assert development_features(event) == [6250.0, 1.0, 125.5, 1.0]
    assert scored.fraudProbability == 0.82
    assert scored.riskCategory == "HIGH"
    assert scored.modelVersion == "development-baseline-v1"
    assert scored.scoringMode == "development-baseline"
    assert json.loads(scored.model_dump_json())["amount"] == 6250.0


def test_missing_required_event_fields_are_rejected(enriched_event):
    del enriched_event["transactionId"]
    scorer = Scorer("development-baseline", "artifacts", "development-baseline-v1", "unused")
    with pytest.raises(InvalidEvent):
        ScoringPipeline(Settings(model_mode="development-baseline"), scorer).score(json.dumps(enriched_event).encode())


@pytest.mark.parametrize(
    ("amount", "suspicious", "expected_probability", "expected_category"),
    [(100.0, False, 0.10, "LOW"), (100.0, True, 0.82, "HIGH")],
)
def test_risk_category_mapping(enriched_event, amount, suspicious, expected_probability, expected_category):
    enriched_event["amount"] = amount
    enriched_event["suspicious"] = suspicious
    scorer = Scorer("development-baseline", "artifacts", "development-baseline-v1", "unused")
    _event, scored = ScoringPipeline(Settings(model_mode="development-baseline"), scorer).score(
        json.dumps(enriched_event).encode()
    )
    assert scored.fraudProbability == expected_probability
    assert scored.riskCategory == expected_category


def test_xgboost_model_load_and_inference(tmp_path):
    xgb = pytest.importorskip("xgboost")
    import numpy as np

    names = list(DEV_FEATURE_NAMES)
    matrix = xgb.DMatrix(np.array([[0, 0, 0, 0], [1, 2, 3, 1], [2, 1, 5, 0], [3, 4, 2, 1]], dtype=float),
                         label=np.array([0, 1, 0, 1]), feature_names=names)
    model = xgb.train({"objective": "binary:logistic", "max_depth": 2, "nthread": 1}, matrix, num_boost_round=2)
    model.save_model(str(tmp_path / "model.json"))
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "modelVersion": "test-model-1",
        "featureSchemaVersion": "test-features-1",
        "featureNames": names,
        "features": [
            {"name": name, "type": "float32", "source": source, "transform": "identity"}
            for name, source in zip(names, ["amount", "priorTransactionCount", "priorAmountTotal", "suspicious"], strict=True)
        ],
    }))
    scorer = Scorer("xgboost", str(tmp_path), "", str(manifest))
    from app.schemas import EnrichedTransaction

    prediction = scorer.predict(EnrichedTransaction.model_validate({
        "schemaVersion": 1,
        "eventId": "evt-1",
        "transactionId": "txn-1",
        "eventTimestamp": "2026-10-21T12:00:00Z",
        "accountId": "acct-1",
        "amount": 100.0,
        "currency": "USD",
        "merchantId": "m-1",
        "deviceId": "d-1",
        "paymentInstrumentId": "p-1",
        "priorTransactionCount": 2,
        "priorAmountTotal": 50.0,
        "suspicious": False,
        "matchedRules": [],
        "enrichedAt": "2026-10-21T12:00:01Z",
    }))
    assert 0 <= prediction.probability <= 1
    assert prediction.model_version == "test-model-1"


def test_manifest_feature_order_must_match_saved_model(tmp_path):
    xgb = pytest.importorskip("xgboost")
    import numpy as np

    matrix = xgb.DMatrix(np.array([[0, 0], [1, 1]], dtype=float), label=np.array([0, 1]),
                         feature_names=["amount", "prior_transaction_count"])
    model = xgb.train({"objective": "binary:logistic", "nthread": 1}, matrix, num_boost_round=1)
    model.save_model(str(tmp_path / "model.json"))
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "modelVersion": "test",
        "featureSchemaVersion": "test",
        "featureNames": ["prior_transaction_count", "amount"],
        "features": [
            {"name": "prior_transaction_count", "type": "float32", "source": "priorTransactionCount", "transform": "identity"},
            {"name": "amount", "type": "float32", "source": "amount", "transform": "identity"},
        ],
    }))
    with pytest.raises(ValueError, match="feature names/order"):
        Scorer("xgboost", str(tmp_path), "", str(manifest))
