from datetime import datetime, timezone
import json

import pytest


@pytest.fixture
def scored_event():
    return {
        "schemaVersion": 1,
        "eventId": "evt-synthetic-001",
        "transactionId": "txn-synthetic-001",
        "eventTimestamp": datetime.now(timezone.utc).isoformat(),
        "accountId": "acct-synthetic-1",
        "amount": 42.5,
        "currency": "USD",
        "merchantId": "merchant-synthetic-1",
        "deviceId": "device-synthetic-shared",
        "paymentInstrumentId": "instrument-synthetic-1",
        "priorTransactionCount": 0,
        "priorAmountTotal": 0.0,
        "layer1Suspicious": False,
        "layer1MatchedRules": [],
        "fraudProbability": 0.1,
        "riskCategory": "LOW",
        "modelVersion": "synthetic-model-v1",
        "featureSchemaVersion": "1",
        "scoringMode": "development-baseline",
        "scoredAt": datetime.now(timezone.utc).isoformat(),
        "scoreId": "synthetic-score-1",
    }


@pytest.fixture
def raw_event(scored_event):
    return json.dumps(scored_event).encode()
