import pytest


@pytest.fixture
def enriched_event():
    return {
        "schemaVersion": 1,
        "eventId": "evt-test-1",
        "transactionId": "txn-test-1",
        "eventTimestamp": "2026-10-21T12:00:00Z",
        "accountId": "acct-test",
        "amount": 6250.0,
        "currency": "USD",
        "merchantId": "merchant-test",
        "deviceId": "device-test",
        "paymentInstrumentId": "card-test",
        "priorTransactionCount": 1,
        "priorAmountTotal": 125.5,
        "suspicious": True,
        "matchedRules": ["HIGH_AMOUNT"],
        "enrichedAt": "2026-10-21T12:00:01Z",
    }
