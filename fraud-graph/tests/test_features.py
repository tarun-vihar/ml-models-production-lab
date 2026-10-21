from app.config import Settings
from app.features import GraphObservations, calculate_risk


def settings():
    return Settings(graph_mode="mock", shared_device_account_threshold=3)


def test_shared_device_below_threshold_is_observation_not_fraud():
    risk = calculate_risk(GraphObservations(2, 1, 1), settings())
    assert risk.score == 0.2
    assert risk.category == "LOW"
    assert risk.evidence == []


def test_multiple_accounts_sharing_device_has_explainable_signal():
    risk = calculate_risk(GraphObservations(3, 1, 1), settings())
    assert risk.score == 0.4
    assert risk.evidence == ["device_shared_by_3_accounts"]


def test_many_devices_and_instruments_raise_risk():
    risk = calculate_risk(GraphObservations(1, 5, 6), settings())
    assert risk.score == 0.3
    assert risk.category == "MEDIUM"
    assert risk.evidence == ["account_has_5_devices", "account_has_6_payment_instruments"]
