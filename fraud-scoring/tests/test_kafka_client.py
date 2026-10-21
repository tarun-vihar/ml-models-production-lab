import sys
from types import SimpleNamespace

from app.config import Settings
from app.kafka_client import client_config


def test_local_config_uses_plaintext_kafka():
    config = client_config(Settings(msk_auth_mode="none", kafka_bootstrap_servers="localhost:9092"))
    assert config == {"bootstrap.servers": "localhost:9092"}


def test_msk_iam_uses_selected_named_profile(monkeypatch):
    calls = []

    class Signer:
        @staticmethod
        def generate_auth_token_from_profile(region, profile):
            calls.append((region, profile))
            return "signed-token", 2_000_000

    monkeypatch.setitem(sys.modules, "aws_msk_iam_sasl_signer", SimpleNamespace(MSKAuthTokenProvider=Signer))
    config = client_config(Settings(msk_auth_mode="iam", aws_region="us-east-1", aws_profile="lab"))
    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "OAUTHBEARER"
    assert config["oauth_cb"]("") == ("signed-token", 2_000)
    assert calls == [("us-east-1", "lab")]


def test_msk_iam_requires_region():
    settings = Settings(msk_auth_mode="iam", aws_region="")
    import pytest

    with pytest.raises(ValueError, match="AWS_REGION is required"):
        settings.validate()
