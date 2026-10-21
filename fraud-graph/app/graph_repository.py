"""Neptune openCypher persistence and explicit in-memory local-test repository."""
import hashlib
import json
import time
from urllib.parse import urlparse

from .observability import NEPTUNE_QUERY_SECONDS


UPSERT_QUERY = """
MERGE (a:Account {`~id`: $accountNodeId})
SET a.accountId = $accountId
MERGE (d:Device {`~id`: $deviceNodeId})
SET d.deviceId = $deviceId
MERGE (p:PaymentInstrument {`~id`: $instrumentNodeId})
SET p.paymentInstrumentId = $paymentInstrumentId
MERGE (m:Merchant {`~id`: $merchantNodeId})
SET m.merchantId = $merchantId
MERGE (t:Transaction {`~id`: $transactionNodeId})
SET t.eventId = $eventId,
    t.transactionId = $transactionId,
    t.eventTimestamp = $eventTimestamp,
    t.amount = $amount,
    t.currency = $currency
MERGE (t)-[:BY_ACCOUNT {`~id`: $byAccountEdgeId}]->(a)
MERGE (t)-[:ON_DEVICE {`~id`: $onDeviceEdgeId}]->(d)
MERGE (t)-[:USES_PAYMENT_INSTRUMENT {`~id`: $usesInstrumentEdgeId}]->(p)
MERGE (t)-[:AT_MERCHANT {`~id`: $atMerchantEdgeId}]->(m)
MERGE (a)-[:USES_DEVICE {`~id`: $accountDeviceEdgeId}]->(d)
MERGE (a)-[:USES_PAYMENT_INSTRUMENT {`~id`: $accountInstrumentEdgeId}]->(p)
RETURN t.eventId AS eventId
"""

OBSERVATIONS_QUERY = """
MATCH (a:Account {`~id`: $accountNodeId})
OPTIONAL MATCH (a)-[:USES_DEVICE]->(accountDevice:Device)
WITH a, count(DISTINCT accountDevice) AS devicesPerAccount
OPTIONAL MATCH (a)-[:USES_PAYMENT_INSTRUMENT]->(instrument:PaymentInstrument)
WITH a, devicesPerAccount, count(DISTINCT instrument) AS instrumentsPerAccount
MATCH (device:Device {`~id`: $deviceNodeId})
OPTIONAL MATCH (device)<-[:USES_DEVICE]-(other:Account)
RETURN count(DISTINCT other) AS accountsPerDevice,
       devicesPerAccount, instrumentsPerAccount
"""


def _stable_id(kind: str, *parts: str) -> str:
    digest = hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).hexdigest()
    return f"{kind}:{digest}"


def _query_parameters(event: dict) -> dict:
    event_id = event["eventId"]
    account_id = event["accountId"]
    device_id = event["deviceId"]
    instrument_id = event["paymentInstrumentId"]
    merchant_id = event["merchantId"]
    transaction_node = _stable_id("node-transaction", event_id)
    account_node = _stable_id("node-account", account_id)
    device_node = _stable_id("node-device", device_id)
    instrument_node = _stable_id("node-payment-instrument", instrument_id)
    merchant_node = _stable_id("node-merchant", merchant_id)
    return {
        "eventId": event_id,
        "transactionId": event["transactionId"],
        "eventTimestamp": (event["eventTimestamp"].isoformat()
                           if hasattr(event["eventTimestamp"], "isoformat")
                           else str(event["eventTimestamp"])),
        "amount": float(event["amount"]),
        "currency": event["currency"],
        "accountId": account_id,
        "deviceId": device_id,
        "paymentInstrumentId": instrument_id,
        "merchantId": merchant_id,
        "transactionNodeId": transaction_node,
        "accountNodeId": account_node,
        "deviceNodeId": device_node,
        "instrumentNodeId": instrument_node,
        "merchantNodeId": merchant_node,
        "byAccountEdgeId": _stable_id("edge-by-account", event_id, account_id),
        "onDeviceEdgeId": _stable_id("edge-on-device", event_id, device_id),
        "usesInstrumentEdgeId": _stable_id("edge-uses-instrument", event_id, instrument_id),
        "atMerchantEdgeId": _stable_id("edge-at-merchant", event_id, merchant_id),
        "accountDeviceEdgeId": _stable_id("edge-account-device", account_id, device_id),
        "accountInstrumentEdgeId": _stable_id("edge-account-instrument", account_id, instrument_id),
    }


class NeptuneGraphRepository:
    """Uses Neptune Data API, which signs calls with SigV4 when configured with IAM."""

    def __init__(self, endpoint: str, region: str, auth_mode: str, profile: str = "",
                 connect_timeout: int = 5, read_timeout: int = 20):
        self.endpoint = endpoint.rstrip("/")
        if not urlparse(self.endpoint).scheme:
            self.endpoint = f"https://{self.endpoint}"
        self.auth_mode = auth_mode
        self._session = None
        self._client = None
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        if auth_mode == "iam":
            import boto3
            from botocore.config import Config

            aws_session = boto3.Session(profile_name=profile or None, region_name=region)
            self._client = aws_session.client(
                "neptunedata",
                endpoint_url=self.endpoint,
                config=Config(
                    connect_timeout=connect_timeout,
                    read_timeout=read_timeout,
                    retries={"total_max_attempts": 1, "mode": "standard"},
                ),
            )
        else:
            import requests
            self._session = requests.Session()

    def _execute(self, query: str, parameters: dict | None = None) -> list[dict]:
        if self._client is not None:
            request = {"openCypherQuery": query}
            if parameters is not None:
                request["parameters"] = json.dumps(parameters, separators=(",", ":"))
            response = self._client.execute_open_cypher_query(**request)
            return response.get("results", [])

        response = self._session.post(
            f"{self.endpoint}/openCypher",
            data={"query": query,
                  "parameters": json.dumps(parameters or {}, separators=(",", ":"))},
            headers={"Accept": "application/json"},
            timeout=(self._connect_timeout, self._read_timeout),
        )
        response.raise_for_status()
        return response.json().get("results", [])

    def verify(self) -> None:
        result = self._execute("RETURN 1 AS ready")
        if not result or result[0].get("ready") != 1:
            raise RuntimeError("Neptune readiness query returned an unexpected response")

    def ensure_schema(self) -> None:
        # Neptune has no Cypher constraint DDL. Deterministic custom `~id` values
        # enforce uniqueness for nodes and relationships on supported engine versions.
        return None

    def upsert_and_observe(self, event: dict) -> dict:
        started = time.perf_counter()
        parameters = _query_parameters(event)
        try:
            self._execute(UPSERT_QUERY, parameters)
            observations = self._execute(OBSERVATIONS_QUERY, {
                "accountNodeId": parameters["accountNodeId"],
                "deviceNodeId": parameters["deviceNodeId"],
            })
        finally:
            NEPTUNE_QUERY_SECONDS.observe(time.perf_counter() - started)
        if not observations:
            raise RuntimeError("Neptune feature query returned no row after graph upsert")
        row = observations[0]
        return {
            "accountsPerDevice": int(row.get("accountsPerDevice", 0)),
            "devicesPerAccount": int(row.get("devicesPerAccount", 0)),
            "instrumentsPerAccount": int(row.get("instrumentsPerAccount", 0)),
        }

    def close(self) -> None:
        if self._session is not None:
            self._session.close()


class InMemoryGraphRepository:
    """Explicit ephemeral mock for local Compose only; it is not Neptune."""

    def __init__(self):
        self.events: dict[str, dict] = {}
        self.account_devices: dict[str, set[str]] = {}
        self.device_accounts: dict[str, set[str]] = {}
        self.account_instruments: dict[str, set[str]] = {}

    def verify(self) -> None:
        return None

    def ensure_schema(self) -> None:
        return None

    def upsert_and_observe(self, event: dict) -> dict:
        self.events[event["eventId"]] = event.copy()
        self.account_devices.setdefault(event["accountId"], set()).add(event["deviceId"])
        self.device_accounts.setdefault(event["deviceId"], set()).add(event["accountId"])
        self.account_instruments.setdefault(event["accountId"], set()).add(event["paymentInstrumentId"])
        return {
            "accountsPerDevice": len(self.device_accounts[event["deviceId"]]),
            "devicesPerAccount": len(self.account_devices[event["accountId"]]),
            "instrumentsPerAccount": len(self.account_instruments[event["accountId"]]),
        }

    def close(self) -> None:
        return None


def create_repository(settings):
    if settings.graph_mode == "mock":
        return InMemoryGraphRepository()
    return NeptuneGraphRepository(
        endpoint=settings.neptune_endpoint,
        region=settings.aws_region,
        auth_mode=settings.neptune_auth_mode,
        profile=settings.aws_profile,
        connect_timeout=settings.neptune_connect_timeout,
        read_timeout=settings.neptune_read_timeout,
    )
