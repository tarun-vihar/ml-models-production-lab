from datetime import datetime, timezone
import hashlib
import json
import logging
from typing import Protocol

from pydantic import ValidationError

from .config import Settings
from .features import GraphObservations, calculate_risk
from .observability import MESSAGES_ANALYZED
from .schemas import GraphScoredTransaction, ScoredTransaction

logger = logging.getLogger(__name__)
GRAPH_ANALYSIS_VERSION = "shared-entity-heuristics-v1"


class InvalidEvent(Exception):
    pass


class GraphRepository(Protocol):
    def upsert_and_observe(self, event: dict) -> dict: ...


class GraphPipeline:
    def __init__(self, settings: Settings, repository: GraphRepository):
        self.settings = settings
        self.repository = repository

    def analyze(self, raw: bytes) -> tuple[ScoredTransaction, dict]:
        try:
            event = ScoredTransaction.model_validate_json(raw)
        except (ValidationError, ValueError) as exc:
            raise InvalidEvent(str(exc)) from exc
        values = event.model_dump()
        observations = self.repository.upsert_and_observe(values)
        stats = GraphObservations(
            accounts_per_device=observations["accountsPerDevice"],
            devices_per_account=observations["devicesPerAccount"],
            instruments_per_account=observations["instrumentsPerAccount"],
        )
        risk = calculate_risk(stats, self.settings)
        graph_event_id = hashlib.sha256(
            f"{event.scoreId}:{GRAPH_ANALYSIS_VERSION}".encode()
        ).hexdigest()
        # Keep the original Layer 2 JSON values and timestamp representation intact.
        output = json.loads(raw)
        output.update({
            "graphSchemaVersion": 1,
            "graphEventId": graph_event_id,
            "graphAnalysisVersion": GRAPH_ANALYSIS_VERSION,
            "analyzedAt": datetime.now(timezone.utc).isoformat(),
            "graphRiskScore": risk.score,
            "graphRiskCategory": risk.category,
            "graphFeatures": {
                "accountsPerDevice": stats.accounts_per_device,
                "devicesPerAccount": stats.devices_per_account,
                "paymentInstrumentsPerAccount": stats.instruments_per_account,
            },
            "graphEvidence": risk.evidence,
        })
        GraphScoredTransaction.model_validate(output)
        MESSAGES_ANALYZED.inc()
        logger.info("Graph analysis complete event_id=%s transaction_id=%s risk=%s",
                    event.eventId, event.transactionId, risk.category)
        return event, output

    @staticmethod
    def dead_letter(raw: bytes, error: Exception) -> bytes:
        try:
            payload = raw.decode("utf-8")
            encoding = "utf-8"
        except UnicodeDecodeError:
            payload = raw.hex()
            encoding = "hex"
        return json.dumps({
            "schemaVersion": 1,
            "errorType": type(error).__name__,
            "error": str(error)[:1000],
            "payloadEncoding": encoding,
            "payload": payload,
            "deadLetteredAt": datetime.now(timezone.utc).isoformat(),
        }).encode()
