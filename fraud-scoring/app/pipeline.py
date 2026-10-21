from datetime import datetime, timezone
import hashlib
import json
import logging
import time

from pydantic import ValidationError

from .config import Settings
from .observability import INFERENCE_FAILURES, MESSAGES_SCORED, SCORING_SECONDS
from .scoring import Scorer
from .schemas import EnrichedTransaction, ScoredTransaction

logger = logging.getLogger(__name__)


class InvalidEvent(Exception):
    pass


class ScoringPipeline:
    def __init__(self, settings: Settings, scorer: Scorer):
        self.settings = settings
        self.scorer = scorer

    def score(self, raw: bytes) -> tuple[EnrichedTransaction, ScoredTransaction]:
        try:
            event = EnrichedTransaction.model_validate_json(raw)
        except (ValidationError, ValueError) as exc:
            raise InvalidEvent(str(exc)) from exc
        started = time.perf_counter()
        try:
            prediction = self.scorer.predict(event)
        except Exception:
            INFERENCE_FAILURES.inc()
            logger.exception("Inference failed transaction_id=%s model_version=%s", event.transactionId, self.scorer.model_version)
            raise
        SCORING_SECONDS.observe(time.perf_counter() - started)
        if prediction.probability >= self.settings.high_threshold:
            category = "HIGH"
        elif prediction.probability >= self.settings.medium_threshold:
            category = "MEDIUM"
        else:
            category = "LOW"
        score_id = hashlib.sha256(f"{event.eventId}:{prediction.model_version}".encode()).hexdigest()
        scored = ScoredTransaction(
            eventId=event.eventId,
            transactionId=event.transactionId,
            eventTimestamp=event.eventTimestamp,
            accountId=event.accountId,
            amount=event.amount,
            currency=event.currency,
            merchantId=event.merchantId,
            deviceId=event.deviceId,
            paymentInstrumentId=event.paymentInstrumentId,
            priorTransactionCount=event.priorTransactionCount,
            priorAmountTotal=event.priorAmountTotal,
            layer1Suspicious=event.suspicious,
            layer1MatchedRules=event.matchedRules,
            fraudProbability=prediction.probability,
            riskCategory=category,
            modelVersion=prediction.model_version,
            featureSchemaVersion=prediction.feature_schema_version,
            scoringMode=prediction.scoring_mode,
            scoredAt=datetime.now(timezone.utc),
            scoreId=score_id,
        )
        MESSAGES_SCORED.inc()
        return event, scored

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
