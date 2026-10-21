from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_serializer


class ScoredTransaction(BaseModel):
    """Layer 2 schemaVersion 1 contract; extra fields are preserved for evolution."""
    model_config = ConfigDict(extra="allow")

    schemaVersion: Literal[1]
    eventId: str = Field(min_length=1)
    transactionId: str = Field(min_length=1)
    eventTimestamp: datetime
    accountId: str = Field(min_length=1)
    amount: Decimal
    currency: str
    merchantId: str = Field(min_length=1)
    deviceId: str = Field(min_length=1)
    paymentInstrumentId: str = Field(min_length=1)
    priorTransactionCount: int
    priorAmountTotal: Decimal
    layer1Suspicious: bool
    layer1MatchedRules: list[str]
    fraudProbability: float = Field(ge=0, le=1)
    riskCategory: Literal["LOW", "MEDIUM", "HIGH"]
    modelVersion: str
    featureSchemaVersion: str
    scoringMode: Literal["xgboost", "development-baseline"]
    scoredAt: datetime
    scoreId: str = Field(min_length=1)

    @field_serializer("amount", "priorAmountTotal")
    def serialize_decimals_as_json_numbers(self, value: Decimal) -> float:
        return float(value)


class GraphScoredTransaction(BaseModel):
    model_config = ConfigDict(extra="allow")

    schemaVersion: Literal[1] = 1
    graphSchemaVersion: Literal[1] = 1
    graphEventId: str
    graphAnalysisVersion: str = "shared-entity-heuristics-v1"
    analyzedAt: datetime
    graphRiskScore: float = Field(ge=0, le=1)
    graphRiskCategory: Literal["LOW", "MEDIUM", "HIGH"]
    graphFeatures: dict[str, int]
    graphEvidence: list[str]
