from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator


class EnrichedTransaction(BaseModel):
    model_config = ConfigDict(extra="allow")

    schemaVersion: Literal[1]
    eventId: str = Field(min_length=1, max_length=100)
    transactionId: str = Field(min_length=1, max_length=150)
    eventTimestamp: datetime
    accountId: str = Field(min_length=1)
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=4)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    merchantId: str = Field(min_length=1)
    deviceId: str = Field(min_length=1)
    paymentInstrumentId: str = Field(min_length=1)
    priorTransactionCount: int = Field(ge=0)
    priorAmountTotal: Decimal = Field(ge=0)
    suspicious: bool
    matchedRules: list[str]
    enrichedAt: datetime

    @field_validator("amount", "priorAmountTotal")
    @classmethod
    def finite_decimal(cls, value: Decimal) -> Decimal:
        if not value.is_finite():
            raise ValueError("amount values must be finite")
        return value


class ScoredTransaction(BaseModel):
    schemaVersion: Literal[1] = 1
    eventId: str
    transactionId: str
    eventTimestamp: datetime
    accountId: str
    amount: Decimal
    currency: str
    merchantId: str
    deviceId: str
    paymentInstrumentId: str
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
    scoreId: str

    @field_serializer("amount", "priorAmountTotal")
    def serialize_decimals_as_json_numbers(self, value: Decimal) -> float:
        return float(value)
