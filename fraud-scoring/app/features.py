from decimal import Decimal

from .schemas import EnrichedTransaction


# Provisional feature contract until a real training manifest is supplied.
# The order is part of the model contract and must match the training pipeline.
DEV_FEATURE_SCHEMA_VERSION = "dev-1"
DEV_FEATURE_NAMES = (
    "amount",
    "prior_transaction_count",
    "prior_amount_total",
    "layer1_suspicious",
)


def development_features(event: EnrichedTransaction) -> list[float]:
    values = {
        "amount": float(event.amount),
        "prior_transaction_count": float(event.priorTransactionCount),
        "prior_amount_total": float(event.priorAmountTotal),
        "layer1_suspicious": float(event.suspicious),
    }
    return [values[name] for name in DEV_FEATURE_NAMES]


def feature_value_map(event: EnrichedTransaction) -> dict[str, float]:
    return dict(zip(DEV_FEATURE_NAMES, development_features(event), strict=True))
