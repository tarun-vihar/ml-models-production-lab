from dataclasses import dataclass

from .config import Settings


@dataclass(frozen=True)
class GraphObservations:
    accounts_per_device: int
    devices_per_account: int
    instruments_per_account: int


@dataclass(frozen=True)
class GraphRisk:
    score: float
    category: str
    evidence: list[str]


def calculate_risk(observations: GraphObservations, settings: Settings) -> GraphRisk:
    """Explainable heuristic only; shared infrastructure is evidence, not a fraud label."""
    score = min(0.70, max(0, observations.accounts_per_device - 1) * 0.20)
    evidence: list[str] = []
    if observations.accounts_per_device >= settings.shared_device_account_threshold:
        evidence.append(
            f"device_shared_by_{observations.accounts_per_device}_accounts"
        )
    if observations.devices_per_account >= 5:
        score += 0.15
        evidence.append(f"account_has_{observations.devices_per_account}_devices")
    if observations.instruments_per_account >= 5:
        score += 0.15
        evidence.append(f"account_has_{observations.instruments_per_account}_payment_instruments")
    score = round(min(score, 1.0), 4)
    category = "HIGH" if score >= settings.high_threshold else (
        "MEDIUM" if score >= settings.medium_threshold else "LOW"
    )
    return GraphRisk(score, category, evidence)
