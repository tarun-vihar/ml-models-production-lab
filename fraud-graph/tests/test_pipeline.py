import json

import pytest

from app.config import Settings
from app.pipeline import GraphPipeline, InvalidEvent


class FakeRepository:
    def __init__(self, counts):
        self.counts = counts
        self.events = []

    def upsert_and_observe(self, event):
        self.events.append(event)
        return self.counts


def test_output_preserves_layer2_fields_and_adds_graph_contract(scored_event, raw_event):
    repo = FakeRepository({"accountsPerDevice": 3, "devicesPerAccount": 1,
                           "instrumentsPerAccount": 1})
    source, output = GraphPipeline(Settings(graph_mode="mock"), repo).analyze(raw_event)
    assert source.eventId == scored_event["eventId"]
    for key, value in scored_event.items():
        assert output[key] == value
    assert output["graphSchemaVersion"] == 1
    assert output["graphFeatures"] == {
        "accountsPerDevice": 3,
        "devicesPerAccount": 1,
        "paymentInstrumentsPerAccount": 1,
    }
    assert output["graphEvidence"] == ["device_shared_by_3_accounts"]
    assert len(output["graphEventId"]) == 64


def test_invalid_event_rejected(scored_event):
    del scored_event["deviceId"]
    with pytest.raises(InvalidEvent):
        GraphPipeline(Settings(graph_mode="mock"), FakeRepository({})).analyze(
            json.dumps(scored_event).encode())


def test_missing_optional_ids_are_not_silently_fabricated(scored_event):
    scored_event["deviceId"] = ""
    with pytest.raises(InvalidEvent):
        GraphPipeline(Settings(graph_mode="mock"), FakeRepository({})).analyze(
            json.dumps(scored_event).encode())
