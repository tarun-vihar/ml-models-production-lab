import json

from app.graph_repository import (
    InMemoryGraphRepository,
    NeptuneGraphRepository,
    OBSERVATIONS_QUERY,
    UPSERT_QUERY,
    _query_parameters,
)


class FakeNeptuneClient:
    def __init__(self):
        self.calls = []

    def execute_open_cypher_query(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) == 1:
            return {"results": [{"eventId": "evt-synthetic-001"}]}
        return {"results": [{"accountsPerDevice": 2,
                              "devicesPerAccount": 1,
                              "instrumentsPerAccount": 1}]}


def test_neptune_uses_parameterized_merge_and_stable_custom_ids(scored_event):
    params1 = _query_parameters(scored_event)
    params2 = _query_parameters(scored_event)
    assert params1 == params2
    assert params1["transactionNodeId"].startswith("node-transaction:")
    assert "MERGE (a:Account" in UPSERT_QUERY
    assert "`~id`: $accountNodeId" in UPSERT_QUERY
    assert "$accountNodeId" in OBSERVATIONS_QUERY

    client = FakeNeptuneClient()
    repository = NeptuneGraphRepository.__new__(NeptuneGraphRepository)
    repository._client = client
    repository._session = None
    result = repository.upsert_and_observe(scored_event)
    assert result == {"accountsPerDevice": 2, "devicesPerAccount": 1,
                      "instrumentsPerAccount": 1}
    assert client.calls[0]["openCypherQuery"] == UPSERT_QUERY
    encoded_params = client.calls[0]["parameters"]
    assert json.loads(encoded_params)["eventId"] == scored_event["eventId"]


def test_mock_graph_is_idempotent_and_counts_shared_device(scored_event):
    graph = InMemoryGraphRepository()
    first = graph.upsert_and_observe(scored_event)
    replay = graph.upsert_and_observe(scored_event)
    second_account = scored_event | {
        "eventId": "evt-synthetic-002",
        "transactionId": "txn-synthetic-002",
        "accountId": "acct-synthetic-2",
        "paymentInstrumentId": "instrument-synthetic-2",
    }
    shared = graph.upsert_and_observe(second_account)
    assert first["accountsPerDevice"] == replay["accountsPerDevice"] == 1
    assert shared["accountsPerDevice"] == 2
    assert len(graph.events) == 2
