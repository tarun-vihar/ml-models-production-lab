from app.config import Settings
from app.pipeline import GraphPipeline
from app.worker import GraphWorker


class FakeRepo:
    def upsert_and_observe(self, _event):
        return {"accountsPerDevice": 1, "devicesPerAccount": 1, "instrumentsPerAccount": 1}


class FakeMessage:
    def __init__(self, value):
        self._value = value

    def value(self): return self._value
    def key(self): return b"input-key"


class FakeProducer:
    def __init__(self, actions, fail=False): self.actions, self.records, self.fail = actions, [], fail
    def produce(self, topic, key, value, on_delivery):
        self.records.append((topic, key, value))
        self.actions.append("produced")
        on_delivery(RuntimeError("synthetic Kafka failure") if self.fail else None, None)
    def flush(self, _timeout): return 1 if self.fail else 0


class FakeConsumer:
    def __init__(self, actions): self.actions = actions
    def commit(self, message, asynchronous): self.actions.append("committed")


def test_offset_committed_after_graph_output_delivery(raw_event):
    actions = []
    settings = Settings(graph_mode="mock")
    producer = FakeProducer(actions)
    worker = GraphWorker(settings, FakeConsumer(actions), producer,
                         GraphPipeline(settings, FakeRepo()))
    worker.process(FakeMessage(raw_event))
    assert actions == ["produced", "committed"]
    assert producer.records[0][0] == settings.output_topic


def test_invalid_event_goes_to_dlq_before_commit():
    actions = []
    settings = Settings(graph_mode="mock")
    producer = FakeProducer(actions)
    worker = GraphWorker(settings, FakeConsumer(actions), producer,
                         GraphPipeline(settings, FakeRepo()))
    worker.process(FakeMessage(b"not-json"))
    assert actions == ["produced", "committed"]
    assert producer.records[0][0] == settings.dlq_topic


def test_input_offset_is_not_committed_when_kafka_publish_fails(raw_event):
    actions = []
    settings = Settings(graph_mode="mock")
    worker = GraphWorker(settings, FakeConsumer(actions), FakeProducer(actions, fail=True),
                         GraphPipeline(settings, FakeRepo()))
    try:
        worker.process(FakeMessage(raw_event))
    except RuntimeError:
        pass
    else:
        raise AssertionError("Kafka publication failure should be surfaced")
    assert actions == ["produced"]
