import json

import pytest

from app.config import Settings
from app.pipeline import ScoringPipeline
from app.scoring import Scorer
from app.worker import ScoringWorker


class FakeMessage:
    def __init__(self, value, key=b"input-key"):
        self._value = value
        self._key = key

    def value(self):
        return self._value

    def key(self):
        return self._key

    def error(self):
        return None


class FakeConsumer:
    def __init__(self):
        self.commits = []

    def commit(self, **kwargs):
        self.commits.append(kwargs)

    def subscribe(self, _topics):
        pass

    def close(self):
        pass


class FakeProducer:
    def __init__(self, fail_topic=None):
        self.records = []
        self.fail_topic = fail_topic

    def produce(self, topic, **kwargs):
        self.records.append((topic, kwargs))
        if topic == self.fail_topic:
            raise RuntimeError("simulated broker failure")
        kwargs["on_delivery"](None, None)

    def flush(self, _timeout):
        return 0


class StopAfterRecord:
    stopped = False

    def is_set(self):
        return self.stopped

    def wait(self, _timeout):
        return False


def valid_event():
    return json.dumps({
        "schemaVersion": 1,
        "eventId": "evt-1",
        "transactionId": "txn-1",
        "eventTimestamp": "2026-10-21T12:00:00Z",
        "accountId": "acct-1",
        "amount": 100.0,
        "currency": "USD",
        "merchantId": "m-1",
        "deviceId": "d-1",
        "paymentInstrumentId": "p-1",
        "priorTransactionCount": 0,
        "priorAmountTotal": 0,
        "suspicious": False,
        "matchedRules": [],
        "enrichedAt": "2026-10-21T12:00:01Z",
    }).encode()


def worker(producer):
    settings = Settings(model_mode="development-baseline")
    scorer = Scorer("development-baseline", "artifacts", "development-baseline-v1", "unused")
    consumer = FakeConsumer()
    return ScoringWorker(settings, consumer, producer, ScoringPipeline(settings, scorer)), consumer


def test_score_published_before_offset_commit():
    producer = FakeProducer()
    service, consumer = worker(producer)
    service.process(FakeMessage(valid_event()))
    assert producer.records[0][0] == "scored-transactions"
    assert json.loads(producer.records[0][1]["value"])["transactionId"] == "txn-1"
    assert len(consumer.commits) == 1


def test_invalid_event_goes_to_dlq_then_commits():
    producer = FakeProducer()
    service, consumer = worker(producer)
    service.process(FakeMessage(b"not-json"))
    topic, record = producer.records[0]
    assert topic == "scoring-dead-letter-transactions"
    assert json.loads(record["value"])["payload"] == "not-json"
    assert len(consumer.commits) == 1


def test_publish_failure_does_not_commit_input():
    producer = FakeProducer(fail_topic="scored-transactions")
    service, consumer = worker(producer)
    with pytest.raises(RuntimeError, match="simulated broker failure"):
        service.process(FakeMessage(valid_event()))
    assert consumer.commits == []


def test_exhausted_retries_go_to_dlq_before_commit():
    message = FakeMessage(valid_event())

    class OneMessageConsumer(FakeConsumer):
        polls = 0
        stop_event = None

        def poll(self, _timeout):
            self.polls += 1
            if self.polls == 1:
                return message
            self.stop_event.stopped = True
            return None

    class AlwaysFailScoredProducer(FakeProducer):
        def produce(self, topic, **kwargs):
            if topic == "scored-transactions":
                raise RuntimeError("simulated output failure")
            super().produce(topic, **kwargs)

    settings = Settings(model_mode="development-baseline", max_processing_attempts=2)
    scorer = Scorer("development-baseline", "artifacts", "development-baseline-v1", "unused")
    consumer = OneMessageConsumer()
    producer = AlwaysFailScoredProducer()
    stop_event = StopAfterRecord()
    consumer.stop_event = stop_event
    service = ScoringWorker(settings, consumer, producer, ScoringPipeline(settings, scorer))
    service.run(stop_event)

    topics = [topic for topic, _ in producer.records]
    assert topics == ["scoring-dead-letter-transactions"]
    assert json.loads(producer.records[0][1]["value"])["error"] == "processing attempts exhausted"
    assert len(consumer.commits) == 1
