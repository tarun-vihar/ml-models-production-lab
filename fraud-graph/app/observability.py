from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

MESSAGES_CONSUMED = Counter("fraud_graph_messages_consumed_total", "Scored records consumed")
MESSAGES_ANALYZED = Counter("fraud_graph_messages_analyzed_total", "Graph records analyzed")
INVALID_EVENTS = Counter("fraud_graph_invalid_events_total", "Invalid input events")
PUBLISH_FAILURES = Counter("fraud_graph_publish_failures_total", "Kafka publish failures")
RETRIES = Counter("fraud_graph_processing_retries_total", "Processing retries")
DEAD_LETTERS = Counter("fraud_graph_dead_letters_total", "Records published to graph DLQ")
CONSUMER_LAG = Gauge("fraud_graph_observed_consumer_lag", "Approximate most recently observed partition lag")
NEPTUNE_QUERY_SECONDS = Histogram("fraud_graph_neptune_query_duration_seconds", "Neptune graph write and feature query duration")
_ready = False


def set_ready(value: bool) -> None:
    global _ready
    _ready = value


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return
        if self.path == "/health/ready":
            self.send_response(200 if _ready else 503)
            self.end_headers()
            self.wfile.write(b"ready" if _ready else b"not ready")
            return
        if self.path == "/metrics":
            body = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)

    def log_message(self, *_: object) -> None:
        return


def start_http_server(port: int) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    Thread(target=server.serve_forever, daemon=True, name="metrics-http").start()
    return server
