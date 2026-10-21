from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

MESSAGES_CONSUMED = Counter("fraud_scoring_messages_consumed_total", "Enriched records consumed")
MESSAGES_SCORED = Counter("fraud_scoring_messages_scored_total", "Transactions scored")
INVALID_EVENTS = Counter("fraud_scoring_invalid_events_total", "Invalid input events")
INFERENCE_FAILURES = Counter("fraud_scoring_inference_failures_total", "Model inference failures")
PUBLISH_FAILURES = Counter("fraud_scoring_publish_failures_total", "Kafka publish failures")
RETRIES = Counter("fraud_scoring_processing_retries_total", "Processing retries")
DEAD_LETTERS = Counter("fraud_scoring_dead_letters_total", "Records published to the scoring dead-letter topic")
CONSUMER_LAG = Gauge("fraud_scoring_observed_consumer_lag", "Approximate lag at the most recently consumed partition offset")
SCORING_SECONDS = Histogram("fraud_scoring_duration_seconds", "Model inference latency")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
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
