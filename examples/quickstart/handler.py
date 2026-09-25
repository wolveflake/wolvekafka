# Copyright 2023-2026 Wolveflake LLC
# SPDX-License-Identifier: Apache-2.0

import base64
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

SUCCESS, RETRY, FAILED = 2, 0, 1
HANDLER = os.environ.get("HANDLER_NAME", "A")
retried = set()


def process_order(order):
    """Put your business logic here; return SUCCESS, RETRY, or FAILED."""
    print(f"[handler {HANDLER}] processing order {order.get('order_id', '?')}", flush=True)
    return SUCCESS


def process_message(message):
    value = base64.b64decode(message.get("value", ""))
    key = base64.b64decode(message.get("key", "")).decode("utf-8", "replace")
    identity = (message["topic"], message["partition"], message["offset"])
    try:
        order = json.loads(value)
        if not isinstance(order, dict):
            raise ValueError("expected an order object")
        if order.get("demo") == "reject":
            status = FAILED
        elif order.get("demo") == "retry_once" and identity not in retried:
            retried.add(identity)
            status = RETRY
        else:
            status = process_order(order)
            retried.discard(identity)
    except (ValueError, UnicodeDecodeError):
        status = FAILED
    label = {SUCCESS: "SUCCESS", RETRY: "RETRY", FAILED: "FAILED"}[status]
    print(f"[handler {HANDLER}] {key} {message['topic']}[{message['partition']}]"
          f"@{message['offset']} {label}: {value.decode('utf-8', 'replace')}", flush=True)
    return {"status": status}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/health":
            self.send_error(404)
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", "0"))
        request = json.loads(self.rfile.read(content_length))
        results = [process_message(message) for message in request.get("messages", [])]

        body = json.dumps({"messages": results}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format, *_args):
        return


if __name__ == "__main__":
    print("[handler] listening on :8081", flush=True)
    HTTPServer(("0.0.0.0", 8081), Handler).serve_forever()
