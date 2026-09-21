# Copyright 2023-2026 Wolveflake LLC
# SPDX-License-Identifier: Apache-2.0

import base64
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


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
        results = []
        for message in request.get("messages", []):
            value = base64.b64decode(message.get("value", "")).decode(
                "utf-8", "replace"
            ).rstrip("\n")
            print(
                f"[handler] got {message['topic']}[{message['partition']}]"
                f"@{message['offset']}: {value}",
                flush=True,
            )
            results.append({"status": 2})

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
