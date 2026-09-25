# Copyright 2023-2026 Wolveflake LLC
# SPDX-License-Identifier: Apache-2.0
"""Update only the demo's runtime routing; atomic replacement exercises hot reload."""
import json
import os
import sys
from pathlib import Path

target = sys.argv[1]
targets = {
    "a": [{"url": "http://handler:8081/", "weight": 100}],
    "b": [{"url": "http://handler-b:8081/", "weight": 100}],
    "split": [
        {"url": "http://handler:8081/", "weight": 50},
        {"url": "http://handler-b:8081/", "weight": 50},
    ],
}
path = Path("/demo-config/config.json")
config = json.loads(path.read_text())
if config["consumers"][0]["lambdas"] == targets[target]:
    print(f"Routing is already set to {target}.")
    sys.exit(3)
config["consumers"][0]["lambdas"] = targets[target]
temporary = path.with_suffix(".tmp")
temporary.write_text(json.dumps(config, indent=2) + "\n")
os.replace(temporary, path)
print(f"Routing set to {target}. WKS watches this file; allow a few seconds for reload.")
