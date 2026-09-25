#!/usr/bin/env python3
# Copyright 2023-2026 Wolveflake LLC
# SPDX-License-Identifier: Apache-2.0
"""Capture real demo command output as asciicast and a self-contained animated SVG."""
import html
import json
from pathlib import Path
import subprocess
import textwrap
import time

root = Path(__file__).resolve().parents[2]
commands = [
    ["./demo", "send", '{"order_id":"123","event":"order.created"}'],
    ["./demo", "retry"],
]
events = []
lines = []
started = time.monotonic()
for command in commands:
    display = " ".join(command[:2])
    if len(command) > 2:
        display += " '" + command[2] + "'"
    stamp = time.monotonic() - started
    events.append([round(stamp, 3), "o", "$ " + display + "\r\n"])
    lines.append((stamp, "$ " + display))
    process = subprocess.Popen(command, cwd=root, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True)
    for line in process.stdout:
        print(line, end="", flush=True)
        stamp = time.monotonic() - started
        events.append([round(stamp, 3), "o", line.rstrip("\n") + "\r\n"])
        for wrapped in textwrap.wrap(line.rstrip(), width=108, subsequent_indent="  "):
            lines.append((stamp, wrapped))
    if process.wait() != 0:
        raise SystemExit("Demo failed; recording not saved.")

assets = root / "docs/assets"
header = {"version": 2, "width": 110, "height": 18, "timestamp": int(time.time()),
          "title": "WKS: process an order and recover from a retry"}
(assets / "wks-demo.cast").write_text("\n".join(json.dumps(x) for x in [header] + events) + "\n")
height = 100 + 25 * len(lines)
duration = max(12, time.monotonic() - started + 5)
parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="1160" height="{height}" viewBox="0 0 1160 {height}" role="img" aria-label="Actual WKS demo output: one successful order, then a retry and recovery">',
         '<style>@media (prefers-reduced-motion:reduce){text{animation:none!important;opacity:1!important}}</style>',
         f'<rect width="1160" height="{height}" rx="14" fill="#0d1117"/>',
         '<circle cx="24" cy="25" r="5" fill="#f85149"/><circle cx="42" cy="25" r="5" fill="#d29922"/><circle cx="60" cy="25" r="5" fill="#3fb950"/>',
         '<text x="90" y="30" font-family="monospace" font-size="13" fill="#8b949e">WKS · captured local demo · loops</text>']
for index, (stamp, line) in enumerate(lines):
    fraction = min(80, 100 * stamp / duration)
    color = "#79c0ff" if line.startswith("$") else ("#d29922" if " RETRY:" in line else "#aff5b4")
    parts.append(f'<style>@keyframes line{index}{{0%,{fraction:.2f}%{{opacity:0}}{fraction+0.1:.2f}%,100%{{opacity:1}}}}</style>')
    parts.append(f'<text x="24" y="{75+25*index}" font-family="monospace" font-size="16" fill="{color}" style="animation:line{index} {duration:.2f}s linear infinite">{html.escape(line)}</text>')
parts.append('</svg>')
(assets / "wks-demo.svg").write_text("\n".join(parts) + "\n")
print("Saved docs/assets/wks-demo.cast and wks-demo.svg")
