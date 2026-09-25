#!/usr/bin/env python3
# Copyright 2023-2026 Wolveflake LLC
# SPDX-License-Identifier: Apache-2.0
"""Capture the local Grafana dashboard with an isolated headless Chrome profile."""
import argparse
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

root = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--chrome", default=shutil.which("chromium") or shutil.which("google-chrome")
                    or "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
parser.add_argument("--url", default="http://127.0.0.1:3000/d/adpmcvw/wks?from=now-15m&to=now&kiosk")
parser.add_argument("--output", type=Path, default=root / "docs/assets/wks-dashboard.png")
parser.add_argument("--size", default="1600,1100")
args = parser.parse_args()
with tempfile.TemporaryDirectory(prefix="wks-capture-") as temporary:
    capture = Path(temporary) / "capture.png"
    with (Path(temporary) / "chrome.log").open("w+") as log:
        process = subprocess.Popen([
            args.chrome, "--headless", "--disable-gpu", "--no-first-run",
            "--no-default-browser-check", "--disable-background-networking",
            f"--user-data-dir={temporary}/profile", f"--window-size={args.size}",
            "--hide-scrollbars", "--timeout=20000", f"--screenshot={capture}", args.url,
        ], stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline and process.poll() is None:
                if capture.exists():
                    # Let the browser finish writing the PNG.
                    time.sleep(1)
                    break
                time.sleep(0.5)
            if not capture.exists():
                log.seek(0)
                raise SystemExit("Screenshot failed:\n" + log.read()[-4000:])
            shutil.copyfile(capture, args.output)
            print(f"Saved {args.output}")
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
