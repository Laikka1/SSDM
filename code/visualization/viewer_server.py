"""Serve the finalized EMA windows and advisories from one local replay."""

import argparse
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from math import isfinite
from os import fstat
from pathlib import Path
from threading import Lock
from urllib.parse import parse_qs, urlsplit

ASSET_DIR = Path(__file__).resolve().parent
ASSETS = {
    "/": ("viewer.html", "text/html"),
    "/viewer.js": ("viewer.js", "text/javascript"),
    "/viewer.css": ("viewer.css", "text/css"),
}


def parse_result(line, kinds):
    fields = dict(
        field.split("=", 1) for field in line.split("  ")[1:] if "=" in field
    )
    try:
        kind = fields["type"]
        symbol = fields["symbol"]
        window_end = datetime.fromisoformat(fields["window_end"])
        if kind not in kinds or not symbol or window_end.tzinfo is None:
            return None
        values = {
            "close": float(fields["close"]),
            "ema38": float(fields["EMA38"]),
            "ema100": float(fields["EMA100"]),
        }
        if kind in ("BUY", "SELL"):
            values["previous38"] = float(fields["previous_EMA38"])
            values["previous100"] = float(fields["previous_EMA100"])
        if not all(isfinite(value) for value in values.values()):
            return None
        result = {"time": round(window_end.timestamp() * 1000), "kind": kind, **values}
        if kind in ("BUY", "SELL"):
            result["reason"] = fields.get("reason", "")
        return symbol, result
    except (KeyError, ValueError, OverflowError):
        return None


class ResultLog:
    def __init__(self, path, kinds):
        self.path = path
        self.kinds = kinds
        self.identity = None
        self.position = 0
        self.by_symbol = {}

    def update(self):
        try:
            with self.path.open("rb") as file:
                stats = fstat(file.fileno())
                identity = (stats.st_dev, stats.st_ino)
                if identity != self.identity or stats.st_size < self.position:
                    self.identity = identity
                    self.position = 0
                    self.by_symbol.clear()
                file.seek(self.position)
                while (line := file.readline()).endswith(b"\n"):
                    self.position += len(line)
                    parsed = parse_result(line.decode("utf-8", errors="replace").rstrip(), self.kinds)
                    if parsed is None:
                        continue
                    symbol, result = parsed
                    records = self.by_symbol.setdefault(symbol, {})
                    key = (result["time"], result["kind"])
                    records[key] = result
        except FileNotFoundError:
            self.identity = None
            self.position = 0
            self.by_symbol.clear()

    def records(self, symbol):
        records = self.by_symbol.get(symbol, {})
        return [records[key] for key in sorted(records)]


class ReplayResults:
    def __init__(self, directory):
        self.run = directory.name
        self.windows = ResultLog(directory / "ema.log", {"EMA"})
        self.advisories = ResultLog(directory / "advisories.log", {"BUY", "SELL"})
        self.lock = Lock()

    def snapshot(self, requested_symbol):
        with self.lock:
            self.windows.update()
            self.advisories.update()
            symbols = sorted(self.windows.by_symbol)
            symbol = requested_symbol if requested_symbol in self.windows.by_symbol else next(iter(symbols), "")
            return {
                "run": self.run,
                "symbols": symbols,
                "symbol": symbol,
                "windows": self.windows.records(symbol),
                "advisories": self.advisories.records(symbol),
            }


class ViewerServer(ThreadingHTTPServer):
    def __init__(self, port, run_directory):
        self.results = ReplayResults(run_directory)
        super().__init__(("127.0.0.1", port), ViewerHandler)


class ViewerHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlsplit(self.path)
        if url.path == "/api/results":
            symbol = parse_qs(url.query).get("symbol", [""])[0]
            try:
                payload = self.server.results.snapshot(symbol)
            except OSError:
                self.send_error(500, "Could not read the replay logs")
                return
            self.send_content(json.dumps(payload, allow_nan=False).encode(), "application/json")
        elif url.path in ASSETS:
            filename, mime_type = ASSETS[url.path]
            self.send_content((ASSET_DIR / filename).read_bytes(), mime_type)
        else:
            self.send_error(404)

    def send_content(self, data, mime_type):
        self.send_response(200)
        self.send_header("Content-Type", f"{mime_type}; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def log_request(self, code="-", size="-"):
        if code != 200:
            super().log_request(code, size)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True, help="Replay directory containing the result logs")
    parser.add_argument("--port", type=int, default=8765, help="Local HTTP port (default: 8765)")
    args = parser.parse_args()
    if not 0 <= args.port <= 65535:
        parser.error("--port must be between 0 and 65535")
    directory = args.run_dir.resolve()
    if directory.exists() and not directory.is_dir():
        parser.error("--run-dir must be a directory")
    try:
        server = ViewerServer(args.port, directory)
    except OSError as exc:
        parser.exit(1, f"Could not start the viewer: {exc}\n")
    with server:
        print(f"Open http://127.0.0.1:{server.server_port}/ to watch {directory}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
