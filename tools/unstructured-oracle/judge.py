#!/usr/bin/env python3
"""The judge of the unstructured study: the frozen answer key against the production path (README.md).

A structured query always goes through lite: the judge starts //core:server, POSTs to /api/pure/v1/execution/execute with
the model text and the DuckDB runtime, and compares the rows it is answered with. It never runs one itself.
"""
from __future__ import annotations

import socket
import subprocess
import tempfile
import time
from decimal import Decimal
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_FILE = REPO_ROOT / "projects/unstructured/model.pure"
DUCKDB_FILE = REPO_ROOT / "projects/unstructured/corpus/unstructured.duckdb"
DECLARED_DUCKDB_PATH = "path: 'projects/unstructured/corpus/unstructured.duckdb'"
RUNTIME = "runtime::DuckDb"
REQUEST_TIMEOUT_SECONDS = 300


class QueryFailed(Exception):
    """The server did not answer a query. An error, never an empty result."""


def model_text() -> str:
    """The model as committed, its DuckDB path made absolute: the server's working directory is not the repository's."""
    text = MODEL_FILE.read_text(encoding="utf-8")
    if text.count(DECLARED_DUCKDB_PATH) != 1:
        raise SystemExit(f"{MODEL_FILE} no longer declares {DECLARED_DUCKDB_PATH} exactly once")
    return text.replace(DECLARED_DUCKDB_PATH, f"path: '{DUCKDB_FILE}'")


class Server:
    """//core:server, built by Bazel and started on a free port for the length of a `with`."""

    def __enter__(self) -> "Server":
        subprocess.run(["bazel", "build", "//core:server"], cwd=REPO_ROOT, check=True, capture_output=True)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            self.port = probe.getsockname()[1]
        self.base = f"http://127.0.0.1:{self.port}"
        self.log = tempfile.NamedTemporaryFile(prefix="unstructured-server-", suffix=".log", delete=False)
        self.process = subprocess.Popen([REPO_ROOT / "bazel-bin/core/server"], cwd=REPO_ROOT, env=server_environment(self.port),
                                        stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 120
        while True:
            try:
                if requests.get(f"{self.base}/health", timeout=5).status_code == 200:
                    return self
            except requests.ConnectionError:
                pass
            if self.process.poll() is not None or time.monotonic() > deadline:
                raise SystemExit(f"//core:server did not start (exit {self.process.poll()}); its log is {self.log.name}")
            time.sleep(0.5)

    def __exit__(self, *_) -> None:
        self.process.terminate()
        self.process.wait(timeout=30)


def server_environment(port: int) -> dict[str, str]:
    import os

    return {**os.environ, "PORT": str(port)}


def answer_of(response: requests.Response, what: str) -> dict:
    if response.status_code != 200:
        raise QueryFailed(f"{what}: HTTP {response.status_code}: {response.text[:400]}")
    return response.json()


def execute(base: str, model: str, query: str) -> list[list]:
    """A Pure query's rows, as the engine API answers them. A refusal or failure raises; [] means nothing matched."""
    try:
        function = answer_of(requests.post(f"{base}/api/pure/v1/grammar/grammarToJson/lambda", data=query.encode("utf-8"),
                                           timeout=REQUEST_TIMEOUT_SECONDS), "grammarToJson/lambda")
        answer = answer_of(requests.post(f"{base}/api/pure/v1/execution/execute", timeout=REQUEST_TIMEOUT_SECONDS, json={
            "clientVersion": "vX_X_X", "function": function, "model": {"_type": "text", "code": model},
            "runtime": {"_type": "runtimePointer", "runtime": RUNTIME}, "context": {"_type": "BaseExecutionContext"}}),
            "execution/execute")
    except requests.RequestException as failure:
        raise QueryFailed(f"the request failed: {failure}") from failure
    return [row["values"] for row in answer["result"]["rows"]]


def comparable(rows: list[list]) -> set[tuple]:
    """Rows as a set, numbers by value: 4, 4.0 and 4.00 are one value, and a boolean is not a number."""
    def cell(value):
        if isinstance(value, bool):  # True == Decimal(1) in Python, so a boolean is carried tagged
            return ("boolean", value)
        if value is None or isinstance(value, str):
            return value
        if isinstance(value, (int, float)):
            return Decimal(repr(value))
        raise SystemExit(f"a cell of a type the key cannot express: {value!r}")
    return {tuple(cell(value) for value in row) for row in rows}
