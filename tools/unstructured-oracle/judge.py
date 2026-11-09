#!/usr/bin/env python3
"""The judge of the unstructured study: the frozen answer key against the production path (README.md).

    run.sh --write        a fresh ledger.tsv (the baseline only when a person commits it)
    run.sh --check        a fresh run against the committed ledger.tsv: any PASS that is not PASS now fails
    judge.py --compare OLD NEW      that gate alone, over two ledger files
    judge.py --index      the keyword arm's index, after load.py and embed.py

A structured query always goes through lite: the judge starts //core:server, POSTs to /api/pure/v1/execution/execute with
the model text and the DuckDB runtime, and compares the rows it is answered with. It never runs one itself. The text arms
are the statements in sql/, run directly on each backend; the judge reads their rows and decides nothing about ranking.
"""
from __future__ import annotations

import hashlib
import importlib.util
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import requests
import yaml

import judge_logic as logic

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_FILE = REPO_ROOT / "projects/unstructured/model.pure"
DUCKDB_FILE = REPO_ROOT / "projects/unstructured/corpus/unstructured.duckdb"
DECLARED_DUCKDB_PATH = "path: 'projects/unstructured/corpus/unstructured.duckdb'"
RUNTIME = "runtime::DuckDb"
REQUEST_TIMEOUT_SECONDS = 300
EVALS = REPO_ROOT / "projects/unstructured/evals"
ANSWER_KEY = EVALS / "answer_key.yaml"
DECISIONS = EVALS / "DECISIONS.md"
HERE = Path(__file__).resolve().parent
SQL = HERE / "sql"
LEDGER = HERE / "ledger.tsv"
SOURCE_OF_CORPUS = {"SALES": "SALES", "LENDING": "LENDING", "EDGAR 13F": "EDGAR", "EDGAR 10-K/10-Q/8-K": "EDGAR"}
ARMS = ("keyword", "vector", "hybrid")
BACKENDS = ("duckdb", "postgres")
NEGATIVE_ANSWER = "nothing in the corpus"
# What a server without pgvector answers: undefined table, type and function or operator. Any other failure of those
# statements, a syntax error above all, is an ERROR, never a refusal.
PGVECTOR_ABSENT = ("42P01", "42704", "42883")


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


def load_embedder():
    specification = importlib.util.spec_from_file_location("embed", REPO_ROOT / "tools/unstructured-corpus/embed.py")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def load_key() -> tuple[list[dict], str]:
    """The cases, and the key's sha256, which must be the one DECISIONS.md last recorded."""
    digest = hashlib.sha256(ANSWER_KEY.read_bytes()).hexdigest()
    recorded = re.findall(r"^answer_key\.yaml sha256: ([0-9a-f]{64})$", DECISIONS.read_text(encoding="utf-8"), re.MULTILINE)
    if not recorded or recorded[-1] != digest:
        raise SystemExit(f"answer_key.yaml has sha256 {digest}; DECISIONS.md last records {recorded[-1] if recorded else 'nothing'}")
    return yaml.safe_load(ANSWER_KEY.read_text(encoding="utf-8"))["cases"], digest


def statement(name: str) -> str:
    return (SQL / name).read_text(encoding="utf-8")


def refuse_drifted_hybrid() -> None:
    for backend in BACKENDS:
        hybrid = statement(f"hybrid_{backend}.sql")
        for arm in ("keyword", "vector"):
            body = "\n".join(line for line in statement(f"{arm}_{backend}.sql").splitlines() if not line.startswith("--")).strip()
            if body not in hybrid:
                raise SystemExit(f"hybrid_{backend}.sql no longer contains {arm}_{backend}.sql verbatim")


def index() -> int:
    import duckdb

    connection = duckdb.connect(str(DUCKDB_FILE))
    try:
        for command in [c for c in re.sub(r"--.*", "", statement("keyword_duckdb_setup.sql")).split(";") if c.strip()]:
            connection.execute(command)
    finally:
        connection.close()
    return 0


def text_pointers(case: dict) -> list[dict]:
    return (case["expected"].get("text") or {}).get("accepted") or []


def superseded_text(case: dict) -> list[dict]:
    return [entry for entry in case.get("superseded") or [] if "rows" not in entry]


def refuse_key_off_corpus(connection, cases: list[dict]) -> None:
    """Every pointer of the key names a passage that exists, at those offsets, with that hash."""
    wrong = []
    for case in cases:
        source = SOURCE_OF_CORPUS[case["corpus"]]
        for entry in text_pointers(case) + superseded_text(case):
            found = connection.execute("SELECT CHAR_END, SHA256 FROM CO_PASSAGE WHERE SOURCE = ? AND FILE_PATH = ? AND CHAR_START = ?",
                                       [source, entry["path"], entry["charStart"]]).fetchone()
            if found != (entry["charEnd"], entry["sha256"]):
                wrong.append(f"{case['id']} {entry['path']}@{entry['charStart']}: the corpus holds {found}")
    if wrong:
        raise SystemExit("the key does not point at this corpus:\n  " + "\n  ".join(wrong))


def refuse_vectors_off_pin(connection, pins: dict[str, str]) -> None:
    held = connection.execute("SELECT EMBED_MODEL, EMBED_MODEL_REVISION, EMBED_MODEL_SHA256, EMBED_DIMENSIONS, EMBED_QUERY_INSTRUCTION FROM CO_EMBEDDING_PIN").fetchall()
    wanted = [(pins["EMBED_MODEL"], pins["EMBED_MODEL_REVISION"], pins["EMBED_MODEL_SHA256"], int(pins["EMBED_DIMENSIONS"]), pins["EMBED_QUERY_INSTRUCTION"])]
    if held != wanted:
        raise SystemExit(f"the stored vectors were made by {held}, the pins say {wanted}: run embed.py")


def normalised(rows: list[tuple]) -> list[dict]:
    return [{"rank": rank, "sha256": sha256, "score": float(score), "locations": locations} for rank, sha256, score, locations in rows]


def question_negative(case: dict, route: str) -> bool:
    """A negative's route has nothing accepted, or only empty row sets; anything else with nothing accepted is a broken key."""
    expected = case["expected"]
    if route == "text":
        empty = not text_pointers(case)
    else:
        empty = all(not query["rows"] for query in expected["structured"])
    negative = expected.get("answer") == NEGATIVE_ANSWER
    if empty != negative:
        raise SystemExit(f"{case['id']}: on the {route} route, empty expected is {empty} and the negative answer is {negative}")
    return negative


def routes_of(case: dict) -> list[str]:
    return {"text": ["text"], "structured": ["structured"], "both": ["text", "structured"]}[case["route"]]


def needs_pgvector(backend: str, arm: str) -> bool:
    return backend == "postgres" and arm != "keyword"


def judge_text(cases, duck, pg, vectors, refusal, failures) -> list[dict]:
    import duckdb
    import psycopg

    ledger = []
    for case in cases:
        if "text" not in routes_of(case):
            continue
        source = SOURCE_OF_CORPUS[case["corpus"]]
        accepted = [logic.pointer(source, e["path"], e["charStart"]) for e in text_pointers(case)]
        superseded = [logic.pointer(source, e["path"], e["charStart"]) for e in superseded_text(case)]
        match = case["expected"]["text"]["match"]
        for arm in ARMS:
            for backend in BACKENDS:
                identity = {"case": case["id"], "route": "text", "arm": arm, "backend": backend, "road": "direct-sql"}
                if case["status"] != "resolved":
                    outcome = {"verdict": logic.UNRESOLVED, "receipts": [], "detail": f"status: {case['status']}"}
                else:
                    parameters = {"question": case["question"], "vector": vectors[case["id"]], "time_aware": case["type"] == "time-aware",
                                  "cut": logic.cut(accepted)}
                    needed = {"keyword": ["question"], "vector": ["vector"], "hybrid": ["question", "vector", "time_aware", "cut"]}[arm]
                    arguments = {name: parameters[name] for name in needed}
                    if backend == "postgres" and "vector" in arguments:
                        arguments["vector"] = "[" + ",".join(repr(x) for x in arguments["vector"]) + "]"
                    try:
                        connection = duck if backend == "duckdb" else pg
                        rows = connection.execute(statement(f"{arm}_{backend}.sql"), arguments).fetchall()
                    except psycopg.Error as failure:
                        if needs_pgvector(backend, arm) and refusal is not None and failure.sqlstate in PGVECTOR_ABSENT:
                            outcome = {"verdict": logic.REFUSED, "receipts": [], "detail": f"{refusal}; {arm}_postgres.sql, run: "
                                       f"{type(failure).__name__} {failure.sqlstate}: {failure.diag.message_primary}"}
                        else:
                            outcome = {"verdict": logic.ERROR, "receipts": [], "detail": f"{type(failure).__name__}: {failure}"[:400]}
                            failures.append(identity)
                    except duckdb.Error as failure:
                        outcome = {"verdict": logic.ERROR, "receipts": [], "detail": f"{type(failure).__name__}: {failure}"[:400]}
                        failures.append(identity)
                    else:
                        if needs_pgvector(backend, arm) and refusal is not None:
                            raise SystemExit(f"{arm}_postgres.sql returned rows on a server the catalogue says has no pgvector: {refusal}")
                        outcome = logic.text_verdict(normalised(rows), accepted, match, superseded, question_negative(case, "text"))
                ledger.append(logic.ledger_row(identity, outcome))
    return ledger


def judge_structured(cases, base: str, model: str, failures) -> list[dict]:
    ledger = []
    for case in cases:
        if "structured" not in routes_of(case):
            continue
        identity = {"case": case["id"], "route": "structured", "arm": "pure", "backend": "duckdb", "road": "lite"}
        if case["status"] != "resolved":
            ledger.append(logic.ledger_row(identity, {"verdict": logic.UNRESOLVED, "receipts": [], "detail": f"status: {case['status']}"}))
            continue
        try:
            results = [execute(base, model, query["query"]) for query in case["expected"]["structured"]]
        except QueryFailed as failure:
            outcome = {"verdict": logic.ERROR, "receipts": [], "detail": str(failure)[:400]}
            failures.append(identity)
        else:
            superseded = [row for entry in case.get("superseded") or [] if "rows" in entry for row in entry["rows"]]
            outcome = logic.structured_verdict(results, [query["rows"] for query in case["expected"]["structured"]], superseded,
                                               question_negative(case, "structured"))
        ledger.append(logic.ledger_row(identity, outcome))
    return ledger


def run(mode: str) -> int:
    import duckdb
    import psycopg

    embed = load_embedder()
    pins = embed.read_pins()
    embed.refuse_off_pin(pins)
    cases, key_sha256 = load_key()
    refuse_drifted_hybrid()
    url = os.environ.get("PG_URL")
    if not url:
        raise SystemExit("PG_URL is not set: tools/unstructured-oracle/run.sh starts the embedded Postgres and sets it")
    duck = duckdb.connect(str(DUCKDB_FILE), read_only=True)
    duck.execute("LOAD fts")
    refuse_key_off_corpus(duck, cases)
    refuse_vectors_off_pin(duck, pins)
    model = embed.load_model(pins)
    texts = [case for case in cases if "text" in routes_of(case)]
    vectors = dict(zip([case["id"] for case in texts], embed.encode_questions(model, [case["question"] for case in texts], pins)))
    failures: list[dict] = []
    with psycopg.connect(url, autocommit=True) as pg:
        refusal = embed.pgvector_refusal(pg)
        ledger = judge_text(cases, duck, pg, vectors, refusal, failures)
    duck.close()  # the server opens this file: DuckDB allows one writer or many readers, never both across processes
    with Server() as server:
        ledger += judge_structured(cases, server.base, model_text(), failures)
    order = {case["id"]: index for index, case in enumerate(cases)}
    ledger.sort(key=lambda row: (order[row["case"]], row["route"] == "structured", ARMS.index(row["arm"]) if row["arm"] in ARMS else 0,
                                 BACKENDS.index(row["backend"])))
    header = [f"answer_key.yaml sha256 {key_sha256}"] + [f"{k}={v}" for k, v in pins.items() if k != "EMBED_QUERY_INSTRUCTION"]
    if mode == "--write":
        logic.write_ledger(LEDGER, header, ledger)
        print(f"wrote {LEDGER.relative_to(REPO_ROOT)}: {len(ledger)} rows")
        return 0
    return gate(logic.read_ledger(LEDGER), ledger)


def gate(old: list[dict], new: list[dict]) -> int:
    failed = logic.regressions(old, new)
    for line in failed:
        print(f"REGRESSION {line}")
    print(f"{len(failed)} regression(s) against {len(old)} baseline rows")
    return 1 if failed else 0


def main() -> int:
    arguments = sys.argv[1:]
    if arguments == ["--index"]:
        return index()
    if len(arguments) == 3 and arguments[0] == "--compare":
        return gate(logic.read_ledger(Path(arguments[1])), logic.read_ledger(Path(arguments[2])))
    if arguments in (["--write"], ["--check"]):
        return run(arguments[0])
    raise SystemExit(__doc__)


if __name__ == "__main__":
    sys.exit(main())
