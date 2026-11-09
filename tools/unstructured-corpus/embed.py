#!/usr/bin/env python3
"""Compute one vector per passage of CO_PASSAGE with the pinned open-source embedder (README: Embeddings).

    UNSTRUCTURED_MODEL_DIR=/some/empty/dir python tools/unstructured-corpus/embed.py --fetch
    UNSTRUCTURED_MODEL_DIR=/that/dir [PG_URL=postgresql://host/database] python tools/unstructured-corpus/embed.py

This is data preparation and is never done by the engine: lite reads the vectors, it does not make them.

--fetch is the one step that touches the network: it downloads the model at pins.env's commit into its own directory and
checks the weights against pins.env's sha256. An embedding run is offline, reads only that directory, and runs no code
from it. Identical passages are one vector, so the table is keyed by the passage's sha256. Run load.py first: it replaces
the DuckDB file, vectors included. Postgres gets the vectors only if pgvector is available there; nothing is ever
installed into a server, and a server without it is reported by name.
"""
from __future__ import annotations

import hashlib
import importlib.metadata
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PINS_FILE = Path(__file__).resolve().with_name("pins.env")
DUCKDB_FILE = REPO_ROOT / "projects/unstructured/corpus/unstructured.duckdb"
WEIGHTS = "model.safetensors"
# Every file sentence-transformers reads; the repository's pickled and ONNX copies of the weights are never fetched.
MODEL_FILES = ["1_Pooling/config.json", "README.md", "config.json", "config_sentence_transformers.json", WEIGHTS,
               "modules.json", "sentence_bert_config.json", "special_tokens_map.json", "tokenizer.json",
               "tokenizer_config.json", "vocab.txt"]
DISTRIBUTIONS = {"sentence-transformers": "SENTENCE_TRANSFORMERS_VERSION", "torch": "TORCH_VERSION",
                 "transformers": "TRANSFORMERS_VERSION", "tokenizers": "TOKENIZERS_VERSION",
                 "huggingface-hub": "HUGGINGFACE_HUB_VERSION", "safetensors": "SAFETENSORS_VERSION",
                 "numpy": "NUMPY_VERSION", "duckdb": "DUCKDB_VERSION", "psycopg": "PSYCOPG_VERSION",
                 "psycopg-binary": "PSYCOPG_BINARY_VERSION", "pyyaml": "PYYAML_VERSION", "requests": "REQUESTS_VERSION"}


def read_pins() -> dict[str, str]:
    pins = {}
    for line in PINS_FILE.read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            pins[key] = value
    return pins


def refuse_off_pin(pins: dict[str, str]) -> None:
    """VERSION: any other installed release or Python minor stops the run; a vector is reproducible only on the pins."""
    python = f"{sys.version_info.major}.{sys.version_info.minor}"
    off = [] if python == pins["PYTHON_VERSION"] else [f"python {python}, pinned {pins['PYTHON_VERSION']}"]
    for distribution, key in DISTRIBUTIONS.items():
        try:
            installed = importlib.metadata.version(distribution)
        except importlib.metadata.PackageNotFoundError:
            installed = "not installed"
        if installed != pins[key]:
            off.append(f"{distribution} {installed}, pinned {pins[key]}")
    if off:
        raise SystemExit("VERSION: off the pins in tools/unstructured-corpus/pins.env: " + "; ".join(off))


def model_directory() -> Path:
    value = os.environ.get("UNSTRUCTURED_MODEL_DIR")
    if not value:
        raise SystemExit("UNSTRUCTURED_MODEL_DIR is not set: name the directory that holds the pinned model")
    return Path(value).resolve()


def refuse_wrong_weights(directory: Path, pins: dict[str, str]) -> None:
    weights = directory / WEIGHTS
    if not weights.is_file():
        raise SystemExit(f"{weights} does not exist: run embed.py --fetch first")
    actual = hashlib.sha256(weights.read_bytes()).hexdigest()
    if actual != pins["EMBED_MODEL_SHA256"]:
        raise SystemExit(f"{weights} has sha256 {actual}, pinned {pins['EMBED_MODEL_SHA256']}")


def fetch(pins: dict[str, str]) -> None:
    from huggingface_hub import snapshot_download

    directory = model_directory()
    snapshot_download(pins["EMBED_MODEL"], revision=pins["EMBED_MODEL_REVISION"], local_dir=str(directory),
                      allow_patterns=MODEL_FILES)
    refuse_wrong_weights(directory, pins)
    print(f"fetched {pins['EMBED_MODEL']}@{pins['EMBED_MODEL_REVISION']} into {directory}")


def load_model(pins: dict[str, str]):
    """The pinned model from its directory, offline, on the CPU, running no code from the directory."""
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
    from sentence_transformers import SentenceTransformer

    directory = model_directory()
    refuse_wrong_weights(directory, pins)
    return SentenceTransformer(str(directory), device="cpu", local_files_only=True, trust_remote_code=False)


def encode(model, texts: list[str], pins: dict[str, str]) -> list[list[float]]:
    """Unit vectors, so cosine similarity and pgvector's cosine distance read them alike."""
    vectors = model.encode(texts, batch_size=32, normalize_embeddings=True, convert_to_numpy=True,
                           show_progress_bar=False)
    wanted = int(pins["EMBED_DIMENSIONS"])
    if vectors.shape != (len(texts), wanted):
        raise SystemExit(f"the model made vectors of shape {vectors.shape}, expected ({len(texts)}, {wanted})")
    return [[float(x) for x in vector] for vector in vectors]


def encode_questions(model, questions: list[str], pins: dict[str, str]) -> list[list[float]]:
    return encode(model, [pins["EMBED_QUERY_INSTRUCTION"] + question for question in questions], pins)


def truncated(model, texts: list[str]) -> int:
    """How many texts are longer than the model reads: the tail of such a passage has no influence on its vector."""
    limit = model.max_seq_length
    return sum(1 for text in texts if len(model.tokenizer(text, truncation=False)["input_ids"]) > limit)


def store_duckdb(rows: list[tuple[str, list[float]]], pins: dict[str, str]) -> None:
    import duckdb

    dimensions = int(pins["EMBED_DIMENSIONS"])
    connection = duckdb.connect(str(DUCKDB_FILE))
    try:
        connection.execute("BEGIN")
        connection.execute("DROP TABLE IF EXISTS CO_PASSAGE_EMBEDDING")
        connection.execute("DROP TABLE IF EXISTS CO_EMBEDDING_PIN")
        connection.execute(f"CREATE TABLE CO_PASSAGE_EMBEDDING (SHA256 VARCHAR PRIMARY KEY, EMBEDDING FLOAT[{dimensions}] NOT NULL)")
        connection.executemany(f"INSERT INTO CO_PASSAGE_EMBEDDING VALUES (?, ?::FLOAT[{dimensions}])", rows)
        connection.execute("CREATE TABLE CO_EMBEDDING_PIN (EMBED_MODEL VARCHAR NOT NULL, EMBED_MODEL_REVISION VARCHAR NOT NULL, "
                           "EMBED_MODEL_SHA256 VARCHAR NOT NULL, EMBED_DIMENSIONS BIGINT NOT NULL, EMBED_QUERY_INSTRUCTION VARCHAR NOT NULL)")
        connection.execute("INSERT INTO CO_EMBEDDING_PIN VALUES (?, ?, ?, ?, ?)",
                           [pins["EMBED_MODEL"], pins["EMBED_MODEL_REVISION"], pins["EMBED_MODEL_SHA256"], dimensions,
                            pins["EMBED_QUERY_INSTRUCTION"]])
        connection.execute("COMMIT")
        print("duckdb:", connection.execute("SELECT count(*), count(DISTINCT SHA256) FROM CO_PASSAGE_EMBEDDING").fetchone(),
              "vectors;", connection.execute("SELECT count(*) FROM (SELECT DISTINCT SHA256 FROM CO_PASSAGE) p "
                                             "ANTI JOIN CO_PASSAGE_EMBEDDING e USING (SHA256)").fetchone()[0],
              "passages without one")
    finally:
        connection.close()


def pgvector_refusal(connection) -> str | None:
    """Why this server cannot hold the vectors, by name, read from the catalogue; None if it can. Changes nothing."""
    version = connection.execute("SHOW server_version").fetchone()[0]
    available = connection.execute("SELECT count(*) FROM pg_available_extensions").fetchone()[0]
    if connection.execute("SELECT count(*) FROM pg_available_extensions WHERE name = 'vector'").fetchone()[0] == 0:
        return f"pgvector: extension 'vector' is not among the {available} extensions PostgreSQL {version} has available"
    return None


def store_postgres(url: str, rows: list[tuple[str, list[float]]], pins: dict[str, str]) -> None:
    import psycopg

    dimensions = int(pins["EMBED_DIMENSIONS"])
    with psycopg.connect(url) as connection:
        refusal = pgvector_refusal(connection)
        if refusal is not None:
            print(f"postgres: REFUSED {refusal}. Nothing was installed; the vector arm on Postgres is a REFUSED row.")
            return
        connection.execute("CREATE EXTENSION IF NOT EXISTS vector")
        connection.execute("DROP TABLE IF EXISTS CO_PASSAGE_EMBEDDING")
        connection.execute(f"CREATE TABLE CO_PASSAGE_EMBEDDING (SHA256 VARCHAR PRIMARY KEY, EMBEDDING vector({dimensions}) NOT NULL)")
        with connection.cursor() as cursor:
            cursor.executemany(f"INSERT INTO CO_PASSAGE_EMBEDDING VALUES (%s, %s::vector({dimensions}))",
                               [(sha256, "[" + ",".join(repr(x) for x in vector) + "]") for sha256, vector in rows])
        connection.commit()
        print("postgres:", connection.execute("SELECT count(*) FROM CO_PASSAGE_EMBEDDING").fetchone()[0], "vectors")


def main() -> int:
    pins = read_pins()
    refuse_off_pin(pins)
    if sys.argv[1:] == ["--fetch"]:
        fetch(pins)
        return 0
    if sys.argv[1:]:
        raise SystemExit(f"unknown arguments {sys.argv[1:]}: the one flag is --fetch")
    import duckdb

    if not DUCKDB_FILE.is_file():
        raise SystemExit(f"{DUCKDB_FILE} does not exist: run tools/unstructured-corpus/load.py first")
    connection = duckdb.connect(str(DUCKDB_FILE), read_only=True)
    try:
        passages = connection.execute("SELECT SHA256, any_value(PASSAGE_TEXT) FROM CO_PASSAGE GROUP BY SHA256 ORDER BY SHA256").fetchall()
    finally:
        connection.close()
    model = load_model(pins)
    texts = [text for _, text in passages]
    print(f"embedding {len(texts)} distinct passages; {truncated(model, texts)} are longer than the model's "
          f"{model.max_seq_length} tokens and are read only that far")
    rows = list(zip([sha256 for sha256, _ in passages], encode(model, texts, pins)))
    store_duckdb(rows, pins)
    url = os.environ.get("PG_URL")
    if url is not None:
        if not url.strip():
            raise SystemExit("PG_URL is set and empty")
        store_postgres(url, rows, pins)
    return 0


if __name__ == "__main__":
    sys.exit(main())
