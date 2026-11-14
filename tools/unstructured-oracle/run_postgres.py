#!/usr/bin/env python3
"""Start the repository's embedded Postgres in a new empty directory, run one command with PG_URL set, stop it.

    python tools/unstructured-oracle/run_postgres.py <PG_ROOT> <command> [argument ...]

PG_ROOT is the unpacked build (the directory holding bin/). Binaries are used as they are; the cluster is thrown away.
"""
from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import tempfile
import importlib.util
import time
from pathlib import Path

STARTING_UP = "57P03"  # the server's "the database system is starting up"


def load_postgres():
    """tools/unstructured-corpus/postgres.py, the study's one way to Postgres."""
    path = Path(__file__).resolve().parents[1] / "unstructured-corpus" / "postgres.py"
    specification = importlib.util.spec_from_file_location("unstructured_postgres", path)
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def free_port() -> int:
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return server.getsockname()[1]


def main() -> int:
    if len(sys.argv) < 3:
        raise SystemExit(__doc__)
    bin_directory = Path(sys.argv[1]).resolve() / "bin"
    command = sys.argv[2:]
    data = Path(tempfile.mkdtemp(prefix="unstructured-pg-"))
    port = free_port()
    try:
        subprocess.run([bin_directory / "initdb", "-D", data / "data", "-U", "postgres", "--auth=trust", "-E", "UTF8",
                        "--locale=C"], check=True, stdout=subprocess.DEVNULL)
        server = subprocess.Popen([bin_directory / "postgres", "-D", data / "data", "-p", str(port), "-c", "unix_socket_directories=",
                                   "-c", "listen_addresses=127.0.0.1"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            postgres = load_postgres()

            deadline = time.monotonic() + 60
            while True:
                try:
                    connection = postgres.connect(f"postgresql://postgres@127.0.0.1:{port}/postgres", autocommit=True)
                    try:
                        postgres.execute(connection, "CREATE DATABASE unstructured", ())
                    finally:
                        connection.close()
                    break
                except (postgres.InterfaceError, postgres.DatabaseError) as failure:
                    # not listening yet, or listening and still starting up; any other refusal is the server's answer
                    if isinstance(failure, postgres.DatabaseError) and postgres.sqlstate(failure) != STARTING_UP:
                        raise
                    if server.poll() is not None or time.monotonic() > deadline:
                        raise SystemExit(f"the embedded Postgres did not start on port {port} (exit {server.poll()})")
                    time.sleep(0.2)
            return subprocess.call(command, env={**os.environ, "PG_URL": f"postgresql://postgres@127.0.0.1:{port}/unstructured"})
        finally:
            server.terminate()
            server.wait(timeout=30)
    finally:
        shutil.rmtree(data, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
