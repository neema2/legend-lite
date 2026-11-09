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
import time
from pathlib import Path


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
            import psycopg

            deadline = time.monotonic() + 60
            while True:
                try:
                    with psycopg.connect(f"postgresql://postgres@127.0.0.1:{port}/postgres", autocommit=True) as connection:
                        connection.execute("CREATE DATABASE unstructured")
                    break
                except psycopg.OperationalError:
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
