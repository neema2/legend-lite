"""Python's engine for the conformance suite (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md, R1; //datacube:conformance_test):
legend-engine's API served over no frame, so a model asked of it with its own Data rows is planned and run as any model
of a project is (Engine._project_of: its rows in a database of their own). It prints one JSON line -- the engine's address
and its token -- and serves until its standard input closes.

    conformance_python_engine
"""

import json
import sys

import legend_lite as ll
from legend_lite.engine import Engine, WebServer


def main() -> None:
    frames = ll.Frames()
    server = WebServer(Engine(frames))
    sys.stdout.write(json.dumps({'url': server.url, 'authorization': server.authorization}) + '\n')
    sys.stdout.flush()
    sys.stdin.read()
    server.close()
    frames.close()


if __name__ == '__main__':
    main()
