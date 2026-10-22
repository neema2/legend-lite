"""An empty Deephaven Community server for the risk spike: the runner creates its inputs.

The update graph runs every 100 ms and a subscription that asks for no interval (the Python
ticking client cannot ask) gets 100 ms too, so Deephaven is timed like the others.
DH_HEAP (default 4g) and DH_CYCLE_MS (default 100) change the heap and both intervals.
"""

import os
import signal
import threading

from deephaven_server import Server

cycle = os.environ.get("DH_CYCLE_MS", "100")
server = Server(port=10000, jvm_args=[
    f"-Xmx{os.environ.get('DH_HEAP', '4g')}",
    "-Dauthentication.psk=spike-psk",
    f"-DPeriodicUpdateGraph.targetCycleDurationMillis={cycle}",
    f"-Dbarrage.minUpdateInterval={cycle}",
])
server.start()
print("deephaven risk server on :10000", flush=True)
stop = threading.Event()
signal.signal(signal.SIGTERM, lambda *_: stop.set())
signal.signal(signal.SIGINT, lambda *_: stop.set())
stop.wait()
