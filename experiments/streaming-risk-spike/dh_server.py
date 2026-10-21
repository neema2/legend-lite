"""An empty Deephaven Community server for the risk spike: the runner creates its inputs.

The update graph runs every 100 ms and a subscription that asks for no interval (the Python
ticking client cannot ask) gets 100 ms too, so Deephaven is timed like the others.
"""

import signal
import threading

from deephaven_server import Server

server = Server(port=10000, jvm_args=[
    "-Xmx4g",
    "-Dauthentication.psk=spike-psk",
    "-DPeriodicUpdateGraph.targetCycleDurationMillis=100",
    "-Dbarrage.minUpdateInterval=100",
])
server.start()
print("deephaven risk server on :10000", flush=True)
stop = threading.Event()
signal.signal(signal.SIGTERM, lambda *_: stop.set())
signal.signal(signal.SIGINT, lambda *_: stop.set())
stop.wait()
