"""The spike's Deephaven server: Deephaven Community Core embedded in this Python process
(`pip install deephaven-server==42.7`), plus a simulated market-data feed.

It stands in for a firm's Deephaven: the feed tables (`trades`, `quotes`) are what a
`###Deephaven` store declares; legend-lite never defines them, it only queries them.

    python start_server.py [--port 10000] [--psk spike-psk] [--trade-period PT0.005S] [--quote-period PT0.001S]

The update graph runs a cycle every --cycle-ms (Deephaven's default is 1000 ms); Barrage
subscribers ask for their own update interval on top of that.
"""

import argparse
import signal
import threading

parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, default=10000)
parser.add_argument("--psk", default="spike-psk")
parser.add_argument("--heap", default="3g")
parser.add_argument("--cycle-ms", type=int, default=100)
parser.add_argument("--symbols", type=int, default=500)
parser.add_argument("--trade-period", default="PT0.005S")  # 200 trades a second
parser.add_argument("--quote-period", default="PT0.001S")  # 1,000 quotes a second
args = parser.parse_args()

from deephaven_server import Server  # noqa: E402  (the JVM starts here)

server = Server(
    port=args.port,
    jvm_args=[
        f"-Xmx{args.heap}",
        f"-Dauthentication.psk={args.psk}",
        f"-DPeriodicUpdateGraph.targetCycleDurationMillis={args.cycle_ms}",
    ],
)
server.start()

from deephaven import time_table  # noqa: E402  (needs the started server)

N = args.symbols

# A deterministic mid per symbol that drifts, plus noise: enough movement for every
# aggregate to change on every cycle. `Math.random()` is evaluated once per row by `update`.
MID = (
    f"50 + (SymIdx % 97) * 3"
    f" + 2 * Math.sin(epochMillis(Timestamp) / 60000.0 + SymIdx)"
    f" + 0.05 * Math.random()"
)

trades = time_table(args.trade_period).update([
    f"SymIdx = (int) Math.floor(Math.random() * {N})",
    "Sym = String.format(`S%03d`, SymIdx)",
    "Side = Math.random() < 0.5 ? `BUY` : `SELL`",
    "VenueIdx = (int) Math.floor(Math.random() * 4)",
    "Venue = VenueIdx == 0 ? `XNYS` : VenueIdx == 1 ? `XNAS` : VenueIdx == 2 ? `ARCX` : `BATS`",
    "Qty = 100 * (1 + (long) Math.floor(Math.random() * 10))",
    f"Price = {MID}",
]).drop_columns(["SymIdx", "VenueIdx"])

quotes = time_table(args.quote_period).update([
    f"SymIdx = (int) Math.floor(Math.random() * {N})",
    "Sym = String.format(`S%03d`, SymIdx)",
    f"Mid = {MID}",
    "Spread = 0.01 + 0.02 * Math.random()",
    "Bid = Mid - Spread / 2",
    "Ask = Mid + Spread / 2",
]).view(["Timestamp", "Sym", "Bid", "Ask"])

print(f"deephaven-spike: server on :{args.port}, tables trades and quotes ticking", flush=True)

stop = threading.Event()
signal.signal(signal.SIGTERM, lambda *_: stop.set())
signal.signal(signal.SIGINT, lambda *_: stop.set())
stop.wait()
