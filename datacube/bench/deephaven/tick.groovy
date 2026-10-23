// THE TICK BENCHMARK'S WORLD (datacube/bench/deephaven/README.md): positions over instruments, books and currencies,
// priced by a feed whose every quote is stamped with the server's clock in the update cycle that computes it (`pxMs`:
// read in the quote's own formula, which that cycle evaluates), so a page measures a tick's latency from the cycle that
// made it to the page seeing it -- not the wait for that cycle, which is the cycle's setting. Sized and paced by the environment the benchmark's
// driver starts the server with:
//   BENCH_POSITIONS, BENCH_INSTRUMENTS  the world's size
//   BENCH_TICK_MS                       how often the feed quotes (the server's update cycle is the driver's too)
//   BENCH_STORM                         "1": every instrument re-quoted each tick; else one instrument a tick
// Its results are the two a grid shows: `agg`, market value by desk and sector (16 rows), and `flat`, every position.

env = System.getenv()
positions = Integer.parseInt(env.getOrDefault("BENCH_POSITIONS", "400"))
instruments = Integer.parseInt(env.getOrDefault("BENCH_INSTRUMENTS", "40"))
tickMs = Integer.parseInt(env.getOrDefault("BENCH_TICK_MS", "100"))
storm = env.getOrDefault("BENCH_STORM", "0") == "1"
period = "PT" + (tickMs / 1000.0) + "S"

instrument = emptyTable(instruments).update(
        "iid = `I` + i",
        "sector = i % 4 == 0 ? `Tech` : (i % 4 == 1 ? `Energy` : (i % 4 == 2 ? `Banks` : `Retail`))",
        "ccy = i % 3 == 0 ? `USD` : (i % 3 == 1 ? `EUR` : `JPY`)")
book = emptyTable(8).update(
        "bid = `B` + i",
        "desk = i % 4 == 0 ? `Rates` : (i % 4 == 1 ? `FX` : (i % 4 == 2 ? `Credit` : `Equities`))",
        "region = i < 4 ? `EMEA` : `AMER`")
position = emptyTable(positions).update(
        "pid = (int) i",
        "book_id = `B` + (((int) (i * 8L / " + positions + ")) % 8)",
        "inst_id = `I` + (i % " + instruments + ")",
        "qty = (long) (100 * (1 + (i % 9)))")
fx = newTable(stringCol("fx_ccy", "USD", "EUR", "JPY"), doubleCol("rate", 1.0d, 1.08d, 0.0067d))

ticker = timeTable(period)
if (storm) {
    // every instrument re-quoted on every tick: the latest tick joined to each one
    price = instrument.view("px_iid = iid", "n = i")
            .naturalJoin(ticker.lastBy(), "", "Timestamp")
            .update("px = 100.0 + n + 5.0 * Math.sin(epochMillis(Timestamp) / 700.0 + n)",
                    "pxMs = isNull(Timestamp) ? 0L : System.currentTimeMillis()")
            .dropColumns("n", "Timestamp")
} else {
    // one instrument a tick, the latest quote of each kept
    price = merge(
                emptyTable(instruments).view("px_iid = `I` + i", "pxMs = 0L", "px = 100.0 + i"),
                ticker.update("px_iid = `I` + (ii % " + instruments + ")",
                        "px = 100.0 + (ii % " + instruments + ") + 5.0 * Math.sin(ii / 37.0)",
                        "pxMs = isNull(Timestamp) ? 0L : System.currentTimeMillis()").view("px_iid", "pxMs", "px"))
            .lastBy("px_iid")
}

flat = position
        .naturalJoin(book, "book_id = bid", "desk, region")
        .naturalJoin(instrument, "inst_id = iid", "sector, ccy")
        .naturalJoin(price, "inst_id = px_iid", "px, pxMs")
        .naturalJoin(fx, "ccy = fx_ccy", "rate")
        .update("mv = qty * px * rate")
agg = flat.aggBy([AggSum("mv"), AggCount("n"), AggMax("pxMs")], "desk", "sector").sort("desk", "sector")
