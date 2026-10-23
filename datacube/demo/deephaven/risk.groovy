// TEMPORARY (docs/DEEPHAVEN_PLAN_2026_10_22.md D0): the demo's reference data and market-data simulator, which the
// Deephaven server runs every time it starts (so a restart re-seeds them). Lite's own modelled publisher replaces the
// reference data (L5): read from its source of record, written through the same modelled tables. The prices tick from
// a time table here; in production they arrive from a feed.

// The world's size and pace, from the server's environment (a test's or a benchmark's; the defaults are the demo's):
//   DEMO_POSITIONS, DEMO_INSTRUMENTS  how many positions and instruments (400 and 40)
//   DEMO_STORM                        "1": every instrument re-priced ten times a second; else one a tenth of a second
env = System.getenv()
positions = Integer.parseInt(env.getOrDefault("DEMO_POSITIONS", "400"))
instruments = Integer.parseInt(env.getOrDefault("DEMO_INSTRUMENTS", "40"))
storm = env.getOrDefault("DEMO_STORM", "0") == "1"

// 40 instruments over four sectors and three currencies
instrument = emptyTable(instruments).update(
        "iid = `I` + i",
        "sector = i % 4 == 0 ? `Tech` : (i % 4 == 1 ? `Energy` : (i % 4 == 2 ? `Banks` : `Retail`))",
        "ccy = i % 3 == 0 ? `USD` : (i % 3 == 1 ? `EUR` : `JPY`)")

// eight books over four desks and two regions, kept in an input table keyed by the book: a test moves one to another
// region while a grid shows them (docs/DEEPHAVEN_PLAN_2026_10_22.md P1: a pivot value appearing on the server)
book = io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable.make(emptyTable(8).update(
        "bid = `B` + i",
        "desk = i % 4 == 0 ? `Rates` : (i % 4 == 1 ? `FX` : (i % 4 == 2 ? `Credit` : `Equities`))",
        "region = i < 4 ? `EMEA` : `AMER`"), "bid")

// 400 positions, each a book's holding of an instrument: position i holds instrument i % 40 in book (i / 40) % 8, so
// every desk holds every sector
position = emptyTable(positions).update(
        "pid = (int) i",
        "book_id = `B` + (((int) (i / " + instruments + ")) % 8)",
        "inst_id = `I` + (i % " + instruments + ")",
        "qty = (long) (100 * (1 + (i % 9)))")

fx = newTable(stringCol("fx_ccy", "USD", "EUR", "JPY"), doubleCol("rate", 1.0d, 1.08d, 0.0067d))

if (storm) {
    // the storm: every instrument re-priced at every tick, the latest tick joined to each one
    price = instrument.view("px_iid = iid", "n = i")
            .naturalJoin(timeTable("PT0.1S").lastBy(), "", "Timestamp")
            .update("px = 100.0 + n + 5.0 * Math.sin((isNull(Timestamp) ? 0L : epochMillis(Timestamp)) / 700.0 + n)")
            .view("px_iid", "px")
} else {
    // the simulator: every instrument priced from the start, then ten re-pricings a second, the latest kept per instrument
    price = merge(
                emptyTable(instruments).view("px_iid = `I` + i", "px = 100.0 + i"),
                timeTable("PT0.1S").view("px_iid = `I` + (ii % " + instruments + ")",
                        "px = 100.0 + (ii % " + instruments + ") + 5.0 * Math.sin(ii / 37.0)"))
            .lastBy("px_iid")
}
