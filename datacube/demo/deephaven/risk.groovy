// TEMPORARY (docs/DEEPHAVEN_PLAN_2026_10_22.md D0): the demo's reference data and market-data simulator, which the
// Deephaven server runs every time it starts (so a restart re-seeds them). Lite's own modelled publisher replaces the
// reference data (L5): read from its source of record, written through the same modelled tables. The prices tick from
// a time table here; in production they arrive from a feed.

// 40 instruments over four sectors and three currencies
instrument = emptyTable(40).update(
        "iid = `I` + i",
        "sector = i % 4 == 0 ? `Tech` : (i % 4 == 1 ? `Energy` : (i % 4 == 2 ? `Banks` : `Retail`))",
        "ccy = i % 3 == 0 ? `USD` : (i % 3 == 1 ? `EUR` : `JPY`)")

// eight books over four desks and two regions
book = emptyTable(8).update(
        "bid = `B` + i",
        "desk = i % 4 == 0 ? `Rates` : (i % 4 == 1 ? `FX` : (i % 4 == 2 ? `Credit` : `Equities`))",
        "region = i < 4 ? `EMEA` : `AMER`")

// 400 positions, each a book's holding of an instrument: position i holds instrument i % 40 in book (i / 40) % 8, so
// every desk holds every sector
position = emptyTable(400).update(
        "pid = (int) i",
        "book_id = `B` + (((int) (i / 40)) % 8)",
        "inst_id = `I` + (i % 40)",
        "qty = (long) (100 * (1 + (i % 9)))")

fx = newTable(stringCol("fx_ccy", "USD", "EUR", "JPY"), doubleCol("rate", 1.0d, 1.08d, 0.0067d))

// the simulator: every instrument priced from the start, then ten re-pricings a second, the latest kept per instrument
price = merge(
            emptyTable(40).view("px_iid = `I` + i", "px = 100.0 + i"),
            timeTable("PT0.1S").view("px_iid = `I` + (ii % 40)", "px = 100.0 + (ii % 40) + 5.0 * Math.sin(ii / 37.0)"))
        .lastBy("px_iid")
