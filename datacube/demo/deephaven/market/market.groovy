// THE MARKET WORLD (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K2, DH28), which the Deephaven server runs every time it
// starts. BESPOKE (the plan's §3): lite writes no rows into Deephaven yet, so the world's rows are made here -- an
// equity market simulated always, and crypto from a public feed where the server can reach it, simulated where it
// cannot (an enterprise network that blocks it, a place that refuses it), so a person who cannot reach the data still
// sees a market. Its platform homes: the modelled publisher over writes (L5), and a feed declared in the store
// (DH16) that the platform starts.
//
// Its tables are the ONE TRADING MODEL's (DH34: the stress corpus's store, core/src/test/resources/stress/30-store.pure),
// every column it declares, so a page plans the corpus's own classes through stress::AllMapping (market/model.ts):
//   EXCHANGE      the listing venues: their MIC, name, city, zone and hours
//   SECTOR        the eleven GICS sectors and crypto: each one's share of the market's capitalisation, its names
//   INSTRUMENT    every name: its id (INST-<ticker>), ticker, name, asset class (EQUITY, CRYPTO), listing, sector
//   EQUITY        each equity's company, shares out, capitalisation at the prior close, beta, lot
//   MARKET_QUOTE  each name's latest quote: its top of book, and the day so far (open, high, low, last, volume) and
//                 the prior close, as a consolidated quote carries them
//   MARKET_TRADE  every print on the tape, in order: its sequence, time, price, size, aggressor (none off-exchange),
//                 venue, condition (@ regular, I an odd lot)
//   NEWS_STORY    the newswire (K15): each story's headline, time, source, category, weight (FLASH, STORY, BRIEF),
//                 sentiment, tickers, and its name or its sector -- written where the simulator moves a price, so a
//                 headline and its move agree; EDGAR's latest filings among them where asked for (MARKET_FILINGS_AGENT)
// and one of its own, outside the corpus:
//   world         what each part of the world is: simulated, live, connecting -- and why

// Its size and pace, from the server's environment (a test's or the demo's; the defaults are the demo's):
//   MARKET_NAMES     how many equities (500)
//   MARKET_QUOTES    quote updates a second over all equities (8000)
//   MARKET_TRADES    trades a second over all equities (800)
//   MARKET_FEED      "coinbase" (the default): Coinbase Exchange's public feed for crypto, simulated if refused; "off"
//   MARKET_FEED_URL  the feed's address (wss://ws-feed.exchange.coinbase.com); a test points it at nothing
//   MARKET_HISTORY   0 (the default): every trade kept; n: the latest n (a long or stormy run's memory)
//   MARKET_SEED      the simulation's random seed (20261024)
//   MARKET_MORNING   seconds of the session before the world starts (7200): the open that long ago, each equity's
//                    morning on the tape, so a chart has a day to draw (0: the session opens as the world starts)
//   MARKET_MORNING_PRINTS  the most prints the morning writes (400000): a thinner tape of larger prints past it
//   MARKET_NEWS      stories a minute over the market besides each name's jump (12): macro, sector, briefs, crypto
//   MARKET_JUMPS     a name's jumps a trading day (0.5): its news, a move of about 2%, each a story at its instant
//   MARKET_FILINGS_AGENT  a contact ("name email") EDGAR's fair-access policy asks of a client: SEC EDGAR's latest 8-K
//                    filings read each minute; unset (the default), the newsroom files its own names' filings instead
//   MARKET_FILINGS_URL    EDGAR's latest filings' address (its Atom feed of 8-Ks)
//
// Realism, simulated: prices move together (one market factor, one per sector, each name's own, and rare jumps: news);
// a cent tick; spreads by liquidity, a name's activity by its size (the largest names quote and trade most); quotes
// that move when the price leaves them; trades at the bid or the ask (the aggressor's side leaning with the move), some
// at the midpoint off-exchange; round lots mostly, odd lots some; each print moves the price a little.

import groovy.transform.CompileStatic
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.Table
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.impl.BlinkTableTools
import io.deephaven.engine.table.impl.sources.ring.RingTableTools
import io.deephaven.engine.util.TableTools
import io.deephaven.stream.TablePublisher

import java.net.URI
import java.net.http.HttpClient
import java.net.http.WebSocket
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/** One name the world quotes and trades: what it is, and where its price, quote and activity stand. */
@CompileStatic
class Name {
    String sym
    String name
    String sector
    String kind
    int sectorIx
    double prevClose
    long shares
    double tick
    /** the corpus's ids: the instrument's, the equity's (none for a coin), the sector's, the listing exchange's */
    String instrumentId
    String equityId
    String sectorId
    String exchange
    /** daily volatility, beta to the market, and the share of the market's activity it takes */
    double vol
    double beta
    double weight
    double spreadBps
    double sizeMedian
    /** the price now, and the quote last published */
    double mid
    double bid
    double ask
    double bidSize
    double askSize
    boolean quoted
    /** the price's move in the last step, which the aggressor's side leans with */
    double drift
    /** the day so far: its first, highest, lowest and last print, and its volume (no print yet: none) */
    double open = Double.NaN
    double high = Double.NaN
    double low = Double.NaN
    double last = Double.NaN
    double volume = 0d

    /** A print of this name: the day's figures moved by it. */
    void printed(double px, double sz) {
        if (Double.isNaN(open)) open = px
        high = Double.isNaN(high) ? px : Math.max(high, px)
        low = Double.isNaN(low) ? px : Math.min(low, px)
        last = px
        volume += sz
    }
}

/** Rows made since the last publish, by table: a list per column, each the corpus's (30-store.pure's MARKET_QUOTE and
 *  MARKET_TRADE, every column it declares, in its order). */
@CompileStatic
class Rows {
    static final double NULL_DOUBLE = io.deephaven.util.QueryConstants.NULL_DOUBLE

    final List<String> qId = []
    final List<Instant> qTs = []
    final List<Double> qBid = []
    final List<Double> qAsk = []
    final List<Double> qMid = []
    final List<Double> qLast = []
    final List<Double> qVolume = []
    final List<Double> qOpen = []
    final List<Double> qHigh = []
    final List<Double> qLow = []
    final List<Double> qClose = []
    final List<String> qSource = []
    final List<String> qInstrument = []
    final List<Double> qBidSize = []
    final List<Double> qAskSize = []
    final List<String> tId = []
    final List<Integer> tSeq = []
    final List<Instant> tTs = []
    final List<Double> tPx = []
    final List<Double> tSz = []
    final List<String> tSide = []
    final List<String> tVenue = []
    final List<String> tCondition = []
    final List<Boolean> tOff = []
    final List<String> tInstrument = []

    static double orNull(double v) { return Double.isNaN(v) ? NULL_DOUBLE : v }

    /** `n`'s quote now: its top of book, and the day so far as it stands. */
    void quote(Name n, Instant ts, String source) {
        qId << ('MQ-' + n.sym); qTs << ts; qBid << n.bid; qAsk << n.ask; qMid << (n.bid + n.ask) / 2d
        qLast << orNull(n.last); qVolume << n.volume; qOpen << orNull(n.open); qHigh << orNull(n.high)
        qLow << orNull(n.low); qClose << n.prevClose; qSource << source; qInstrument << n.instrumentId
        qBidSize << n.bidSize; qAskSize << n.askSize
    }

    /** A print of `n` on the tape: no aggressor (`side` null) off-exchange; an equity's lot under 100 is odd (I). */
    void trade(long seq, Instant ts, Name n, double px, double sz, String side, String venue, boolean offExchange) {
        tId << ('MT-' + seq); tSeq << (int) seq; tTs << ts; tPx << px; tSz << sz; tSide << side; tVenue << venue
        tCondition << (n.kind == 'EQUITY' && sz < 100d ? 'I' : '@'); tOff << offExchange; tInstrument << n.instrumentId
        n.printed(px, sz)
    }

    Table quotes() {
        int k = qId.size()
        return TableTools.newTable(
                TableTools.stringCol('QUOTE_ID', qId as String[]),
                TableTools.instantCol('QUOTE_TIME', qTs as Instant[]),
                TableTools.doubleCol('BID', qBid as double[]),
                TableTools.doubleCol('ASK', qAsk as double[]),
                TableTools.doubleCol('MID', qMid as double[]),
                TableTools.doubleCol('LAST', qLast as double[]),
                TableTools.doubleCol('VOLUME', qVolume as double[]),
                TableTools.doubleCol('OPEN_PRICE', qOpen as double[]),
                TableTools.doubleCol('HIGH_PRICE', qHigh as double[]),
                TableTools.doubleCol('LOW_PRICE', qLow as double[]),
                TableTools.doubleCol('CLOSE_PRICE', qClose as double[]),
                TableTools.stringCol('SOURCE', qSource as String[]),
                TableTools.col('IS_DELAYED', ([Boolean.FALSE] * k) as Boolean[]),
                TableTools.stringCol('SNAPSHOT_ID', new String[k]),
                TableTools.stringCol('INSTRUMENT_ID', qInstrument as String[]),
                TableTools.doubleCol('BID_SIZE', qBidSize as double[]),
                TableTools.doubleCol('ASK_SIZE', qAskSize as double[]))
    }

    Table trades() {
        return TableTools.newTable(
                TableTools.stringCol('PRINT_ID', tId as String[]),
                TableTools.intCol('SEQUENCE_NUMBER', tSeq as int[]),
                TableTools.instantCol('PRINT_TIME', tTs as Instant[]),
                TableTools.doubleCol('PRICE', tPx as double[]),
                TableTools.doubleCol('QUANTITY', tSz as double[]),
                TableTools.stringCol('AGGRESSOR_SIDE', tSide as String[]),
                TableTools.stringCol('VENUE', tVenue as String[]),
                TableTools.stringCol('TRADE_CONDITION', tCondition as String[]),
                TableTools.col('IS_OFF_EXCHANGE', tOff as Boolean[]),
                TableTools.stringCol('INSTRUMENT_ID', tInstrument as String[]))
    }
}

/** The morning's prints (Simulator.morning), each column an array grown as it fills: hundreds of thousands of rows,
 *  never a boxed value each. */
@CompileStatic
class MorningRows {
    int size = 0
    String[] id
    int[] seq
    Instant[] ts
    double[] px
    double[] sz
    String[] side
    String[] venue
    String[] condition
    Boolean[] off
    String[] instrument

    MorningRows(int capacity) {
        grow(capacity)
    }

    void grow(int capacity) {
        id = id == null ? new String[capacity] : Arrays.copyOf(id, capacity)
        seq = seq == null ? new int[capacity] : Arrays.copyOf(seq, capacity)
        ts = ts == null ? new Instant[capacity] : Arrays.copyOf(ts, capacity)
        px = px == null ? new double[capacity] : Arrays.copyOf(px, capacity)
        sz = sz == null ? new double[capacity] : Arrays.copyOf(sz, capacity)
        side = side == null ? new String[capacity] : Arrays.copyOf(side, capacity)
        venue = venue == null ? new String[capacity] : Arrays.copyOf(venue, capacity)
        condition = condition == null ? new String[capacity] : Arrays.copyOf(condition, capacity)
        off = off == null ? new Boolean[capacity] : Arrays.copyOf(off, capacity)
        instrument = instrument == null ? new String[capacity] : Arrays.copyOf(instrument, capacity)
    }

    /** A print of `n`: `side` 1 a buyer's, 0 a seller's, -1 none (off-exchange); `venue` its index in VENUES. */
    void add(long number, Instant at, Name n, double price, double size, int aggressor, int venueAt) {
        if (this.size == id.length) grow(id.length * 2)
        int i = this.size++
        id[i] = 'MT-' + number
        seq[i] = (int) number
        ts[i] = at
        px[i] = price
        sz[i] = size
        side[i] = aggressor < 0 ? null : (aggressor == 1 ? 'B' : 'S')
        venue[i] = Simulator.VENUES[venueAt]
        condition[i] = size < 100d ? 'I' : '@'
        off[i] = aggressor < 0
        instrument[i] = n.instrumentId
    }

    /** The rows as MARKET_TRADE's columns, in its order. */
    Table table() {
        return TableTools.newTable(
                TableTools.stringCol('PRINT_ID', Arrays.copyOf(id, size)),
                TableTools.intCol('SEQUENCE_NUMBER', Arrays.copyOf(seq, size)),
                TableTools.instantCol('PRINT_TIME', Arrays.copyOf(ts, size)),
                TableTools.doubleCol('PRICE', Arrays.copyOf(px, size)),
                TableTools.doubleCol('QUANTITY', Arrays.copyOf(sz, size)),
                TableTools.stringCol('AGGRESSOR_SIDE', Arrays.copyOf(side, size)),
                TableTools.stringCol('VENUE', Arrays.copyOf(venue, size)),
                TableTools.stringCol('TRADE_CONDITION', Arrays.copyOf(condition, size)),
                TableTools.col('IS_OFF_EXCHANGE', Arrays.copyOf(off, size)),
                TableTools.stringCol('INSTRUMENT_ID', Arrays.copyOf(instrument, size)))
    }
}

/** The simulation: a market factor, one per sector, each name's own, and jumps; quotes and trades at each name's
 *  pace. Stepped by the world's clock, its rows into a Rows. */
@CompileStatic
class Simulator {
    static final double DAY = 23_400d   // seconds in a trading day: a daily volatility's horizon
    /** the lit venues by MIC, and off-exchange (a TRF's print: the FINRA facility) */
    static final String[] VENUES = ['XNAS', 'XNYS', 'ARCX', 'BATS', 'EDGX', 'IEXG', 'FINRA'] as String[]
    static final double[] VENUE_SHARE = [0.30d, 0.20d, 0.10d, 0.15d, 0.08d, 0.04d, 0.13d] as double[]
    final Random random
    final List<Name> names
    final int sectors
    final double quoteRate
    final double tradeRate
    final AtomicLong seq
    /** the newswire (K15): its stories where the prices move, its shocks to them; none, no news */
    Newsroom news
    /** a name's jumps a trading day (its news, a 2% move about): one every two days */
    double jumpsADay = 0.5d

    Simulator(Random random, List<Name> names, int sectors, double quoteRate, double tradeRate, AtomicLong seq) {
        this.random = random
        this.names = names
        this.sectors = sectors
        this.quoteRate = quoteRate
        this.tradeRate = tradeRate
        this.seq = seq
    }

    double gauss() { return random.nextGaussian() }

    /** How many of an event with `mean` expected this step: its floor, and one more by the remainder's chance. */
    int events(double mean) {
        int n = (int) Math.floor(mean)
        return n + (random.nextDouble() < mean - n ? 1 : 0)
    }

    double roundTo(double v, double tick) { return Math.round(v / tick) * tick }

    /** `dt` seconds of the market, `names` those simulated now, their rows into `rows` stamped `now`. */
    void step(double dt, Instant now, Collection<Name> stepped, Rows rows) {
        double root = Math.sqrt(dt / DAY)
        double market = gauss() * 0.010d * root
        double[] sector = new double[sectors]
        for (int k = 0; k < sectors; k++) sector[k] = gauss() * 0.008d * root
        // the newswire's macro and sector stories, their shocks the market's and their sectors'
        if (news != null) market += news.beat(dt, now, sector, false)
        for (Name n : stepped) {
            double r = n.beta * market + 0.6d * sector[n.sectorIx] + gauss() * n.vol * root
            // news: about one a name every two trading days, a 2% move, reported as it happens
            double jump = random.nextDouble() < dt * jumpsADay / DAY ? gauss() * 0.02d : 0d
            r += jump
            n.mid = Math.max(n.tick, n.mid * Math.exp(r))
            n.drift = r
            if (jump != 0d && news != null) news.jump(n, jump, now)
            int quotes = events(quoteRate * n.weight * dt)
            // a quote the price has left is moved, at once
            if (!n.quoted || n.mid <= n.bid || n.mid >= n.ask) quotes = Math.max(quotes, 1)
            for (int q = 0; q < quotes; q++) quote(n, now, rows)
            int trades = events(tradeRate * n.weight * dt)
            for (int t = 0; t < trades; t++) trade(n, now, rows)
        }
    }

    void quote(Name n, Instant now, Rows rows) {
        requote(n)
        rows.quote(n, now, n.kind == 'EQUITY' ? 'SIP' : 'SIM')
    }

    /** `n`'s top of book about its price now: its spread by its liquidity, a size each side. */
    void requote(Name n) {
        long ticks = Math.max(1L, Math.round(n.mid * n.spreadBps / 1e4d / n.tick))
        double bid = Math.floor(n.mid / n.tick - ticks / 2d) * n.tick
        n.bid = roundTo(bid, n.tick)
        n.ask = roundTo(bid + ticks * n.tick, n.tick)
        n.bidSize = size(n)
        n.askSize = size(n)
        n.quoted = true
    }

    /**
     * THE MORNING (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K14): the session from its `open`, `seconds` before the
     * world starts, so a name's chart has a day to draw -- each second its price on the same factors as a beat's (the
     * market's, its sector's, its own, a rare jump), its quote moved where the price leaves it (never published: a
     * quote is the latest), its prints at the day's pace, busiest at the open (the first half of an intraday U). The
     * tape keeps a `thin` share of them, each that much larger, so a minute's volume is a minute's at any thinning and
     * the morning's rows are bounded. Each second's prints are put in time order before they are numbered, so a
     * print's sequence is its place on the tape. The names' days (open, high, low, last, volume) are moved by them.
     * The newswire's morning (K15) is written as it goes, each story at the start of its second.
     */
    MorningRows morning(double seconds, Instant open, double thin) {
        MorningRows out = new MorningRows(Math.max(16, (int) (tradeRate * thin * morningPace(seconds) * 1.2d)))
        List<double[]> second = []
        List<Name> whose = []
        for (int t = 0; t < (int) seconds; t++) {
            double root = Math.sqrt(1d / DAY)
            double market = gauss() * 0.010d * root
            double[] sector = new double[sectors]
            for (int k = 0; k < sectors; k++) sector[k] = gauss() * 0.008d * root
            // the second's stories at its start, so a jump's story precedes the prints it moved
            Instant start = open.plusSeconds(t)
            if (news != null) market += news.beat(1d, start, sector, true)
            double pace = 1d + 2d * Math.exp(-t / 900d)
            second.clear()
            whose.clear()
            for (Name n : names) {
                double r = n.beta * market + 0.6d * sector[n.sectorIx] + gauss() * n.vol * root
                double jump = random.nextDouble() < jumpsADay / DAY ? gauss() * 0.02d : 0d
                r += jump
                n.mid = Math.max(n.tick, n.mid * Math.exp(r))
                n.drift = r
                if (jump != 0d && news != null) news.jump(n, jump, start)
                if (!n.quoted || n.mid <= n.bid || n.mid >= n.ask) requote(n)
                int trades = events(tradeRate * n.weight * pace * thin)
                // the name's prints in this second, in time order
                double[] at = new double[trades]
                for (int k = 0; k < trades; k++) at[k] = random.nextDouble()
                Arrays.sort(at)
                for (int k = 0; k < trades; k++) {
                    double[] print = morningPrint(n, thin)
                    second << ([at[k], print[0], print[1], print[2], print[3]] as double[])
                    whose << n
                }
            }
            // the second's prints across the names in time order, numbered so
            Integer[] order = (0..<second.size()).toArray(new Integer[0]) as Integer[]
            Arrays.sort(order, { Integer a, Integer b -> Double.compare(second[a][0], second[b][0]) } as Comparator<Integer>)
            for (Integer i : order) {
                double[] p = second[i]
                Name n = whose[i]
                Instant ts = open.plusNanos((long) ((t + p[0]) * 1e9d))
                out.add(seq.incrementAndGet(), ts, n, p[1], p[2], (int) p[3], (int) p[4])
                n.printed(p[1], p[2])
            }
        }
        return out
    }

    /** The prints a morning of `seconds` would have, each name at its weight, before any thinning. */
    double morningPace(double seconds) {
        // the integral of the day's pace (1 + 2 e^(-t/900)) over the morning, at the world's trade rate
        return seconds + 1800d * (1d - Math.exp(-seconds / 900d))
    }

    /** One of `n`'s morning prints, `1 / thin` times a print's size: its price, size, side (1 buy, 0 sell, -1 none)
     *  and venue's index; the price moved its way. */
    double[] morningPrint(Name n, double thin) {
        boolean buy = random.nextDouble() < 0.5d + 0.25d * Math.tanh(n.drift * 2000)
        double u = random.nextDouble()
        int venue = VENUES.length - 1
        for (int v = 0; v < VENUES.length; v++) {
            u -= VENUE_SHARE[v]
            if (u < 0) { venue = v; break }
        }
        boolean off = VENUES[venue] == 'FINRA'
        double px = off ? roundTo((n.bid + n.ask) / 2, n.tick / 2) : (buy ? n.ask : n.bid)
        double sz = random.nextDouble() < 0.10d ? (double) (1 + random.nextInt(99))
                : Math.max(100d, Math.round(n.sizeMedian * 0.5d * Math.exp(gauss() * 0.9d) / 100d) * 100d)
        sz = sz < 100d ? sz : Math.max(100d, Math.round(sz / thin / 100d) * 100d)
        n.mid *= Math.exp((buy ? 1 : -1) * 0.00002d * Math.sqrt(Math.max(1d, sz / Math.max(1d, n.sizeMedian))))
        return [px, sz, off ? -1d : (buy ? 1d : 0d), (double) venue] as double[]
    }

    /** A size at the quote: an equity's in round lots, a coin's its median's notional about. */
    double size(Name n) {
        double s = n.sizeMedian * Math.exp(gauss() * 0.7d)
        return n.kind == 'EQUITY' ? Math.max(100d, Math.round(s / 100d) * 100d) : Math.round(s * 1e6d) / 1e6d
    }

    void trade(Name n, Instant now, Rows rows) {
        boolean buy = random.nextDouble() < 0.5d + 0.25d * Math.tanh(n.drift * 2000)
        double u = random.nextDouble()
        String venue = VENUES[VENUES.length - 1]
        for (int v = 0; v < VENUES.length; v++) {
            u -= VENUE_SHARE[v]
            if (u < 0) { venue = VENUES[v]; break }
        }
        // off-exchange at the midpoint (to half a tick), with no aggressor; else at the side the aggressor takes
        boolean off = n.kind == 'EQUITY' && venue == 'FINRA'
        double px = off ? roundTo((n.bid + n.ask) / 2, n.tick / 2) : (buy ? n.ask : n.bid)
        double sz
        if (n.kind == 'EQUITY') {
            sz = random.nextDouble() < 0.10d ? (double) (1 + random.nextInt(99))
                    : Math.max(100d, Math.round(n.sizeMedian * 0.5d * Math.exp(gauss() * 0.9d) / 100d) * 100d)
        } else {
            sz = Math.round(n.sizeMedian * 0.3d * Math.exp(gauss() * 1.1d) * 1e6d) / 1e6d
        }
        rows.trade(seq.incrementAndGet(), now, n, px, sz, off ? null : (buy ? 'B' : 'S'),
                n.kind == 'EQUITY' ? venue : 'SIM', off)
        // a print moves the price a little its way
        n.mid *= Math.exp((buy ? 1 : -1) * 0.00002d * Math.sqrt(Math.max(1d, sz / Math.max(1d, n.sizeMedian))))
    }
}

/**
 * THE NEWSWIRE (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K15): the stories a terminal's feed shows, each the world's
 * own, written where the simulator moves a price so that a headline and its move agree. A name's jump is reported at
 * its instant (its category by chance, its wording by its sign, its size in the headline, a FLASH from 3%); a macro
 * story moves the whole market and a sector's story its sector, each by the shock its headline names; a brief (an
 * analyst's note, a filing, an appointment, a dividend) nudges its name its way or not at all; crypto's briefs say where
 * a coin stands against its prior close, live or simulated. The names are the world's (made up), the brokers, funds and
 * wires too; no story is about a real company. Its own random, so the prices' draws are the simulation's as they were.
 * BESPOKE (the plan's §3), like the rest of the world.
 */
@CompileStatic
class Newsroom {
    static final String[] BROKERS = ['Halberd Securities', 'Northgate Capital', 'Ridgeway & Co', 'Pinecrest Research',
                                     'Calder Partners', 'Ashby Markets'] as String[]
    static final String[] FUNDS = ['Larkspur Capital', 'Greyfield Partners', 'Ostrander Value Fund', 'Kestrel Holdings'] as String[]
    static final String[] FILING_ITEMS = ['results of operations', 'change in directors or officers',
                                          'entry into a material agreement', 'Regulation FD disclosure', 'other events'] as String[]
    /** each sector's name on the wire, and why it moves: up, then down (Simulator's sectors, in the world's order) */
    static final String[] GROUPS = ['Tech stocks', 'Drugmakers', 'Banks', 'Retailers', 'Media and telecom shares',
                                    'Industrials', 'Consumer staples', 'Energy shares', 'Utilities', 'Real estate shares',
                                    'Miners'] as String[]
    static final List<List<String>> RALLY = [
            ['AI spending forecasts are raised', 'the chip demand outlook improves'],
            ['a drug pricing plan is softened', 'the regulator speeds up approvals'],
            ['yields climb and the curve steepens', 'stress tests clear the way for buybacks'],
            ['consumer spending beats forecasts', 'the holiday sales outlook is raised'],
            ['advertising spending rebounds', 'spectrum auction rules are eased'],
            ['factory orders rise more than expected', 'an infrastructure bill clears its vote'],
            ['investors seek defensive shares', 'input costs fall'],
            ['oil rises on supply cuts', 'gas climbs on a cold forecast'],
            ['bond yields fall', 'power demand forecasts rise on data centres'],
            ['mortgage rates fall', 'office occupancy rises'],
            ['metal prices rally on China demand', 'copper hits a six-month high']]
    static final List<List<String>> SLIDE = [
            ['export curbs on chips are widened', 'a bellwether\'s outlook disappoints'],
            ['a drug pricing proposal is tightened', 'insurers flag higher costs'],
            ['commercial property loan worries return', 'a regional lender cuts its outlook'],
            ['consumer confidence falls', 'tariffs on imports are raised'],
            ['the advertising outlook is cut', 'subscriber growth slows'],
            ['factory activity contracts', 'freight volumes fall'],
            ['input costs rise', 'a large grocer warns on margins'],
            ['oil falls on demand worries', 'inventories rise more than forecast'],
            ['bond yields rise', 'a regulator rejects a rate increase'],
            ['mortgage rates climb', 'office vacancies rise'],
            ['metal prices fall on weak China data', 'steel tariffs are lifted']]
    static final List<List<String>> PRODUCTS = [
            ['an AI accelerator', 'a cloud platform', 'a new chip line'], ['a cancer drug', 'a diabetes treatment', 'a heart device'],
            ['a digital bank', 'a payments app', 'a wealth platform'], ['an electric SUV', 'a holiday collection', 'a store format'],
            ['a streaming tier', 'a 5G network', 'an ad platform'], ['a jet engine', 'a freight network', 'a robotics line'],
            ['a snack range', 'a beverage line', 'a store brand'], ['an offshore field', 'an LNG project', 'a refinery expansion'],
            ['a grid upgrade', 'a solar farm', 'a new rate plan'], ['an office tower', 'a logistics park', 'a data centre campus'],
            ['a copper mine', 'a lithium plant', 'a packaging line']]

    final Random random
    final List<Name> equities
    final List<Name> coins
    final List<String> sectorIds
    /** each sector's largest names, the tickers a sector's story names */
    final List<List<Name>> leaders
    /** the equities' running weights, for a brief's name drawn by its share of the market's activity */
    final double[] cumulative
    /** stories a minute over the market, besides the jumps (the simulator's): macro, sector, briefs and crypto */
    final double rate
    /** EDGAR's filings live: the simulated ones stop */
    volatile boolean filingsLive = false
    long count = 0L
    Instant lastAt = Instant.EPOCH
    /** the stories since the last publish: NEWS_STORY's columns (30-store.pure), in its order */
    final List<String> id = []
    final List<Instant> ts = []
    final List<String> headline = []
    final List<String> body = []
    final List<String> source = []
    final List<String> category = []
    final List<String> urgency = []
    final List<Double> sentiment = []
    final List<String> tickers = []
    final List<String> region = []
    final List<String> instrument = []
    final List<String> sector = []

    Newsroom(Random random, List<Name> equities, List<Name> coins, List<String> sectorIds, double rate) {
        this.random = random
        this.equities = equities
        this.coins = coins
        this.sectorIds = sectorIds
        this.rate = rate
        leaders = (0..<sectorIds.size()).collect { int k ->
            equities.findAll { Name e -> e.sectorIx == k }.sort { Name e -> -e.weight }.take(3) } as List<List<Name>>
        cumulative = new double[equities.size()]
        double sum = 0d
        for (int i = 0; i < equities.size(); i++) { sum += equities[i].weight; cumulative[i] = sum }
    }

    String pick(String[] xs) { return xs[random.nextInt(xs.length)] }
    String pick(List<String> xs) { return xs[random.nextInt(xs.size())] }
    /** a move's size as a headline writes it */
    static String pct(double r) { return String.format('%.1f%%', Math.abs(Math.expm1(r) * 100d)) }
    static String price(Name n) { return String.format(n.mid >= 10d ? '$%,.2f' : '$%,.4f', n.mid) }
    /** the wire's score of a move: its share of `scale`, to a twentieth, within -1 and 1 */
    static double score(double r, double scale) { return Math.round(Math.max(-1d, Math.min(1d, r / scale)) * 20d) / 20d }
    String target(Name n, boolean up) {
        return String.format('$%.0f', n.mid * (up ? 1.12d + 0.2d * random.nextDouble() : 0.88d - 0.15d * random.nextDouble()))
    }

    /** A story at `at`, kept in time order (two in one instant a millisecond apart, so its id is its place). */
    void story(Instant at, String head, String text, String src, String cat, String urg, double sent, String syms,
               String reg, Name n, String sectorId) {
        Instant when = at.isAfter(lastAt) ? at : lastAt.plusMillis(1)
        lastAt = when
        id << String.format('NS-%07d', ++count); ts << when; headline << head; body << text; source << src
        category << cat; urgency << urg; sentiment << (Double.isNaN(sent) ? Rows.NULL_DOUBLE : sent); tickers << syms
        region << reg; instrument << n?.instrumentId; sector << (n != null ? n.sectorId : sectorId)
    }

    /** One step of `dt` seconds at `at`: the market's shock a macro story names (0 when none), a sector's added to its
     *  factor in `factors`, the briefs and (not in the morning, crypto's day being the feed's) a coin's brief. */
    double beat(double dt, Instant at, double[] factors, boolean morning) {
        double perStory = dt * rate / 60d / 12d
        double market = 0d
        if (random.nextDouble() < perStory) market = macro(at)
        if (random.nextDouble() < 1.5d * perStory) {
            int k = random.nextInt(GROUPS.length)
            factors[k] += industry(k, at)
        }
        double briefs = 9d * perStory
        int b = (int) Math.floor(briefs) + (random.nextDouble() < briefs - Math.floor(briefs) ? 1 : 0)
        for (int i = 0; i < b; i++) brief(at)
        if (!morning && !coins.isEmpty() && random.nextDouble() < 0.5d * perStory) crypto(at)
        return market
    }

    /** A macro headline and its shock to the market (a log move), its sign the headline's. */
    double macro(Instant at) {
        double size = 0.0012d + 0.004d * random.nextDouble() * random.nextDouble()
        boolean up = random.nextBoolean()
        String head
        String reg = 'Americas'
        switch (random.nextInt(7)) {
            case 0:
                head = up ? String.format('US jobless claims fall to %dK, fewer than forecast', 200 + random.nextInt(20))
                        : String.format('US jobless claims rise to %dK, the most in two months', 230 + random.nextInt(20))
                break
            case 1:
                head = String.format(up ? 'US inflation cools to %.1f%% annual pace, below estimates'
                        : 'US inflation runs hotter than forecast at %.1f%% annual pace', 2.4d + random.nextDouble())
                break
            case 2:
                head = up ? 'Fed official says rate cuts could come sooner than markets expect'
                        : 'Fed minutes show officials wary of cutting rates too soon'
                break
            case 3:
                head = String.format(up ? 'Treasury yields slide after soft retail sales; 10-year at %.2f%%'
                        : 'US 10-year yield climbs to %.2f%%, highest in a month', 3.9d + random.nextDouble() * 0.8d)
                break
            case 4:
                head = String.format(up ? 'Oil falls %.1f%% as supply concerns ease' : 'Oil jumps %.1f%% after a pipeline outage',
                        1.5d + 2.5d * random.nextDouble())
                break
            case 5:
                head = up ? 'ECB signals a rate cut at its next meeting' : 'ECB holds rates and pushes back on cut bets'
                reg = 'EMEA'
                break
            default:
                head = up ? 'China unveils stimulus for property and consumer spending'
                        : 'China factory activity contracts for a third month'
                reg = 'APAC'
        }
        double shock = up ? size : -size
        String text = String.format('Stocks %s broadly on the headline, the market about %s %s.', up ? 'rise' : 'fall',
                pct(shock), up ? 'higher' : 'lower')
        story(at, head, text, 'Macro Desk', 'Macro', size >= 0.004d ? 'FLASH' : 'STORY', score(shock, 0.005d), null, reg,
                null, null)
        return shock
    }

    /** A sector's story and its shock to the sector's factor (a name takes 0.6 of it), its sign the headline's. */
    double industry(int k, Instant at) {
        double size = 0.003d + 0.012d * random.nextDouble() * random.nextDouble()
        boolean up = random.nextBoolean()
        double move = 0.6d * size * (up ? 1 : -1)
        String head = GROUPS[k] + (up ? ' rally as ' : ' slide as ') + pick(up ? RALLY[k] : SLIDE[k])
        List<Name> lead = leaders[k]
        String text = String.format('The sector moves about %s %s; %s among the most active.', pct(move),
                up ? 'higher' : 'lower', lead.collect { Name e -> e.name + ' (' + e.sym + ')' }.join(', '))
        story(at, head, text, 'Market Wire', 'Industry', Math.abs(move) >= 0.006d ? 'FLASH' : 'STORY', score(move, 0.008d),
                lead.collect { Name e -> e.sym }.join(' ') ?: null, 'Americas', null, sectorIds[k])
        return size * (up ? 1 : -1)
    }

    /** A name's jump, reported at its instant, `n.mid` already moved: a story its size, a brief when it is small. */
    void jump(Name n, double r, Instant at) {
        boolean up = r > 0
        if (n.kind == 'CRYPTO') {
            story(at, String.format('%s %s %s as %s', n.name, up ? 'jumps' : 'slides', pct(r),
                    up ? 'fund inflows accelerate' : 'exchange outflows rise'),
                    String.format('%s trades at %s.', n.sym, price(n)), 'Crypto Desk', 'Crypto',
                    Math.abs(r) >= 0.03d ? 'FLASH' : 'STORY', score(r, 0.05d), n.sym, null, n, null)
            return
        }
        if (Math.abs(r) < 0.005d) {
            String b = pick(BROKERS)
            story(at, String.format('%s %s %s price target to %s', b, up ? 'raises' : 'cuts', n.name, target(n, up)),
                    String.format('%s (%s) at %s.', n.name, n.sym, price(n)), 'Street Desk', 'Analyst', 'BRIEF',
                    score(r, 0.04d), n.sym, 'Americas', n, null)
            return
        }
        String p = pct(r)
        String[] products = PRODUCTS[n.sectorIx] as String[]
        String cat
        String head
        switch (random.nextInt(8)) {
            case 0:
                cat = 'Earnings'
                head = up ? String.format('%s quarterly profit beats estimates; shares up %s', n.name, p)
                        : String.format('%s misses quarterly estimates as margins narrow; shares down %s', n.name, p)
                break
            case 1:
                cat = 'Guidance'
                head = up ? n.name + ' raises its full-year outlook on strong demand'
                        : n.name + ' cuts its full-year guidance, citing weaker orders'
                break
            case 2:
                cat = 'M&A'
                head = up ? String.format('%s jumps %s on a report of a takeover approach', n.name, p)
                        : String.format('%s falls %s after agreeing a costly acquisition', n.name, p)
                break
            case 3:
                cat = 'Analyst'
                String b = pick(BROKERS)
                head = String.format(up ? '%s upgrades %s to buy, target %s' : '%s downgrades %s to sell, target %s', b,
                        n.name, target(n, up))
                break
            case 4:
                cat = 'Regulatory'
                head = up ? n.name + ' wins regulatory approval for ' + pick(products)
                        : 'Regulator opens a probe into ' + n.name + ' -- filing'
                break
            case 5:
                cat = 'Management'
                head = up ? String.format('%s names a new chief executive; shares rise %s', n.name, p)
                        : n.name + '\'s chief executive to step down'
                break
            default:
                cat = 'Product'
                head = up ? String.format('%s unveils %s; shares climb %s', n.name, pick(products), p)
                        : String.format('%s recalls %s; shares drop %s', n.name, pick(products), p)
        }
        story(at, head, String.format('%s (%s) shares %s %s to %s.', n.name, n.sym, up ? 'rose' : 'fell', p, price(n)),
                'Market Wire', cat, Math.abs(r) >= 0.03d ? 'FLASH' : 'STORY', score(r, 0.04d), n.sym, 'Americas', n, null)
    }

    /** A brief about a name drawn by its share of the market's activity: its nudge, a little its way, if any. */
    void brief(Instant at) {
        int i = Arrays.binarySearch(cumulative, random.nextDouble() * cumulative[cumulative.length - 1])
        Name n = equities[i < 0 ? Math.min(-i - 1, equities.size() - 1) : i]
        double nudge = 0d
        String head
        String cat
        String src = 'Market Wire'
        switch (random.nextInt(filingsLive ? 6 : 7)) {
            case 0:
                nudge = (random.nextBoolean() ? 1 : -1) * (0.002d + 0.003d * random.nextDouble())
                head = String.format('%s %s %s price target to %s', pick(BROKERS), nudge > 0 ? 'raises' : 'cuts', n.name,
                        target(n, nudge > 0))
                cat = 'Analyst'
                src = 'Street Desk'
                break
            case 1:
                int rating = random.nextInt(3)
                nudge = (rating - 1) * 0.002d
                head = String.format('%s starts %s at %s', pick(BROKERS), n.name, ['sell', 'hold', 'buy'][rating])
                cat = 'Analyst'
                src = 'Street Desk'
                break
            case 2:
                nudge = 0.001d + 0.002d * random.nextDouble()
                head = String.format('%s discloses %.1f%% stake in %s -- filing', pick(FUNDS), 5d + 4d * random.nextDouble(), n.name)
                cat = 'Filing'
                src = 'Filings'
                break
            case 3:
                head = n.name + (random.nextBoolean() ? ' appoints a new chief financial officer' : ' adds two directors to its board')
                cat = 'Management'
                break
            case 4:
                head = String.format('%s declares a quarterly dividend of $%.2f a share', n.name, n.mid * (0.002d + 0.006d * random.nextDouble()))
                cat = 'Dividend'
                break
            case 5:
                nudge = 0.001d + 0.002d * random.nextDouble()
                head = String.format('%s authorises a $%.1fbn share buyback', n.name, Math.max(0.1d, n.shares * n.mid * 0.02d / 1e9d))
                cat = 'Management'
                break
            default:
                head = String.format('%s files 8-K: %s', n.name, pick(FILING_ITEMS))
                cat = 'Filing'
                src = 'Filings'
        }
        if (nudge != 0d) n.mid = Math.max(n.tick, n.mid * Math.exp(nudge))
        story(at, head, String.format('%s (%s) at %s.', n.name, n.sym, price(n)), src, cat, 'BRIEF',
                nudge == 0d ? Double.NaN : score(nudge, 0.01d), n.sym, 'Americas', n, null)
    }

    /** Where a coin stands against its prior close, from its price now: the feed's when live, else the simulator's. */
    void crypto(Instant at) {
        Name n = coins[Math.min(coins.size() - 1, (int) Math.floor(Math.pow(random.nextDouble(), 2) * coins.size()))]
        double r = Math.log(n.mid / n.prevClose)
        story(at, String.format('%s at %s, %s %s from its prior close', n.name, price(n), r >= 0 ? 'up' : 'down', pct(r)),
                null, 'Crypto Desk', 'Crypto', 'BRIEF', score(r, 0.05d), n.sym, null, n, null)
    }

    /** A filing from EDGAR (a real company's, a public record), at its own time -- not moved after the stories before
     *  it, so it can sit below them in a feed's newest-first order: EDGAR's wire carries no ticker. */
    void filing(Instant at, String form, String company, String items) {
        id << String.format('NS-%07d', ++count); ts << at; headline << (form + ': ' + company); body << items
        source << 'SEC EDGAR'; category << 'Filing'; urgency << 'BRIEF'; sentiment << Rows.NULL_DOUBLE; tickers << null
        region << 'Americas'; instrument << null; sector << null
    }

    boolean pending() { return !id.isEmpty() }

    /** The stories so far as NEWS_STORY's rows, in its order; the newsroom's buffer emptied. */
    Table table() {
        Table t = TableTools.newTable(
                TableTools.stringCol('STORY_ID', id as String[]),
                TableTools.instantCol('PUBLISH_TIME', ts as Instant[]),
                TableTools.stringCol('HEADLINE', headline as String[]),
                TableTools.stringCol('BODY', body as String[]),
                TableTools.stringCol('SOURCE', source as String[]),
                TableTools.stringCol('CATEGORY', category as String[]),
                TableTools.stringCol('URGENCY', urgency as String[]),
                TableTools.doubleCol('SENTIMENT', sentiment as double[]),
                TableTools.stringCol('TICKERS', tickers as String[]),
                TableTools.stringCol('REGION', region as String[]),
                TableTools.stringCol('INSTRUMENT_ID', instrument as String[]),
                TableTools.stringCol('SECTOR_ID', sector as String[]))
        [id, headline, body, source, category, urgency, tickers, region, instrument, sector].each { ((List) it).clear() }
        ts.clear()
        sentiment.clear()
        return t
    }
}

/**
 * CRYPTO FROM COINBASE EXCHANGE'S PUBLIC FEED (DH28): its ticker channel as quotes, its matches as trades (the
 * aggressor the maker's other side). Live from the first message to a close or an error; the world simulates crypto
 * whenever it is not, from the last price it had, and asks again a minute later.
 */
@CompileStatic
class CoinbaseFeed implements WebSocket.Listener {
    final URI url
    final List<String> products
    final ConcurrentLinkedQueue<Map> messages = new ConcurrentLinkedQueue<>()
    final StringBuilder partial = new StringBuilder()
    final com.fasterxml.jackson.databind.ObjectMapper json = new com.fasterxml.jackson.databind.ObjectMapper()
    volatile String state = 'connecting'
    volatile String detail = ''
    volatile long lastMessageMs = 0L
    volatile WebSocket socket

    CoinbaseFeed(URI url, List<String> products) {
        this.url = url
        this.products = products
    }

    void connect() {
        state = 'connecting'
        detail = url.toString()
        try {
            HttpClient client = HttpClient.newBuilder().proxy(java.net.ProxySelector.getDefault())
                    .connectTimeout(Duration.ofSeconds(5)).build()
            socket = client.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(5)).buildAsync(url, this)
                    .get(8, TimeUnit.SECONDS)
            String ids = products.collect { '"' + it + '"' }.join(',')
            socket.sendText('{"type":"subscribe","product_ids":[' + ids + '],"channels":["ticker","matches"]}', true)
        } catch (Throwable e) {
            refused(e.toString())
        }
    }

    void refused(String why) {
        state = 'refused'
        detail = why.length() > 200 ? why.substring(0, 200) : why
    }

    @Override
    CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
        partial.append(data)
        if (last) {
            try {
                Map m = json.readValue(partial.toString(), Map)
                if (m.get('type') == 'ticker' || m.get('type') == 'match' || m.get('type') == 'last_match') {
                    messages.add(m)
                    lastMessageMs = System.currentTimeMillis()
                    state = 'live'
                    detail = 'Coinbase Exchange'
                } else if (m.get('type') == 'error') {
                    refused('the feed said: ' + m.get('message') + ' ' + m.get('reason'))
                }
            } catch (Throwable e) {
                refused('unreadable: ' + e)
            }
            partial.setLength(0)
        }
        ws.request(1)
        return null
    }

    @Override
    void onError(WebSocket ws, Throwable error) { refused(error.toString()) }

    @Override
    CompletionStage<?> onClose(WebSocket ws, int code, String reason) {
        refused('closed ' + code + ' ' + reason)
        return null
    }
}

/**
 * FILINGS FROM SEC EDGAR (K15, DH28): its latest 8-K filings, a public record, polled each minute, each a brief on the
 * newswire at its own time. EDGAR's fair-access policy asks every client for a contact in its User-Agent, so it is
 * asked only with the person's own (MARKET_FILINGS_AGENT, "name email"); without one, or refused, the newsroom files
 * its own names' filings in its place.
 */
@CompileStatic
class EdgarFeed {
    static final java.util.regex.Pattern ENTRY = java.util.regex.Pattern.compile('(?s)<entry>(.*?)</entry>')
    static final java.util.regex.Pattern TITLE = java.util.regex.Pattern.compile('(?s)<title>(.*?)</title>')
    static final java.util.regex.Pattern UPDATED = java.util.regex.Pattern.compile('<updated>([^<]+)</updated>')
    static final java.util.regex.Pattern ID = java.util.regex.Pattern.compile('<id>([^<]+)</id>')
    static final java.util.regex.Pattern SUMMARY = java.util.regex.Pattern.compile('(?s)<summary[^>]*>(.*?)</summary>')
    /** "8-K - Company, Inc. (0001589149) (Filer)": the form and the company */
    static final java.util.regex.Pattern PARTS = java.util.regex.Pattern.compile('^(\\S+) - (.*?) \\(\\d+\\)')
    static final java.util.regex.Pattern ITEM = java.util.regex.Pattern.compile('Item (\\d+\\.\\d+): ([^<\\n]+)')
    final URI url
    final String agent
    /** the filings read and not yet filed: [its time, its form, its company, its items] */
    final ConcurrentLinkedQueue<String[]> filings = new ConcurrentLinkedQueue<>()
    final Set<String> seen = new HashSet<>()
    volatile String state = 'connecting'
    volatile String detail = ''

    EdgarFeed(URI url, String agent) {
        this.url = url
        this.agent = agent
        this.detail = url.toString()
    }

    static String first(java.util.regex.Pattern p, String text) {
        java.util.regex.Matcher m = p.matcher(text)
        return m.find() ? m.group(1).trim() : null
    }

    static String unescape(String s) {
        return s == null ? null : s.replace('&lt;', '<').replace('&gt;', '>').replace('&quot;', '"').replace('&#39;', "'")
                .replace('&amp;', '&')
    }

    void poll() {
        try {
            HttpClient client = HttpClient.newBuilder().proxy(java.net.ProxySelector.getDefault())
                    .connectTimeout(Duration.ofSeconds(5)).build()
            java.net.http.HttpRequest ask = java.net.http.HttpRequest.newBuilder(url).timeout(Duration.ofSeconds(15))
                    .header('User-Agent', agent).header('Accept', 'application/atom+xml').GET().build()
            java.net.http.HttpResponse<String> got = client.send(ask, java.net.http.HttpResponse.BodyHandlers.ofString())
            if (got.statusCode() != 200) {
                refused('EDGAR answered ' + got.statusCode())
                return
            }
            read(got.body())
            state = 'live'
            detail = 'SEC EDGAR\'s latest 8-K filings, read each minute'
        } catch (Throwable e) {
            refused(e.toString())
        }
    }

    /** Each entry not seen before, oldest first, so their stories are filed in their order. */
    void read(String atom) {
        List<String[]> found = []
        java.util.regex.Matcher m = ENTRY.matcher(atom)
        while (m.find()) {
            String e = m.group(1)
            String accession = first(ID, e)
            String title = unescape(first(TITLE, e))
            String updated = first(UPDATED, e)
            if (accession == null || title == null || updated == null || !seen.add(accession)) continue
            java.util.regex.Matcher t = PARTS.matcher(title)
            if (!t.find()) continue
            List<String> items = []
            java.util.regex.Matcher i = ITEM.matcher(unescape(first(SUMMARY, e)) ?: '')
            while (i.find()) items << ('Item ' + i.group(1) + ': ' + i.group(2).trim())
            found << ([java.time.OffsetDateTime.parse(updated).toInstant().toString(), t.group(1), t.group(2),
                       items.join('; ')] as String[])
        }
        for (int k = found.size() - 1; k >= 0; k--) filings.add(found[k])
    }

    void refused(String why) {
        state = 'refused'
        detail = why.length() > 200 ? why.substring(0, 200) : why
    }
}

/** The world: its names, the simulator, the feed, and the clock that steps them and publishes their rows. */
@CompileStatic
class World {
    final Map<String, Name> bySym = new LinkedHashMap<>()
    final List<Name> equities = []
    final List<Name> coins = []
    final Simulator simulator
    final CoinbaseFeed feed
    final TablePublisher quotes
    final TablePublisher trades
    final TablePublisher world
    final AtomicLong seq
    final String equitiesDetail
    /** the newswire (K15): its stories' publisher, its newsroom, and EDGAR's filings where asked for (else none) */
    TablePublisher news
    Newsroom newsroom
    EdgarFeed filings
    long lastStepNanos = System.nanoTime()
    long retryAtMs = 0L
    String said = ''
    String newsDetail = 'none'

    World(Simulator simulator, CoinbaseFeed feed, TablePublisher quotes, TablePublisher trades, TablePublisher world,
          AtomicLong seq, String equitiesDetail) {
        this.simulator = simulator
        this.feed = feed
        this.quotes = quotes
        this.trades = trades
        this.world = world
        this.seq = seq
        this.equitiesDetail = equitiesDetail
    }

    boolean cryptoLive() {
        return feed != null && feed.state == 'live' && System.currentTimeMillis() - feed.lastMessageMs < 15_000L
    }

    /** One beat of the clock: the simulation's step, the feed's messages, the rows published, the world said. */
    void beat() {
        try {
            long t = System.nanoTime()
            double dt = Math.min(1.0d, (t - lastStepNanos) / 1e9d)
            lastStepNanos = t
            Instant now = Instant.now()
            Rows rows = new Rows()
            boolean live = cryptoLive()
            simulator.step(dt, now, live ? equities : (equities + coins), rows)
            if (feed != null) {
                drain(rows, live)
                if (!live && feed.state != 'connecting' && System.currentTimeMillis() >= retryAtMs) {
                    // asked again a minute after it was refused, or went quiet
                    retryAtMs = System.currentTimeMillis() + 60_000L
                    Thread.start { feed.connect() }
                }
            }
            if (!rows.qId.isEmpty()) quotes.add(rows.quotes())
            if (!rows.tId.isEmpty()) trades.add(rows.trades())
            if (newsroom != null) {
                newsroom.filingsLive = filings != null && filings.state == 'live'
                for (String[] f; filings != null && (f = filings.filings.poll()) != null; ) {
                    newsroom.filing(Instant.parse(f[0]), f[1], f[2], f[3])
                }
                if (newsroom.pending()) news.add(newsroom.table())
            }
            say(live)
        } catch (Throwable e) {
            System.err.println('the market world\'s beat failed: ' + e)
            e.printStackTrace()
        }
    }

    /** The feed's messages as rows; dropped while crypto is simulated, so one source makes its rows at a time. */
    void drain(Rows rows, boolean live) {
        for (Map m; (m = feed.messages.poll()) != null; ) {
            if (!live) continue
            Name n = bySym.get((String) m.get('product_id'))
            if (n == null) continue
            Instant ts = m.get('time') != null ? Instant.parse((String) m.get('time')) : Instant.now()
            if (m.get('type') == 'ticker') {
                if (m.get('best_bid') == null || m.get('best_ask') == null) continue
                n.bid = Double.parseDouble((String) m.get('best_bid'))
                n.ask = Double.parseDouble((String) m.get('best_ask'))
                n.bidSize = Double.parseDouble((String) m.get('best_bid_size'))
                n.askSize = Double.parseDouble((String) m.get('best_ask_size'))
                n.mid = (n.bid + n.ask) / 2
                n.quoted = true
                rows.quote(n, ts, 'CBX')
            } else {
                double px = Double.parseDouble((String) m.get('price'))
                // the side the feed names is the maker's: the aggressor took the other
                String side = m.get('side') == 'sell' ? 'B' : 'S'
                rows.trade(seq.incrementAndGet(), ts, n, px, Double.parseDouble((String) m.get('size')), side, 'CBX', false)
                n.mid = px
            }
        }
    }

    void say(boolean live) {
        String crypto = feed == null ? 'simulated|the feed is off (MARKET_FEED=off)'
                : live ? 'live|' + feed.detail
                : feed.state == 'connecting' ? 'simulated|connecting to ' + feed.detail
                : 'simulated|the feed was refused: ' + feed.detail
        String filed = filings == null
                ? 'simulated|EDGAR asks for a contact in its User-Agent: MARKET_FILINGS_AGENT ("name email") reads its filings'
                : filings.state == 'live' ? 'live|' + filings.detail
                : filings.state == 'connecting' ? 'simulated|asking ' + filings.detail
                : 'simulated|EDGAR was refused: ' + filings.detail
        String text = crypto + '\n' + filed
        if (text == said) return
        said = text
        String[] c = crypto.split('\\|', 2)
        String[] f = filed.split('\\|', 2)
        // said in the server's log too, as an operator reads it
        System.out.println('the market world\'s crypto is ' + c[0] + ': ' + c[1])
        System.out.println('the market world\'s filings are ' + f[0] + ': ' + f[1])
        world.add(TableTools.newTable(
                TableTools.stringCol('item', 'equities', 'crypto', 'news', 'filings'),
                TableTools.stringCol('state', 'simulated', c[0], 'simulated', f[0]),
                TableTools.stringCol('detail', equitiesDetail, c[1], newsDetail, f[1])))
    }
}

// ---------------------------------------------------------------- the bank's own day (K9-K13)

/** A parent order an algorithm works into fills on the market's prices: its desk's book and trader, its name, side and
 *  size, the mid when it arrived, and how much is done. */
@CompileStatic
class WorkingOrder {
    String id
    String bookId
    String traderId
    Name name
    String side
    double quantity
    double filled
    double arrival
    String strategy
    String timeInForce
    double limit = Double.NaN
    int priority
    Instant created
    String status
    /** fills it is worked in, about: a slice's size is its quantity over them */
    int slices
}

/** A currency pair the desk makes prices in: its mid, its prior close (the feed's), its pip and spread in pips. A cross
 *  (`leg1` set) is quoted about its legs' product, or their ratio (`divide`), `basis` pips of its own about it. */
@CompileStatic
class Pair {
    String pair
    String base
    double mid
    double prevClose
    double pip
    double spreadPips
    double vol
    double weight
    Pair leg1
    Pair leg2
    boolean divide
    double basis

    /** A cross's mid as its legs imply it. */
    double implied() {
        return divide ? leg1.mid / leg2.mid : leg1.mid * leg2.mid
    }
}

/** A client's request for a quote: asked, quoted (or declined) after the desk's response, then done, missed or let
 *  expire by the client. */
@CompileStatic
class Request {
    String id
    Instant asked
    Pair pair
    String side
    double notional
    String tenor
    String clientId
    String dealerId
    double clientEdge
    double quotedRate = Double.NaN
    double spreadBps = Double.NaN
    String status
    int responseMs = -1
    boolean hit
    long quoteAtMs
    long decideAtMs
}

/**
 * THE BANK'S OWN DAY (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K9-K13), bespoke as the market is (the plan's ledger):
 * its desks' parent orders worked by algorithms into fills on the market's own prices (TRADE_ORDER, EXECUTION,
 * ORDER_ROUTE), its FX prices (FX_RATE) and the clients' requests for quotes on them (RFQ) -- each the corpus's table,
 * every column it declares, in its order. Stepped by the world's clock on the market's own thread, so a fill reads the
 * quote as it is.
 */
@CompileStatic
class Day {
    static final double DAY = 23_400d
    static final String[] STRATEGIES = ['VWAP', 'TWAP', 'POV', 'IS', 'DARK'] as String[]
    static final String[] BROKERS = ['GSCO', 'MSCO', 'JPMS', 'CITI', 'BARC'] as String[]
    static final String[] LIT = ['XNAS', 'XNYS', 'ARCX', 'BATS', 'EDGX', 'IEXG'] as String[]
    static final String[] DARK = ['SIGMA-X', 'CROSSFINDER', 'MS-POOL', 'LIQUIDNET'] as String[]
    static final String[] TENORS = ['SPOT', 'SPOT', 'SPOT', 'SPOT', 'SPOT', 'SPOT', '1W', '1M', '1M', '3M'] as String[]
    final Random random
    final List<Map<String, Object>> books
    final List<Pair> pairs
    final List<String> clients
    final List<Double> clientEdge
    final List<String> dealers
    final TablePublisher orders
    final TablePublisher executions
    final TablePublisher routes
    final TablePublisher rates
    final TablePublisher rfqs
    final double orderRate
    final double rfqRate
    final List<WorkingOrder> working = []
    final List<Request> pending = []
    long orderSeq
    long execSeq
    long routeSeq
    long rfqSeq
    long lastNanos = System.nanoTime()

    Day(Random random, List<Map<String, Object>> books, List<Pair> pairs, List<String> clients, List<Double> clientEdge,
        List<String> dealers, TablePublisher orders, TablePublisher executions, TablePublisher routes,
        TablePublisher rates, TablePublisher rfqs, double orderRate, double rfqRate) {
        this.random = random
        this.books = books
        this.pairs = pairs
        this.clients = clients
        this.clientEdge = clientEdge
        this.dealers = dealers
        this.orders = orders
        this.executions = executions
        this.routes = routes
        this.rates = rates
        this.rfqs = rfqs
        this.orderRate = orderRate
        this.rfqRate = rfqRate
    }

    int events(double mean) {
        int n = (int) Math.floor(mean)
        return n + (random.nextDouble() < mean - n ? 1 : 0)
    }

    static double round(double v, double to) { return Math.round(v / to) * to }

    /** One beat: new orders and their fills, the FX prices moved, the clients' requests asked and answered. */
    void beat() {
        try {
            long t = System.nanoTime()
            double dt = Math.min(1.0d, (t - lastNanos) / 1e9d)
            lastNanos = t
            Instant now = Instant.now()
            work(dt, now)
            price(dt, now)
            request(dt, now)
        } catch (Throwable e) {
            System.err.println('the bank\'s day failed a beat: ' + e)
            e.printStackTrace()
        }
    }

    // ------------------------------------------------ orders, worked into fills

    void work(double dt, Instant now) {
        OrderRows o = new OrderRows()
        int arriving = working.size() < 80 ? events(orderRate * dt) : 0
        for (int i = 0; i < arriving; i++) o.order(arrive(now))
        for (Iterator<WorkingOrder> it = working.iterator(); it.hasNext(); ) {
            WorkingOrder w = it.next()
            if (w.status == 'NEW') {
                w.status = 'WORKING'
                o.order(w)
                continue
            }
            // a slice about every two seconds, the dark strategy's less often (it waits for a match)
            double every = w.strategy == 'DARK' ? 4d : 2d
            if (random.nextDouble() >= dt / every) continue
            slice(w, now, o)
            o.order(w)
            if (w.status == 'FILLED' || w.status == 'CANCELLED') it.remove()
        }
        if (!o.orderId.isEmpty()) orders.add(o.orders())
        if (!o.execId.isEmpty()) executions.add(o.executions())
        if (!o.routeId.isEmpty()) routes.add(o.routes())
    }

    WorkingOrder arrive(Instant now) {
        Map<String, Object> book = books[random.nextInt(books.size())]
        List<Name> names = (List<Name>) book.get('names')
        List<String> traders = (List<String>) book.get('traders')
        // a name by its activity: the largest are traded most
        double u = random.nextDouble() * (names.sum { Name n -> n.weight } as double)
        Name name = names[names.size() - 1]
        for (Name n : names) { if ((u -= n.weight) < 0) { name = n; break } }
        double notional = 250_000d * Math.exp(random.nextGaussian() * 0.9d)
        double qty = name.kind == 'EQUITY' ? Math.max(100d, round(notional / name.mid, 100d))
                : round(notional / name.mid, name.mid > 1000 ? 0.01d : 1d)
        WorkingOrder w = new WorkingOrder(id: String.format('ORD-%06d', ++orderSeq), bookId: (String) book.get('id'),
                traderId: traders[random.nextInt(traders.size())], name: name,
                side: random.nextBoolean() ? 'BUY' : 'SELL', quantity: qty, filled: 0d,
                arrival: (name.bid + name.ask) / 2d, strategy: STRATEGIES[random.nextInt(STRATEGIES.length)],
                timeInForce: random.nextDouble() < 0.9d ? 'DAY' : 'IOC', priority: 1 + random.nextInt(3), created: now,
                status: 'NEW', slices: 6 + random.nextInt(20))
        // a limit on a third of them, a little through the arrival price
        if (random.nextDouble() < 0.33d) {
            w.limit = round(w.arrival * (w.side == 'BUY' ? 1.003d : 0.997d), name.tick)
        }
        working << w
        return w
    }

    /** One child slice of `w`: routed, and filled -- or, now and then, rejected by its venue -- at the market's price. */
    void slice(WorkingOrder w, Instant now, OrderRows o) {
        Name n = w.name
        double remaining = w.quantity - w.filled
        double lot = n.kind == 'EQUITY' ? 100d : (n.mid > 1000 ? 0.01d : 1d)
        double clip = Math.min(remaining, Math.max(lot, round(w.quantity / w.slices * Math.exp(random.nextGaussian() * 0.4d), lot)))
        boolean dark = w.strategy == 'DARK' ? random.nextDouble() < 0.8d : random.nextDouble() < 0.15d
        String venue = n.kind != 'EQUITY' ? 'CBX' : dark ? DARK[random.nextInt(DARK.length)] : LIT[random.nextInt(LIT.length)]
        String broker = BROKERS[random.nextInt(BROKERS.length)]
        boolean buy = w.side == 'BUY'
        // the price: across the spread (aggressive), at the near side (passive), or the midpoint (dark)
        double u = random.nextDouble()
        String liquidity = dark ? 'MIDPOINT' : u < 0.45d ? 'REMOVE' : 'ADD'
        double px = liquidity == 'MIDPOINT' ? (n.bid + n.ask) / 2d : liquidity == 'REMOVE' ? (buy ? n.ask : n.bid) : (buy ? n.bid : n.ask)
        boolean rejected = random.nextDouble() < 0.03d || (!Double.isNaN(w.limit) && (buy ? px > w.limit : px < w.limit))
        long route = ++routeSeq
        o.route(String.format('RT-%07d', route), now, venue, clip, px, rejected ? 'REJECTED' : 'FILLED',
                dark ? 'MIDPEG' : liquidity == 'REMOVE' ? 'IOC' : 'LIMIT', w.priority, 1 + random.nextInt(4),
                n.kind == 'EQUITY', rejected ? (!Double.isNaN(w.limit) && (buy ? px > w.limit : px < w.limit)
                ? 'PRICE_OUTSIDE_LIMIT' : 'VENUE_REJECT') : null, broker, w.id)
        if (rejected) return
        w.filled += clip
        boolean done = w.quantity - w.filled < lot / 2d
        w.status = done ? 'FILLED' : 'PARTIALLY_FILLED'
        o.execution(String.format('EX-%07d', ++execSeq), now, clip, px, venue, broker,
                round(clip * (n.kind == 'EQUITY' ? 0.003d : px * 0.0005d), 0.01d), done ? 'FULL' : 'PARTIAL', w.strategy,
                w.arrival, !done, (int) execSeq, venue, w.id)
        // an order of a day's time in force is cancelled now and then with some left; an IOC's rest at once
        if (!done && (w.timeInForce == 'IOC' || random.nextDouble() < 0.01d)) w.status = 'CANCELLED'
    }

    // ------------------------------------------------ FX prices

    void price(double dt, Instant now) {
        RateRows r = new RateRows()
        Set<Pair> moved = new HashSet<Pair>()
        for (Pair p : pairs) {
            if (p.leg1 != null) continue
            p.mid *= Math.exp(random.nextGaussian() * p.vol * Math.sqrt(dt / DAY))
            // about ten prices a second for the most traded, fewer for the rest
            if (random.nextDouble() < Math.min(1d, 10d * p.weight * dt / 0.15d)) {
                r.rate(p, now)
                moved << p
            }
        }
        // each cross about its legs' product (or ratio), a few tenths of a pip of its own about it (a basis that reverts:
        // about 0.3 pip), priced again whenever a leg is -- never a free lunch against its legs, as a real desk's
        for (Pair p : pairs) {
            if (p.leg1 == null) continue
            p.basis = 0.7d * p.basis + 0.21d * random.nextGaussian()
            p.mid = p.implied() + p.basis * p.pip
            if (moved.contains(p.leg1) || moved.contains(p.leg2) || random.nextDouble() < Math.min(1d, 10d * p.weight * dt / 0.15d)) r.rate(p, now)
        }
        if (!r.id.isEmpty()) rates.add(r.rates())
    }

    // ------------------------------------------------ the clients' requests

    void request(double dt, Instant now) {
        RfqRows q = new RfqRows()
        long ms = System.currentTimeMillis()
        int asking = events(rfqRate * dt)
        for (int i = 0; i < asking; i++) {
            int c = random.nextInt(clients.size())
            double u = random.nextDouble() * (pairs.sum { Pair p -> p.weight } as double)
            Pair pair = pairs[pairs.size() - 1]
            for (Pair p : pairs) { if ((u -= p.weight) < 0) { pair = p; break } }
            Request r = new Request(id: String.format('RFQ-%06d', ++rfqSeq), asked: now, pair: pair,
                    side: random.nextBoolean() ? 'BUY' : 'SELL',
                    notional: Math.max(1d, Math.round(5d * Math.exp(random.nextGaussian() * 0.8d))) * 1e6d,
                    tenor: TENORS[random.nextInt(TENORS.length)], clientId: clients[c],
                    dealerId: dealers[random.nextInt(dealers.size())], clientEdge: clientEdge[c], status: 'NEW')
            r.responseMs = 40 + (int) Math.round(Math.abs(random.nextGaussian()) * 250d)
            r.quoteAtMs = ms + r.responseMs
            pending << r
            q.rfq(r)
        }
        for (Iterator<Request> it = pending.iterator(); it.hasNext(); ) {
            Request r = it.next()
            if (r.status == 'NEW' && ms >= r.quoteAtMs) {
                if (random.nextDouble() < 0.04d) {
                    // the desk declines: no price
                    r.status = 'DECLINED'
                    q.rfq(r)
                    it.remove()
                    continue
                }
                // a spread wider for a larger ticket and a longer tenor, narrower for a client the desk wants
                double bps = Math.max(0.2d, 0.6d + 0.08d * r.notional / 1e6d + (r.tenor == 'SPOT' ? 0d : 1.5d)
                        - r.clientEdge + random.nextGaussian() * 0.3d)
                r.spreadBps = Math.round(bps * 100d) / 100d
                double mid = r.pair.mid
                r.quotedRate = round(mid * (1d + (r.side == 'BUY' ? 1d : -1d) * bps / 1e4d), r.pair.pip / 10d)
                r.status = 'QUOTED'
                r.decideAtMs = ms + 300L + (long) (random.nextDouble() * 2500d)
                q.rfq(r)
                continue
            }
            if (r.status == 'QUOTED' && ms >= r.decideAtMs) {
                // the tighter the price, the likelier it is hit; a few let it lapse
                double hitChance = 1d / (1d + Math.exp(2.2d * (r.spreadBps - 1.2d)))
                double u = random.nextDouble()
                r.status = u < 0.06d ? 'EXPIRED' : u < 0.06d + 0.94d * hitChance ? 'DONE' : 'MISSED'
                r.hit = r.status == 'DONE'
                q.rfq(r)
                it.remove()
            }
        }
        if (!q.id.isEmpty()) rfqs.add(q.rfqs())
    }
}

/** An order's, a fill's and a route's rows, made in a beat: every column of TRADE_ORDER, EXECUTION and ORDER_ROUTE. */
@CompileStatic
class OrderRows {
    static final double NULL_DOUBLE = io.deephaven.util.QueryConstants.NULL_DOUBLE
    static final int NULL_INT = io.deephaven.util.QueryConstants.NULL_INT
    final List<String> orderId = []
    final List<java.time.LocalDate> orderDate = []
    final List<String> side = []
    final List<Double> quantity = []
    final List<Double> filled = []
    final List<Double> remaining = []
    final List<Double> limit = []
    final List<String> tif = []
    final List<String> status = []
    final List<Integer> priority = []
    final List<Instant> created = []
    final List<Instant> modified = []
    final List<Boolean> algo = []
    final List<String> strategy = []
    final List<String> instrument = []
    final List<String> trader = []
    final List<String> book = []
    final List<String> execId = []
    final List<Instant> execTime = []
    final List<Double> execQty = []
    final List<Double> execPx = []
    final List<String> execVenue = []
    final List<String> execBroker = []
    final List<Double> execCommission = []
    final List<String> execType = []
    final List<String> execAlgo = []
    final List<Double> execArrival = []
    final List<Boolean> execPartial = []
    final List<Integer> execSeq = []
    final List<String> execMarket = []
    final List<String> execOrder = []
    final List<String> routeId = []
    final List<Instant> routeTime = []
    final List<String> routeDest = []
    final List<Double> routeQty = []
    final List<Double> routePx = []
    final List<String> routeStatus = []
    final List<String> routeType = []
    final List<Integer> routePriority = []
    final List<Integer> routeLatency = []
    final List<Boolean> routeSmart = []
    final List<String> routeReject = []
    final List<String> routeBroker = []
    final List<String> routeOrder = []

    void order(WorkingOrder w) {
        orderId << w.id; orderDate << w.created.atZone(java.time.ZoneOffset.UTC).toLocalDate(); side << w.side
        quantity << w.quantity; filled << w.filled; remaining << (w.status == 'CANCELLED' ? 0d : w.quantity - w.filled)
        limit << (Double.isNaN(w.limit) ? NULL_DOUBLE : w.limit); tif << w.timeInForce; status << w.status
        priority << w.priority; created << w.created; modified << Instant.now(); algo << Boolean.TRUE
        strategy << w.strategy; instrument << w.name.instrumentId; trader << w.traderId; book << w.bookId
    }

    void execution(String id, Instant at, double qty, double px, String venue, String broker, double commission,
                   String type, String algoName, double arrival, boolean partial, int seq, String market, String order) {
        execId << id; execTime << at; execQty << qty; execPx << px; execVenue << venue; execBroker << broker
        execCommission << commission; execType << type; execAlgo << algoName; execArrival << arrival
        execPartial << partial; execSeq << seq; execMarket << market; execOrder << order
    }

    void route(String id, Instant at, String dest, double qty, double px, String st, String type, int prio, int latency,
               boolean smart, String reject, String broker, String order) {
        routeId << id; routeTime << at; routeDest << dest; routeQty << qty; routePx << px; routeStatus << st
        routeType << type; routePriority << prio; routeLatency << latency; routeSmart << smart; routeReject << reject
        routeBroker << broker; routeOrder << order
    }

    Table orders() {
        int k = orderId.size()
        return TableTools.newTable(
                TableTools.stringCol('ORDER_ID', orderId as String[]),
                TableTools.col('ORDER_DATE', orderDate as java.time.LocalDate[]),
                TableTools.stringCol('SIDE', side as String[]),
                TableTools.doubleCol('QUANTITY', quantity as double[]),
                TableTools.doubleCol('FILLED_QUANTITY', filled as double[]),
                TableTools.doubleCol('REMAINING_QUANTITY', remaining as double[]),
                TableTools.doubleCol('LIMIT_PRICE', limit as double[]),
                TableTools.doubleCol('STOP_PRICE', ([NULL_DOUBLE] * k) as double[]),
                TableTools.stringCol('TIME_IN_FORCE', tif as String[]),
                TableTools.stringCol('STATUS', status as String[]),
                TableTools.intCol('PRIORITY', priority as int[]),
                TableTools.instantCol('CREATED_TIME', created as Instant[]),
                TableTools.instantCol('LAST_MODIFIED_TIME', modified as Instant[]),
                TableTools.col('EXPIRY_DATE', new java.time.LocalDate[k]),
                TableTools.col('IS_ALGO_ORDER', algo as Boolean[]),
                TableTools.stringCol('ALGO_STRATEGY', strategy as String[]),
                TableTools.stringCol('INSTRUMENT_ID', instrument as String[]),
                TableTools.stringCol('TRADER_ID', trader as String[]),
                TableTools.stringCol('BOOK_ID', book as String[]))
    }

    Table executions() {
        int k = execId.size()
        return TableTools.newTable(
                TableTools.stringCol('EXECUTION_ID', execId as String[]),
                TableTools.instantCol('EXECUTION_TIME', execTime as Instant[]),
                TableTools.doubleCol('FILLED_QUANTITY', execQty as double[]),
                TableTools.doubleCol('FILL_PRICE', execPx as double[]),
                TableTools.stringCol('VENUE', execVenue as String[]),
                TableTools.stringCol('BROKER', execBroker as String[]),
                TableTools.doubleCol('COMMISSION', execCommission as double[]),
                TableTools.stringCol('FILL_TYPE', execType as String[]),
                TableTools.stringCol('EXECUTION_ALGO', execAlgo as String[]),
                TableTools.doubleCol('ARRIVAL_PRICE', execArrival as double[]),
                // the source keeps no slippage or impact of its own: the screens compute them, the database does
                TableTools.doubleCol('SLIPPAGE', ([NULL_DOUBLE] * k) as double[]),
                TableTools.doubleCol('MARKET_IMPACT', ([NULL_DOUBLE] * k) as double[]),
                TableTools.col('IS_PARTIAL_FILL', execPartial as Boolean[]),
                TableTools.intCol('SEQUENCE_NUMBER', execSeq as int[]),
                TableTools.stringCol('LAST_MARKET', execMarket as String[]),
                TableTools.stringCol('ORDER_ID', execOrder as String[]))
    }

    Table routes() {
        return TableTools.newTable(
                TableTools.stringCol('ROUTE_ID', routeId as String[]),
                TableTools.instantCol('ROUTE_DATE', routeTime as Instant[]),
                TableTools.stringCol('DESTINATION', routeDest as String[]),
                TableTools.doubleCol('ROUTE_QUANTITY', routeQty as double[]),
                TableTools.doubleCol('ROUTED_PRICE', routePx as double[]),
                TableTools.stringCol('STATUS', routeStatus as String[]),
                TableTools.stringCol('ROUTE_TYPE', routeType as String[]),
                TableTools.intCol('PRIORITY', routePriority as int[]),
                TableTools.intCol('LATENCY_MS', routeLatency as int[]),
                TableTools.col('IS_SMART_ROUTED', routeSmart as Boolean[]),
                TableTools.stringCol('REJECT_REASON', routeReject as String[]),
                TableTools.stringCol('BROKER', routeBroker as String[]),
                TableTools.stringCol('ORDER_ID', routeOrder as String[]))
    }
}

/** An FX price's row: every column of FX_RATE. Its change on the day is the feed's, against the pair's prior close. */
@CompileStatic
class RateRows {
    static final double NULL_DOUBLE = io.deephaven.util.QueryConstants.NULL_DOUBLE
    final List<String> id = []
    final List<String> pair = []
    final List<Double> spot = []
    final List<Double> bid = []
    final List<Double> ask = []
    final List<Instant> at = []
    final List<Double> change = []
    final List<Double> changePct = []
    final List<String> base = []

    void rate(Pair p, Instant now) {
        double half = p.spreadPips * p.pip / 2d
        double mid = Day.round(p.mid, p.pip / 10d)
        id << ('FX-' + p.pair.replace('/', '')); pair << p.pair; spot << mid
        bid << Day.round(mid - half, p.pip / 10d); ask << Day.round(mid + half, p.pip / 10d); at << now
        change << mid - p.prevClose; changePct << (mid / p.prevClose - 1d); base << p.base
    }

    Table rates() {
        int k = id.size()
        return TableTools.newTable(
                TableTools.stringCol('RATE_ID', id as String[]),
                TableTools.stringCol('CURRENCY_PAIR', pair as String[]),
                TableTools.doubleCol('SPOT_RATE', spot as double[]),
                TableTools.doubleCol('BID_RATE', bid as double[]),
                TableTools.doubleCol('ASK_RATE', ask as double[]),
                TableTools.instantCol('RATE_DATE', at as Instant[]),
                TableTools.stringCol('SOURCE', (['SIM'] * k) as String[]),
                TableTools.doubleCol('MID_RATE', spot as double[]),
                TableTools.doubleCol('DAILY_CHANGE', change as double[]),
                TableTools.doubleCol('DAILY_CHANGE_PCT', changePct as double[]),
                TableTools.col('IS_OFFICIAL', ([Boolean.FALSE] * k) as Boolean[]),
                TableTools.stringCol('FIXING_TIME', new String[k]),
                TableTools.stringCol('SNAPSHOT_ID', new String[k]),
                TableTools.stringCol('BASE_CURRENCY_CODE', base as String[]))
    }
}

/** A request's row, at each of its states: every column of RFQ. */
@CompileStatic
class RfqRows {
    static final double NULL_DOUBLE = io.deephaven.util.QueryConstants.NULL_DOUBLE
    static final int NULL_INT = io.deephaven.util.QueryConstants.NULL_INT
    final List<String> id = []
    final List<Instant> asked = []
    final List<String> pair = []
    final List<String> side = []
    final List<Double> notional = []
    final List<String> tenor = []
    final List<Double> rate = []
    final List<Double> spread = []
    final List<String> status = []
    final List<Integer> response = []
    final List<Boolean> hit = []
    final List<String> client = []
    final List<String> dealer = []

    void rfq(Request r) {
        id << r.id; asked << r.asked; pair << r.pair.pair; side << r.side; notional << r.notional; tenor << r.tenor
        rate << (Double.isNaN(r.quotedRate) ? NULL_DOUBLE : r.quotedRate)
        spread << (Double.isNaN(r.spreadBps) ? NULL_DOUBLE : r.spreadBps); status << r.status
        response << (r.status == 'NEW' ? NULL_INT : r.responseMs); hit << r.hit; client << r.clientId
        dealer << r.dealerId
    }

    Table rfqs() {
        return TableTools.newTable(
                TableTools.stringCol('RFQ_ID', id as String[]),
                TableTools.instantCol('REQUEST_TIME', asked as Instant[]),
                TableTools.stringCol('CURRENCY_PAIR', pair as String[]),
                TableTools.stringCol('SIDE', side as String[]),
                TableTools.doubleCol('NOTIONAL', notional as double[]),
                TableTools.stringCol('TENOR', tenor as String[]),
                TableTools.doubleCol('QUOTED_RATE', rate as double[]),
                TableTools.doubleCol('SPREAD_BPS', spread as double[]),
                TableTools.stringCol('STATUS', status as String[]),
                TableTools.intCol('RESPONSE_TIME_MS', response as int[]),
                TableTools.col('IS_HIT', hit as Boolean[]),
                TableTools.stringCol('COUNTERPARTY_ID', client as String[]),
                TableTools.stringCol('TRADER_ID', dealer as String[]))
    }
}

/** Black and Scholes, for an option's settlement price at the prior close (the exchange's, which the world plays):
 *  its normal distribution by Abramowitz and Stegun 26.2.17. */
@CompileStatic
class Black {
    static double cdf(double x) {
        double t = 1d / (1d + 0.2316419d * Math.abs(x))
        double d = 0.3989422804014327d * Math.exp(-x * x / 2d)
        double p = d * t * (0.319381530d + t * (-0.356563782d + t * (1.781477937d + t * (-1.821255978d + t * 1.330274429d))))
        return x >= 0 ? 1d - p : p
    }

    static double price(boolean call, double s, double k, double t, double v, double r) {
        double d1 = (Math.log(s / k) + (r + v * v / 2d) * t) / (v * Math.sqrt(t))
        double d2 = d1 - v * Math.sqrt(t)
        return call ? s * cdf(d1) - k * Math.exp(-r * t) * cdf(d2) : k * Math.exp(-r * t) * cdf(-d2) - s * cdf(-d1)
    }
}

// ---------------------------------------------------------------- the world's settings

env = System.getenv()
NAMES = Integer.parseInt(env.getOrDefault('MARKET_NAMES', '500'))
QUOTES = Double.parseDouble(env.getOrDefault('MARKET_QUOTES', '8000'))
TRADES = Double.parseDouble(env.getOrDefault('MARKET_TRADES', '800'))
FEED = env.getOrDefault('MARKET_FEED', 'coinbase')
FEED_URL = env.getOrDefault('MARKET_FEED_URL', 'wss://ws-feed.exchange.coinbase.com')
HISTORY = Integer.parseInt(env.getOrDefault('MARKET_HISTORY', '0'))
MORNING = Double.parseDouble(env.getOrDefault('MARKET_MORNING', '7200'))
MORNING_PRINTS = Double.parseDouble(env.getOrDefault('MARKET_MORNING_PRINTS', '400000'))
NEWS = Double.parseDouble(env.getOrDefault('MARKET_NEWS', '12'))
JUMPS = Double.parseDouble(env.getOrDefault('MARKET_JUMPS', '0.5'))
FILINGS_AGENT = env.get('MARKET_FILINGS_AGENT')
FILINGS_URL = env.getOrDefault('MARKET_FILINGS_URL',
        'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&count=40&output=atom')
random = new Random(Long.parseLong(env.getOrDefault('MARKET_SEED', '20261024')))

// ---------------------------------------------------------------- the names

SECTORS = ['Technology', 'Health Care', 'Financials', 'Consumer Discretionary', 'Communication Services',
           'Industrials', 'Consumer Staples', 'Energy', 'Utilities', 'Real Estate', 'Materials']
SECTOR_SHARE = [0.22, 0.12, 0.13, 0.10, 0.08, 0.09, 0.06, 0.05, 0.05, 0.05, 0.05]
// each sector's GICS code (Information Technology 45, Health Care 35, ...): its corpus id is SEC-<code>
GICS = ['45', '35', '40', '25', '50', '20', '30', '10', '55', '60', '15']
SECTOR_WORDS = [['Systems', 'Software', 'Semiconductor', 'Networks', 'Cloud'], ['Therapeutics', 'Biosciences', 'Health', 'Medical', 'Pharma'],
                ['Bancorp', 'Financial', 'Capital', 'Insurance', 'Trust'], ['Retail', 'Motors', 'Brands', 'Apparel', 'Leisure'],
                ['Media', 'Telecom', 'Interactive', 'Broadcasting', 'Studios'], ['Industries', 'Aerospace', 'Machinery', 'Logistics', 'Rail'],
                ['Foods', 'Beverages', 'Household', 'Grocers', 'Farms'], ['Energy', 'Petroleum', 'Drilling', 'Pipelines', 'Refining'],
                ['Power', 'Electric', 'Water', 'Gas', 'Utilities'], ['Realty', 'Properties', 'Towers', 'Storage', 'Residential'],
                ['Materials', 'Chemicals', 'Mining', 'Steel', 'Packaging']]
PREFIXES = ['Apex', 'Blue', 'Cedar', 'Delta', 'Ember', 'Frontier', 'Granite', 'Harbor', 'Iron', 'Juniper', 'Keystone',
            'Lumen', 'Meridian', 'North', 'Orion', 'Pioneer', 'Quarry', 'Ridge', 'Summit', 'Tidal', 'Union', 'Vantage',
            'Willow', 'Xeno', 'Yarrow', 'Zenith', 'Atlas', 'Beacon', 'Crest', 'Dawn', 'Echo', 'Falcon', 'Glacier', 'Horizon']

usedSyms = new HashSet<String>()
equities = []
for (int i = 0; i < NAMES; i++) {
    // a sector by its share of the market
    double u = random.nextDouble()
    int k = 0
    while (k < SECTORS.size() - 1 && (u -= SECTOR_SHARE[k]) >= 0) k++
    String sym
    do {
        int len = random.nextDouble() < 0.6 ? 4 : 3
        sym = (0..<len).collect { String.valueOf((char) (65 + random.nextInt(26))) }.join('')
    } while (!usedSyms.add(sym))
    // rank by size: the first names the largest (a market capitalisation from about 2 trillion down to 1 billion)
    double cap = 2.0e12 * Math.pow(i + 1, -1.15) + 1.0e9
    double px = Math.max(3d, Math.min(900d, 60d * Math.exp(random.nextGaussian() * 0.9)))
    Name n = new Name(sym: sym, name: PREFIXES[random.nextInt(PREFIXES.size())] + ' ' + SECTOR_WORDS[k][random.nextInt(5)],
            sector: SECTORS[k], kind: 'EQUITY', sectorIx: k, prevClose: Math.round(px * 100) / 100d,
            shares: Math.round(cap / px), tick: 0.01d,
            vol: 0.012d + 0.025d * Math.min(1d, i / (double) NAMES) * Math.exp(random.nextGaussian() * 0.3),
            beta: Math.max(0.3d, 1.0d + random.nextGaussian() * 0.3), weight: Math.pow(i + 1, -0.9),
            spreadBps: 1.0d + 25d * Math.pow(i / (double) NAMES, 1.5), sizeMedian: 300d + 1500d * Math.pow(i + 1, -0.5))
    // the day opens a little away from its prior close
    n.mid = Math.max(n.tick, n.prevClose * Math.exp(random.nextGaussian() * 0.008))
    // the corpus's ids; a listing on Nasdaq or the NYSE by the name's place (no draw, so the world's names are as
    // they were)
    n.instrumentId = 'INST-' + sym
    n.equityId = 'EQ-' + sym
    n.sectorId = 'SEC-' + GICS[k]
    n.exchange = i % 5 < 3 ? 'XNAS' : 'XNYS'
    equities << n
}
double totalWeight = equities.sum { it.weight } as double
equities.each { it.weight = it.weight / totalWeight }

// crypto: Coinbase Exchange's product ids; their prior close (24 hours ago) the feed's, else a simulated one
COINS = [['BTC-USD', 'Bitcoin', 67000d], ['ETH-USD', 'Ether', 2600d], ['SOL-USD', 'Solana', 150d],
         ['XRP-USD', 'XRP', 0.55d], ['DOGE-USD', 'Dogecoin', 0.12d], ['LTC-USD', 'Litecoin', 70d],
         ['ADA-USD', 'Cardano', 0.35d], ['AVAX-USD', 'Avalanche', 27d]]
coins = []
COINS.eachWithIndex { c, i ->
    double px = c[2] as double
    Name n = new Name(sym: c[0] as String, name: c[1] as String, sector: 'Crypto', kind: 'CRYPTO', sectorIx: 0,
            prevClose: px, shares: 0L, tick: px >= 10d ? 0.01d : 0.0001d, vol: 0.035d, beta: 1.5d,
            weight: Math.pow(i + 1, -1.0) / 2.72d, spreadBps: 1.5d, sizeMedian: 3000d / px)
    n.mid = px * Math.exp(random.nextGaussian() * 0.02)
    n.instrumentId = 'INST-' + n.sym
    n.sectorId = 'SEC-CRYPTO'
    coins << n
}

// ---------------------------------------------------------------- the feed: tried first, its prior closes taken

seq = new AtomicLong()
feed = null
if (FEED == 'coinbase') {
    feed = new CoinbaseFeed(URI.create(FEED_URL), coins.collect { it.sym })
    feed.connect()
    // the feed's first ticker for each coin, for a few seconds: its 24 hours' open is the coin's prior close
    Map<String, Double> opens = [:]
    long until = System.currentTimeMillis() + 8_000L
    while (System.currentTimeMillis() < until && opens.size() < coins.size() && feed.state != 'refused') {
        for (Map m : feed.messages) {
            if (m.get('type') == 'ticker' && m.get('open_24h') != null) {
                opens.put(m.get('product_id') as String, Double.parseDouble(m.get('open_24h') as String))
            }
        }
        Thread.sleep(100)
    }
    if (opens.size() == coins.size()) {
        coins.each { it.prevClose = opens.get(it.sym); it.mid = it.prevClose }
    } else if (feed.state != 'refused') {
        feed.refused('no ticker for every coin in 8 s')
    }
}

// ---------------------------------------------------------------- the tables

// the publishers' callbacks: the clock publishes on its own beat, and nothing is kept to let go of at a shutdown
NO_FLUSH = { p -> } as java.util.function.Consumer
NO_SHUTDOWN = { -> } as Runnable

// the reference tables, as the corpus declares them (30-store.pure): every column, a null where the world has no value
nulls = { int k -> new String[k] }
names = equities + coins
today = java.time.LocalDate.now(java.time.ZoneOffset.UTC)
facts = new Random(Long.parseLong(env.getOrDefault('MARKET_SEED', '20261024')) + 1L)

EXCHANGE = TableTools.newTable(
        TableTools.stringCol('MIC', 'XNAS', 'XNYS'),
        TableTools.stringCol('NAME', 'Nasdaq Stock Market', 'New York Stock Exchange'),
        TableTools.stringCol('CITY', 'New York', 'New York'),
        TableTools.stringCol('TIMEZONE', 'America/New_York', 'America/New_York'),
        TableTools.stringCol('OPEN_TIME', '09:30', '09:30'),
        TableTools.stringCol('CLOSE_TIME', '16:00', '16:00'),
        TableTools.col('IS_ELECTRONIC', Boolean.TRUE, Boolean.FALSE),
        TableTools.stringCol('OPERATING_MIC', 'XNAS', 'XNYS'),
        TableTools.stringCol('WEBSITE', 'https://www.nasdaq.com', 'https://www.nyse.com'),
        TableTools.stringCol('REGULATORY_BODY', 'SEC', 'SEC'),
        TableTools.stringCol('COUNTRY_ID', 'US', 'US'))

totalCap = equities.sum { it.shares * it.prevClose } as double
SECTOR = TableTools.newTable(
        TableTools.stringCol('SECTOR_ID', ((0..<SECTORS.size()).collect { 'SEC-' + GICS[it] } + ['SEC-CRYPTO']) as String[]),
        TableTools.stringCol('NAME', (SECTORS + ['Crypto']) as String[]),
        TableTools.stringCol('GICS_CODE', (GICS + [null]) as String[]),
        TableTools.intCol('LEVEL', ([1] * (SECTORS.size() + 1)) as int[]),
        TableTools.stringCol('DESCRIPTION', nulls(SECTORS.size() + 1)),
        TableTools.col('IS_ACTIVE', ([Boolean.TRUE] * (SECTORS.size() + 1)) as Boolean[]),
        TableTools.stringCol('PARENT_SECTOR_ID', nulls(SECTORS.size() + 1)),
        TableTools.doubleCol('MARKET_CAP_WEIGHT', ((0..<SECTORS.size()).collect { k ->
            (equities.findAll { it.sectorIx == k }.sum { it.shares * it.prevClose } ?: 0d) / totalCap } +
            [io.deephaven.util.QueryConstants.NULL_DOUBLE]) as double[]),
        TableTools.intCol('NUMBER_OF_COMPANIES', ((0..<SECTORS.size()).collect { k ->
            equities.count { it.sectorIx == k } } + [coins.size()]) as int[]),
        TableTools.col('LAST_UPDATED', ([today] * (SECTORS.size() + 1)) as java.time.LocalDate[]))

int nn = names.size()
INSTRUMENT = TableTools.newTable(
        TableTools.stringCol('INSTRUMENT_ID', names.collect { it.instrumentId } as String[]),
        TableTools.stringCol('TICKER', names.collect { it.sym } as String[]),
        TableTools.stringCol('NAME', names.collect { it.name } as String[]),
        TableTools.stringCol('ISIN', nulls(nn)),
        TableTools.stringCol('CUSIP', nulls(nn)),
        TableTools.stringCol('SEDOL', nulls(nn)),
        TableTools.stringCol('ASSET_CLASS', names.collect { it.kind } as String[]),
        TableTools.stringCol('PRODUCT_TYPE', names.collect { it.kind == 'EQUITY' ? 'COMMON_STOCK' : 'SPOT' } as String[]),
        TableTools.col('IS_ACTIVE', ([Boolean.TRUE] * nn) as Boolean[]),
        TableTools.col('LISTING_DATE', new java.time.LocalDate[nn]),
        TableTools.col('MATURITY_DATE', new java.time.LocalDate[nn]),
        TableTools.doubleCol('NOTIONAL_AMOUNT', ([io.deephaven.util.QueryConstants.NULL_DOUBLE] * nn) as double[]),
        TableTools.stringCol('CURRENCY', (['USD'] * nn) as String[]),
        TableTools.stringCol('DESCRIPTION', names.collect { it.sector } as String[]),
        TableTools.col('ISSUE_DATE', new java.time.LocalDate[nn]),
        TableTools.stringCol('EQUITY_ID', names.collect { it.equityId } as String[]),
        TableTools.stringCol('BOND_ID', nulls(nn)),
        TableTools.stringCol('FUTURE_ID', nulls(nn)),
        TableTools.stringCol('FX_FORWARD_ID', nulls(nn)),
        TableTools.stringCol('SWAP_ID', nulls(nn)),
        TableTools.stringCol('CDS_ID', nulls(nn)),
        TableTools.stringCol('STRUCTURED_PRODUCT_ID', nulls(nn)),
        TableTools.stringCol('ETF_ID', nulls(nn)),
        TableTools.stringCol('CURRENCY_CODE', (['USD'] * nn) as String[]),
        TableTools.stringCol('EXCHANGE_MIC', names.collect { it.exchange } as String[]),
        TableTools.stringCol('SECTOR_ID', names.collect { it.sectorId } as String[]))

int ne = equities.size()
EQUITY = TableTools.newTable(
        TableTools.stringCol('EQUITY_ID', equities.collect { it.equityId } as String[]),
        TableTools.stringCol('TICKER', equities.collect { it.sym } as String[]),
        TableTools.stringCol('COMPANY_NAME', equities.collect { it.name + ' Inc.' } as String[]),
        TableTools.stringCol('SHARE_CLASS', (['COMMON'] * ne) as String[]),
        TableTools.doubleCol('MARKET_CAP', equities.collect { it.shares * it.prevClose } as double[]),
        TableTools.doubleCol('SHARES_OUTSTANDING', equities.collect { (double) it.shares } as double[]),
        // the facts the simulation does not move, from a draw of their own: a yield to 4%, an earnings multiple
        TableTools.doubleCol('DIVIDEND_YIELD', equities.collect { Math.round(facts.nextDouble() * 400) / 10000d } as double[]),
        TableTools.doubleCol('PE_RATIO', equities.collect { Math.round((8 + facts.nextDouble() * 32) * 10) / 10d } as double[]),
        TableTools.doubleCol('BETA', equities.collect { Math.round(it.beta * 100) / 100d } as double[]),
        TableTools.stringCol('PRIMARY_EXCHANGE', equities.collect { it.exchange } as String[]),
        TableTools.col('IS_ADR', ([Boolean.FALSE] * ne) as Boolean[]),
        TableTools.intCol('LOT_SIZE', ([100] * ne) as int[]),
        TableTools.doubleCol('LAST_PRICE', equities.collect { it.prevClose } as double[]),
        TableTools.col('PRICE_DATE', ([today.minusDays(1)] * ne) as java.time.LocalDate[]),
        TableTools.stringCol('INSTRUMENT_ID', equities.collect { it.instrumentId } as String[]))

// the market, ticking: each name's latest quote, and the tape
quotePublisher = TablePublisher.of('quotes', TableDefinition.of(
        ColumnDefinition.ofString('QUOTE_ID'), ColumnDefinition.ofTime('QUOTE_TIME'), ColumnDefinition.ofDouble('BID'),
        ColumnDefinition.ofDouble('ASK'), ColumnDefinition.ofDouble('MID'), ColumnDefinition.ofDouble('LAST'),
        ColumnDefinition.ofDouble('VOLUME'), ColumnDefinition.ofDouble('OPEN_PRICE'),
        ColumnDefinition.ofDouble('HIGH_PRICE'), ColumnDefinition.ofDouble('LOW_PRICE'),
        ColumnDefinition.ofDouble('CLOSE_PRICE'), ColumnDefinition.ofString('SOURCE'),
        ColumnDefinition.ofBoolean('IS_DELAYED'), ColumnDefinition.ofString('SNAPSHOT_ID'),
        ColumnDefinition.ofString('INSTRUMENT_ID'), ColumnDefinition.ofDouble('BID_SIZE'),
        ColumnDefinition.ofDouble('ASK_SIZE')), NO_FLUSH, NO_SHUTDOWN)
MARKET_QUOTE = quotePublisher.table().lastBy('QUOTE_ID')

tradePublisher = TablePublisher.of('trades', TableDefinition.of(
        ColumnDefinition.ofString('PRINT_ID'), ColumnDefinition.ofInt('SEQUENCE_NUMBER'),
        ColumnDefinition.ofTime('PRINT_TIME'), ColumnDefinition.ofDouble('PRICE'), ColumnDefinition.ofDouble('QUANTITY'),
        ColumnDefinition.ofString('AGGRESSOR_SIDE'), ColumnDefinition.ofString('VENUE'),
        ColumnDefinition.ofString('TRADE_CONDITION'), ColumnDefinition.ofBoolean('IS_OFF_EXCHANGE'),
        ColumnDefinition.ofString('INSTRUMENT_ID')), NO_FLUSH, NO_SHUTDOWN)
MARKET_TRADE = HISTORY > 0 ? RingTableTools.of(tradePublisher.table(), HISTORY)
        : BlinkTableTools.blinkToAppendOnly(tradePublisher.table())

// the newswire (K15): every story kept, in the order it crossed
newsPublisher = TablePublisher.of('news', TableDefinition.of(
        ColumnDefinition.ofString('STORY_ID'), ColumnDefinition.ofTime('PUBLISH_TIME'),
        ColumnDefinition.ofString('HEADLINE'), ColumnDefinition.ofString('BODY'), ColumnDefinition.ofString('SOURCE'),
        ColumnDefinition.ofString('CATEGORY'), ColumnDefinition.ofString('URGENCY'), ColumnDefinition.ofDouble('SENTIMENT'),
        ColumnDefinition.ofString('TICKERS'), ColumnDefinition.ofString('REGION'),
        ColumnDefinition.ofString('INSTRUMENT_ID'), ColumnDefinition.ofString('SECTOR_ID')), NO_FLUSH, NO_SHUTDOWN)
NEWS_STORY = BlinkTableTools.blinkToAppendOnly(newsPublisher.table())

worldPublisher = TablePublisher.of('world', TableDefinition.of(
        ColumnDefinition.ofString('item'), ColumnDefinition.ofString('state'), ColumnDefinition.ofString('detail')),
        NO_FLUSH, NO_SHUTDOWN)
world = worldPublisher.table().lastBy('item')

// ---------------------------------------------------------------- the clock: a beat every 20 ms

simulator = new Simulator(random, equities, SECTORS.size(), QUOTES, TRADES, seq)
// the newswire on its own random, so the prices' draws are the simulation's as they were
newsroom = new Newsroom(new Random(Long.parseLong(env.getOrDefault('MARKET_SEED', '20261024')) + 2L), equities, coins,
        GICS.collect { 'SEC-' + it }, NEWS)
simulator.news = newsroom
simulator.jumpsADay = JUMPS
// THE MORNING (K14): the session opened MORNING seconds ago, each equity's morning on the tape before the clock starts;
// crypto's day is the feed's (or the simulator's from now), never made up before the world began
sessionOpen = Instant.now().minusMillis((long) (MORNING * 1000d))
if (MORNING >= 1d) {
    double thin = Math.min(1d, MORNING_PRINTS / (TRADES * simulator.morningPace(MORNING)))
    long t0 = System.nanoTime()
    MorningRows morning = simulator.morning(MORNING, sessionOpen, thin)
    tradePublisher.add(morning.table())
    if (newsroom.pending()) newsPublisher.add(newsroom.table())
    // each name's quote published at the clock's first beat, its day so far the morning's
    equities.each { Name n -> n.quoted = false }
    System.out.println(String.format('the market world\'s morning: the open %s, %,d prints (a %.3f share of its pace, each %.1f times a print) in %.1f s',
            sessionOpen, morning.size, thin, 1d / thin, (System.nanoTime() - t0) / 1e9d))
}
// a coin's activity on its own scale: the simulator's rates are the equities'; coins quote and trade a tenth as often
coins.each { it.weight = it.weight / 10d }
marketWorld = new World(simulator, feed, quotePublisher, tradePublisher, worldPublisher, seq,
        String.format('%d names, %,.0f quotes and %,.0f trades a second, the session open since %s', NAMES, QUOTES, TRADES,
                sessionOpen.toString().replaceFirst('\\.\\d+Z$', 'Z')))
(equities + coins).each { marketWorld.bySym.put(it.sym, it) }
marketWorld.news = newsPublisher
marketWorld.newsroom = newsroom
marketWorld.newsDetail = String.format('%.0f stories a minute besides each name\'s jump, each where its move is', NEWS)
if (FILINGS_AGENT != null && !FILINGS_AGENT.trim().isEmpty()) {
    marketWorld.filings = new EdgarFeed(URI.create(FILINGS_URL), FILINGS_AGENT.trim())
    filingsClock = Executors.newSingleThreadScheduledExecutor { r -> Thread t = new Thread(r, 'market-filings'); t.daemon = true; t }
    filingsClock.scheduleWithFixedDelay({ marketWorld.filings.poll() } as Runnable, 0L, 60L, TimeUnit.SECONDS)
}
marketWorld.equities.addAll(equities)
marketWorld.coins.addAll(coins)
marketClock = Executors.newSingleThreadScheduledExecutor { r -> Thread t = new Thread(r, 'market-world'); t.daemon = true; t }
marketClock.scheduleAtFixedRate({ marketWorld.beat() } as Runnable, 0L, 20L, TimeUnit.MILLISECONDS)

// ---------------------------------------------------------------- the bank's own day (K9-K13)
//
// Its reference: a business unit, six desks (four over the market's sectors and crypto, equity derivatives, FX), three
// traders a desk, its books, each book's positions at the prior close, each desk's limits, the FX desk's clients; the
// listed options on the largest names, each an instrument of its own; and, ticking, its orders and fills, its FX
// prices and the clients' requests (the Day). Every table the corpus's, every column it declares (30-store.pure).
//   MARKET_ORDERS  parent orders a second over the equity and crypto books (0.8)
//   MARKET_RFQS    clients' requests for a quote a second (2)

ORDERS_PER_SECOND = Double.parseDouble(env.getOrDefault('MARKET_ORDERS', '0.8'))
RFQS_PER_SECOND = Double.parseDouble(env.getOrDefault('MARKET_RFQS', '2'))
bank = new Random(Long.parseLong(env.getOrDefault('MARKET_SEED', '20261024')) + 2L)
NULL_D = io.deephaven.util.QueryConstants.NULL_DOUBLE
NULL_I = io.deephaven.util.QueryConstants.NULL_INT
yesterday = today.minusDays(1)

BUSINESS_UNIT = TableTools.newTable(
        TableTools.stringCol('BU_ID', 'BU-MARKETS'), TableTools.stringCol('NAME', 'Global Markets'),
        TableTools.stringCol('HEAD', 'TR-01'), TableTools.stringCol('DIVISION', 'Markets'),
        TableTools.col('IS_ACTIVE', Boolean.TRUE), TableTools.intCol('HEADCOUNT', 18),
        TableTools.doubleCol('BUDGET', 4.2e7d), TableTools.stringCol('COST_CENTER', 'CC-100'),
        TableTools.stringCol('REGION', 'AMER'), TableTools.col('ESTABLISHED_DATE', java.time.LocalDate.of(2009, 1, 5)),
        TableTools.stringCol('DESCRIPTION', 'Sales and trading'), TableTools.stringCol('LEGAL_ENTITY_ID', 'LE-US'))

// the desks: each its sectors (none: equity derivatives, FX), its gross limit and day's P&L target
DESKS = [
        [id: 'DESK-TMT', name: 'TMT', asset: 'EQUITY', sectors: ['Technology', 'Communication Services']],
        [id: 'DESK-HCF', name: 'Healthcare & Financials', asset: 'EQUITY', sectors: ['Health Care', 'Financials', 'Real Estate']],
        [id: 'DESK-CYC', name: 'Cyclicals & Defensives', asset: 'EQUITY',
         sectors: ['Consumer Discretionary', 'Industrials', 'Materials', 'Energy', 'Consumer Staples', 'Utilities']],
        [id: 'DESK-DIG', name: 'Digital Assets', asset: 'CRYPTO', sectors: ['Crypto']],
        [id: 'DESK-EQD', name: 'Equity Derivatives', asset: 'EQUITY_DERIVATIVES', sectors: []],
        [id: 'DESK-FX', name: 'FX e-Trading', asset: 'FX', sectors: []]]
FIRST = ['Ava', 'Ben', 'Chloe', 'Dev', 'Elena', 'Farid', 'Grace', 'Hiro', 'Isla', 'Jonah', 'Kira', 'Leo', 'Maya', 'Nikhil',
         'Olivia', 'Pablo', 'Quinn', 'Rosa']
LAST = ['Adler', 'Brooks', 'Chen', 'Dubois', 'Evans', 'Fischer', 'Gupta', 'Hayes', 'Ito', 'Jensen', 'Kowalski', 'Lopez',
        'Mensah', 'Novak', 'Okafor', 'Petrov', 'Quint', 'Rossi']
SENIORITY = ['MD', 'VP', 'Associate']
traderIds = []
deskTraders = [:]
DESKS.eachWithIndex { d, i ->
    deskTraders[d.id] = (0..2).collect { j -> String.format('TR-%02d', i * 3 + j + 1) }
    traderIds.addAll(deskTraders[d.id])
}
int nt = traderIds.size()
TRADER = TableTools.newTable(
        TableTools.stringCol('TRADER_ID', traderIds as String[]),
        TableTools.stringCol('FIRST_NAME', (0..<nt).collect { FIRST[it] } as String[]),
        TableTools.stringCol('LAST_NAME', (0..<nt).collect { LAST[it] } as String[]),
        TableTools.stringCol('BADGE', (0..<nt).collect { String.format('B%05d', 41000 + it * 37) } as String[]),
        TableTools.stringCol('EMAIL', (0..<nt).collect { (FIRST[it] + '.' + LAST[it]).toLowerCase() + '@bank.example' } as String[]),
        TableTools.col('HIRE_DATE', (0..<nt).collect { java.time.LocalDate.of(2012 + it % 12, 1 + it % 12, 1 + it % 27) } as java.time.LocalDate[]),
        TableTools.stringCol('SENIORITY', (0..<nt).collect { SENIORITY[it % 3] } as String[]),
        TableTools.col('IS_ACTIVE', ([Boolean.TRUE] * nt) as Boolean[]),
        TableTools.stringCol('SPECIALIZATION', (0..<nt).collect { DESKS[it.intdiv(3)].name } as String[]),
        TableTools.stringCol('LICENSE', (['Series 7'] * nt) as String[]),
        TableTools.doubleCol('MAX_NOTIONAL', (0..<nt).collect { [5e7d, 2e7d, 1e7d][it % 3] } as double[]),
        TableTools.doubleCol('PNL_YTD', (0..<nt).collect { Math.round(bank.nextGaussian() * 2e6d) as double } as double[]),
        TableTools.stringCol('PHONE', new String[nt]),
        TableTools.stringCol('DESK_ID', (0..<nt).collect { DESKS[it.intdiv(3)].id } as String[]),
        TableTools.stringCol('MANAGER_ID', (0..<nt).collect { it % 3 == 0 ? null : traderIds[it - it % 3] } as String[]))

// the books: two an equity desk (making markets and taking a view), one for crypto, two for derivatives, one for FX
BOOKS = []
DESKS.each { d ->
    def kinds = d.asset == 'EQUITY' ? [['MM', 'market making', 0.8d], ['DIR', 'directional', 1.6d]]
            : d.asset == 'CRYPTO' ? [['MM', 'market making', 1.0d]]
            : d.asset == 'FX' ? [['FLOW', 'client flow', 1.0d]]
            : [['VOL', 'volatility', 1.0d], ['FLOW', 'client flow', 1.0d]]
    kinds.each { k -> BOOKS << [id: 'BK-' + d.id.substring(5) + '-' + k[0], name: d.name + ' ' + k[1], strategy: k[1], desk: d, scale: k[2]] }
}

// the options: a listed chain on the four largest names -- three expiries, nine strikes about the prior close, calls and
// puts -- each an instrument of its own; its settlement at the prior close by Black and Scholes on its implied volatility
// (a smile: dearer away from the money, puts dearer than calls)
RATE = 0.05d
optionRows = []
equities.take(4).each { Name u ->
    double step = u.prevClose >= 200 ? 5d : u.prevClose >= 50 ? 2.5d : 1d
    double atm = Math.round(u.prevClose / step) * step
    [7, 30, 91].each { int days ->
        java.time.LocalDate expiry = today.plusDays(days)
        (-4..4).each { int j ->
            double strike = atm + j * step * Math.max(1d, Math.round(u.prevClose * 0.05d / step))
            if (strike <= 0) return
            ['CALL', 'PUT'].each { String type ->
                double m = Math.log(strike / u.prevClose)
                double iv = Math.round(u.vol * Math.sqrt(252d) * (1d + 1.2d * m * m - 0.6d * m) * (1d + 0.05d * Math.sqrt(days / 30d)) * 10000d) / 10000d
                double t = days / 365d
                // a listed option trades at a tick at least
                double premium = Math.max(0.01d, Math.round(Black.price(type == 'CALL', u.prevClose, strike, t, iv, RATE) * 100d) / 100d)
                String id = String.format('%s-%s-%s-%s', u.sym, expiry.toString().replace('-', ''), type.substring(0, 1),
                        strike == Math.floor(strike) ? String.valueOf((long) strike) : String.valueOf(strike))
                optionRows << [id: 'OPT-' + id, ticker: id, type: type, strike: strike, expiry: expiry, premium: premium,
                               iv: iv, itm: type == 'CALL' ? u.prevClose > strike : u.prevClose < strike, underlying: u,
                               instrument: 'INST-OPT-' + id, oi: 100 + bank.nextInt(20000)]
            }
        }
    }
}
int no = optionRows.size()
OPTION = TableTools.newTable(
        TableTools.stringCol('OPTION_ID', optionRows.collect { it.id } as String[]),
        TableTools.stringCol('OPTION_TYPE', optionRows.collect { it.type } as String[]),
        TableTools.doubleCol('STRIKE_PRICE', optionRows.collect { it.strike } as double[]),
        TableTools.col('EXPIRATION_DATE', optionRows.collect { it.expiry } as java.time.LocalDate[]),
        TableTools.doubleCol('PREMIUM', optionRows.collect { it.premium } as double[]),
        TableTools.doubleCol('IMPLIED_VOLATILITY', optionRows.collect { it.iv } as double[]),
        // the source's own greeks are its end of day's: the screens compute them live, so none here
        TableTools.doubleCol('DELTA', ([NULL_D] * no) as double[]),
        TableTools.doubleCol('GAMMA', ([NULL_D] * no) as double[]),
        TableTools.doubleCol('THETA', ([NULL_D] * no) as double[]),
        TableTools.doubleCol('VEGA', ([NULL_D] * no) as double[]),
        TableTools.stringCol('EXERCISE_STYLE', (['AMERICAN'] * no) as String[]),
        TableTools.col('IS_IN_THE_MONEY', optionRows.collect { it.itm } as Boolean[]),
        TableTools.doubleCol('UNDERLYING_PRICE', optionRows.collect { ((Name) it.underlying).prevClose } as double[]),
        TableTools.intCol('CONTRACT_SIZE', ([100] * no) as int[]),
        TableTools.intCol('OPEN_INTEREST', optionRows.collect { it.oi } as int[]),
        TableTools.stringCol('UNDERLYING_ID', optionRows.collect { ((Name) it.underlying).instrumentId } as String[]),
        TableTools.stringCol('INSTRUMENT_ID', optionRows.collect { it.instrument } as String[]))
// each option an instrument: its own row beside the names'
INSTRUMENT = TableTools.merge(INSTRUMENT, TableTools.newTable(
        TableTools.stringCol('INSTRUMENT_ID', optionRows.collect { it.instrument } as String[]),
        TableTools.stringCol('TICKER', optionRows.collect { it.ticker } as String[]),
        TableTools.stringCol('NAME', optionRows.collect { ((Name) it.underlying).sym + ' ' + it.expiry + ' ' + it.strike + ' ' + it.type } as String[]),
        TableTools.stringCol('ISIN', nulls(no)), TableTools.stringCol('CUSIP', nulls(no)), TableTools.stringCol('SEDOL', nulls(no)),
        TableTools.stringCol('ASSET_CLASS', (['OPTION'] * no) as String[]),
        TableTools.stringCol('PRODUCT_TYPE', (['LISTED_OPTION'] * no) as String[]),
        TableTools.col('IS_ACTIVE', ([Boolean.TRUE] * no) as Boolean[]),
        TableTools.col('LISTING_DATE', new java.time.LocalDate[no]),
        TableTools.col('MATURITY_DATE', optionRows.collect { it.expiry } as java.time.LocalDate[]),
        TableTools.doubleCol('NOTIONAL_AMOUNT', ([NULL_D] * no) as double[]),
        TableTools.stringCol('CURRENCY', (['USD'] * no) as String[]),
        TableTools.stringCol('DESCRIPTION', optionRows.collect { ((Name) it.underlying).sector } as String[]),
        TableTools.col('ISSUE_DATE', new java.time.LocalDate[no]),
        TableTools.stringCol('EQUITY_ID', nulls(no)), TableTools.stringCol('BOND_ID', nulls(no)),
        TableTools.stringCol('FUTURE_ID', nulls(no)), TableTools.stringCol('FX_FORWARD_ID', nulls(no)),
        TableTools.stringCol('SWAP_ID', nulls(no)), TableTools.stringCol('CDS_ID', nulls(no)),
        TableTools.stringCol('STRUCTURED_PRODUCT_ID', nulls(no)), TableTools.stringCol('ETF_ID', nulls(no)),
        TableTools.stringCol('CURRENCY_CODE', (['USD'] * no) as String[]),
        TableTools.stringCol('EXCHANGE_MIC', (['XCBO'] * no) as String[]),
        TableTools.stringCol('SECTOR_ID', optionRows.collect { ((Name) it.underlying).sectorId } as String[])))

// each book's positions at the prior close: an equity book holds names of its desk's sectors (a market maker small
// and two-way, a directional book larger and mostly long); crypto's its coins; derivatives' the options
positionRows = []
BOOKS.each { b ->
    List candidates = b.desk.asset == 'EQUITY' ? equities.findAll { ((List) b.desk.sectors).contains(it.sector) }.take(60)
            : b.desk.asset == 'CRYPTO' ? coins : b.desk.asset == 'EQUITY_DERIVATIVES' ? optionRows : []
    Collections.shuffle(candidates = new ArrayList(candidates), bank)
    candidates.take(b.desk.asset == 'EQUITY' ? 30 : candidates.size() < 40 ? candidates.size() : 40).each { held ->
        boolean option = held instanceof Map
        double px = option ? (double) held.premium : ((Name) held).prevClose
        double unit = option ? 100d : 1d
        double gross = (b.desk.asset == 'EQUITY' ? 2.5e6d : b.desk.asset == 'CRYPTO' ? 1.5e6d : 2.0e5d) * (double) b.scale * Math.exp(bank.nextGaussian() * 0.6d)
        boolean longSide = bank.nextDouble() < (b.strategy == 'directional' ? 0.75d : 0.5d)
        double qty = Math.max(option ? 1d : 1d, Math.round(gross / Math.max(0.01d, px * unit)))
        if (!option && ((Name) held).kind == 'EQUITY') qty = Math.max(100d, Math.round(qty / 100d) * 100d)
        if (option || ((Name) held).kind != 'EQUITY') qty = Math.max(1d, Math.round(qty))
        qty = longSide ? qty : -qty
        double cost = Math.round(px * (1d + bank.nextGaussian() * 0.04d) * 100d) / 100d
        positionRows << [id: String.format('POS-%05d', positionRows.size() + 1), qty: qty, cost: cost, px: px, unit: unit,
                         book: b.id, instrument: option ? held.instrument : ((Name) held).instrumentId]
    }
}
int np = positionRows.size()
POSITION = TableTools.newTable(
        TableTools.stringCol('POSITION_ID', positionRows.collect { it.id } as String[]),
        TableTools.doubleCol('QUANTITY', positionRows.collect { it.qty } as double[]),
        TableTools.doubleCol('AVERAGE_COST', positionRows.collect { it.cost } as double[]),
        TableTools.doubleCol('MARKET_VALUE', positionRows.collect { it.qty * it.px * it.unit } as double[]),
        TableTools.doubleCol('UNREALIZED_PN_L', positionRows.collect { it.qty * (it.px - it.cost) * it.unit } as double[]),
        TableTools.doubleCol('REALIZED_PN_L', positionRows.collect { Math.round(bank.nextGaussian() * 2e4d) as double } as double[]),
        TableTools.stringCol('CURRENCY', (['USD'] * np) as String[]),
        TableTools.col('POSITION_DATE', ([yesterday] * np) as java.time.LocalDate[]),
        TableTools.stringCol('DIRECTION', positionRows.collect { it.qty >= 0 ? 'LONG' : 'SHORT' } as String[]),
        TableTools.doubleCol('COST_BASIS', positionRows.collect { it.qty * it.cost * it.unit } as double[]),
        TableTools.doubleCol('ACCRUED_INTEREST', ([NULL_D] * np) as double[]),
        TableTools.instantCol('LAST_UPDATED', ([Instant.now()] * np) as Instant[]),
        TableTools.col('IS_OPEN', ([Boolean.TRUE] * np) as Boolean[]),
        TableTools.col('SETTLEMENT_DATE', new java.time.LocalDate[np]),
        TableTools.stringCol('BOOK_ID', positionRows.collect { it.book } as String[]),
        TableTools.stringCol('PORTFOLIO_ID', nulls(np)),
        TableTools.stringCol('INSTRUMENT_ID', positionRows.collect { it.instrument } as String[]))

// each desk's gross at the prior close, by which its limits are set
deskGross = DESKS.collectEntries { d -> [d.id, (positionRows.findAll { p -> BOOKS.find { it.id == p.book }.desk.id == d.id }
        .sum { Math.abs(it.qty * it.px * it.unit) } ?: 0d) as double] }
int nb = BOOKS.size()
BOOK = TableTools.newTable(
        TableTools.stringCol('BOOK_ID', BOOKS.collect { it.id } as String[]),
        TableTools.stringCol('NAME', BOOKS.collect { it.name } as String[]),
        TableTools.stringCol('BOOK_TYPE', (['TRADING'] * nb) as String[]),
        TableTools.stringCol('CURRENCY', (['USD'] * nb) as String[]),
        TableTools.col('IS_ACTIVE', ([Boolean.TRUE] * nb) as Boolean[]),
        TableTools.col('OPEN_DATE', ([java.time.LocalDate.of(2020, 1, 2)] * nb) as java.time.LocalDate[]),
        TableTools.col('CLOSE_DATE', new java.time.LocalDate[nb]),
        TableTools.stringCol('LEGAL_ENTITY', (['LE-US'] * nb) as String[]),
        TableTools.stringCol('STRATEGY', BOOKS.collect { it.strategy } as String[]),
        TableTools.doubleCol('PNL_YTD', BOOKS.collect { Math.round(bank.nextGaussian() * 3e6d) as double } as double[]),
        TableTools.intCol('POSITION_COUNT', BOOKS.collect { b -> positionRows.count { it.book == b.id } } as int[]),
        // a book's own day loss limit: a share of its desk's
        TableTools.doubleCol('RISK_LIMIT', BOOKS.collect { b -> Math.max(5e4d, Math.round(deskGross[b.desk.id] * 0.002d / 1e4d) * 1e4d) } as double[]),
        TableTools.stringCol('DESCRIPTION', nulls(nb)),
        TableTools.stringCol('DESK_ID', BOOKS.collect { it.desk.id } as String[]))

int nd = DESKS.size()
DESK = TableTools.newTable(
        TableTools.stringCol('DESK_ID', DESKS.collect { it.id } as String[]),
        TableTools.stringCol('NAME', DESKS.collect { it.name } as String[]),
        TableTools.stringCol('DESK_TYPE', (['TRADING'] * nd) as String[]),
        TableTools.stringCol('ASSET_CLASS', DESKS.collect { it.asset } as String[]),
        TableTools.stringCol('REGION', (['AMER'] * nd) as String[]),
        TableTools.col('IS_ACTIVE', ([Boolean.TRUE] * nd) as Boolean[]),
        TableTools.stringCol('HEAD_TRADER', DESKS.collect { deskTraders[it.id][0] } as String[]),
        TableTools.stringCol('COST_CENTER', (0..<nd).collect { 'CC-' + (110 + it) } as String[]),
        TableTools.stringCol('FLOOR', (['7'] * nd) as String[]),
        TableTools.stringCol('BUILDING', (['200 Market Street'] * nd) as String[]),
        TableTools.stringCol('PHONE', new String[nd]),
        TableTools.doubleCol('RISK_LIMIT', DESKS.collect { Math.round(deskGross[it.id] * 1.5d / 1e6d) * 1e6d } as double[]),
        TableTools.doubleCol('PNL_TARGET', DESKS.collect { Math.round(deskGross[it.id] * 0.0005d / 1e3d) * 1e3d } as double[]),
        TableTools.stringCol('BUSINESS_UNIT_ID', (['BU-MARKETS'] * nd) as String[]))

// each desk's limits: its day's loss, set by how far its book moves in a day (a quarter of a percent of an equity desk's
// gross, about a bad day's; crypto's and options' moves are wider), so a bad day breaches it; and its gross notional,
// with some room above the prior close's
limitRows = []
DESKS.each { d ->
    double gross = deskGross[d.id] as double
    if (gross <= 0) return
    double loss = d.asset == 'CRYPTO' ? 0.012d : d.asset == 'EQUITY_DERIVATIVES' ? 0.04d : 0.0025d
    limitRows << [id: 'LIM-' + d.id.substring(5) + '-LOSS', type: 'DAY_LOSS', amount: Math.max(2.5e4d, Math.round(gross * loss / 1e4d) * 1e4d), desk: d.id]
    limitRows << [id: 'LIM-' + d.id.substring(5) + '-GROSS', type: 'GROSS_NOTIONAL', amount: Math.round(gross * (1.1d + 0.5d * bank.nextDouble()) / 1e6d) * 1e6d, desk: d.id]
}
int nl = limitRows.size()
RISK_LIMIT = TableTools.newTable(
        TableTools.stringCol('LIMIT_ID', limitRows.collect { it.id } as String[]),
        TableTools.stringCol('LIMIT_TYPE', limitRows.collect { it.type } as String[]),
        TableTools.doubleCol('LIMIT_AMOUNT', limitRows.collect { it.amount } as double[]),
        // the source's utilisation is its end of day's: the screens compute it live
        TableTools.doubleCol('CURRENT_UTILIZATION', ([NULL_D] * nl) as double[]),
        TableTools.doubleCol('UTILIZATION_PCT', ([NULL_D] * nl) as double[]),
        TableTools.stringCol('CURRENCY', (['USD'] * nl) as String[]),
        TableTools.col('EFFECTIVE_DATE', ([java.time.LocalDate.of(2026, 1, 2)] * nl) as java.time.LocalDate[]),
        TableTools.col('EXPIRY_DATE', new java.time.LocalDate[nl]),
        TableTools.stringCol('STATUS', (['ACTIVE'] * nl) as String[]),
        TableTools.stringCol('APPROVED_BY', (['CRO'] * nl) as String[]),
        TableTools.col('APPROVAL_DATE', ([java.time.LocalDate.of(2025, 12, 15)] * nl) as java.time.LocalDate[]),
        TableTools.doubleCol('WARNING_THRESHOLD', ([0.8d] * nl) as double[]),
        TableTools.doubleCol('HARD_LIMIT', limitRows.collect { it.amount * 1.2d } as double[]),
        TableTools.col('IS_ACTIVE', ([Boolean.TRUE] * nl) as Boolean[]),
        TableTools.stringCol('DESK_ID', limitRows.collect { it.desk } as String[]))

// the FX desk's clients: hedge funds, real money, corporates and banks, each an edge the desk prices it with
CLIENTS = [['Aurora Capital', 'HEDGE_FUND', 'T1'], ['Bluewater Partners', 'HEDGE_FUND', 'T1'], ['Cinder Asset Mgmt', 'ASSET_MANAGER', 'T1'],
           ['Driftwood Pension', 'PENSION', 'T2'], ['Eastgate Insurance', 'INSURER', 'T2'], ['Fjord Shipping', 'CORPORATE', 'T3'],
           ['Galena Metals', 'CORPORATE', 'T3'], ['Halcyon Fund', 'HEDGE_FUND', 'T2'], ['Ironbark Bank', 'BANK', 'T2'],
           ['Jasper Retail', 'CORPORATE', 'T3'], ['Kestrel Macro', 'HEDGE_FUND', 'T1'], ['Larch Endowment', 'ASSET_MANAGER', 'T2'],
           ['Monarch Airlines', 'CORPORATE', 'T3'], ['Nimbus Tech', 'CORPORATE', 'T3'], ['Opal Sovereign', 'SOVEREIGN', 'T1'],
           ['Pelican Bank', 'BANK', 'T2'], ['Quartz Quant', 'HEDGE_FUND', 'T1'], ['Rowan Family Office', 'ASSET_MANAGER', 'T3'],
           ['Sable Commodities', 'CORPORATE', 'T2'], ['Tern Global', 'ASSET_MANAGER', 'T2']]
int nc = CLIENTS.size()
clientIds = (0..<nc).collect { String.format('CP-%03d', it + 1) }
COUNTERPARTY = TableTools.newTable(
        TableTools.stringCol('COUNTERPARTY_ID', clientIds as String[]),
        TableTools.stringCol('LEGAL_NAME', CLIENTS.collect { it[0] + ' Ltd' } as String[]),
        TableTools.stringCol('SHORT_NAME', CLIENTS.collect { it[0] } as String[]),
        TableTools.stringCol('LEI', (0..<nc).collect { String.format('5493%016d', 7_000_000_000L + it * 7919L) } as String[]),
        TableTools.stringCol('COUNTERPARTY_TYPE', CLIENTS.collect { it[1] } as String[]),
        TableTools.stringCol('DOMICILE', (0..<nc).collect { ['US', 'GB', 'DE', 'SG', 'JP'][it % 5] } as String[]),
        TableTools.stringCol('INCORPORATION_COUNTRY', (0..<nc).collect { ['US', 'GB', 'DE', 'SG', 'JP'][it % 5] } as String[]),
        TableTools.col('IS_ACTIVE', ([Boolean.TRUE] * nc) as Boolean[]),
        TableTools.col('ONBOARD_DATE', (0..<nc).collect { java.time.LocalDate.of(2015 + it % 10, 1 + it % 12, 1) } as java.time.LocalDate[]),
        TableTools.stringCol('CREDIT_RATING', (0..<nc).collect { ['AA', 'A', 'BBB'][it % 3] } as String[]),
        TableTools.stringCol('RATING_AGENCY', (['S&P'] * nc) as String[]),
        TableTools.stringCol('TIER', CLIENTS.collect { it[2] } as String[]),
        TableTools.stringCol('PARENT_ENTITY_ID', nulls(nc)),
        TableTools.stringCol('INDUSTRY', CLIENTS.collect { it[1] } as String[]),
        TableTools.stringCol('WEBSITE', nulls(nc)),
        TableTools.stringCol('CLASSIFICATION_ID', nulls(nc)),
        TableTools.stringCol('COUNTRY_CODE', (0..<nc).collect { ['US', 'GB', 'DE', 'SG', 'JP'][it % 5] } as String[]))
// a first-tier client is priced tightest
clientEdges = CLIENTS.collect { it[2] == 'T1' ? 0.5d : it[2] == 'T2' ? 0.2d : 0d }

// the pairs the FX desk prices: the majors, and three crosses quoted about their legs' product (the screen's cross by a
// live join reads the difference); a cross's volatility is its legs' (its own column unused)
PAIRS = [['EUR/USD', 'EUR', 1.0850d, 0.0001d, 0.3d, 0.0055d, 0.30d], ['GBP/USD', 'GBP', 1.2700d, 0.0001d, 0.6d, 0.0065d, 0.12d],
         ['USD/JPY', 'USD', 151.20d, 0.01d, 0.4d, 0.0070d, 0.15d], ['USD/CHF', 'USD', 0.8800d, 0.0001d, 0.8d, 0.0060d, 0.05d],
         ['AUD/USD', 'AUD', 0.6550d, 0.0001d, 0.6d, 0.0080d, 0.08d], ['USD/CAD', 'USD', 1.3750d, 0.0001d, 0.8d, 0.0055d, 0.07d],
         ['NZD/USD', 'NZD', 0.6000d, 0.0001d, 1.0d, 0.0085d, 0.04d], ['EUR/GBP', 'EUR', 0.8543d, 0.0001d, 0.8d, 0.0045d, 0.07d],
         ['EUR/JPY', 'EUR', 164.05d, 0.01d, 1.0d, 0.0080d, 0.07d], ['GBP/JPY', 'GBP', 192.02d, 0.01d, 1.5d, 0.0095d, 0.05d]]
pairs = PAIRS.collect { p -> new Pair(pair: p[0], base: p[1], mid: p[2], prevClose: (p[2] as double) * Math.exp(bank.nextGaussian() * 0.003d),
        pip: p[3], spreadPips: p[4], vol: p[5], weight: p[6]) }
// each cross's legs, and its mid and prior close its legs': EUR/GBP as EUR/USD over GBP/USD, the yen crosses as their
// dollar legs' product
pairOf = pairs.collectEntries { Pair p -> [(p.pair): p] }
[['EUR/GBP', 'EUR/USD', 'GBP/USD', true], ['EUR/JPY', 'EUR/USD', 'USD/JPY', false], ['GBP/JPY', 'GBP/USD', 'USD/JPY', false]].each { c ->
    Pair p = pairOf[c[0]] as Pair
    p.leg1 = pairOf[c[1]] as Pair
    p.leg2 = pairOf[c[2]] as Pair
    p.divide = c[3] as boolean
    p.mid = p.implied()
    p.prevClose = p.divide ? p.leg1.prevClose / p.leg2.prevClose : p.leg1.prevClose * p.leg2.prevClose
}

orderPublisher = TablePublisher.of('orders', TableDefinition.of(
        ColumnDefinition.ofString('ORDER_ID'), ColumnDefinition.fromGenericType('ORDER_DATE', java.time.LocalDate),
        ColumnDefinition.ofString('SIDE'), ColumnDefinition.ofDouble('QUANTITY'), ColumnDefinition.ofDouble('FILLED_QUANTITY'),
        ColumnDefinition.ofDouble('REMAINING_QUANTITY'), ColumnDefinition.ofDouble('LIMIT_PRICE'),
        ColumnDefinition.ofDouble('STOP_PRICE'), ColumnDefinition.ofString('TIME_IN_FORCE'), ColumnDefinition.ofString('STATUS'),
        ColumnDefinition.ofInt('PRIORITY'), ColumnDefinition.ofTime('CREATED_TIME'), ColumnDefinition.ofTime('LAST_MODIFIED_TIME'),
        ColumnDefinition.fromGenericType('EXPIRY_DATE', java.time.LocalDate),
        ColumnDefinition.ofBoolean('IS_ALGO_ORDER'), ColumnDefinition.ofString('ALGO_STRATEGY'),
        ColumnDefinition.ofString('INSTRUMENT_ID'), ColumnDefinition.ofString('TRADER_ID'), ColumnDefinition.ofString('BOOK_ID')),
        NO_FLUSH, NO_SHUTDOWN)
TRADE_ORDER = orderPublisher.table().lastBy('ORDER_ID')
executionPublisher = TablePublisher.of('executions', TableDefinition.of(
        ColumnDefinition.ofString('EXECUTION_ID'), ColumnDefinition.ofTime('EXECUTION_TIME'),
        ColumnDefinition.ofDouble('FILLED_QUANTITY'), ColumnDefinition.ofDouble('FILL_PRICE'), ColumnDefinition.ofString('VENUE'),
        ColumnDefinition.ofString('BROKER'), ColumnDefinition.ofDouble('COMMISSION'), ColumnDefinition.ofString('FILL_TYPE'),
        ColumnDefinition.ofString('EXECUTION_ALGO'), ColumnDefinition.ofDouble('ARRIVAL_PRICE'), ColumnDefinition.ofDouble('SLIPPAGE'),
        ColumnDefinition.ofDouble('MARKET_IMPACT'), ColumnDefinition.ofBoolean('IS_PARTIAL_FILL'),
        ColumnDefinition.ofInt('SEQUENCE_NUMBER'), ColumnDefinition.ofString('LAST_MARKET'), ColumnDefinition.ofString('ORDER_ID')),
        NO_FLUSH, NO_SHUTDOWN)
EXECUTION = BlinkTableTools.blinkToAppendOnly(executionPublisher.table())
routePublisher = TablePublisher.of('routes', TableDefinition.of(
        ColumnDefinition.ofString('ROUTE_ID'), ColumnDefinition.ofTime('ROUTE_DATE'), ColumnDefinition.ofString('DESTINATION'),
        ColumnDefinition.ofDouble('ROUTE_QUANTITY'), ColumnDefinition.ofDouble('ROUTED_PRICE'), ColumnDefinition.ofString('STATUS'),
        ColumnDefinition.ofString('ROUTE_TYPE'), ColumnDefinition.ofInt('PRIORITY'), ColumnDefinition.ofInt('LATENCY_MS'),
        ColumnDefinition.ofBoolean('IS_SMART_ROUTED'), ColumnDefinition.ofString('REJECT_REASON'), ColumnDefinition.ofString('BROKER'),
        ColumnDefinition.ofString('ORDER_ID')), NO_FLUSH, NO_SHUTDOWN)
ORDER_ROUTE = BlinkTableTools.blinkToAppendOnly(routePublisher.table())
ratePublisher = TablePublisher.of('rates', TableDefinition.of(
        ColumnDefinition.ofString('RATE_ID'), ColumnDefinition.ofString('CURRENCY_PAIR'), ColumnDefinition.ofDouble('SPOT_RATE'),
        ColumnDefinition.ofDouble('BID_RATE'), ColumnDefinition.ofDouble('ASK_RATE'), ColumnDefinition.ofTime('RATE_DATE'),
        ColumnDefinition.ofString('SOURCE'), ColumnDefinition.ofDouble('MID_RATE'), ColumnDefinition.ofDouble('DAILY_CHANGE'),
        ColumnDefinition.ofDouble('DAILY_CHANGE_PCT'), ColumnDefinition.ofBoolean('IS_OFFICIAL'),
        ColumnDefinition.ofString('FIXING_TIME'), ColumnDefinition.ofString('SNAPSHOT_ID'),
        ColumnDefinition.ofString('BASE_CURRENCY_CODE')), NO_FLUSH, NO_SHUTDOWN)
FX_RATE = ratePublisher.table().lastBy('RATE_ID')
rfqPublisher = TablePublisher.of('rfqs', TableDefinition.of(
        ColumnDefinition.ofString('RFQ_ID'), ColumnDefinition.ofTime('REQUEST_TIME'), ColumnDefinition.ofString('CURRENCY_PAIR'),
        ColumnDefinition.ofString('SIDE'), ColumnDefinition.ofDouble('NOTIONAL'), ColumnDefinition.ofString('TENOR'),
        ColumnDefinition.ofDouble('QUOTED_RATE'), ColumnDefinition.ofDouble('SPREAD_BPS'), ColumnDefinition.ofString('STATUS'),
        ColumnDefinition.ofInt('RESPONSE_TIME_MS'), ColumnDefinition.ofBoolean('IS_HIT'), ColumnDefinition.ofString('COUNTERPARTY_ID'),
        ColumnDefinition.ofString('TRADER_ID')), NO_FLUSH, NO_SHUTDOWN)
RFQ = rfqPublisher.table().lastBy('RFQ_ID')

// the books that send orders: the equity and crypto desks', each its desk's names and traders
orderBooks = BOOKS.findAll { it.desk.asset == 'EQUITY' || it.desk.asset == 'CRYPTO' }.collect { b ->
    [id: b.id, names: (b.desk.asset == 'CRYPTO' ? coins : equities.findAll { ((List) b.desk.sectors).contains(it.sector) }),
     traders: deskTraders[b.desk.id]] as Map<String, Object>
}
bankDay = new Day(bank, orderBooks, pairs, clientIds, clientEdges, deskTraders['DESK-FX'], orderPublisher, executionPublisher,
        routePublisher, ratePublisher, rfqPublisher, ORDERS_PER_SECOND, RFQS_PER_SECOND)
// every pair priced once at the start, so the screens have a rate for each before the first beat moves it
pairs.each { p -> RateRows r = new RateRows(); r.rate(p, Instant.now()); ratePublisher.add(r.rates()) }
marketClock.scheduleAtFixedRate({ bankDay.beat() } as Runnable, 10L, 100L, TimeUnit.MILLISECONDS)
