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
        for (Name n : stepped) {
            double r = n.beta * market + 0.6d * sector[n.sectorIx] + gauss() * n.vol * root
            // news: about one a name every two trading days, a 2% move
            if (random.nextDouble() < dt / (2 * DAY)) r += gauss() * 0.02d
            n.mid = Math.max(n.tick, n.mid * Math.exp(r))
            n.drift = r
            int quotes = events(quoteRate * n.weight * dt)
            // a quote the price has left is moved, at once
            if (!n.quoted || n.mid <= n.bid || n.mid >= n.ask) quotes = Math.max(quotes, 1)
            for (int q = 0; q < quotes; q++) quote(n, now, rows)
            int trades = events(tradeRate * n.weight * dt)
            for (int t = 0; t < trades; t++) trade(n, now, rows)
        }
    }

    void quote(Name n, Instant now, Rows rows) {
        long ticks = Math.max(1L, Math.round(n.mid * n.spreadBps / 1e4d / n.tick))
        double bid = Math.floor(n.mid / n.tick - ticks / 2d) * n.tick
        n.bid = roundTo(bid, n.tick)
        n.ask = roundTo(bid + ticks * n.tick, n.tick)
        n.bidSize = size(n)
        n.askSize = size(n)
        n.quoted = true
        rows.quote(n, now, n.kind == 'EQUITY' ? 'SIP' : 'SIM')
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
    long lastStepNanos = System.nanoTime()
    long retryAtMs = 0L
    String said = ''

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
        String text = crypto
        if (text == said) return
        said = text
        String[] c = crypto.split('\\|', 2)
        // said in the server's log too, as an operator reads it
        System.out.println('the market world\'s crypto is ' + c[0] + ': ' + c[1])
        world.add(TableTools.newTable(
                TableTools.stringCol('item', 'equities', 'crypto'),
                TableTools.stringCol('state', 'simulated', c[0]),
                TableTools.stringCol('detail', equitiesDetail, c[1])))
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

worldPublisher = TablePublisher.of('world', TableDefinition.of(
        ColumnDefinition.ofString('item'), ColumnDefinition.ofString('state'), ColumnDefinition.ofString('detail')),
        NO_FLUSH, NO_SHUTDOWN)
world = worldPublisher.table().lastBy('item')

// ---------------------------------------------------------------- the clock: a beat every 20 ms

simulator = new Simulator(random, equities, SECTORS.size(), QUOTES, TRADES, seq)
// a coin's activity on its own scale: the simulator's rates are the equities'; coins quote and trade a tenth as often
coins.each { it.weight = it.weight / 10d }
marketWorld = new World(simulator, feed, quotePublisher, tradePublisher, worldPublisher, seq,
        String.format('%d names, %,.0f quotes and %,.0f trades a second', NAMES, QUOTES, TRADES))
(equities + coins).each { marketWorld.bySym.put(it.sym, it) }
marketWorld.equities.addAll(equities)
marketWorld.coins.addAll(coins)
marketClock = Executors.newSingleThreadScheduledExecutor { r -> Thread t = new Thread(r, 'market-world'); t.daemon = true; t }
marketClock.scheduleAtFixedRate({ marketWorld.beat() } as Runnable, 0L, 20L, TimeUnit.MILLISECONDS)
