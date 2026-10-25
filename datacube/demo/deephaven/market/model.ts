// THE MARKET WORLD'S MODEL (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K2, DH34): the ONE TRADING MODEL -- the stress
// corpus's classes, store and mappings (//scripts/corpus:trading_model, `corpus` here) -- with a connection from its
// store to the Deephaven server running the market world (market.groovy publishes the corpus's tables, every column the
// store declares), and the world's own status table beside it, which no class of the corpus models. A page plans the
// corpus's classes (marketdata::MarketTrade, products::Instrument, ...) through stress::AllMapping on the runtime.

/** The market's connection at `host`:`port` over TLS, its store `store`: the model's own (store::DB), and a page's
 *  server bases' (src/server-base.ts `connectionOf`: their Database in a model of their own). */
export const MARKET_CONNECTION = (host: string, port: number) => (store: string): string => `RelationalDatabaseConnection market::Live
{
  store: ${store};
  type: Deephaven;
  specification: Deephaven { host: '${host}'; port: ${port}; useTls: true; };
  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };
}`;

/** The model, its connections at `host`:`port` over TLS, over the corpus's text. */
export const MARKET_MODEL = (corpus: string, host: string, port: number): string => `${corpus}
###Relational
Database market::World
(
  Table world (item VARCHAR(16) PRIMARY KEY, state VARCHAR(16) NOT NULL, detail VARCHAR(256) NOT NULL)
)
###Connection
${MARKET_CONNECTION(host, port)('store::DB')}
RelationalDatabaseConnection market::Status
{
  store: market::World;
  type: Deephaven;
  specification: Deephaven { host: '${host}'; port: ${port}; useTls: true; };
  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };
}
###Pure
// BLACK AND SCHOLES, as functions of the model (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K11): a European option's
// price and greeks from its underlying's price s, its strike k, its volatility v, the rate r -- and what a query computes
// once each, as columns, and hands on: the square root of its time to expiry (rootT), its discount (disc), d1, the normal
// density at d1 (nd1) and the normal distribution at d1, d2 and their negations (cd1, cd2, cmd1, cmd2). Each function is
// inlined where it is called, so a value written into a function would be written into each formula as often as the
// function reads it. Programs, not the platform's: the database computes every price and greek, live. The normal
// distribution by Abramowitz and Stegun 26.2.17 (|error| < 7.5e-8).
function market::options::pdf(x: Float[1]): Float[1]
{
  0.3989422804014327 * exp(0.0 - (($x * $x) / 2.0))
}
function market::options::poly(t: Float[1]): Float[1]
{
  $t * (0.319381530 + ($t * ((0.0 - 0.356563782) + ($t * (1.781477937 + ($t * ((0.0 - 1.821255978) + ($t * 1.330274429))))))))
}
function market::options::tail(x: Float[1]): Float[1]
{
  market::options::pdf($x) * market::options::poly(1.0 / (1.0 + (0.2316419 * abs($x))))
}
function market::options::cdf(x: Float[1]): Float[1]
{
  if($x >= 0.0, |1.0 - market::options::tail($x), |market::options::tail($x))
}
function market::options::d1(s: Float[1], k: Float[1], t: Float[1], v: Float[1], r: Float[1], rootT: Float[1]): Float[1]
{
  (log($s / $k) + (($r + (($v * $v) / 2.0)) * $t)) / ($v * $rootT)
}
function market::options::d2(d1: Float[1], v: Float[1], rootT: Float[1]): Float[1]
{
  $d1 - ($v * $rootT)
}
function market::options::price(call: Boolean[1], s: Float[1], k: Float[1], disc: Float[1], cd1: Float[1], cd2: Float[1],
  cmd1: Float[1], cmd2: Float[1]): Float[1]
{
  if($call, |($s * $cd1) - (($k * $disc) * $cd2), |(($k * $disc) * $cmd2) - ($s * $cmd1))
}
function market::options::delta(call: Boolean[1], cd1: Float[1]): Float[1]
{
  if($call, |$cd1, |$cd1 - 1.0)
}
function market::options::gamma(s: Float[1], v: Float[1], rootT: Float[1], nd1: Float[1]): Float[1]
{
  $nd1 / (($s * $v) * $rootT)
}
// a vega per volatility point, a theta per day
function market::options::vega(s: Float[1], rootT: Float[1], nd1: Float[1]): Float[1]
{
  (($s * $nd1) * $rootT) / 100.0
}
function market::options::theta(call: Boolean[1], s: Float[1], k: Float[1], v: Float[1], r: Float[1], rootT: Float[1],
  disc: Float[1], nd1: Float[1], cd2: Float[1], cmd2: Float[1]): Float[1]
{
  ((0.0 - ((($s * $nd1) * $v) / (2.0 * $rootT))) - if($call, |(($r * $k) * $disc) * $cd2, |0.0 - ((($r * $k) * $disc) * $cmd2))) / 365.0
}
###Runtime
Runtime market::LiveRuntime
{
  mappings: [ stress::AllMapping ];
  connections: [ store::DB: [ live: market::Live ], market::World: [ status: market::Status ] ];
}`;

export const MARKET_RUNTIME = 'market::LiveRuntime';
