// THE TRADING DESK'S MODEL (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K2, DH34; docs/ONE_CONNECTION_DESK_2026_10_28.md
// D2): three files, in order --
//   1. the ONE TRADING MODEL: the stress corpus's classes, store and mappings (//scripts/corpus:trading_model, `corpus`
//      here), which the market world publishes on its Deephaven server (market.groovy: every column the store declares);
//   2. the DESK'S OWN MODEL (market.pure): the world's status table, the options' and the screens' functions -- what
//      every screen of the desk shows;
//   3. its ENVIRONMENTS (environments.pure): a pair of connections and a runtime for the tab's DuckDB and for a Deephaven
//      server, each an execution context of the data space `market::TradingDesk`.
// A page plans the corpus's classes (marketdata::MarketTrade, products::Instrument, ...) through stress::AllMapping, and
// the desk's saved queries name the screens' functions (screens.ts) and the data space; the page names the context it
// runs on (Open With ▸).

/** The desk's model: its three files' text, joined as a project's files are (the corpus, the desk's own, the
 *  environments). */
export const DESK_MODEL = (corpus: string, desk: string, environments: string): string =>
  `${corpus}\n${desk}\n${environments}`;

/** The desk's data space (environments.pure). */
export const DESK_SPACE = 'market::TradingDesk';

/** An execution context of the desk's data space: its name, and the runtime it runs on. */
export interface DeskContext {
  readonly name: string;
  readonly runtime: string;
}

/** The tab's own DuckDB: the data space's default. */
export const IN_TAB: DeskContext = { name: 'In this tab', runtime: 'market::InTabRuntime' };

/** A Deephaven server. */
export const ON_DEEPHAVEN: DeskContext = { name: 'Deephaven', runtime: 'market::DeephavenRuntime' };
