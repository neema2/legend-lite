// CRYPTO FROM COINBASE EXCHANGE'S PUBLIC FEED (market.groovy's CoinbaseFeed, DH28), straight from the tab: its ticker
// channel as quotes, its matches as trades (the aggressor the maker's other side). A browser's WebSocket needs no
// server, and Coinbase's feed asks no key. Live from its first message to a close or an error; the world simulates crypto
// whenever it is not, from the last price it had, and asks again a minute later.

/** A message of the feed's the world reads: a ticker or a match, as Coinbase writes them (every number a string). */
export interface CoinbaseMessage {
  readonly type: 'ticker' | 'match' | 'last_match';
  readonly product_id: string;
  readonly time?: string;
  readonly best_bid?: string;
  readonly best_ask?: string;
  readonly best_bid_size?: string;
  readonly best_ask_size?: string;
  readonly open_24h?: string;
  readonly price?: string;
  readonly size?: string;
  readonly side?: 'buy' | 'sell';
}

export class CoinbaseFeed {
  readonly url: string;
  readonly products: readonly string[];
  /** the feed's messages not yet read by the world */
  readonly messages: CoinbaseMessage[] = [];
  state: 'connecting' | 'live' | 'refused' = 'connecting';
  detail = '';
  lastMessageMs = 0;
  #socket: WebSocket | null = null;

  constructor(url: string, products: readonly string[]) {
    this.url = url;
    this.products = products;
  }

  connect(): void {
    this.state = 'connecting';
    this.detail = this.url;
    this.#socket?.close();
    if (typeof WebSocket === 'undefined') {
      this.refused('no WebSocket here');
      return;
    }
    let socket: WebSocket;
    try {
      socket = new WebSocket(this.url);
    } catch (e) {
      this.refused(String(e));
      return;
    }
    this.#socket = socket;
    socket.onopen = () => {
      socket.send(JSON.stringify({ type: 'subscribe', product_ids: this.products, channels: ['ticker', 'matches'] }));
    };
    socket.onmessage = (event: MessageEvent) => {
      if (this.#socket !== socket) return;
      try {
        const m = JSON.parse(String(event.data)) as { type?: string; message?: string; reason?: string };
        if (m.type === 'ticker' || m.type === 'match' || m.type === 'last_match') {
          this.messages.push(m as CoinbaseMessage);
          this.lastMessageMs = Date.now();
          this.state = 'live';
          this.detail = 'Coinbase Exchange';
        } else if (m.type === 'error') {
          this.refused(`the feed said: ${m.message ?? ''} ${m.reason ?? ''}`);
        }
      } catch (e) {
        this.refused(`unreadable: ${String(e)}`);
      }
    };
    socket.onerror = () => {
      if (this.#socket === socket) this.refused(`could not reach ${this.url}`);
    };
    socket.onclose = (event: CloseEvent) => {
      if (this.#socket === socket) this.refused(`closed ${event.code} ${event.reason}`);
    };
  }

  refused(why: string): void {
    this.state = 'refused';
    this.detail = why.length > 200 ? why.substring(0, 200) : why;
  }

  close(): void {
    const socket = this.#socket;
    this.#socket = null;
    socket?.close();
  }
}
