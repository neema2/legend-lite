// A NOTEBOOK'S CUBE, ITS SCRIPT: anywidget's module for `legend_lite.notebook.DataCube`
// (docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md, "In a notebook"). Small on purpose: anywidget sends a widget's
// script with every widget and imports it afresh each time, and DataCube is about 1.3 MB. So this loader carries two
// things only:
//   - the widget's channel as a fetch: each call the cube makes travels to the kernel as a message, `{kind: 'call',
//     id, method, path, query, body}`, and its answer comes back as `{kind: 'answer', id, status, type, headers}` with
//     the body as one binary buffer (Arrow stays binary). No HTTP, no port: the notebook's own authenticated channel,
//     so the cube works wherever the notebook does (this machine, a remote JupyterHub, VS Code, Colab);
//   - DataCube's module (widget.ts: `widget.js`, and its styles `widget.css`), fetched over that channel ONCE per
//     notebook page, kept by its hash (the widget's `_module`), and reused by every later cube on the page; its styles
//     adopted by the page and by the shadow root a cube is in (marimo's).

/** The part of anywidget's model a cube uses (anywidget's AFM: https://anywidget.dev/en/afm/). */
export interface WidgetModel {
  get(name: string): unknown;
  on(event: string, callback: (...args: never[]) => void): void;
  off(event: string, callback: (...args: never[]) => void): void;
  send(content: unknown, callbacks?: unknown, buffers?: ArrayBuffer[] | ArrayBufferView[]): void;
}

/** DataCube's module (widget.ts): a cube in `el`, its calls over `fetch`; what it returns takes it down. */
export interface CubeModule {
  render(model: WidgetModel, el: HTMLElement, fetch: typeof globalThis.fetch): () => void;
}

/** What the cube's calls go under: a path the channel carries (`kernel/cube.json?...`), never a URL a browser fetches. */
export const BASE = 'kernel';

interface Answer {
  readonly kind: 'answer';
  readonly id: string;
  readonly status: number;
  readonly type: string;
  readonly headers: Readonly<Record<string, string>>;
}

const isAnswer = (m: unknown): m is Answer => typeof m === 'object' && m !== null && (m as { kind?: unknown }).kind === 'answer';

/** A call cut short because its view was taken down (its channel closed), not because the kernel refused it. */
export class ChannelClosed extends Error {
  constructor() {
    super('the cube was closed');
    this.name = 'ChannelClosed';
  }
}

/**
 * The widget's channel as a fetch, for one view of the widget. Every view of a model hears every answer, so a call's id
 * names its view (a random prefix) as well as its number; an answer to another view's call is not this one's.
 */
export function channel(model: WidgetModel): { readonly fetch: typeof globalThis.fetch; close(): void } {
  const view = Math.random().toString(36).slice(2);
  let next = 0;
  const waiting = new Map<string, { resolve(r: Response): void; reject(e: unknown): void }>();
  // a message's buffers, as anywidget's front end hands them: views of the ArrayBuffers its binary frames were read into
  const answered = (message: unknown, buffers?: readonly DataView<ArrayBuffer>[]): void => {
    if (!isAnswer(message)) return;
    const call = waiting.get(message.id);
    if (call === undefined) return;
    waiting.delete(message.id);
    // a status that has no body (a 204) is answered with none: a Response refuses one, even empty
    const bodiless = [101, 103, 204, 205, 304].includes(message.status);
    call.resolve(new Response(bodiless ? null : buffers?.[0] ?? null, {
      status: message.status,
      headers: { ...message.headers, 'Content-Type': message.type },
    }));
  };
  model.on('msg:custom', answered as (...args: never[]) => void);
  const fetch = (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => new Promise((resolve, reject) => {
    const url = String(input);
    if (!url.startsWith(`${BASE}/`)) {
      reject(new TypeError(`the widget's channel carries the engine's calls only, not ${url}`));
      return;
    }
    if (init?.body !== undefined && init.body !== null && typeof init.body !== 'string') {
      reject(new TypeError('a call over the widget\'s channel carries its body as text'));
      return;
    }
    const signal = init?.signal ?? undefined;
    if (signal?.aborted) {
      reject(signal.reason);
      return;
    }
    const rest = url.slice(BASE.length);
    const q = rest.indexOf('?');
    const id = `${view}:${next++}`;
    waiting.set(id, { resolve, reject });
    // a call given up on is forgotten here; the kernel's answer, when it comes, finds no one waiting
    signal?.addEventListener('abort', () => { waiting.delete(id); reject(signal.reason); }, { once: true });
    model.send({
      kind: 'call',
      id,
      method: init?.method ?? 'GET',
      path: q < 0 ? rest : rest.slice(0, q),
      query: q < 0 ? '' : rest.slice(q + 1),
      body: init?.body ?? null,
    });
  });
  return {
    fetch,
    close() {
      model.off('msg:custom', answered as (...args: never[]) => void);
      for (const call of waiting.values()) call.reject(new ChannelClosed());
      waiting.clear();
    },
  };
}

/** Where a page keeps DataCube's module, by its hash: one load per page, whichever of its notebooks asks. */
const LOADED = Symbol.for('legend-lite.datacube.module');

/** DataCube's module, and its styles as one sheet a page or a shadow root adopts. */
export interface Loaded {
  readonly module: CubeModule;
  readonly styles: CSSStyleSheet;
}

/** DataCube's module, fetched over `fetch` the first time this page asks for this hash. A load that fails is forgotten,
 *  so the next cube tries again. */
export function loaded(hash: string, fetch: typeof globalThis.fetch): Promise<Loaded> {
  const store = globalThis as typeof globalThis & { [LOADED]?: Map<string, Promise<Loaded>> };
  const modules = (store[LOADED] ??= new Map());
  let module = modules.get(hash);
  if (module === undefined) {
    module = load(fetch);
    modules.set(hash, module);
    module.catch(() => { if (modules.get(hash) === module) modules.delete(hash); });
  }
  return module;
}

async function file(fetch: typeof globalThis.fetch, name: string): Promise<string> {
  const answer = await fetch(`${BASE}/${name}`);
  if (!answer.ok) throw new Error(`the kernel did not give DataCube's ${name}: ${answer.status} ${await answer.text()}`);
  return answer.text();
}

async function load(fetch: typeof globalThis.fetch): Promise<Loaded> {
  const [script, css] = await Promise.all([file(fetch, 'widget.js'), file(fetch, 'widget.css')]);
  const url = URL.createObjectURL(new Blob([script], { type: 'text/javascript' }));
  let module: CubeModule;
  try {
    module = (await import(/* @vite-ignore */ url)) as CubeModule;
  } finally {
    URL.revokeObjectURL(url);
  }
  const styles = new CSSStyleSheet();
  styles.replaceSync(css);
  return { module, styles };
}

/**
 * DataCube's styles where the cube is: the page itself (the menus and dialogs DataCube opens on the page's body) and,
 * when the cube is inside a shadow root -- marimo puts each widget in one -- that root as well, which a page's styles
 * never reach. One sheet for the page and the module, adopted once by each root. Every selector is scoped to DataCube's
 * own classes, so nothing else changes (the design, "Loading DataCube into the notebook page once").
 */
function styled(styles: CSSStyleSheet, el: HTMLElement): void {
  for (const root of new Set([document, el.getRootNode()])) {
    if ((root instanceof Document || root instanceof ShadowRoot) && !root.adoptedStyleSheets.includes(styles)) {
      root.adoptedStyleSheets = [...root.adoptedStyleSheets, styles];
    }
  }
}

/** anywidget's entry: the cube in `el` once DataCube's module is here; what it returns takes the view down. */
export default {
  render({ model, el }: { model: WidgetModel; el: HTMLElement }): () => void {
    const link = channel(model);
    let gone = false;
    let takeDown: (() => void) | undefined;
    const open = (): void => {
      loaded(String(model.get('_module')), link.fetch).then(({ module, styles }) => {
        if (gone) return;
        styled(styles, el);
        takeDown = module.render(model, el, link.fetch);
      }, (e: unknown) => {
        if (gone) return;
        // the load another view started, cut short when that view was taken down (its cell run again, its output
        // cleared): this view loads it over its own channel (the audit of step 7, S2). Any other failure is said
        if (e instanceof ChannelClosed) open();
        else el.textContent = `DataCube did not load: ${e instanceof Error ? e.message : String(e)}`;
      });
    };
    open();
    return () => {
      gone = true;
      takeDown?.();
      link.close();
    };
  },
};
