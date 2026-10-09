// A NOTEBOOK'S CUBE, ITS SCRIPT: anywidget's module for `legend_lite.notebook.DataCube`
// (docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md, "In a notebook"). Small on purpose: anywidget sends a widget's
// script with every widget and imports it afresh each time, and DataCube is about 1.3 MB. So this loader carries two
// things only:
//   - the widget's channel as a fetch: each call the cube makes travels to the kernel as a message, `{kind: 'call',
//     id, method, path, query, body}`, and its answer comes back as `{kind: 'answer', id, status, type, headers}` with
//     the body as one binary buffer (Arrow stays binary). No HTTP, no port: the notebook's own authenticated channel,
//     so the cube works wherever the notebook does (this machine, a remote JupyterHub, VS Code, Colab);
//   - DataCube's module (widget.ts: `widget.js`, and its styles `widget.css`), fetched over that channel ONCE per
//     notebook page, kept by its hash (the widget's `_module`), and reused by every later cube on the page.

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
    call.resolve(new Response(buffers?.[0] ?? null, {
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
      for (const call of waiting.values()) call.reject(new Error('the cube was closed'));
      waiting.clear();
    },
  };
}

/** Where a page keeps DataCube's module, by its hash: one load per page, whichever of its notebooks asks. */
const LOADED = Symbol.for('legend-lite.datacube.module');

type Loaded = Map<string, Promise<CubeModule>>;

/** DataCube's module, fetched over `fetch` the first time this page asks for this hash. A load that fails is forgotten,
 *  so the next cube tries again. */
export function loaded(hash: string, fetch: typeof globalThis.fetch): Promise<CubeModule> {
  const store = globalThis as typeof globalThis & { [LOADED]?: Loaded };
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

async function load(fetch: typeof globalThis.fetch): Promise<CubeModule> {
  const [script, styles] = await Promise.all([file(fetch, 'widget.js'), file(fetch, 'widget.css')]);
  // DataCube's styles, once for the page: every selector is scoped to its own classes, so nothing else on the page
  // changes (the design, "Loading DataCube into the notebook page once")
  const style = document.createElement('style');
  style.dataset['legendLite'] = 'datacube';
  style.textContent = styles;
  document.head.append(style);
  const url = URL.createObjectURL(new Blob([script], { type: 'text/javascript' }));
  try {
    return (await import(/* @vite-ignore */ url)) as CubeModule;
  } finally {
    URL.revokeObjectURL(url);
  }
}

/** anywidget's entry: the cube in `el` once DataCube's module is here; what it returns takes the view down. */
export default {
  render({ model, el }: { model: WidgetModel; el: HTMLElement }): () => void {
    const link = channel(model);
    let gone = false;
    let takeDown: (() => void) | undefined;
    loaded(String(model.get('_module')), link.fetch).then((module) => {
      if (!gone) takeDown = module.render(model, el, link.fetch);
    }, (e: unknown) => {
      if (!gone) el.textContent = `DataCube did not load: ${e instanceof Error ? e.message : String(e)}`;
    });
    return () => {
      gone = true;
      takeDown?.();
      link.close();
    };
  },
};
