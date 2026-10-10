// A NOTEBOOK'S CUBE (docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md, "In a notebook"): DataCube's module for
// `legend_lite.notebook.DataCube`, which the widget's loader (widget-loader.ts) fetches over the widget's channel once
// per notebook page. One file, no lazy chunks (a module imported from a blob URL loads no chunk beside it), its styles
// beside it (widget-styles.css, bundled as widget.css), added to the page by the loader.
//
// The cube is engine-cube.ts's, its calls over the channel. It follows its frame by the widget's `version`, which
// Python sets when the frame changes (Engine.watch): no polling. Its keys stay with it.
//
// Or A PAGE (the widget's `page_key`, Python's `ll.Page`; docs/DATACUBE_PYTHON_PAGES_DESIGN_2026_10_09.md):
// engine-page.ts's, its sheets, grids and charts over the engine's frames, following the widget's `versions` -- the
// page's and each frame's -- which Python sets as it changes them.

import { EngineCube, type EngineLink } from './engine-cube.ts';
import { EnginePage, type Versions } from './engine-page.ts';
import { BASE, type WidgetModel } from './widget-loader.ts';

const px = (model: WidgetModel): string => `${Number(model.get('height')) || 480}px`;

export function render(model: WidgetModel, el: HTMLElement, fetch: typeof globalThis.fetch): () => void {
  const page = model.get('page_key');
  if (typeof page === 'string' && page !== '') return renderPage(model, el, fetch, page);
  const link: EngineLink = { baseUrl: BASE, fetch, table: String(model.get('table')) };
  const { note, host, undo } = frame(model, el);
  let gone = false;
  let cube: EngineCube | undefined;
  const stopped = (why: string): void => {
    note.textContent = `not following the frame: ${why}`;
    note.style.display = 'block';
  };
  // one re-read at a time, in order: a version that moves twice re-reads after the first has finished
  let reading: Promise<void> = Promise.resolve();
  const moved = (): void => {
    reading = reading.then(async () => {
      if (gone || cube === undefined || Number(model.get('version')) === cube.config.version) return;
      const why = await cube.reread();
      if (why !== undefined) stopped(why);
      else note.style.display = 'none';
    }).catch((e: unknown) => stopped(e instanceof Error ? e.message : String(e)));
  };
  model.on('change:version', moved);

  EngineCube.open(host, link).then((opened) => {
    if (gone) {
      opened.dispose();
      return;
    }
    cube = opened;
    // a change while it opened
    moved();
  }, (e: unknown) => {
    if (!gone) host.textContent = e instanceof Error ? e.message : String(e);
  });

  return () => {
    gone = true;
    model.off('change:version', moved);
    undo();
    cube?.dispose();
  };
}

/**
 * The widget's element made ready for DataCube: its height (the widget's `height`), a note above it (why it no longer
 * follows, when it does not), and its keys its own. What it returns undoes it.
 */
function frame(model: WidgetModel, el: HTMLElement): { note: HTMLElement; host: HTMLElement; undo: () => void } {
  const note = document.createElement('div');
  note.style.cssText = 'display:none;font:12px/16px ui-sans-serif, system-ui, sans-serif;color:#a00;padding:2px 0';
  const host = document.createElement('div');
  host.style.cssText = 'position:relative;box-sizing:border-box;height:100%';
  el.style.height = px(model);
  el.replaceChildren(note, host);
  // a key pressed in the cube is the cube's: not the notebook's shortcuts (an arrow moves in the grid, not between
  // cells). JupyterLab skips shortcuts under this mark; any page's handler further up never hears the key.
  el.dataset['lmSuppressShortcuts'] = 'true';
  const keep = (e: Event): void => e.stopPropagation();
  el.addEventListener('keydown', keep);
  const resized = (): void => { el.style.height = px(model); };
  model.on('change:height', resized);
  return { note, host, undo: () => { el.removeEventListener('keydown', keep); model.off('change:height', resized); } };
}

/** A notebook's PAGE: the engine's page over its frames, following the widget's `versions`. */
function renderPage(model: WidgetModel, el: HTMLElement, fetch: typeof globalThis.fetch, name: string): () => void {
  const { note, host, undo } = frame(model, el);
  let gone = false;
  let page: EnginePage | undefined;
  const stopped = (why: string): void => {
    note.textContent = `not following the page: ${why}`;
    note.style.display = 'block';
  };
  // one follow at a time, in order: versions that move twice follow after the first has finished
  let following: Promise<void> = Promise.resolve();
  const moved = (): void => {
    following = following.then(async () => {
      const versions = model.get('versions') as Versions | undefined;
      if (gone || page === undefined || versions === undefined) return;
      const why = await page.follow(versions);
      if (why !== undefined) stopped(why);
      else note.style.display = 'none';
    }).catch((e: unknown) => stopped(e instanceof Error ? e.message : String(e)));
  };
  model.on('change:versions', moved);
  EnginePage.open(host, { baseUrl: BASE, fetch, page: name }).then((opened) => {
    if (gone) {
      opened.dispose();
      return;
    }
    page = opened;
    // a change while it opened
    moved();
  }, (e: unknown) => {
    if (!gone) host.textContent = e instanceof Error ? e.message : String(e);
  });
  return () => {
    gone = true;
    model.off('change:versions', moved);
    undo();
    page?.dispose();
  };
}
