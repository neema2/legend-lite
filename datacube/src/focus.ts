// WHERE THE FOCUS IS, across shadow roots. A cube can live inside a shadow root -- marimo puts each widget in one
// (docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md, "In a marimo notebook") -- and there the document's activeElement is
// the shadow root's host, never the element inside it that has the focus: a grid asking the document whether it had the
// focus would always hear no, and drop it on every re-render (found by //datacube:marimo_test).

/** The element that has the focus, inside whatever shadow roots hold it; null when nothing has. */
export function focusedElement(doc: Document): Element | null {
  let active = doc.activeElement;
  while (active?.shadowRoot?.activeElement) active = active.shadowRoot.activeElement;
  return active;
}

/**
 * Whether a key pressed on `target` belongs to a field typing or choosing in place -- an input, a text area, a select, or
 * contenteditable -- rather than to whatever holds it: Escape, Ctrl+Z, a shortcut, each the field's own there. One rule
 * for every window and the cube (app.ts, ui/chart-panel.ts): a check that only looked at tag names would hand the cube's
 * undo to someone mid-word in a contenteditable.
 */
export function isTextEntry(target: EventTarget | null): boolean {
  if (!target || typeof (target as Element).closest !== 'function') {
    return false;
  }
  const el = target as HTMLElement;
  const tag = el.tagName;
  return (
    tag === 'INPUT'
    || tag === 'TEXTAREA'
    || tag === 'SELECT'
    || el.isContentEditable === true
    || el.closest('[contenteditable="true"]') !== null
  );
}
