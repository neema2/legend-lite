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
