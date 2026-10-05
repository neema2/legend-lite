// One of upstream's icons (icons.ts) as a DOM element: the vendored SVG, 1em square in the current text colour, so
// CSS sizes and colours it as upstream's react-icons are (`font-size` on the wrapper, `color` for the paint).

import { ICONS, type IconName } from './icons.ts';

/** `<span class="icon icon--<name>">` around the icon's SVG; `size` (a CSS length) sets its font-size when given. */
export function icon(name: IconName, size?: string): HTMLSpanElement {
  const span = document.createElement('span');
  span.className = `icon icon--${name}`;
  if (size !== undefined) span.style.fontSize = size;
  // the markup is this package's own (icons.ts, generated from react-icons' paths), never input
  const template = document.createElement('template');
  template.innerHTML = ICONS[name];
  span.append(template.content);
  return span;
}
