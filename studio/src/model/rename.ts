// Rename or move an element (plan A7): its full path replaced wherever the workspace's text writes it -- its own
// declaration and every reference by full path -- as upstream's rename updates the references in its graph. A path
// written inside a comment or a string is text, and is left as written; a longer path that only starts with it
// (`a::B` in `a::B::c`, `a::Bc`) is another element's.

/** `text` with every reference to `from` (a full path) written as `to`. */
export function renamePath(text: string, from: string, to: string): string {
  let out = '';
  let i = 0;
  while (i < text.length) {
    // comments and strings: kept as written
    if (text.startsWith('//', i)) {
      const nl = text.indexOf('\n', i);
      const end = nl < 0 ? text.length : nl;
      out += text.slice(i, end);
      i = end;
      continue;
    }
    if (text.startsWith('/*', i)) {
      const close = text.indexOf('*/', i + 2);
      const end = close < 0 ? text.length : close + 2;
      out += text.slice(i, end);
      i = end;
      continue;
    }
    if (text[i] === "'") {
      let j = i + 1;
      while (j < text.length && text[j] !== "'") j += text[j] === '\\' ? 2 : 1;
      out += text.slice(i, j + 1);
      i = j + 1;
      continue;
    }
    if (text.startsWith(from, i) && !continuesName(text[i - 1]) && !continuesName(text[i + from.length]) && !text.startsWith('::', i + from.length)
      && !(text[i - 1] === ':' && text[i - 2] === ':')) {
      out += to;
      i += from.length;
      continue;
    }
    out += text[i];
    i++;
  }
  return out;
}

/** Does `c` carry a name on (a letter, a digit, `_` or `$`)? */
function continuesName(c: string | undefined): boolean {
  return c !== undefined && /[\w$]/.test(c);
}
