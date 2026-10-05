// A function's body, replaced in its text (plan A5: the query builder's Save Query on a function). The body is the last
// top-level `{ … }` of the function's text -- its stereotypes and tagged values come before its name, its signature
// before the body -- so only what is between those braces is replaced; the signature, comments and the rest stay as
// written. Whoever calls this reads the result back with the grammar (query-builder.ts) and refuses a mismatch.

/** Where the function's body is in `text`: [start, end), between its braces. */
export function functionBodyRange(text: string): { start: number; end: number } | undefined {
  let depth = 0;
  let open = -1;
  let last: { start: number; end: number } | undefined;
  for (let i = 0; i < text.length;) {
    if (text.startsWith('//', i)) {
      const nl = text.indexOf('\n', i);
      i = nl < 0 ? text.length : nl;
      continue;
    }
    if (text.startsWith('/*', i)) {
      const close = text.indexOf('*/', i + 2);
      i = close < 0 ? text.length : close + 2;
      continue;
    }
    const c = text[i]!;
    if (c === "'") {
      let j = i + 1;
      while (j < text.length && text[j] !== "'") j += text[j] === '\\' ? 2 : 1;
      i = j + 1;
      continue;
    }
    if (c === '{' || c === '(' || c === '[') {
      if (depth === 0 && c === '{') open = i;
      depth++;
    } else if (c === '}' || c === ')' || c === ']') {
      depth--;
      if (depth === 0 && c === '}' && open >= 0) last = { start: open + 1, end: i };
    }
    i++;
  }
  return last;
}

/** `text` with its function's body made `statements` (each Pure text, no `;`), one per line under the function. */
export function withFunctionBody(text: string, statements: readonly string[]): string {
  const range = functionBodyRange(text);
  if (!range) throw new Error('this function has no body to replace');
  const body = statements.map((s) => `  ${s.trim().split('\n').join('\n  ')}`).join(';\n');
  return `${text.slice(0, range.start)}\n${body}\n${text.slice(range.end)}`;
}
