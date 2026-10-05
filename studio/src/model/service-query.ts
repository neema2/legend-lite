// A service's query, replaced in its text (plan A5: the query builder's Save Query). Studio edits text, so the builder's
// query goes back into the element as text: the lambda after `query:`, up to the `;` that ends it, is replaced and the
// rest of the file -- its comments, its layout -- stays as written. Whoever calls this reads the result back with the
// grammar (query-builder.ts), so a splice that went wrong is refused, never saved.

/** Where a service's `query:` lambda is in `text`: [start, end), end at its closing `;`. */
export function serviceQueryRange(text: string): { start: number; end: number } | undefined {
  let i = 0;
  let found = -1;
  // the `query:` key at the top of the execution (outside comments and strings)
  while (i < text.length && found < 0) {
    const skip = skipTrivia(text, i);
    if (skip !== i) { i = skip; continue; }
    if (text[i] === "'") { i = skipString(text, i); continue; }
    if (/^query\s*:/.test(text.slice(i, i + 32)) && !/[\w$]/.test(text[i - 1] ?? '')) found = text.indexOf(':', i) + 1;
    else i++;
  }
  if (found < 0) return undefined;
  let start = found;
  while (start < text.length && /\s/.test(text[start]!)) start++;
  // the lambda: to the first `;` outside brackets, strings and comments
  let depth = 0;
  for (let j = start; j < text.length;) {
    const skip = skipTrivia(text, j);
    if (skip !== j) { j = skip; continue; }
    const c = text[j]!;
    if (c === "'") { j = skipString(text, j); continue; }
    if (c === '(' || c === '[' || c === '{') depth++;
    else if (c === ')' || c === ']' || c === '}') depth--;
    else if (c === ';' && depth === 0) return { start, end: j };
    if (depth < 0) return undefined;
    j++;
  }
  return undefined;
}

/**
 * `text` with its service's query replaced by `query` (the lambda as Pure text, `|...` or `{...|...}`): its later
 * lines indented under the `query:` line's.
 */
export function withServiceQuery(text: string, query: string): string {
  const range = serviceQueryRange(text);
  if (!range) throw new Error('this service has no `query:` to replace (a single execution has one)');
  const lineStart = text.lastIndexOf('\n', range.start) + 1;
  const indent = /^[ \t]*/.exec(text.slice(lineStart))![0];
  const body = query.trim().split('\n').map((l, n) => (n === 0 || l === '' ? l : indent + l)).join('\n');
  return text.slice(0, range.start) + body + text.slice(range.end);
}

/** Past a comment or whitespace at `i`, or `i` itself. */
function skipTrivia(text: string, i: number): number {
  if (text.startsWith('//', i)) {
    const nl = text.indexOf('\n', i);
    return nl < 0 ? text.length : nl + 1;
  }
  if (text.startsWith('/*', i)) {
    const close = text.indexOf('*/', i + 2);
    return close < 0 ? text.length : close + 2;
  }
  return i;
}

/** Past the string literal opening at `i` (Pure's: single quotes, a backslash escapes). */
function skipString(text: string, i: number): number {
  let j = i + 1;
  while (j < text.length && text[j] !== "'") j += text[j] === '\\' ? 2 : 1;
  return j + 1;
}
