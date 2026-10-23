// THE ONE API LEDGERS' SOURCE SCANNING (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md §3): where in a source a call is made
// -- the function, method or object property it is made in -- so a ledger names a gap by where it lives, not by a line
// number that moves with every edit; and that function's body, so a ledger can ask whether it also tells a command.
// Plain text over the source, comments left out, scope read from the indentation (the sources are formatted).

/** A source's lines, comments blanked: a `//` line, a block comment's lines; code after a block comment kept. */
export function codeLines(text: string): string[] {
  return text.split('\n').map((line) => {
    const t = line.trim();
    if (t.startsWith('//') || t.startsWith('*')) return '';
    if (t.startsWith('/*')) {
      const end = line.indexOf('*/');
      return end < 0 ? '' : ' '.repeat(end + 2) + line.slice(end + 2);
    }
    return line;
  });
}

const KEYWORDS = new Set(['if', 'for', 'while', 'switch', 'return', 'catch', 'function', 'await', 'void', 'new', 'typeof',
  'else', 'try', 'do', 'throw', 'case', 'super', 'import']);

/** A class member's header (two spaces in): `  async #name(`, `  get name(`, `  name<T>(`. */
const MEMBER = /^ {2}(?:static\s+)?(?:async\s+)?(?:get\s+|set\s+)?(#?[A-Za-z_$][\w$]*)\s*(?:<[^(]*>)?\(/;
/** An object property that is a function: `name: (`, `name: async (`. */
const PROPERTY = /^\s+(#?[A-Za-z_$][\w$]*)\s*:\s*(?:async\s*)?(?:\([^)]*\)|[A-Za-z_$][\w$]*)\s*=>/;
/** A method deeper in: `name(...) {`. */
const METHOD = /^\s+(?:async\s+)?(#?[A-Za-z_$][\w$]*)\s*\([^)]*\)\s*(?::[^{]*)?\{\s*$/;
/** A function inside another: `const name = async (...) =>`, `name = (...) =>`, `function name(`. */
const INNER = /^\s+(?:(?:(?:const|let)\s+)?([A-Za-z_$][\w$]*)\s*=\s*(?:async\s*)?(?:\([^)]*\)?|[A-Za-z_$][\w$]*\s*=>)|(?:async\s+)?function\s+([A-Za-z_$][\w$]*))/;
/** The end of a signature written over several lines: its closing parenthesis, its return type, its body opened. */
const SIGNATURE_END = /\)\s*:\s*[^=]*\{\s*$/;
/** A top-level function or arrow constant. */
const TOP = /^(?:export\s+)?(?:async\s+)?(?:function\s+([A-Za-z_$][\w$]*)|const\s+([A-Za-z_$][\w$]*)\s*=)/;

const indent = (line: string): number => line.length - line.trimStart().length;

/** The header `line` opens, by name, or undefined: a function, a method, a function property, an inner function. */
function header(line: string): string | undefined {
  for (const re of [PROPERTY, METHOD, MEMBER]) {
    const m = re.exec(line);
    // a parameter of a signature written over several lines (`then: (s) => S = ..., x): R {`) is no property
    if (re === PROPERTY && SIGNATURE_END.test(line)) continue;
    if (m && !KEYWORDS.has(m[1]!)) return m[1]!;
  }
  const inner = INNER.exec(line);
  if (inner && (inner[2] !== undefined || /=>|\($/.test(line))) return (inner[1] ?? inner[2])!;
  const top = TOP.exec(line);
  return top ? (top[1] ?? top[2])! : undefined;
}

/**
 * What the line at `index` is in: the nearest header above it that is less indented than every line between (so a
 * sibling -- a local arrow function defined just before the call -- is not taken for it), by name and line.
 */
export function enclosingAt(lines: readonly string[], index: number, self = true): { name: string; at: number } {
  // a function written on one line, the call in its body: `rename = (name) => page.setTitle(name);`
  const own = self ? header(lines[index]!) : undefined;
  if (own !== undefined && /=>|\{\s*$/.test(lines[index]!)) return { name: own, at: index };
  let least = indent(lines[index]!);
  for (let i = index - 1; i >= 0; i -= 1) {
    const line = lines[i]!;
    if (line.trim() === '') continue;
    const at = indent(line);
    if (at < least) {
      const name = header(line);
      if (name !== undefined) return { name, at: i };
      // the close of a signature written over several lines (`): R {`): its header is at the same depth, above
      if (line.trim().startsWith(')')) {
        least = at + 1;
        continue;
      }
    }
    least = Math.min(least, at);
  }
  return { name: '(top)', at: -1 };
}

export function enclosing(lines: readonly string[], index: number): string {
  return enclosingAt(lines, index).name;
}

/** The body of the function whose header is at `at`: its lines to the first after it no deeper than the header. */
export function bodyAt(lines: readonly string[], at: number): string {
  if (at < 0) return lines.join('\n');
  const depth = indent(lines[at]!);
  const out = [lines[at]!];
  for (let i = at + 1; i < lines.length; i += 1) {
    const line = lines[i]!;
    out.push(line);
    if (line.trim() !== '' && indent(line) <= depth) break;
  }
  return out.join('\n');
}

/**
 * The lines of the function headed at `at` that are its own: not those of a function defined inside it, which tells or
 * writes for itself (the S7 audit's H-10: a large function -- the app's start -- whose inner functions tell a command
 * read as telling one, so a write of its own escaped the ledger).
 */
export function ownBodyAt(lines: readonly string[], at: number): string {
  const depth = at < 0 ? -1 : indent(lines[at]!);
  const out = at < 0 ? [] : [lines[at]!];
  for (let i = at + 1; i < lines.length; i += 1) {
    const line = lines[i]!;
    // a local helper (`const done = (...) => {`) is the function's own work; a function of its own (a property's, a
    // method's, `function name`, a `let` set again later) is not
    let e = enclosingAt(lines, i);
    while (e.at !== at && e.at >= 0 && LOCAL.test(lines[e.at]!)) e = enclosingAt(lines, e.at, false);
    if (e.at === at) out.push(line);
    if (at >= 0 && line.trim() !== '' && indent(line) <= depth) break;
  }
  return out.join('\n');
}

/** A local helper's header: `const name = ...` (a `let`, which is set again later, is a way in of its own: a callback). */
const LOCAL = /^\s+const\s+[A-Za-z_$][\w$]*\s*=/;

/** Each call of `pattern` (a global regex; its first group, or the match, names the call) as `where:call`, counted --
 *  or, with `keep`, only the calls whose enclosing function's own lines (ownBodyAt) `keep` accepts. */
export function sites(text: string, pattern: RegExp, keep?: (body: string) => boolean): Map<string, number> {
  const lines = codeLines(text);
  const found = new Map<string, number>();
  lines.forEach((line, i) => {
    for (const m of line.matchAll(pattern)) {
      const where = enclosingAt(lines, i);
      if (keep && !keep(ownBodyAt(lines, where.at))) continue;
      const key = `${where.name}:${m[1] ?? m[0]}`;
      found.set(key, (found.get(key) ?? 0) + 1);
    }
  });
  return found;
}

/** The body of `export type <name> =`: its lines up to the next statement at the left margin. */
function unionBody(text: string, name: string): string {
  const start = text.indexOf(`export type ${name} =`);
  if (start < 0) throw new Error(`no union ${name}`);
  const lines = text.slice(start).split('\n');
  const end = lines.findIndex((line, i) => i > 0 && line.length > 0 && !/^\s/.test(line));
  return lines.slice(0, end < 0 ? undefined : end).join('\n');
}

/** The `kind`s a union of object types declares inline (`| { readonly kind: 'x'; ... }`). */
export function unionKinds(text: string, name: string): string[] {
  return [...new Set([...unionBody(text, name).matchAll(/readonly kind: '([A-Za-z]+)'/g)].map((m) => m[1]!))];
}

/** The `_type`s of a union of named interfaces (`= FileSource | QuerySource`), each read from its interface. */
export function unionTypes(text: string, name: string): string[] {
  const names = unionBody(text, name).replace(/^[^=]*=/, '').split('|').map((n) => n.replace(/[;\s]/g, '')).filter(Boolean);
  return names.map((n) => {
    const at = text.indexOf(`export interface ${n} {`);
    if (at < 0) throw new Error(`no interface ${n}`);
    const m = /readonly _type: '([A-Za-z]+)'/.exec(text.slice(at));
    if (!m) throw new Error(`${n} names no _type`);
    return m[1]!;
  });
}

/** The strings of `export const <name> = [ ... ]`. */
export function listed(text: string, name: string): string[] {
  const start = text.indexOf(`export const ${name} = [`);
  if (start < 0) throw new Error(`no list ${name}`);
  const body = text.slice(start, text.indexOf(']', start));
  return [...body.matchAll(/'([A-Za-z]+)'/g)].map((m) => m[1]!);
}

/** Each top-level exported function of a module with its body: its lines to the closing brace at the left margin. */
export function exportedFunctions(text: string): { name: string; body: string }[] {
  const lines = text.split('\n');
  const out: { name: string; body: string }[] = [];
  lines.forEach((line, i) => {
    const m = /^export (?:async )?function (\w+)/.exec(line);
    if (!m) return;
    const end = lines.findIndex((l, j) => j > i && l === '}');
    out.push({ name: m[1]!, body: lines.slice(i, end < 0 ? undefined : end + 1).join('\n') });
  });
  return out;
}

/** The body of the function `name` in `text` (a top-level function, or a class member). */
export function functionBody(text: string, name: string): string {
  const lines = text.split('\n');
  const at = lines.findIndex((line) => new RegExp(`^(?:export )?(?:async )?function ${name}\\b|^ {2}(?:async )?${name}\\(`).test(line));
  if (at < 0) throw new Error(`no function ${name}`);
  return bodyAt(lines, at);
}

/** Python's code without its docstrings and comments (what a scan for the dicts it builds should read). */
export function pythonCode(text: string): string {
  return text.replace(/"""[\s\S]*?"""/g, '""').replace(/'''[\s\S]*?'''/g, "''")
    .split('\n').filter((line) => !line.trim().startsWith('#')).join('\n');
}

/** A ledger held to what was found: every site found listed with its count, every one listed found -- in words. */
export function heldTo(found: ReadonlyMap<string, number>, ledger: Readonly<Record<string, { readonly count?: number }>>,
  what: string): string[] {
  const problems: string[] = [];
  for (const [key, n] of found) {
    const entry = ledger[key];
    if (entry === undefined) problems.push(`${what}: ${key} (${n}) is a new gap -- make it a command, or list it with its item`);
    else if ((entry.count ?? 1) !== n) problems.push(`${what}: ${key} is found ${n} times, listed ${entry.count ?? 1}`);
  }
  for (const key of Object.keys(ledger)) {
    if (!found.has(key)) problems.push(`${what}: ${key} is listed but gone -- close its item and take it off the ledger`);
  }
  return problems;
}
