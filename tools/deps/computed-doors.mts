// THE COMPUTED-SPECIFIER SCANNER (carried from the run wall's computedDoors, the final audit of 9af16fbe58's T4): each
// import door a module opens whose module is chosen at run time. Shared by //tools/deps:layers_alone (a layer's
// closure) and //tools/deps:app_runs_test (the apps' and their pages' modules), so the two read a door alike (2026-11-15,
// the C5-T review's S1 and B5: every computed load in the product, a layer's or an app's, is named where it is read).

/** `text` with its comments blanked (each kept as spaces, so offsets hold). */
export function withoutComments(text: string): string {
  return text.replace(/\/\*[\s\S]*?\*\//g, (m) => m.replace(/[^\n]/g, ' '))
    .replace(/(^|[^:'"`\\])\/\/[^\n]*/g, (m, lead: string) => lead + ' '.repeat(m.length - lead.length));
}

/** `code` with each string literal's text blanked, its quotes and every offset kept (a template's substitutions too). */
export function blankStrings(code: string): string {
  return code.replace(/'(?:[^'\\\n]|\\.)*'|"(?:[^"\\\n]|\\.)*"|`(?:[^`\\]|\\.)*`/g,
    (m) => m[0] + ' '.repeat(m.length - 2) + m[m.length - 1]);
}

/** The offset just past the bracket that closes the one at `open`. */
export function closing(code: string, open: number): number {
  const pairs: Record<string, string> = { '(': ')', '{': '}', '[': ']' };
  const stack: string[] = [];
  for (let i = open; i < code.length; i++) {
    const c = code[i]!;
    if (c === '(' || c === '{' || c === '[') stack.push(pairs[c]!);
    else if (c === ')' || c === '}' || c === ']') {
      stack.pop();
      if (stack.length === 0) return i + 1;
    } else if (c === "'" || c === '"' || c === '`') {
      const end = code.indexOf(c, i + 1);
      i = end < 0 ? code.length : end;
    }
  }
  return code.length;
}

/** A chain of receivers before a door's name, any length (`self.`, `globalThis.`, `a.b?.`). */
export const CHAIN = '(?:[\\w$]+\\s*\\??\\.\\s*)*';

/** A door's start: its receivers' chain from its first name, or its name alone. */
export const RECEIVER = `(?<![\\w$.])${CHAIN}`;

/** A specifier read as a literal: a quoted one, or a template with no substitution. */
export const LITERAL_SPEC = /^\s*(?:'[^'\\\n]*'|"[^"\\\n]*"|`[^`\\$]*`)\s*$/;

/** Each import door `text` opens whose specifier is NOT a literal: a dynamic `import()`, a `require()`, an
 *  `import.meta.resolve()`, an `importScripts()` (each argument), a worker's module, or a `new URL(...,
 *  import.meta.url)`, through any receiver -- its first argument a template with a substitution, a name, a call, or any
 *  other expression. A worker built from a literal `new URL('...', import.meta.url)` is read. */
export function computedDoors(text: string): string[] {
  const code = withoutComments(text);
  const blank = blankStrings(code);
  const out: string[] = [];
  const doors = new RegExp(`${RECEIVER}(?:import|require|importScripts)\\s*\\(|\\bimport\\.meta\\.resolve\\s*\\(`
    + `|\\bnew\\s+${CHAIN}(?:Shared)?Worker\\s*\\(|\\bnew\\s+${CHAIN}URL\\s*\\(`, 'g');
  for (const m of blank.matchAll(doors)) {
    const open = m.index + m[0].length - 1;
    const close = closing(blank, open);
    const args = code.slice(open + 1, close - 1);
    // the arguments: split at each comma outside brackets (strings blanked, so theirs never count)
    const blankArgs = blank.slice(open + 1, close - 1);
    const each: string[] = [];
    let depth = 0;
    let start = 0;
    for (let i = 0; i <= blankArgs.length; i++) {
      const c = i < blankArgs.length ? blankArgs[i]! : ',';
      if (c === '(' || c === '[' || c === '{') depth++;
      else if (c === ')' || c === ']' || c === '}') depth--;
      else if (c === ',' && depth === 0) {
        each.push(args.slice(start, i));
        start = i + 1;
      }
    }
    const first = each[0] ?? '';
    const door = m[0].replace(/\s+/g, ' ').replace(/\s*\.\s*/g, '.').replace(/\s*\($/, '(');
    if (/URL\($/.test(door) && !/,\s*import\.meta\.url\s*$/.test(args)) continue; // a URL, not a module's
    if (/importScripts\($/.test(door)) {
      if (each.every((a) => LITERAL_SPEC.test(a))) continue;
      out.push(`${door}${each.map((a) => a.replace(/\s+/g, ' ').trim()).join(', ')})`);
      continue;
    }
    if (LITERAL_SPEC.test(first)) continue;
    if (/(?:Shared)?Worker\($/.test(door)
      && /^\s*new\s+URL\(\s*(?:'[^'\\\n]*'|"[^"\\\n]*"|`[^`\\$]*`)\s*,\s*import\.meta\.url\s*\)\s*$/.test(first)) continue;
    out.push(`${door}${first.replace(/\s+/g, ' ').trim()}${each.length > 1 ? ', ...' : ''})`);
  }
  return out;
}
