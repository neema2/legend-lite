// A model's text as Studio's files: one element per file (plan A7: whole-project text mode, and the model importer).
// An element starts at a line, outside any comment, string or bracket, that begins with a declaration's keyword; it takes
// the `//` comment lines just above it, and runs to the next element or section. Its file is its text under its section
// (`###Service`, ...; Pure needs none). Whoever splits reads each file back with the compiler: a file that does not hold
// exactly one element is refused there, never guessed at here.

/** The keywords a top-level element is declared with, in every section Studio edits. */
const KEYWORDS = new Set([
  'Class', 'Enum', 'Association', 'Profile', 'function', 'Measure', 'native',
  'Mapping', 'Runtime', 'SingleConnectionRuntime', 'Database',
  'RelationalDatabaseConnection', 'JsonModelConnection', 'XmlModelConnection', 'ModelChainConnection',
  'Service', 'Data', 'DataSpace', 'Diagram', 'Binding', 'SchemaSet', 'GenerationSpecification', 'FileGeneration',
]);

/** Each element of `text` as a file's text: its section line (none for Pure), its comments, its declaration. */
export function splitElements(text: string): string[] {
  const lines = text.split('\n');
  // where each line starts outside any comment, string or bracket: element starts and section lines are only there
  const top = topLevelLines(text, lines);
  const out: string[] = [];
  let section = 'Pure';
  let current: string[] | undefined;
  let currentSection = 'Pure';
  const finish = (): void => {
    if (!current) return;
    while (current.length > 0 && current[current.length - 1]!.trim() === '') current.pop();
    if (current.length > 0) out.push(`${currentSection === 'Pure' ? '' : `###${currentSection}\n`}${current.join('\n')}\n`);
    current = undefined;
  };
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]!;
    if (!top[i]) { current?.push(line); continue; }
    const header = /^###(\w+)\s*$/.exec(line.trim());
    if (header) {
      finish();
      section = header[1]!;
      continue;
    }
    const word = /^\s*([A-Za-z]+)\b/.exec(line)?.[1];
    if (word !== undefined && KEYWORDS.has(word)) {
      // the comment lines just above belong to this element, not the one before
      const leading: string[] = [];
      while (current && current.length > 0 && /^\s*\/\//.test(current[current.length - 1]!)) leading.unshift(current.pop()!);
      finish();
      current = [...leading, line];
      currentSection = section;
      continue;
    }
    if (current) current.push(line);
    else if (line.trim() !== '') {
      // before any element: a comment of the next one (kept), else text no element holds
      current = [line];
      currentSection = section;
    }
  }
  finish();
  return out;
}

/** For each line: does it start outside every comment, string and bracket? */
function topLevelLines(text: string, lines: readonly string[]): boolean[] {
  const starts: number[] = [];
  let at = 0;
  for (const l of lines) {
    starts.push(at);
    at += l.length + 1;
  }
  const top: boolean[] = [];
  let depth = 0;
  let i = 0;
  /** Each line start up to `upTo` recorded: top level when `open` (no comment or string spans it) and depth is 0. */
  const reach = (upTo: number, open: boolean): void => {
    while (top.length < starts.length && starts[top.length]! <= upTo) top.push(open && depth === 0);
  };
  while (i < text.length) {
    reach(i, true);
    const c = text[i]!;
    if (text.startsWith('//', i)) {
      const nl = text.indexOf('\n', i);
      i = nl < 0 ? text.length : nl;
      continue;
    }
    if (text.startsWith('/*', i)) {
      const close = text.indexOf('*/', i + 2);
      const end = close < 0 ? text.length : close + 2;
      reach(end - 1, false);
      i = end;
      continue;
    }
    if (c === "'") {
      let j = i + 1;
      while (j < text.length && text[j] !== "'") j += text[j] === '\\' ? 2 : 1;
      reach(j, false);
      i = j + 1;
      continue;
    }
    if (c === '{' || c === '(' || c === '[') depth++;
    else if ((c === '}' || c === ')' || c === ']') && depth > 0) depth--;
    i++;
  }
  reach(text.length, true);
  return top;
}

/** A workspace's files as one text, each under its section (a file without one is Pure's). */
export function joinFiles(texts: readonly string[]): string {
  return texts.map((t) => (/^\s*###\w+/.test(t) ? t : `###Pure\n${t}`).replace(/\s*$/, '\n')).join('\n');
}
