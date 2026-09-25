// THE WINDOWS GUARDRAIL. CI runs this package on Windows too, and a
// macOS run cannot see a POSIX-only call: 254f2b1c7 went red on the
// Windows lane alone, over `readFileSync(new URL(...).pathname)` --
// `/C:/x` read as `C:\C:\x`. Every rule below bans a spelling that is
// right on one platform and wrong on the other, and names the portable
// one. A line that is portable for a reason the rule cannot see says
// so with a `portable: <why>` comment on that line.
//
// Scanned: everything a build or a person runs from this package --
// src, test, demo, bench, tools.

import assert from 'node:assert/strict';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, sep } from 'node:path';
import { describe, it } from 'node:test';

import { servedPath } from '../demo/static-files.ts';

const ROOTS = ['src', 'test', 'demo', 'bench', 'tools'];
const SELF = join('test', 'portability.test.ts');

function sources(dir: string, out: string[] = []): string[] {
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) {
      if (name !== 'node_modules') sources(p, out);
    } else if (/\.(ts|mjs|js|cjs)$/.test(name) && p !== SELF) {
      out.push(p);
    }
  }
  return out;
}

const FILES = ROOTS.flatMap((r) => sources(r));

interface Rule {
  readonly name: string;
  /** Only files this is true of are checked (default: every file). */
  readonly applies?: (text: string) => boolean;
  readonly bad: RegExp;
  readonly instead: string;
  /** A line the rule must flag, and its portable spelling, which it must not. */
  readonly witness: readonly [string, string];
}

const TOUCHES_FILES = (text: string): boolean =>
  /from ['"](node:)?(fs|fs\/promises|path)['"]/.test(text);

const RULES: readonly Rule[] = [
  {
    name: 'a URL pathname handed to the file system',
    applies: TOUCHES_FILES,
    bad: /\.pathname\b|\{[^}]*\bpathname\b[^}]*\}\s*=/,
    instead: 'pass the URL itself to fs, or fileURLToPath(url); an HTTP route'
      + ' compare says `portable:`',
    witness: ["const { pathname } = new URL('x', import.meta.url);",
      "readFileSync(new URL('x', import.meta.url))"],
  },
  {
    name: 'a file: URL built from a string',
    bad: /['"`]file:\/\/[^'"`]*\$\{|['"`]file:\/\/[^'"`]*['"`]\s*\+/,
    instead: 'pathToFileURL(path) -- a Windows path is not a URL path',
    witness: ['const base = `file://${process.cwd()}/`;',
      "const base = pathToFileURL(process.cwd() + sep).href;"],
  },
  {
    name: 'a hard-coded POSIX directory',
    bad: /['"`]\/(tmp|dev\/null|dev\/stdout|dev\/stderr|usr|etc)\b/,
    instead: 'os.tmpdir(), os.devNull, or a path the caller passes',
    witness: ["mkdtempSync('/tmp/dc-')", "mkdtempSync(join(tmpdir(), 'dc-'))"],
  },
  {
    name: 'a POSIX environment variable',
    bad: /process\.env\.(HOME|USER|TMPDIR|PATH|SHELL)\b/,
    instead: 'os.homedir(), os.userInfo(), os.tmpdir(); PATH is `Path` on'
      + ' Windows and splits on path.delimiter',
    witness: ["join(process.env.HOME ?? '', '.cache')", "join(homedir(), '.cache')"],
  },
  {
    name: 'a POSIX command or shell',
    bad: /\b(exec|execSync|execFile|execFileSync|spawn|spawnSync)\(\s*['"`](sh|bash|zsh|rm|cp|mv|mkdir|cat|ls|which|chmod|ln|find|grep|sed|awk|touch|kill|pkill|tar|curl|open|xdg-open)\b|\bshell:\s*true\b/,
    instead: 'node:fs / node:child_process with the program itself (process.execPath)',
    witness: ["execSync('rm -rf out')", "rmSync('out', { recursive: true, force: true })"],
  },
  {
    name: 'a symbolic link',
    bad: /\bsymlink(Sync)?\(/,
    instead: 'a copy -- a symlink on Windows needs Developer Mode or admin',
    witness: ["symlinkSync(from, to)", "copyFileSync(from, to)"],
  },
  {
    name: 'a static server that maps a URL to a file by hand',
    applies: (text) => /\bcreateServer\(/.test(text) && /\b(readFile|sendFile)\(/.test(text),
    bad: /\bnormalize\(|\.replace\(\/\^\(\\\.\\\./,
    instead: "servedPath(root, req.url) from demo/static-files.ts",
    witness: ["const rel = normalize(p).replace(/^(\\.\\.[/])+/, '');",
      'const file = servedPath(ROOT, req.url);'],
  },
];

function isComment(line: string): boolean {
  const t = line.trim();
  return t.startsWith('//') || t.startsWith('*') || t.startsWith('/*');
}

function violations(rule: Rule, file: string, text: string): string[] {
  if (rule.applies && !rule.applies(text)) return [];
  const out: string[] = [];
  text.split('\n').forEach((line, i) => {
    if (isComment(line) || /\bportable:\s*\S/.test(line)) return;
    if (rule.bad.test(line)) out.push(`${file}:${i + 1}: ${line.trim()}`);
  });
  return out;
}

describe('the Windows guardrail', () => {
  it('scans the whole package', () => {
    // A guardrail that scans nothing passes for the wrong reason.
    for (const root of ROOTS) {
      assert.ok(FILES.some((f) => f.startsWith(root + sep)), `nothing under ${root}/`);
    }
    assert.ok(FILES.length > 100, `${FILES.length} files`);
  });

  for (const rule of RULES) {
    it(`bans ${rule.name}`, () => {
      const [bad, good] = rule.witness;
      // The rule is PROVEN to see the mistake, and not its fix.
      const fixture = "import { x } from 'node:fs';\nimport { createServer } from 'node:http';\n"
        + 'createServer(); readFile();\n';
      assert.equal(violations(rule, 'witness', fixture + bad).length, 1,
        `the rule misses its own witness: ${bad}`);
      assert.deepEqual(violations(rule, 'witness', fixture + good), [],
        `the rule flags the portable spelling: ${good}`);
      const found = FILES.flatMap((f) => violations(rule, f, readFileSync(f, 'utf8')));
      assert.deepEqual(found, [], `use ${rule.instead}:\n${found.join('\n')}`);
    });
  }

  it('a `portable:` exemption gives its reason', () => {
    const bare = FILES.flatMap((f) => readFileSync(f, 'utf8').split('\n')
      .flatMap((line, i) => (/\bportable:\s*$/.test(line) ? [`${f}:${i + 1}`] : [])));
    assert.deepEqual(bare, []);
  });
});

describe('servedPath, on this platform', () => {
  // The demo servers' one URL -> file mapping. The Windows lane runs
  // these same cases with `\` separators and a drive letter.
  const root = mkdtempSync(join(tmpdir(), 'dc-served-'));
  writeFileSync(join(root, 'a b.txt'), 'x');
  const under = (...parts: string[]): string => join(root, ...parts);

  it('maps a path under the root, decoding it', () => {
    assert.equal(servedPath(root, '/a%20b.txt'), under('a b.txt'));
    assert.equal(servedPath(root, '/demo/x.js?v=1#h'), under('demo', 'x.js'));
    assert.equal(servedPath(`${root}${sep}`, '/demo/x.js'), under('demo', 'x.js'));
  });

  it('serves the index for /', () => {
    assert.equal(servedPath(root, '/'), under('demo', 'index.html'));
    assert.equal(servedPath(root, undefined), under('demo', 'index.html'));
    assert.equal(servedPath(root, '/', 'other/page.html'), under('other', 'page.html'));
  });

  it('never leaves the root', () => {
    for (const url of ['/../x', '/demo/../../x', '/%2e%2e/x', '/..%2fx', '/..%5cx',
      '/demo%2F..%2F..%2Fx']) {
      const got = servedPath(root, url);
      assert.ok(got === undefined || got.startsWith(root + sep), `${url} -> ${got}`);
    }
    assert.equal(servedPath(root, '/..%2fx'), undefined);
    assert.equal(servedPath(root, '/a%5cb'), undefined);
  });

  it('cleans up', () => rmSync(root, { recursive: true, force: true }));
});
