// Guardrails on the shape of the source, in the spirit of the repo's
// own CodeShapeGuardrailTest: each one exists because the mistake it
// bans actually happened here.

import assert from 'node:assert/strict';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { describe, it } from 'node:test';

function sources(dir: string, out: string[] = []): string[] {
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (name.endsWith('.ts')) out.push(p);
  }
  return out;
}

const FILES = sources('src');

function isComment(line: string): boolean {
  const t = line.trim();
  return t.startsWith('//') || t.startsWith('*') || t.startsWith('/*');
}

describe('source guardrails', () => {
  it('finds source files at all', () => {
    // A guardrail that scans nothing passes for the wrong reason.
    assert.ok(FILES.length > 10, `${FILES.length} files`);
  });

  it('has no cross-realm DOM instanceof checks', () => {
    // This bit twice. The constructor belongs to the document's
    // realm, so in an iframe -- or a test DOM -- the check is false
    // and the code silently does nothing. Duck-type the method
    // actually used instead.
    const bad: string[] = [];
    for (const file of FILES) {
      readFileSync(file, 'utf8')
        .split('\n')
        .forEach((line, i) => {
          if (isComment(line)) return;
          if (
            /instanceof\s+(HTMLElement|Element|Node|HTMLInputElement)\b/.test(
              line,
            )
          ) {
            bad.push(`${file}:${i + 1}`);
          }
        });
    }
    assert.deepEqual(bad, [], `duck-type instead: ${bad.join(', ')}`);
  });

  it('has no literal control characters in source', () => {
    // Two separators were written as literal NUL bytes by accident.
    // An invisible byte survives no reformat and no code review, and
    // it silently breaks every search for the code around it.
    const control = new RegExp('[\\u0000-\\u0008\\u000b\\u000c\\u000e-\\u001f]');
    const bad: string[] = [];
    for (const file of FILES) {
      if (control.test(readFileSync(file, 'utf8'))) bad.push(file);
    }
    assert.deepEqual(bad, [], `use an escape sequence: ${bad.join(', ')}`);
  });

  it('keeps the demo-only planner out of the product', () => {
    // "One planner" is an architectural commitment, and a convenient
    // second one is exactly how such commitments rot.
    const bad = FILES.filter((f) =>
      /DemoOnlyPlanner|filterToSql/.test(readFileSync(f, 'utf8')),
    );
    assert.deepEqual(bad, [], `demo code in src: ${bad.join(', ')}`);
  });
});
