// A model's text as Studio's files (plan A7: text mode and the importer): every demo project's files joined into one
// text and split again give the same files, each holding one element by the real grammar; comments stay with the
// element below them; a keyword inside a body, a comment or a string starts nothing.

import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { describe, it } from 'node:test';

import { joinFiles, splitElements } from '../src/model/split.ts';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import { compiler } from './modules.ts';

const MANIFEST = runfileFromEnv('STUDIO_PROJECTS_MANIFEST');
const files = async (): Promise<string[]> => {
  const manifest = JSON.parse(await readFile(MANIFEST, 'utf8')) as { steps: { files: string[] }[] };
  // a later step's file replaces an element an earlier one wrote (Currency-1.1): each project's files once, in order
  const names = [...new Set(manifest.steps.flatMap((s) => s.files))].filter((f) => !f.includes('-1.1'));
  return Promise.all(names.map((f) => readFile(join(dirname(MANIFEST), f), 'utf8')));
};
const trim = (s: string): string => s.replace(/\s+$/, '');

describe("a model's text split into one file per element", () => {
  it("gives back every demo project's files, joined then split, each one element", async () => {
    const all = await files();
    const split = splitElements(joinFiles(all));
    assert.deepEqual(split.map(trim), all.map(trim));
    for (const f of split) assert.equal((await compiler.elements(f)).length, 1, f);
  });

  it('keeps comments with the element below, and starts nothing inside a body, a comment or a string', () => {
    const text = [
      '// the first',
      'Class a::A',
      '{',
      "  note: String[1] = 'Class a::Fake';",
      '  /* Enum a::NotOne',
      '     Class a::NorThis */',
      '}',
      '',
      '// about B',
      '// in two lines',
      'Enum a::B',
      '{',
      '  X',
      '}',
      '###Mapping',
      'Mapping a::M',
      '(',
      ')',
    ].join('\n');
    assert.deepEqual(splitElements(text), [
      "// the first\nClass a::A\n{\n  note: String[1] = 'Class a::Fake';\n  /* Enum a::NotOne\n     Class a::NorThis */\n}\n",
      '// about B\n// in two lines\nEnum a::B\n{\n  X\n}\n',
      '###Mapping\nMapping a::M\n(\n)\n',
    ]);
  });
});
