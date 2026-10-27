// Files in a text box (src/text-files.ts; docs/DATACUBE_UI_REVIEW_2026_10_24.md §8): the one rule for what a text box's
// files are -- read by the page's command, its reader and Python alike.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { MAX_TEXT_FILES, MAX_TEXT_FILE_SIZE, bytesWords, csvSeparator, pickedRefusal, readTextFiles, textFileFormat } from '../src/text-files.ts';

const SHA = 'a'.repeat(64);
const file = (name: string, sha = SHA, size = 10): Record<string, unknown> => ({ name, size, sha256: sha });

describe('a text box\'s files', () => {
  it('are drawn by their names\' extensions: Markdown, text, CSV (and TSV), JSON; any other a kind it does not show', () => {
    assert.equal(textFileFormat('notes.md'), 'markdown');
    assert.equal(textFileFormat('README.Markdown'), 'markdown');
    assert.equal(textFileFormat('a.txt'), 'text');
    assert.equal(textFileFormat('server.log'), 'text');
    assert.equal(textFileFormat('trades.csv'), 'csv');
    assert.equal(textFileFormat('trades.TSV'), 'csv');
    assert.equal(textFileFormat('shape.json'), 'json');
    assert.equal(textFileFormat('deck.pptx'), undefined);
    assert.equal(textFileFormat('Makefile'), undefined);
    assert.equal(csvSeparator('trades.tsv'), '\t');
    assert.equal(csvSeparator('trades.csv'), ',');
  });

  it('are read as each one\'s name, size and fingerprint, in order', () => {
    const b = 'b'.repeat(64);
    assert.deepEqual(readTextFiles([file('notes.md'), file('t.csv', b, 0)]),
      [{ name: 'notes.md', size: 10, sha256: SHA }, { name: 't.csv', size: 0, sha256: b }]);
    assert.deepEqual(readTextFiles([]), []);
  });

  it('refuse, in words, what a text box cannot show', () => {
    assert.equal(readTextFiles({}), "a text box's files are a list");
    assert.equal(readTextFiles(Array.from({ length: MAX_TEXT_FILES + 1 }, (_, i) => file(`n${i}.md`, i.toString(16).padStart(64, '0')))),
      `a text box shows ${MAX_TEXT_FILES} files at most`);
    assert.equal(readTextFiles(['notes.md']), 'a file is its name, size and sha256');
    assert.equal(readTextFiles([{ ...file('notes.md'), bytes: 'x' }]), 'a file is its name, size and sha256, not its bytes');
    assert.equal(readTextFiles([file(' ')]), 'a file has a name');
    assert.equal(readTextFiles([file('notes.md', SHA, -1)]), "notes.md's size is a whole number of bytes");
    assert.equal(readTextFiles([file('notes.md', 'ABC')]), "notes.md's sha256 is 64 lower-case hex digits");
    assert.match(readTextFiles([file('deck.pptx')]) as string, /^deck\.pptx is not a kind of file a text box shows: Markdown, text, CSV or JSON/);
    assert.equal(readTextFiles([file('a.md'), file('b.md')]), 'b.md is in the text box twice');
    assert.equal(readTextFiles([file('huge.log', SHA, 100 * 1024 * 1024)]), 'huge.log is 100 MB, larger than the 64 MB a text box takes');
    assert.deepEqual(readTextFiles([file('big.log', SHA, MAX_TEXT_FILE_SIZE)]), [{ name: 'big.log', size: MAX_TEXT_FILE_SIZE, sha256: SHA }]);
  });

  it('picked, are refused in the same words before they are read: too many, a kind it does not show, too big', () => {
    // the screen reads each whole to fingerprint it (the 2026-10-25 audit: a gigabyte read before its kind was looked at)
    assert.equal(pickedRefusal([{ name: 'notes.md', size: 10 }, { name: 't.csv', size: 0 }]), undefined);
    assert.match(pickedRefusal([{ name: 'notes.md', size: 10 }, { name: 'clip.mp4', size: 4e9 }])!, /^clip\.mp4 is not a kind of file a text box shows/);
    assert.equal(pickedRefusal([{ name: 'huge.log', size: MAX_TEXT_FILE_SIZE + 1 }]), 'huge.log is 64.0 MB, larger than the 64 MB a text box takes');
    assert.equal(pickedRefusal(Array.from({ length: MAX_TEXT_FILES + 1 }, (_, i) => ({ name: `n${i}.md`, size: 1 }))),
      `a text box shows ${MAX_TEXT_FILES} files at most`);
  });

  it('say their size as a person reads it', () => {
    assert.equal(bytesWords(1), '1 byte');
    assert.equal(bytesWords(812), '812 bytes');
    assert.equal(bytesWords(14 * 1024), '14 KB');
    assert.equal(bytesWords(3.25 * 1024 * 1024), '3.3 MB');
    assert.equal(bytesWords(2 * 1024 * 1024), '2 MB');
  });
});
