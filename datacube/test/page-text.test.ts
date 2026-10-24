// A text tile's words (src/page-text.ts): its small Markdown read into blocks and runs (drawn on a page and edited:
// page-text-dom.test.ts); its rule (src/page-text-rule.ts).

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { readRuns, readText } from '../src/page-text.ts';
import { MAX_TEXT, textProblem } from '../src/page-text-rule.ts';

describe("a text tile's words", () => {
  it('a heading, paragraphs parted by a blank line, a line break kept, lists of either kind', () => {
    assert.deepEqual(readText('# Sales\n\nThe year so far.\nBy desk.\n\n- EMEA\n- AMER\n1. first\n2. second'), [
      { kind: 'heading', level: 1, runs: [{ kind: 'text', text: 'Sales' }] },
      { kind: 'paragraph', runs: [{ kind: 'text', text: 'The year so far.' }, { kind: 'break' }, { kind: 'text', text: 'By desk.' }] },
      { kind: 'list', ordered: false, items: [[{ kind: 'text', text: 'EMEA' }], [{ kind: 'text', text: 'AMER' }]] },
      { kind: 'list', ordered: true, items: [[{ kind: 'text', text: 'first' }], [{ kind: 'text', text: 'second' }]] },
    ]);
  });

  it('bold, italic, code and a link; a mark with no end, or escaped, its own text; a link to anywhere else its words', () => {
    assert.deepEqual(readRuns('**up** 3% *vs* `plan`, see [the deck](https://x.test/d)'), [
      { kind: 'bold', runs: [{ kind: 'text', text: 'up' }] },
      { kind: 'text', text: ' 3% ' },
      { kind: 'italic', runs: [{ kind: 'text', text: 'vs' }] },
      { kind: 'text', text: ' ' },
      { kind: 'code', text: 'plan' },
      { kind: 'text', text: ', see ' },
      { kind: 'link', href: 'https://x.test/d', runs: [{ kind: 'text', text: 'the deck' }] },
    ]);
    assert.deepEqual(readRuns('2 * 3 and \\*kept\\* and **open'), [{ kind: 'text', text: '2 * 3 and *kept* and **open' }]);
    assert.deepEqual(readRuns('[run](javascript:alert(1))'), [{ kind: 'text', text: 'run' }, { kind: 'text', text: ')' }]);
  });

  it('refused in words: blank, too long, not text', () => {
    assert.equal(textProblem('# ok'), null);
    assert.equal(textProblem('  \n '), 'a text tile has words: its text is blank');
    assert.match(textProblem('x'.repeat(MAX_TEXT + 1)) ?? '', /holds at most 10,000 characters, not 10,001/);
    assert.equal(textProblem(3), "a text tile's text is words, not 3");
  });
});
