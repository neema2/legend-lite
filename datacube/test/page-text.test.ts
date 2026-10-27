// A text tile's words (src/page-text.ts): GitHub's Markdown read into blocks and runs (drawn on a page and edited:
// page-text-dom.test.ts); its rule (src/page-text-rule.ts).

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { readRuns, readText, type TextBlock } from '../src/page-text.ts';
import { MAX_TEXT, textProblem } from '../src/page-text-rule.ts';

const t = (text: string) => ({ kind: 'text', text }) as const;
const p = (...runs: object[]) => ({ kind: 'paragraph', runs }) as const;

describe("a text tile's words", () => {
  it('a heading, paragraphs parted by a blank line, a line break kept, lists of either kind', () => {
    assert.deepEqual(readText('# Sales\n\nThe year so far.\nBy desk.\n\n- EMEA\n- AMER\n1. first\n2. second'), [
      { kind: 'heading', level: 1, runs: [t('Sales')] },
      p(t('The year so far.'), { kind: 'break' }, t('By desk.')),
      { kind: 'list', ordered: false, items: [{ runs: [t('EMEA')] }, { runs: [t('AMER')] }] },
      { kind: 'list', ordered: true, items: [{ runs: [t('first')] }, { runs: [t('second')] }] },
    ]);
  });

  it('bold, italic, code and a link; a mark with no end, or escaped, its own text; a link to anywhere else its words', () => {
    assert.deepEqual(readRuns('**up** 3% *vs* `plan`, see [the deck](https://x.test/d)'), [
      { kind: 'bold', runs: [t('up')] },
      t(' 3% '),
      { kind: 'italic', runs: [t('vs')] },
      t(' '),
      { kind: 'code', text: 'plan' },
      t(', see '),
      { kind: 'link', href: 'https://x.test/d', runs: [t('the deck')] },
    ]);
    assert.deepEqual(readRuns('2 * 3 and \\*kept\\* and **open'), [t('2 * 3 and *kept* and **open')]);
    // its address to the parenthesis that closes it (a balanced pair inside it its own): no link, its words kept
    assert.deepEqual(readRuns('[run](javascript:alert(1))'), [t('run')]);
  });

  it('a link\'s words to their own bracket, its address past parentheses in it, no link in a link (the 2026-10-25 audit)', () => {
    // a bracket before a link was the link's first: `[1] or [docs](...)` drew one link saying "1] or [docs"
    assert.deepEqual(readRuns('See [1] or [docs](https://x.test)'), [t('See [1] or '), { kind: 'link', href: 'https://x.test', runs: [t('docs')] }]);
    assert.deepEqual(readRuns('[Foo](https://w.test/Foo_(bar)) end'),
      [{ kind: 'link', href: 'https://w.test/Foo_(bar)', runs: [t('Foo')] }, t(' end')]);
    assert.deepEqual(readRuns('[a [b] c](https://x.test)'), [{ kind: 'link', href: 'https://x.test', runs: [t('a [b] c')] }]);
    assert.deepEqual(readRuns('[https://a.test](https://b.test)'), [{ kind: 'link', href: 'https://b.test', runs: [t('https://a.test')] }]);
  });

  it('reads a long line in time linear in it, and deep nesting as words (the 2026-10-25 audit: a file is 2 MB)', () => {
    // each unclosed mark searched to the line's end: 60 KB of `*a ` took 4 s; 3,000 nested quotes overflowed the stack
    for (const line of ['*a '.repeat(40_000), '**a '.repeat(30_000), '['.repeat(100_000), '<'.repeat(100_000), '`` a '.repeat(30_000)]) {
      const started = performance.now();
      readRuns(line);
      // linear: tens of milliseconds here; quadratic, 13 s for the stars
      assert.ok(performance.now() - started < 5000, `${line.slice(0, 6)}... read in ${Math.round(performance.now() - started)} ms`);
    }
    const quotes = readText(`${'> '.repeat(3000)}deep`);
    let depth = 0;
    for (let b: TextBlock | undefined = quotes[0]; b?.kind === 'quote'; b = b.blocks[0]) depth++;
    assert.equal(depth, 17, 'quotes read to the deepest, the rest their words');
    // nested past the deepest: read, not thrown (a stack overflowed)
    assert.ok(readText(Array.from({ length: 500 }, (_, i) => `${'  '.repeat(i)}- item`).join('\n')).length > 0);
    assert.ok(readRuns(`${'['.repeat(5000)}x${'](https://a.test)'.repeat(5000)}`).length > 0);
  });

  // the user, 2026-10-25: a full Markdown editor -- GitHub's
  it("GitHub's blocks: every heading, one underlined, fenced code kept as typed, quotes, rules, an ordered list's start", () => {
    assert.deepEqual(readText('#### Four\n###### Six\nTitle\n===\nSub\n---'), [
      { kind: 'heading', level: 4, runs: [t('Four')] },
      { kind: 'heading', level: 6, runs: [t('Six')] },
      { kind: 'heading', level: 1, runs: [t('Title')] },
      { kind: 'heading', level: 2, runs: [t('Sub')] },
    ]);
    assert.deepEqual(readText('```sql\nselect *\n  from **t**\n```\n\n~~~\nplain\n~~~'), [
      { kind: 'code', language: 'sql', text: 'select *\n  from **t**' },
      { kind: 'code', text: 'plain' },
    ]);
    assert.deepEqual(readText('> Said **once**\n> - a\n> - b\n\n---\n\n3. three\n4. four'), [
      { kind: 'quote', blocks: [p(t('Said '), { kind: 'bold', runs: [t('once')] }), { kind: 'list', ordered: false, items: [{ runs: [t('a')] }, { runs: [t('b')] }] }] },
      { kind: 'rule' },
      { kind: 'list', ordered: true, start: 3, items: [{ runs: [t('three')] }, { runs: [t('four')] }] },
    ]);
    // `#hashtag` is no heading, as on GitHub
    assert.deepEqual(readText('#hashtag'), [p(t('#hashtag'))]);
  });

  it('a list in a list, by its indent; tasks done and not', () => {
    const [list] = readText('- [x] shipped\n- [ ] review\n  - EMEA\n  - AMER\n    1. desk') as [TextBlock];
    assert.deepEqual(list, {
      kind: 'list', ordered: false, items: [
        { runs: [t('shipped')], checked: true },
        {
          runs: [t('review')], checked: false, blocks: [{
            kind: 'list', ordered: false, items: [
              { runs: [t('EMEA')] },
              { runs: [t('AMER')], blocks: [{ kind: 'list', ordered: true, items: [{ runs: [t('desk')] }] }] },
            ],
          }],
        },
      ],
    });
  });

  it('a table: its head, each column aligned as its delimiter says, a `|` escaped or in code its words, short rows filled', () => {
    assert.deepEqual(readText('| Desk | Notional | Note |\n|:--|--:|:-:|\n| FX | 1,200 | a \\| b |\n| Rates | `x|y` |'), [{
      kind: 'table',
      align: ['left', 'right', 'center'],
      head: [[t('Desk')], [t('Notional')], [t('Note')]],
      rows: [
        [[t('FX')], [t('1,200')], [t('a | b')]],
        [[t('Rates')], [{ kind: 'code', text: 'x|y' }], []],
      ],
    }]);
    // no delimiter row: words with a pipe in them
    assert.deepEqual(readText('a | b'), [p(t('a | b'))]);
  });

  it("GitHub's runs: struck, both, __bold__; an `_` inside a word the word's; addresses in angle brackets or bare", () => {
    assert.deepEqual(readRuns('~~old~~ ***new*** __big__ _small_'), [
      { kind: 'strike', runs: [t('old')] }, t(' '),
      { kind: 'bold', runs: [{ kind: 'italic', runs: [t('new')] }] }, t(' '),
      { kind: 'bold', runs: [t('big')] }, t(' '),
      { kind: 'italic', runs: [t('small')] },
    ]);
    // a column's name, as a data page writes them
    assert.deepEqual(readRuns('trade_id and desk_name'), [t('trade_id and desk_name')]);
    assert.deepEqual(readRuns('see <https://x.test/a> or https://x.test/b.'), [
      t('see '), { kind: 'link', href: 'https://x.test/a', runs: [t('https://x.test/a')] },
      t(' or '), { kind: 'link', href: 'https://x.test/b', runs: [t('https://x.test/b')] }, t('.'),
    ]);
    // an image is never fetched: its words, linked to it; a link's title is not its address
    assert.deepEqual(readRuns('![the chart](https://x.test/c.png) [deck](https://x.test/d "Q3")'), [
      { kind: 'link', href: 'https://x.test/c.png', runs: [t('the chart')] }, t(' '),
      { kind: 'link', href: 'https://x.test/d', runs: [t('deck')] },
    ]);
  });

  it('refused in words: blank, too long, not text', () => {
    assert.equal(textProblem('# ok'), null);
    assert.equal(textProblem('  \n '), 'a text tile has words: its text is blank');
    assert.match(textProblem('x'.repeat(MAX_TEXT + 1)) ?? '', /holds at most 10,000 characters, not 10,001/);
    assert.equal(textProblem(3), "a text tile's text is words, not 3");
  });
});
