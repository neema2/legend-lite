// A text tile's words drawn on a page (src/page-text.ts textElement): elements whose every word is text -- markup typed
// is shown, never run -- a link opening in a new tab. And its editor: a toolbar that writes the marks, the words, and
// what they will look like beside them.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { JSDOM } from 'jsdom';

import { FILE_COLUMNS, FILE_ROWS, csvRows, fileElement, readText, textEditor, textElement } from '../src/page-text.ts';
import { textLines } from '../src/page/cube-page.ts';

describe("a text tile's words drawn", () => {
  it('as elements whose every word is text: markup typed is shown, never run', () => {
    const doc = new JSDOM('').window.document;
    const el = textElement(doc, readText('## <b>Note</b>\n\n<img src=x onerror=alert(1)> [go](https://x.test)\n\n1. one\n2. two'));
    assert.equal(el.querySelector('h4')?.textContent, '<b>Note</b>');
    assert.equal(el.querySelector('img'), null);
    assert.match(el.querySelector('p')?.textContent ?? '', /<img src=x/);
    const a = el.querySelector('a');
    assert.deepEqual([a?.getAttribute('href'), a?.target, a?.rel], ['https://x.test', '_blank', 'noopener noreferrer']);
    assert.deepEqual([...el.querySelectorAll('ol li')].map((li) => li.textContent), ['one', 'two']);
  });

  it("GitHub's blocks drawn: a table aligned, code as typed, a quote, a rule, struck words, a task's box shown but not pressed", () => {
    const doc = new JSDOM('').window.document;
    const el = textElement(doc, readText([
      '| Desk | Notional |', '|---|--:|', '| FX | 1,200 |', '',
      '```', '<script>alert(1)</script>', '```', '',
      '> quoted', '', '---', '', '~~old~~', '', '- [x] done', '', '#### four', '', '5. five',
    ].join('\n')));
    assert.deepEqual([...el.querySelectorAll('th')].map((th) => [th.textContent, th.style.textAlign]), [['Desk', ''], ['Notional', 'right']]);
    assert.deepEqual([...el.querySelectorAll('td')].map((td) => td.textContent), ['FX', '1,200']);
    assert.equal(el.querySelector('.dc-text-table > table') !== null, true, 'a table scrolls in its own box');
    assert.equal(el.querySelector('pre > code')?.textContent, '<script>alert(1)</script>');
    assert.equal(el.querySelector('script'), null);
    assert.equal(el.querySelector('blockquote > p')?.textContent, 'quoted');
    assert.notEqual(el.querySelector('hr'), null);
    assert.equal(el.querySelector('del')?.textContent, 'old');
    const box = el.querySelector<HTMLInputElement>('li.dc-text-task > input[type=checkbox]');
    assert.deepEqual([box?.checked, box?.disabled], [true, true]);
    // done in its HTML too, as an export writes it (the 2026-10-25 audit: the property alone exported an empty box)
    assert.ok(box?.outerHTML.includes('checked'), box?.outerHTML ?? 'no box');
    assert.equal(el.querySelector('h6')?.textContent, 'four', 'a deep heading, the smallest');
    assert.equal(el.querySelector('ol')?.getAttribute('start'), '5');
  });

  it('as an export writes it: a line a block -- a task\u2019s box, a list in a list indented, code, a quote, a table, a rule', () => {
    const doc = new JSDOM('').window.document;
    const el = textElement(doc, readText('# Q3\n\n- [x] shipped\n  1. EMEA\n\n```\na\nb\n```\n\n> said\n\n| Desk | N |\n|---|---|\n| FX | 2 |\n\n---'));
    assert.deepEqual([...el.children].flatMap(textLines),
      ['Q3', '- [x] shipped', '  1. EMEA', 'a', 'b', '> said', 'Desk | N', 'FX | 2', '---']);
  });

  it('its editor: OK judged by the rule first, the words told; Escape cancels; Ctrl+Enter is OK', () => {
    const dom = new JSDOM('');
    const doc = dom.window.document;
    const told: string[] = [];
    let cancelled = 0;
    const editor = textEditor(doc, { words: '# T', onOk: (w) => told.push(w), onCancel: () => { cancelled += 1; } });
    doc.body.append(...editor.elements);
    const ok = [...doc.querySelectorAll('button')].find((b) => b.textContent === 'OK')!;
    editor.area.value = '  ';
    ok.click();
    assert.match(doc.querySelector('.dc-text-problem')?.textContent ?? '', /blank/);
    assert.deepEqual(told, [], 'refused: nothing told');
    editor.area.value = 'New';
    editor.area.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Enter', ctrlKey: true }));
    assert.deepEqual(told, ['New']);
    editor.area.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'Escape' }));
    assert.equal(cancelled, 1);
  });

  // the user, 2026-10-25: Markdown with a toolbar, and what it will look like beside it
  it('its toolbar writes the marks around what is chosen, and the preview beside the words follows them', () => {
    const dom = new JSDOM('');
    const doc = dom.window.document;
    const editor = textEditor(doc, { words: 'Sales up\nEMEA\nAMER', onOk: () => {}, onCancel: () => {} });
    doc.body.append(...editor.elements);
    const area = editor.area;
    // read afresh each time: an assertion on the words narrows `area.value` to what it said
    const words = (): string => area.value;
    const tool = (name: string) => doc.querySelector<HTMLButtonElement>(`.dc-text-tool[data-mark="${name}"]`) ?? assert.fail(`no ${name}`);
    const preview = () => doc.querySelector('.dc-text-preview') ?? assert.fail('no preview');
    assert.deepEqual([...doc.querySelectorAll('.dc-text-tool')].map((b) => b.getAttribute('aria-label')), [
      'Bold', 'Italic', 'Strikethrough', 'Inline code', 'Heading', 'Bulleted list', 'Numbered list', 'Task list', 'Quote',
      'Link', 'Code block', 'Table', 'Horizontal rule',
    ]);
    assert.equal(preview().querySelector('p')?.textContent, 'Sales upEMEAAMER', 'drawn from the first');
    // the preview's words are not the tile's drawn words, which a page exports
    assert.equal(doc.querySelector('.dc-text-tile'), null);

    area.setSelectionRange(6, 8);
    tool('Bold').click();
    assert.equal(words(), 'Sales **up**\nEMEA\nAMER');
    assert.equal(preview().querySelector('strong')?.textContent, 'up', 'the preview follows');

    // the lines chosen made a list, and again, not
    area.setSelectionRange(13, words().length);
    tool('Bulleted list').click();
    assert.equal(words(), 'Sales **up**\n- EMEA\n- AMER');
    assert.deepEqual([...preview().querySelectorAll('li')].map((li) => li.textContent), ['EMEA', 'AMER']);
    tool('Bulleted list').click();
    assert.equal(words(), 'Sales **up**\nEMEA\nAMER');
    // one kind of list made another, not taken apart; a blank line left blank (the 2026-10-25 audit)
    area.value = '- a\n\n- b';
    area.setSelectionRange(0, area.value.length);
    tool('Task list').click();
    assert.equal(words(), '- [ ] a\n\n- [ ] b');
    tool('Numbered list').click();
    assert.equal(words(), '1. a\n\n2. b');
    tool('Numbered list').click();
    assert.equal(words(), 'a\n\nb');
    area.value = 'Sales **up**\nEMEA\nAMER';

    // a heading, one level deeper each press
    area.setSelectionRange(0, 0);
    tool('Heading').click();
    tool('Heading').click();
    assert.match(words(), /^## Sales \*\*up\*\*\n/);

    // a link: the address chosen, to type over
    area.setSelectionRange(words().indexOf('EMEA'), words().indexOf('EMEA') + 4);
    tool('Link').click();
    assert.equal(words().slice(area.selectionStart, area.selectionEnd), 'https://');
    assert.match(words(), /\[EMEA\]\(https:\/\/\)/);

    // a table, on lines of its own
    area.setSelectionRange(words().length, words().length);
    tool('Table').click();
    assert.match(words(), /AMER\n\n\| Column \| Column \|\n\| --- \| --- \|\n\| Cell \| Cell \|$/);
    assert.notEqual(preview().querySelector('table'), null);

    // Ctrl+I, as the toolbar's Italic
    area.setSelectionRange(words().length - 6, words().length - 4);
    area.dispatchEvent(new dom.window.KeyboardEvent('keydown', { key: 'i', ctrlKey: true }));
    assert.match(words(), /\| \*Ce\*ll \|$/);

    // typed where the browser types (insertText), so its undo takes a toolbar's edit back (the 2026-10-25 audit)
    const typed: string[] = [];
    (doc as unknown as { execCommand: (c: string, ui: boolean, text: string) => boolean }).execCommand = (c, _ui, text) => {
      typed.push(c);
      area.setRangeText(text, area.selectionStart, area.selectionEnd, 'end');
      return true;
    };
    area.value = 'up';
    area.setSelectionRange(0, 2);
    tool('Bold').click();
    assert.deepEqual([words(), typed], ['**up**', ['insertText']]);
  });
});

describe("a text box's file, drawn (docs/DATACUBE_UI_REVIEW_2026_10_24.md §8)", () => {
  const doc = new JSDOM('').window.document;

  it('Markdown as the box draws its own words, never as HTML, and none of it the tile\'s own words', () => {
    const el = fileElement(doc, 'markdown', '# Plan\n\nBy **desk** <img src=x onerror=alert(1)>', ',');
    assert.equal(el.className, 'dc-text-file dc-text-words', 'the words\' type; an export takes the box\'s own words');
    assert.equal(el.querySelector('h3')?.textContent, 'Plan');
    assert.equal(el.querySelector('strong')?.textContent, 'desk');
    assert.equal(el.querySelector('img'), null, 'typed markup is text');
  });

  it('text as it is; JSON indented, or as it is and said when it does not read', () => {
    assert.equal(fileElement(doc, 'text', 'a  b\n<b>x</b>', ',').querySelector('pre')?.textContent, 'a  b\n<b>x</b>');
    assert.equal(fileElement(doc, 'json', '{"a":[1,2]}', ',').querySelector('pre')?.textContent, '{\n  "a": [\n    1,\n    2\n  ]\n}');
    const broken = fileElement(doc, 'json', '{"a":', ',');
    assert.match(broken.querySelector('.dc-text-file-note')?.textContent ?? '', /^This is not JSON a browser reads/);
    assert.equal(broken.querySelector('pre')?.textContent, '{"a":');
    // a file's start (over 2 MB): shown as it is, never said to be no JSON (the 2026-10-25 audit)
    const cut = fileElement(doc, 'json', '{"a": [1, 2', ',', 'its first 2 MB');
    assert.equal(cut.querySelector('.dc-text-file-note'), null);
    assert.equal(cut.querySelector('pre')?.textContent, '{"a": [1, 2');
  });

  it('CSV as a table: its first line the header, its first rows, the rest counted', () => {
    const el = fileElement(doc, 'csv', 'region,notional\nEMEA,"1,200"\nAPAC,5\n', ',');
    assert.deepEqual([...el.querySelectorAll('th')].map((t) => t.textContent), ['region', 'notional']);
    assert.deepEqual([...el.querySelectorAll('tbody tr')].map((r) => [...r.querySelectorAll('td')].map((c) => c.textContent)),
      [['EMEA', '1,200'], ['APAC', '5']]);
    const long = ['n', ...Array.from({ length: FILE_ROWS + 3 }, (_, i) => String(i))].join('\n');
    const many = fileElement(doc, 'csv', long, ',');
    assert.equal(many.querySelectorAll('tbody tr').length, FILE_ROWS);
    assert.equal(many.querySelector('.dc-text-file-note')?.textContent, 'and 3 more rows');
    assert.equal(fileElement(doc, 'csv', '', ',').querySelector('.dc-text-file-note')?.textContent, 'The file is empty.');
    // counted in the part read, when that is the file's start; its columns past FILE_COLUMNS counted (the 2026-10-25
    // audit: a file of commas drew a header of two million cells)
    assert.equal(fileElement(doc, 'csv', long, ',', 'its first 2 MB').querySelector('.dc-text-file-note')?.textContent,
      'and 3 more rows in its first 2 MB');
    const wide = fileElement(doc, 'csv', `${','.repeat(FILE_COLUMNS + 4)}\n1,2`, ',');
    assert.equal(wide.querySelectorAll('th').length, FILE_COLUMNS);
    assert.equal(wide.querySelector('.dc-text-file-note')?.textContent, 'and 5 more columns');
  });

  it('reads CSV by RFC 4180: quoted separators, line breaks and quotes; CRLF; tabs for a TSV; no row for a last empty line', () => {
    assert.deepEqual(csvRows('a,b\r\n"x,1","say ""hi"""\r\n"two\nlines",3\r\n', ',', 10),
      { rows: [['a', 'b'], ['x,1', 'say "hi"'], ['two\nlines', '3']], more: 0 });
    assert.deepEqual(csvRows('a\tb\n1\t2', '\t', 10), { rows: [['a', 'b'], ['1', '2']], more: 0 });
    assert.deepEqual(csvRows('a,,\n', ',', 10), { rows: [['a', '', '']], more: 0 });
    assert.deepEqual(csvRows('h\n1\n2\n3', ',', 2), { rows: [['h'], ['1']], more: 2 });
  });
});
