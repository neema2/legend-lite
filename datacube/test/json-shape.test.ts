import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  fieldsOf,
  inferShape,
  parseJson,
  scalarTypeOf,
  type Field,
} from '../src/json-shape.ts';

/** Every extraction under a field, by default column name. */
function byName(fields: readonly Field[]): Map<string, { expression: string; type: string }> {
  const out = new Map<string, { expression: string; type: string }>();
  const walk = (f: Field): void => {
    for (const e of f.extractions) out.set(e.name, e);
    f.children.forEach(walk);
  };
  fields.forEach(walk);
  return out;
}

describe('parseJson', () => {
  it('keeps numbers as written', () => {
    // JSON.parse makes 1.0 an integer and rounds past 2^53: an id
    // column would read as a float, a float column as integers.
    const n = parseJson('[1, 1.0, 9007199254740993, -2e3]');
    assert.equal(n.t, 'array');
    assert.deepEqual(n.t === 'array' ? n.items : [], [
      { t: 'number', text: '1' },
      { t: 'number', text: '1.0' },
      { t: 'number', text: '9007199254740993' },
      { t: 'number', text: '-2e3' },
    ]);
  });

  it('reads strings with escapes, and refuses malformed text', () => {
    assert.deepEqual(parseJson('{"a\\"b":"c\\nd"}'),
      { t: 'object', entries: [['a"b', { t: 'string', value: 'c\nd' }]] });
    assert.throws(() => parseJson('{"a":1'));
    assert.throws(() => parseJson('[1] x'));
  });
});

describe('inferShape', () => {
  it('types a number position from its text', () => {
    const ints = inferShape(['{"v":1}', '{"v":9007199254740993}']);
    assert.equal(scalarTypeOf(ints.shape.fields.get('v')!), 'integer');
    const mixed = inferShape(['{"v":1}', '{"v":1.5}']);
    assert.equal(scalarTypeOf(mixed.shape.fields.get('v')!), 'float');
  });

  it('knows dates and timestamps, and falls to text when mixed', () => {
    const s = inferShape([
      '{"d":"2025-01-02","t":"2025-01-02T10:00:00Z","m":1}',
      '{"d":"2025-02-03","t":"2025-01-02 10:00","m":"x"}',
    ]).shape;
    assert.equal(scalarTypeOf(s.fields.get('d')!), 'date');
    assert.equal(scalarTypeOf(s.fields.get('t')!), 'datetime');
    assert.equal(scalarTypeOf(s.fields.get('m')!), 'text');
  });

  it('counts what it could not read, and skips empty cells', () => {
    const s = inferShape(['{"a":1}', null, 'not json']);
    assert.equal(s.rows, 3);
    assert.equal(s.unreadable, 1);
    assert.equal(s.shape.present, 1);
  });

  it('accepts a cell a driver already decoded', () => {
    const s = inferShape([{ a: 'x' }]);
    assert.equal(scalarTypeOf(s.shape.fields.get('a')!), 'text');
  });
});

describe('fieldsOf', () => {
  const orders = inferShape([
    '{"tier":"gold","contact":{"email":"a@x"},"n":1}',
    '{"tier":"silver","n":2.5}',
    '{"tier":"gold","n":3}',
  ]);

  it('reaches a nested key through get, typed by the sample', () => {
    const f = byName(fieldsOf('customer', '$x.customer', orders));
    assert.deepEqual(f.get('customer_tier'), {
      ...f.get('customer_tier'),
      expression: "$x.customer->get('tier')->to(@String)", type: 'String' });
    assert.equal(f.get('customer_contact_email')?.expression,
      "$x.customer->get('contact')->get('email')->to(@String)");
    assert.equal(f.get('customer_n')?.type, 'Float');
  });

  it('reports how often a key is there', () => {
    const [root] = fieldsOf('customer', '$x.customer', orders);
    const contact = root!.children.find((c) => c.path.join('.') === 'contact');
    assert.equal(Math.round(contact!.presence * 100), 33);
  });

  it('offers count, text, and contains for an array of scalars', () => {
    const tags = inferShape(['["gift","b2b"]', '["gift"]', '[]']);
    const f = byName(fieldsOf('tags', '$x.tags', tags));
    assert.equal(f.get('tags_count')?.expression, '$x.tags->toMany(@Variant)->size()');
    assert.equal(f.get('tags_list')?.expression, "$x.tags->toMany(@String)->joinStrings(', ')");
    assert.equal(f.get('tags_has_gift')?.expression, "$x.tags->toMany(@String)->contains('gift')");
    assert.equal(f.get('tags_has_gift')?.type, 'Boolean');
  });

  it('offers per-field text, first and total for an array of objects', () => {
    const items = inferShape(['[{"sku":"A","qty":2},{"sku":"B","qty":1}]']);
    const f = byName(fieldsOf('items', '$x.items', items));
    assert.equal(f.get('items_first_sku')?.expression,
      "$x.items->get(0)->get('sku')->to(@String)");
    assert.equal(f.get('items_qty_total')?.expression,
      "$x.items->toMany(@Variant)->map(e | $e->get('qty')->to(@Integer)->toOne())->sum()");
    assert.equal(f.get('items_sku_total'), undefined, 'text has no total');
  });

  it('quotes a key that is not a plain name', () => {
    const odd = inferShape(['{"it\'s":1}']);
    const f = byName(fieldsOf('c', '$x.c', odd));
    assert.equal(f.get('c_it_s')?.expression, "$x.c->get('it\\'s')->to(@Integer)");
  });
});

describe('freeName', () => {
  it('keeps a free name and numbers a taken one', async () => {
    const { freeName } = await import('../src/ui/json-fields.ts');
    assert.equal(freeName('tier', new Set(['x'])), 'tier');
    assert.equal(freeName('tier', new Set(['tier', 'tier_2'])), 'tier_3');
  });
});
