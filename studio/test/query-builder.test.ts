// The query builder's Save Query in Studio (plan A5): a service's query replaced in its text, the rest kept as written,
// and read back with the real grammar -- the query the builder composed (PRETTY, many lines) is what the service holds.

import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { describe, it } from 'node:test';

import type { PService } from '../../engine-client/src/legend/pmcd.ts';
import { queryBuilder } from '../src/backend/query-builder.ts';
import { serviceQueryRange, withServiceQuery } from '../src/model/service-query.ts';
import { runfileFromEnv } from '../../tools/js/runfiles.mts';
import { grammar } from './modules.ts';

const PROJECTS = dirname(runfileFromEnv('STUDIO_PROJECTS_MANIFEST'));
const serviceText = (): Promise<string> => readFile(join(PROJECTS, 'party', 'PartyService.pure'), 'utf8');

const builder = queryBuilder({
  modelJson: (t) => grammar.modelJson(t),
  lambdaJson: (t) => grammar.lambdaJson(t),
  lambdaText: (l, style) => grammar.lambdaText(l, style),
  plane: () => Promise.reject(new Error('no plane: Save Query only reads text')),
});
const functionText = (): Promise<string> => readFile(join(PROJECTS, 'party', 'allParties.pure'), 'utf8');

describe("a service's query in its text", () => {
  it('is the lambda after query:, to its closing semicolon', async () => {
    const text = await serviceText();
    const r = serviceQueryRange(text)!;
    assert.equal(text.slice(r.start, r.end), '|demo::party::Party.all()->project(~[name: p | $p.name, country: p | $p.country])');
  });

  it('is found past comments and strings that say query:, and ends past semicolons inside braces', () => {
    const text = "Service a::S\n{\n  // query: not this;\n  documentation: 'query: nor this;';\n  execution: Single\n  {\n    query: {|let x = 1; a::C.all()};\n    mapping: a::M;\n  }\n}\n";
    const r = serviceQueryRange(text)!;
    assert.equal(text.slice(r.start, r.end), '{|let x = 1; a::C.all()}');
  });

  it("takes a many-line query, its later lines indented under query:'s", () => {
    const out = withServiceQuery('  execution: Single\n  {\n    query: |a::C.all();\n    mapping: a::M;\n  }\n', '|a::C.all()\n  ->take(5)');
    assert.equal(out, '  execution: Single\n  {\n    query: |a::C.all()\n      ->take(5);\n    mapping: a::M;\n  }\n');
  });
});

describe('Save Query', () => {
  it('writes the composed query into the service, which the grammar reads back as that query', async () => {
    const text = await serviceText();
    const query = await grammar.lambdaText(
      await grammar.lambdaJson("|demo::party::Party.all()->filter(p | $p.country == 'GB')->project(~[name: p | $p.name])"), 'PRETTY');
    const next = await builder.serviceWithQuery(text, query);
    // the rest of the file as written: its comment, pattern, mapping and runtime
    assert.ok(next.startsWith('###Service\n// Every party, its name and country, served at /parties'));
    assert.ok(next.includes('    mapping: demo::party::PartyMapping;\n    runtime: demo::party::Runtime;\n'));
    const s = (await grammar.modelJson(next)).elements.find((e): e is PService => e._type === 'service')!;
    assert.equal(await grammar.lambdaText(s.execution.func!, 'STANDARD'), await grammar.lambdaText(await grammar.lambdaJson(query), 'STANDARD'));
  });

  it("writes a function's query into its body, the signature and comment as written, read back as that query", async () => {
    const text = await functionText();
    const query = await grammar.lambdaText(await grammar.lambdaJson(
      "|demo::party::Party.all()->filter(p | $p.country == 'GB')->project(~[name: p | $p.name])->from(demo::party::PartyMapping, demo::party::Runtime)"), 'PRETTY');
    const next = await builder.functionWithQuery(text, query);
    assert.ok(next.startsWith('// Every party, its name and country: a function the demo runs in the tab, on the party rows (PartyData).\nfunction demo::party::allParties(): meta::pure::metamodel::relation::Relation<Any>[1]\n{\n'));
    const f = (await grammar.modelJson(next)).elements.find((e) => e._type === 'function') as unknown as { body: never[] };
    assert.equal(await grammar.lambdaText({ _type: 'lambda', parameters: [], body: f.body } as never, 'STANDARD'),
      await grammar.lambdaText({ ...(await grammar.lambdaJson(query)), parameters: [] }, 'STANDARD'));
  });

  it("refuses a function query whose parameters are not the function's", async () => {
    await assert.rejects(builder.functionWithQuery(await functionText(), "{x: String[1]|demo::party::Party.all()->project(~[name: p | $p.name])->from(demo::party::PartyMapping, demo::party::Runtime)}"),
      /parameters differ from the function's/);
  });

  it('refuses a service with no query: to replace', async () => {
    await assert.rejects(builder.serviceWithQuery('Class a::C {}', '|a::C.all()'), /no `query:`/);
  });
});
