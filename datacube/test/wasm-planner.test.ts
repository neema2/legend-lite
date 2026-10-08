import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { PlanError } from '../src/planner.ts';
import { fromElement, toJson } from '../../pure-protocol/src/index.ts';
import {
  fileUrlToPath,
  pathToFileUrl,
  PlannerUnavailableError,
  WasmPlanner,
} from '../src/wasm-planner.ts';

/** A module's OK answer: the SQL and its result type (here, typing nothing). */
function ok(sql: string): string {
  return `OK\n${JSON.stringify({ sql, type: { _type: 'relationType', columns: [] } })}`;
}

/**
 * A stand-in for the 4 MB module.
 *
 * These tests are about the SEAM -- how the answer string is parsed,
 * what is cached, which error type surfaces -- not about the planner,
 * which has its own 69-query differential against the JVM. Loading
 * the real module here would make a fast unit suite slow and would
 * test legend-lite twice.
 */
function fakeRuntime(
  answer: (model: string, query: string, runtime: string) => string,
  onLoad?: () => void,
  onWarm?: (model: string) => void,
) {
  return async () => ({
    async load() {
      onLoad?.();
      return {
        exports: {
          planOrError: (m: string, q: string, r: string) => answer(m, q, r),
          relationTypeOrError: () => 'OK\n{"_type":"relationType","columns":[]}',
          tableModelOrError: () => 'ERR\nfake\nnot in this fake',
          catalogColumnsSqlOrError: () => 'ERR\nfake\nnot in this fake',
          planJsonOrError: (m: string, q: string, r: string) => answer(m, q, r),
          relationTypeJsonOrError: () => 'OK\n{"_type":"relationType","columns":[]}',
          composeLambdaOrError: () => 'ERR\nfake\nnot in this fake',
          lambdaJsonOrError: () => 'ERR\nfake\nnot in this fake',
          modelJsonOrError: () => 'ERR\nfake\nnot in this fake',
          testDataSqlOrError: () => 'ERR\nfake\nnot in this fake',
          warmModel: (m: string) => { onWarm?.(m); return 1; },
        },
      };
    },
  });
}

function planner(
  answer: (model: string, query: string, runtime: string) => string,
  extra: {
    cache?: boolean;
    onLoad?: () => void;
    onWarm?: (model: string) => void;
  } = {},
) {
  return new WasmPlanner({
    model: '###Relational\nDatabase trades::DB ( Table T ( a VARCHAR(1) ) )',
    runtime: 'trades::RT',
    ...(extra.cache === undefined ? {} : { cache: extra.cache }),
    loadRuntime: fakeRuntime(answer, extra.onLoad, extra.onWarm),
  });
}

describe('WasmPlanner', () => {
  it('returns the SQL an OK answer carries', async () => {
    const p = planner(() => ok('SELECT t0.a FROM T AS t0'));
    assert.equal((await p.planText('grammar')).sql,
      'SELECT t0.a FROM T AS t0',
    );
  });

  it('carries the compiler\'s result type beside the SQL', async () => {
    const type = {
      _type: 'relationType',
      columns: [
        { name: 'region', genericType: { rawType: { _type: 'packageableType', fullPath: 'String' } } },
        { name: 'total', genericType: { rawType: { _type: 'packageableType', fullPath: 'Decimal' } } },
      ],
    };
    const p = planner(() => `OK\n${JSON.stringify({ sql: 'SELECT 1', type })}`);
    assert.deepEqual(await p.planText('g'), {
      sql: 'SELECT 1',
      columns: [{ name: 'region', type: 'String' }, { name: 'total', type: 'Decimal' }],
    });
  });

  it('passes the model and runtime through to the module', async () => {
    let seen: string[] = [];
    const p = planner((m, q, r) => {
      seen = [m, q, r];
      return ok('SELECT 1');
    });
    await p.planText('the-grammar');
    assert.match(seen[0]!, /Database trades::DB/);
    assert.equal(seen[1], 'the-grammar');
    assert.equal(seen[2], 'trades::RT');
  });

  it('keeps multi-line SQL intact', async () => {
    // The OK tag is stripped by finding the FIRST newline; every
    // newline after that belongs to the SQL.
    const sql = 'SELECT t0.a\nFROM T AS t0\nWHERE t0.a = 1';
    const p = planner(() => ok(sql));
    assert.equal((await p.planText('g')).sql, sql);
  });

  it('raises a PlanError carrying the compiler message on ERR', async () => {
    const p = planner(() =>
      'ERR\ncom.legend.compiler.spec.TypeInferenceException\n'
      + "unknown column 'nope'");
    await assert.rejects(() => p.planText('g2'), (e: unknown) => {
      assert.ok(e instanceof PlanError);
      // The exception CLASS is dropped; the message is what a user
      // can act on, and it must match the HTTP planner's text.
      assert.equal(e.message, "unknown column 'nope'");
      assert.equal(e.subject, 'g2');
      return true;
    });
  });

  it('keeps a multi-line compiler message whole', async () => {
    const p = planner(() =>
      'ERR\ncom.legend.parser.ParseException\n[1:40] expected expression,'
      + '\n  got end of input');
    await assert.rejects(() => p.planText('g'), (e: unknown) => {
      assert.ok(e instanceof PlanError);
      assert.equal(
        e.message,
        '[1:40] expected expression,\n  got end of input',
      );
      return true;
    });
  });

  it('does not cache a refusal', async () => {
    // A refusal is about the query, not the module, so re-asking is
    // cheap and correct -- but caching it would also mean a cache
    // entry whose value is an exception, which the cache cannot hold.
    let calls = 0;
    const p = planner(() => {
      calls++;
      return 'ERR\nX\nnope';
    });
    await assert.rejects(() => p.planText('g'));
    await assert.rejects(() => p.planText('g'));
    assert.equal(calls, 2);
    assert.equal(p.cacheSize, 0);
  });

  it('caches by grammar, and can be told not to', async () => {
    let calls = 0;
    const cached = planner(() => {
      calls++;
      return ok('SELECT 1');
    });
    await cached.planText('g');
    await cached.planText('g');
    assert.equal(calls, 1);
    assert.equal(cached.cacheSize, 1);

    calls = 0;
    const uncached = planner(() => {
      calls++;
      return ok('SELECT 1');
    }, { cache: false });
    await uncached.planText('g');
    await uncached.planText('g');
    assert.equal(calls, 2);
  });

  it('loads the module once however many plans race', async () => {
    // An initial render issues several plans at once. Memoising a
    // boolean rather than the promise would start several 4 MB
    // fetches; this pins that it does not.
    let loads = 0;
    const p = planner(() => ok('SELECT 1'), { onLoad: () => { loads++; } });
    await Promise.all([
      p.planText('a'),
      p.planText('b'),
      p.planText('c'),
    ]);
    assert.equal(loads, 1);
  });

  it('warmUp loads the module without planning', async () => {
    let loads = 0;
    let plans = 0;
    const p = planner(() => {
      plans++;
      return ok('SELECT 1');
    }, { onLoad: () => { loads++; } });
    await p.warmUp();
    assert.equal(loads, 1);
    assert.equal(plans, 0);
    await p.planText('g');
    assert.equal(loads, 1, 'warmUp must satisfy the later load');
  });

  it('warmUp BUILDS THE BOOT LAYER, not just the module', async () => {
    // The first version only loaded the module, and browser timings
    // showed why that is useless: instantiate is ~45ms and the ~1.1s
    // that makes a first plan slow is boot-layer construction, which
    // stayed on the critical path. warmUp must force that work, with
    // the real model, or it warms nothing.
    const warmed: string[] = [];
    const p = planner(() => ok('SELECT 1'),
      { onWarm: (m) => { warmed.push(m); } });
    await p.warmUp();
    assert.equal(warmed.length, 1, 'warmUp must call warmModel');
    assert.match(warmed[0]!, /Database trades::DB/,
      'warmModel must get the REAL model, so the graph it builds is'
      + ' the one the first plan wants');
  });

  it('honours an abort raised before the call', async () => {
    const ctl = new AbortController();
    const reason = new Error('superseded');
    ctl.abort(reason);
    let planned = false;
    const p = planner(() => {
      planned = true;
      return ok('SELECT 1');
    });
    await assert.rejects(
      () => p.planText('g', ctl.signal),
      (e: unknown) => e === reason,
    );
    assert.equal(planned, false, 'an aborted plan must not do the work');
  });

  it('discards an answer aborted while planning', async () => {
    // The module is synchronous, so the abort cannot interrupt it --
    // but a stale answer must still not reach the grid.
    const ctl = new AbortController();
    const reason = new Error('superseded');
    const p = planner(() => {
      ctl.abort(reason);
      return ok('SELECT 1');
    });
    await assert.rejects(
      () => p.planText('g', ctl.signal),
      (e: unknown) => e === reason,
    );
  });

  it('reports a missing runtime asset as unavailable, not a bad query',
    async () => {
      // PlanError means "your query is wrong" and is shown to the
      // user. A missing asset is an operational fault; conflating
      // them files every failed deploy as a bad query.
      const p = new WasmPlanner({
        model: 'm',
        runtime: 'r',
        loadRuntime: () => Promise.reject(new Error('404')),
      });
      await assert.rejects(() => p.planText('g'), (e: unknown) => {
        assert.ok(e instanceof PlannerUnavailableError);
        assert.ok(!(e instanceof PlanError));
        assert.match(e.message, /bazel build \/\/datacube:site/);
        return true;
      });
    });

  it('lets a retry succeed after a transient load failure', async () => {
    let attempt = 0;
    const p = new WasmPlanner({
      model: 'm',
      runtime: 'r',
      loadRuntime: async () => {
        attempt++;
        if (attempt === 1) throw new Error('network blip');
        return {
          async load() {
            return {
              exports: {
                planOrError: () => ok('SELECT 1'),
                relationTypeOrError: () => 'OK\n{"_type":"relationType","columns":[]}',
                tableModelOrError: () => 'ERR\nfake\nnot in this fake',
                catalogColumnsSqlOrError: () => 'ERR\nfake\nnot in this fake',
                planJsonOrError: () => 'ERR\nfake\nnot in this fake',
                relationTypeJsonOrError: () => 'ERR\nfake\nnot in this fake',
                composeLambdaOrError: () => 'ERR\nfake\nnot in this fake',
                lambdaJsonOrError: () => 'ERR\nfake\nnot in this fake',
                modelJsonOrError: () => 'ERR\nfake\nnot in this fake',
                testDataSqlOrError: () => 'ERR\nfake\nnot in this fake',
                warmModel: () => 1,
              },
            };
          },
        };
      },
    });
    await assert.rejects(() => p.planText('g'), PlannerUnavailableError);
    assert.equal((await p.planText('g')).sql, 'SELECT 1');
  });

  it('plans a TREE through the JSON entry, sending the query\'s exact JSON, cached by it', async () => {
    const seen: string[] = [];
    const p = planner((_m, q) => {
      seen.push(q);
      return ok('SELECT 1');
    });
    const query = fromElement('trades::T').select(['a']).lambda();
    assert.equal((await p.plan(query)).sql, 'SELECT 1');
    await p.plan(fromElement('trades::T').select(['a']).lambda());
    assert.deepEqual(seen, [toJson(query)], 'an equal tree is the same query: planned once');
  });

  it('keeps the text entry and the tree entry apart in its cache', async () => {
    let calls = 0;
    const p = planner(() => {
      calls++;
      return ok('SELECT 1');
    });
    const query = fromElement('trades::T').select(['a']).lambda();
    await p.plan(query);
    await p.planText(toJson(query));
    assert.equal(calls, 2);
  });

  it('rejects an answer with neither tag rather than guessing', async () => {
    const p = planner(() => 'SELECT 1');
    await assert.rejects(() => p.planText('g'),
      PlannerUnavailableError);
  });
});

describe('fileUrlToPath', () => {
  // Node's own fileURLToPath is the reference; WasmPlanner cannot import
  // it (it is bundled for the browser too). Both platform modes are
  // checked on every platform, so a Windows-only fault fails on a Mac.
  const VALID = [
    'file:///C:/users/runneradmin/_bazel/x/wasm/planner/classes.wasm',
    'file:///c:/lower/drive.wasm',
    'file:///D:/a%20space/%E6%97%A5%E6%9C%AC/classes.wasm',
    'file://server/share/planner/classes.wasm',
    'file:///tmp/wasm/planner/classes.wasm',
    'file:///Users/me/a%20b/classes.wasm',
  ];

  for (const windows of [true, false]) {
    for (const url of VALID) {
      it(`matches node's fileURLToPath (windows: ${windows}) for ${url}`, () => {
        let expected: string | Error;
        try {
          expected = fileURLToPath(url, { windows });
        } catch (e) {
          expected = e as Error;
        }
        if (expected instanceof Error) {
          assert.throws(() => fileUrlToPath(url, windows));
        } else {
          assert.equal(fileUrlToPath(url, windows), expected);
        }
      });
    }
  }
});

describe('pathToFileUrl', () => {
  // Node's own pathToFileURL is the reference, in both platform modes
  // on every platform -- the working directory is turned into the base
  // every WASM asset resolves against, and `file://${cwd}` was wrong
  // on Windows.
  const CASES: [string, boolean][] = [
    ['C:\\Users\\runneradmin\\work\\', true],
    ['C:\\a space\\\u65e5\u672c\\x.wasm', true],
    ['d:\\lower\\', true],
    ['\\\\server\\share\\planner\\', true],
    ['/Users/me/a b/', false],
    ['/home/runner/work/x#y?z/', false],
    ['/odd/100%@$=+,;\\name/', false],
    ['C:\\odd\\100%@$=+,;\\', true],
  ];
  for (const [path, windows] of CASES) {
    it(`matches node's pathToFileURL (windows: ${windows}) for ${path}`, () => {
      assert.equal(pathToFileUrl(path, windows), pathToFileURL(path, { windows }).href);
    });
  }
});

describe('WasmPlanner.withModel: another source on the page, the same module', () => {
  it('plans over its own model and runtime, loading the module once', async () => {
    let loads = 0;
    const asked: string[] = [];
    const first = planner((m, _q, r) => { asked.push(`${m}|${r}`); return ok('SELECT 1'); }, { onLoad: () => { loads += 1; } });
    const second = first.withModel('model B', 'b::RT');
    await second.planText('g');
    await first.planText('g');
    assert.equal(loads, 1, 'one module for both');
    assert.deepEqual(asked.map((a) => a.split('|')[1]), ['b::RT', 'trades::RT']);
    assert.equal(asked[0]!.split('|')[0], 'model B');
  });

  it('a borrowed planner\'s dispose leaves its maker working', async () => {
    const first = planner(() => ok('SELECT 1'));
    first.withModel('model B', 'b::RT').dispose();
    assert.equal((await first.planText('g')).sql, 'SELECT 1');
  });
});
