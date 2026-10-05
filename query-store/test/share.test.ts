// A saved query's share link: the query, round trip, and nothing of the store; a bad link refused.

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';

import { isQueryFragment, queryFragment, readQueryFragment } from '../src/share.ts';
import type { Query } from '../src/wire.ts';
import { runfileNamed } from '../../tools/js/runfiles.mts';

const fixture = (name: string): Query => JSON.parse(readFileSync(runfileNamed('SAVED_QUERIES', `${name}.json`), 'utf8')) as Query;

describe('a saved query share link', () => {
  for (const name of ['explicit-context', 'data-space-context', 'default-parameter-values', 'graph-fetch']) {
    it(`${name}: the query comes back, its store fields do not`, async () => {
      const q = fixture(name);
      const link = await queryFragment(q);
      assert.ok(isQueryFragment(link) && isQueryFragment(`#${link}`));
      assert.match(link, /^q1\.[A-Za-z0-9_-]+$/);
      const back = await readQueryFragment(`#${link}`);
      for (const f of ['name', 'groupId', 'artifactId', 'versionId', 'executionContext', 'content', 'defaultParameterValues'] as const) {
        assert.deepEqual(back[f], q[f], f);
      }
      for (const f of ['id', 'owner', 'createdAt', 'lastUpdatedAt', 'lastOpenAt', 'version', 'validUntil', 'deletedAt']) {
        assert.equal(f in back, false, f);
      }
      assert.ok(link.length < 1000, `${link.length} characters`);
    });
  }

  it('refuses what it cannot read, saying why', async () => {
    await assert.rejects(readQueryFragment('p1.abc'), /not a saved query link/);
    await assert.rejects(readQueryFragment('q9.abc'), /a q9 link, which this version cannot read/);
    await assert.rejects(readQueryFragment('q1.!!!'), /not base64url|cut short/);
    const link = await queryFragment(fixture('explicit-context'));
    await assert.rejects(readQueryFragment(link.slice(0, link.length - 6)), /cut short or altered/);
  });
});
