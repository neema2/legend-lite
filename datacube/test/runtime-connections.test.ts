// What a runtime runs on (src/runtime-connections.ts; docs/ONE_CONNECTION_DESK_2026_10_28.md step 5): each store a
// runtime binds and its connection, read from the model's elements -- a pointer followed, a Deephaven server's address
// and key reference read -- and where its grids run: the tab, one Deephaven server, or refused by name.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { engineOf, runtimeConnections } from '../src/runtime-connections.ts';

const connection = (name: string, value: unknown): unknown => ({ _type: 'connection', package: 'p', name, connectionValue: value });
const relational = (databaseType: string, spec: unknown, auth: unknown = { _type: 'test' }): unknown =>
  ({ _type: 'RelationalDatabaseConnection', databaseType, datasourceSpecification: spec, authenticationStrategy: auth });
const deephaven = (host: string, port: number): unknown => relational('Deephaven',
  { _type: 'deephaven', host, port, useTls: true }, { _type: 'preSharedKey', keyVaultReference: 'dh.psk' });
const runtime = (name: string, bindings: readonly (readonly [string, string])[]): unknown => ({
  _type: 'runtime', package: 'p', name,
  runtimeValue: {
    _type: 'engineRuntime', connectionStores: [], mappings: [],
    connections: bindings.map(([store, conn]) => ({
      store: { path: store }, storeConnections: [{ id: 'c', connection: { _type: 'connectionPointer', connection: conn } }],
    })),
  },
});

const elements = [
  connection('Duck', relational('DuckDB', { _type: 'duckDB' })),
  connection('Live', deephaven('localhost', 10000)),
  connection('Status', deephaven('localhost', 10000)),
  connection('Other', deephaven('dh.example', 10000)),
  runtime('InTab', [['s::DB', 'p::Duck']]),
  runtime('OnServer', [['s::DB', 'p::Live'], ['s::World', 'p::Status']]),
  runtime('Mixed', [['s::DB', 'p::Live'], ['s::World', 'p::Duck']]),
  runtime('TwoServers', [['s::DB', 'p::Live'], ['s::World', 'p::Other']]),
  runtime('Dangling', [['s::DB', 'p::Gone']]),
];

describe('what a runtime runs on', () => {
  it("reads each store's connection, a Deephaven server's address and its key's reference", () => {
    assert.deepEqual(runtimeConnections(elements, 'p::OnServer'), [
      { store: 's::DB', connection: 'p::Live', databaseType: 'Deephaven',
        deephaven: { host: 'localhost', port: 10000, useTls: true, keyVaultReference: 'dh.psk' } },
      { store: 's::World', connection: 'p::Status', databaseType: 'Deephaven',
        deephaven: { host: 'localhost', port: 10000, useTls: true, keyVaultReference: 'dh.psk' } },
    ]);
    assert.deepEqual(runtimeConnections(elements, 'p::InTab'), [{ store: 's::DB', connection: 'p::Duck', databaseType: 'DuckDB' }]);
  });

  it('runs a runtime on the tab, or on its one Deephaven server', () => {
    assert.deepEqual(engineOf('p::InTab', runtimeConnections(elements, 'p::InTab')), { kind: 'tab' });
    assert.deepEqual(engineOf('p::OnServer', runtimeConnections(elements, 'p::OnServer')),
      { kind: 'deephaven', server: { host: 'localhost', port: 10000, useTls: true, keyVaultReference: 'dh.psk' } });
  });

  it('refuses, by name, a runtime the model lacks, a connection it lacks, a server beside another database, two servers', () => {
    assert.throws(() => runtimeConnections(elements, 'p::Nope'), /the model declares no runtime p::Nope/);
    assert.throws(() => runtimeConnections(elements, 'p::Dangling'), /binds s::DB to p::Gone, which the model does not declare/);
    assert.throws(() => engineOf('p::Mixed', runtimeConnections(elements, 'p::Mixed')), /a Deephaven server beside another database/);
    assert.throws(() => engineOf('p::TwoServers', runtimeConnections(elements, 'p::TwoServers')), /binds two Deephaven servers/);
  });

  it('refuses a Deephaven connection whose sign-in is not a key by vault reference', () => {
    const bad = [connection('Bad', relational('Deephaven', { _type: 'deephaven', host: 'h', port: 1, useTls: true })),
      runtime('R', [['s::DB', 'p::Bad']])];
    assert.throws(() => runtimeConnections(bad, 'p::R'), /a key by vault reference/);
  });
});
