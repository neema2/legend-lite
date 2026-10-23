// GRAPH MODE'S G0 (docs/DATACUBE_GRAPH_MODE_2026_10_18.md): a Cypher query planned in the tab -- by the same compiler
// the page plans every grid with, in WebAssembly -- and run on the tab's DuckDB, answering as the island's own
// integration test holds (core: CypherIslandIntegrationTest, its rows derived by hand from Cypher's semantics there). No
// server. And the island as a node of the tree DataCube sends (its `plan`, never text: the tab's grammar parse refuses
// lite's islands), composed with Pure as a grid's query composes with its source (G1).

import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import { openDatabase } from '#test-host';

import { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import type { LitePlanner } from '../src/lite-planner.ts';
import { plannerFor } from './catalog-builder.ts';
import { agg, asc, col, fn, from, lambda, variable, type ValueSpecification } from '../../pure-protocol/src/index.ts';

// the island's integration model (core/src/test/java/com/legend/integration/CypherIslandIntegrationTest.java): people,
// firms and cities -- a person's firm an association, a firm's city a class-typed property -- over three tables
const MODEL = `
###Pure
Class test::Person { name: String[1]; age: Integer[0..1]; active: Boolean[0..1]; }
Class test::Firm { legalName: String[1]; founded: Integer[0..1]; hq: test::City[0..1]; }
Class test::City { name: String[1]; }
Association test::PersonFirm { firm: test::Firm[0..1]; employees: test::Person[*]; }
###Relational
Database store::DB
(
    Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), AGE INTEGER, ACTIVE BIT, FIRM_ID INTEGER)
    Table T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100), FOUNDED INTEGER, CITY_ID INTEGER)
    Table T_CITY (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
    Join PersonFirm(T_PERSON.FIRM_ID = T_FIRM.ID)
    Join FirmCity(T_FIRM.CITY_ID = T_CITY.ID)
)
###Mapping
Mapping test::M
(
    test::Person: Relational
    {
        ~mainTable [store::DB] T_PERSON
        name: [store::DB] T_PERSON.NAME,
        age: [store::DB] T_PERSON.AGE,
        active: [store::DB] T_PERSON.ACTIVE
    }
    test::Firm: Relational
    {
        ~mainTable [store::DB] T_FIRM
        legalName: [store::DB] T_FIRM.LEGAL_NAME,
        founded: [store::DB] T_FIRM.FOUNDED,
        hq: [store::DB] @FirmCity
    }
    test::City: Relational
    {
        ~mainTable [store::DB] T_CITY
        name: [store::DB] T_CITY.NAME
    }
    test::PersonFirm: Relational { AssociationMapping (
        employees: [store::DB]@PersonFirm,
        firm: [store::DB]@PersonFirm
    ) }
)
###Connection
RelationalDatabaseConnection store::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
###Runtime
Runtime test::RT { mappings: [ test::M ]; connections: [ store::DB: [ c1: store::Conn ] ]; }
`;

let engine: DuckDbEngine;
let planner: LitePlanner;

before(async () => {
  const db = await openDatabase();
  engine = new DuckDbEngine(db.connection);
  // the island's integration rows: two cities, two firms (Globex with no city), four people (Diana with no firm)
  for (const sql of [
    'CREATE TABLE T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), AGE INTEGER, ACTIVE BOOLEAN, FIRM_ID INTEGER)',
    'CREATE TABLE T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100), FOUNDED INTEGER, CITY_ID INTEGER)',
    'CREATE TABLE T_CITY (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))',
    "INSERT INTO T_CITY VALUES (1, 'NYC'), (2, 'LA')",
    "INSERT INTO T_FIRM VALUES (1, 'Acme', 1990, 1), (2, 'Globex', NULL, NULL)",
    "INSERT INTO T_PERSON VALUES (1, 'Alice', 30, true, 1), (2, 'Bob', 40, NULL, 1), (3, 'Charlie', 25, false, 2), (4, 'Diana', NULL, true, NULL)",
  ]) await engine.run(sql, 0);
  planner = plannerFor(MODEL, 'test::RT');
});

after(async () => engine?.close());

/** A query's rows, each its cells joined by '|' (null as 'null'), as the island's integration test writes them. */
async function rows(query: string): Promise<string[]> {
  return written(await engine.execute(await planner.planText(query), 0));
}

function written(t: ResultTable): string[] {
  return Array.from({ length: t.rowCount }, (_, i) => t.columns.map((c) => {
    const v = c.values[i];
    return v === null || v === undefined ? 'null' : String(v);
  }).join('|'));
}

describe("a Cypher query planned in the tab and run on its DuckDB, answering as the island's integration test holds", () => {
  it("a match returns each node's property", async () => {
    assert.deepEqual(await rows('#Cypher{MATCH (p:Person) RETURN p.name ORDER BY p.name}#'), ['Alice', 'Bob', 'Charlie', 'Diana']);
  });

  it('a hop along an association, and a hop back counted by group', async () => {
    assert.deepEqual(await rows('#Cypher{MATCH (p:Person)-[:firm]->(f:Firm) RETURN p.name AS n, f.legalName AS firm ORDER BY n}#'),
      ['Alice|Acme', 'Bob|Acme', 'Charlie|Globex']);
    assert.deepEqual(await rows('#Cypher{MATCH (f:Firm)<-[:firm]-(p:Person) RETURN f.legalName AS firm, count(*) AS people ORDER BY firm}#'),
      ['Acme|2', 'Globex|1']);
  });

  it("nodes compared by their identity (their class's key): a cartesian product joined on it", async () => {
    assert.deepEqual(await rows('#Cypher{MATCH (a:Person), (b:Person) WHERE a = b RETURN count(*) AS c}#'), ['4']);
  });

  it("the island as a node of the tree DataCube sends, composed with Pure -- grouped, sorted -- as a grid's query composes with its source (G1)", async () => {
    // the node a Cypher source would be in a grid's query (the protocol's form of #Cypher{...}#), built as DataCube
    // builds the rest of the query; its rows computed for the data: Acme's people 30 and 40, Globex's 25
    const cypher = { _type: 'classInstance', type: 'Cypher', value: { cypher: 'MATCH (p:Person)-[:firm]->(f:Firm) RETURN f.legalName AS firm, p.age AS age' } };
    const query = from(cypher as unknown as ValueSpecification)
      .groupBy(['firm'], [agg('total', lambda(['x'], col('x', 'age')), lambda(['y'], fn('sum', variable('y'))))])
      .sort([asc('firm')])
      .lambda();
    assert.deepEqual(written(await engine.execute(await planner.plan(query), 0)), ['Acme|70', 'Globex|25']);
  });
});
