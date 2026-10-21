// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

/**
 * THE ONE-PAGE REFERENCE of lite's generated GraphQL (docs/GRAPHQL_STUDY_2026_10_18.md §8.4, G14; PR6): what an
 * agent reads before writing a document, as the MCP server's instructions and the {@code describe} tool's text.
 * Five examples: three right, two labelled wrong (the research's finding: the wrong examples are what keeps an
 * agent off Hasura's and bare-value spellings). The schema itself (the SDL) is the authority; this page is the
 * idiom.
 */
public final class GraphqlReference {

    private GraphqlReference() {
    }

    public static final String TEXT = """
            legend-lite GraphQL: the one-page reference

            The schema is generated from the Pure class model of a data space. Every class C of the data space is a
            root field `C(where: CWhere, orderBy: [COrderBy!], limit: Int, offset: Int): [C!]!`, with
            `CAggregate(where: CWhere): CAggregate!` and `CGroups(where, having, orderBy, limit, offset): [CGroup!]!`
            beside it. A class with subclasses is `interface CInterface`; its exact type keeps the class name C.

            SELECTIONS. A class's properties by name. A to-one property is an object (null when absent); a to-many
            property is a list field taking the same `where`, `orderBy`, `limit`, `offset`; a derived property with
            parameters takes them as arguments. Beside every to-many property p: `pAggregate(where: ...) { count
            sum { x } avg { x } min { x } max { x } }` and `pGroups(...)`, the parent's own groups (GROUPS below).
            `__typename` is the exact class.

            WHERE (Prisma's names). `{ field: { op: value } }` with the ops equals, not, in, notIn, lt, lte, gt,
            gte; strings add contains, startsWith, endsWith and `mode: insensitive`; `{ equals: null }` and
            `{ not: null }` test for null. A to-one object takes its own where under its name
            (`firm: { legalName: { equals: "Acme" } }`, `firm: { is: null }`, `firm: { isNot: null }`); a to-many
            takes `some`, `every` or `none` with a where inside. `AND: [...]`, `OR: [...]`, `NOT: { ... }`.
            A subclass's fields: `asManager: { budget: { gt: 1000 } }`; `asManager: { isNot: null }` is membership.

            ORDER BY. A list of single-key objects, the FIRST key primary: `orderBy: [{ lastName: ASC },
            { age: DESC }]`; a to-one path `{ firm: { legalName: ASC } }`; a subclass's `{ asManager: { budget: DESC } }`;
            a to-many's aggregate `{ employeesAggregate: { count: DESC } }`, `{ employeesAggregate: { max: { age: ASC } } }`.
            LIMIT and OFFSET are counts applied after the order; a limit without an orderBy is warned.

            AGGREGATES. `CAggregate(where: ...) { count sum { salary } avg { age } min { born } max { salary } }`
            is one object. sum and avg take numbers; min and max take numbers, dates and strings (by code point).

            GROUPS. `CGroups(where, having, orderBy, limit, offset) { key { a b { c } born(truncate: YEAR) } count
            sum { x } avg { x } }`: the selection under `key` IS the grouping (a to-one property nests its own key
            object; a date key takes `truncate: YEAR | QUARTER | MONTH | WEEK | DAY | HOUR`); `having` filters the
            groups: `{ count: { gte: 2 }, sum: { salary: { gt: 1000 } }, AND, OR, NOT }`; `orderBy: [{ count: DESC },
            { key: { a: ASC } }, { sum: { x: DESC } }]`, a key named in orderBy is selected under `key`. Under a
            parent object, `employeesGroups(...)` takes the same arguments and answers each parent's groups as a list
            (`[]` for a parent with none).

            TEMPORAL. A class versioned by date is read as of its dates: its roots take `businessDate: Date!`
            (business-temporal), `processingDate: DateTime!` (processing-temporal) or both (bitemporal), and so does
            every field reaching it: `listing(businessDate: "2024-01-01") { price }`, `null` where no version holds.
            A version holds from its first day up to, not on, its last. Each object answers the `businessDate` it was
            read as of. A dated navigation is not offered in `where`, `orderBy` or a group's `key`.

            INHERITANCE. `... on Manager { budget }` adds a subclass's fields where the row is one; spread on the
            INTERFACE's root (`PersonInterface`'s field `Person`) to see every row; `... on Person` alone selects
            exact instances only, which is refused when it would hide rows: ask `__typename` instead.

            RESULTS. Lists are arrays, never null; a to-one is an object or null; nulls are present; BigInt and
            Decimal are JSON numbers; dates are ISO strings; enums are their names; keys in the order written. An
            error anywhere fails the whole request: there is no partial data.

            EXAMPLES
            Right:
              1. { Person(where: { age: { gt: 30 } }, orderBy: [{ lastName: ASC }], limit: 10) { fullName age firm { legalName } } }
              2. { Firm(where: { employees: { some: { age: { gt: 50 } } } }) { legalName employeesAggregate { count avg { salary } } } }
              3. { PersonGroups(having: { count: { gte: 2 } }, orderBy: [{ count: DESC }]) { key { firm { legalName } } count max { age } } }
            Wrong:
              4. { Person(where: { age: { _gt: 30 } }) { fullName } }    -- Hasura's `_gt`: the operator is `gt`.
              5. { Person(where: { age: 30 }) { fullName } }    -- a bare value: a condition is an object, `age: { equals: 30 }`.

            TOOLS. describe (the SDL of the data space, or of the classes named, with this page); validate (a
            document's verdict: every error with its path and the nearest valid name, the warnings, the typed result
            shape); query (the answer in the GraphQL-over-HTTP envelope); explain (the Pure and the SQL per root
            field, with the verdict).
            """;
}
