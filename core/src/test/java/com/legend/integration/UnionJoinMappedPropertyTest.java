package com.legend.integration;

import com.legend.testing.LaneFloor;

import com.legend.testing.Lean;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * UnionSynthesis finding A (batch 164; ROOT CAUSE found and fixed in batch
 * 166): an inheritance Operation over same-table, filtered members where
 * a CLASS-typed property named {@code columns} is mapped by JOIN on one
 * member. The union root reads its lifted navigate slot as
 * {@code $u_row.columns} — and the Typer's TDS reflection surface typed
 * ANY row's {@code .columns} as the column-name list (String), so the
 * ctor failed "expected R, got String" (the system store's
 * {@code Relation.columns : RelationalOperationElement[*]} — the shape
 * that parked Column). Row-vs-Relation rule: on a bare ROW a declared
 * column of that name wins; reflection serves a TABLE always. The
 * property is named {@code columns} here on purpose: this test IS the
 * witness of the clash.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class UnionJoinMappedPropertyTest {

    private static final String MODEL = """
            ###Pure
            Class ux::A { name: String[1]; columns: ux::R[*]; }
            Class ux::B extends ux::A { }
            Class ux::C extends ux::A { }
            Class ux::R { id: String[1]; }
            Class ux::R1 extends ux::R { }
            ###Relational
            Database ux::db
            (
                Table T (id VARCHAR(10) PRIMARY KEY, kind VARCHAR(10), name VARCHAR(20), ref_id VARCHAR(10))
                Table RT (id VARCHAR(10) PRIMARY KEY)
                Filter isB(T.kind = 'B')
                Filter isC(T.kind = 'C')
                Join TR(T.ref_id = RT.id)
            )
            ###Mapping
            Mapping ux::m
            (
                *ux::A: Operation
                {
                    meta::pure::router::operations::inheritance_OperationSetImplementation_1__SetImplementation_MANY_()
                }
                ux::B[b]: Relational
                {
                    ~filter [ux::db]isB
                    ~mainTable [ux::db]T
                    name: [ux::db]T.name,
                    columns[r1]: [ux::db]@TR
                }
                ux::C[c]: Relational
                {
                    ~filter [ux::db]isC
                    ~mainTable [ux::db]T
                    name: [ux::db]T.name
                }
                *ux::R: Operation
                {
                    meta::pure::router::operations::inheritance_OperationSetImplementation_1__SetImplementation_MANY_()
                }
                ux::R1[r1]: Relational
                {
                    ~mainTable [ux::db]RT
                    id: [ux::db]RT.id
                }
            )
            ###Connection
            RelationalDatabaseConnection ux::conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime ux::rt { mappings: [ ux::m ]; connections: [ ux::db: [ environment: ux::conn ] ]; }
            """;

    private static List<List<Object>> rows(Connection connection, String query) throws Exception {
        var result = Lean.run(MODEL, query, "ux::rt", connection);
        List<List<Object>> out = new ArrayList<>();
        for (var row : result.rows()) {
            out.add(new ArrayList<>(row));
        }
        out.sort((x, y) -> String.valueOf(x).compareTo(String.valueOf(y)));
        return out;
    }

    @Test
    @DisplayName("a class-typed property join-mapped on one member only: the other member's rows read it as empty")
    void joinMappedOnOneMemberOnly() throws Exception {
        try (Connection connection = LaneFloor.backend().open()) {
            try (var stmt = connection.createStatement()) {
                stmt.execute("create table T (id varchar(10) primary key, kind varchar(10), name varchar(20), ref_id varchar(10))");
                stmt.execute("create table RT (id varchar(10) primary key)");
                stmt.execute("insert into RT values ('r1')");
                stmt.execute("insert into T values ('1', 'B', 'b1', 'r1'), ('2', 'C', 'c1', 'r1'), ('3', 'X', 'x1', 'r1')");
            }
            assertEquals(List.of(List.of("b1"), List.of("c1")),
                    rows(connection, "|ux::A.all()->project(~[name: a|$a.name])"),
                    "the parent extent is the union of the filtered members");
            assertEquals(List.of(List.of("b1", "r1"), java.util.Arrays.asList("c1", null)),
                    rows(connection, "|ux::A.all()->project(~[name: a|$a.name, ref: a|$a.columns.id])"),
                    "the join-mapped property navigates on B rows and is empty on C rows");
            assertEquals(List.of(List.of("b1", "r1")),
                    rows(connection, "|ux::A.all()->filter(a|$a->instanceOf(ux::B))->cast(@ux::B)->project(~[name: a|$a.name, ref: a|$a.columns.id])"),
                    "cast to the mapping member reads the join on the same row");
        }
    }
}
