package com.legend.exec;

import com.legend.testing.Lean;
import com.legend.database.Databases;
import com.legend.testbackend.TestBackends;
import com.legend.test.StorelessRuntime;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Class-instance VALUES in SQL (the STRUCT design): {@code ^Class(…)} lowers
 * to a struct with the MODEL's canonical layout — declared stored properties
 * in declaration order, an omitted optional a NULL field, never a shape
 * inferred from the instance's own value set. Composite results unwrap to
 * ordered field maps at the executor.
 */
class StructValueTest {

    private static final String MODEL = """
            Class m::Person
            {
              firstName: String[1];
              lastName: String[1];
              nick: String[0..1];
            }
            Class m::Holder
            {
              name: String[1];
              addresses: m::Addr[*];
              values: m::Val[*];
            }
            Class m::Addr { val: String[1]; }
            Class m::Val { val: Integer[1]; }
            ###Relational
            Database m::DB ( Table T (ID INTEGER) )
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = TestBackends.platform().open();
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private Lean.Result run(String query) throws SQLException {
        return Lean.run(StorelessRuntime.with(MODEL, Databases.PLATFORM), query, StorelessRuntime.RUNTIME, conn);
    }

    @Test
    @DisplayName("literal field access inlines — no struct round-trip")
    void literalFieldAccess() throws SQLException {
        var r = run("|^m::Person(firstName='John', lastName='Doe').firstName");
        assertEquals("John", r.one());
    }

    @Test
    @DisplayName("an instance VALUE root unwraps to the model's canonical field map")
    void instanceRootIsOrderedMap() throws SQLException {
        var r = run("|^m::Person(firstName='John', lastName='Doe')");
        Object v = r.one();
        Map<?, ?> m = assertInstanceOf(Map.class, v);
        assertEquals(List.of("firstName", "lastName", "nick"),
                m.keySet().stream().map(Object::toString).toList(),
                "layout = DECLARED properties in DECLARATION order, not the value set");
        assertEquals("John", m.get("firstName"));
        assertNull(m.get("nick"), "omitted optional is a NULL field, still present");
    }

    @Test
    @DisplayName("map over a literal instance list reads fields per element")
    void mapOverInstanceList() throws SQLException {
        var r = run("|[^m::Person(firstName='Pierre', lastName='Doe'),"
                + " ^m::Person(firstName='Kevin', lastName='RoeDoe')]"
                + "->map(p|$p.lastName + ', ' + $p.firstName)");
        assertEquals(List.of("Doe, Pierre", "RoeDoe, Kevin"),
                r.values());
    }

    @Test
    @DisplayName("zip truncates to the shorter list and yields first/second pairs")
    void zipTruncatesAndYieldsPairs() throws SQLException {
        var r = run("|[1, 2, 3]->zip(['a', 'b'])");
        List<?> pairs = r.values();
        assertEquals(2, pairs.size(), "real pure zip TRUNCATES (DuckDB list_zip pads — wrong)");
        Map<?, ?> p0 = assertInstanceOf(Map.class, pairs.get(0));
        assertEquals(List.of("first", "second"),
                p0.keySet().stream().map(Object::toString).toList());
        assertEquals(1L, ((Number) p0.get("first")).longValue());
        assertEquals("a", p0.get("second"));
    }

    @Test
    @DisplayName("nested pairs nest their field maps")
    void nestedPairs() throws SQLException {
        var r = run("|[1, 2]->zip(['a', 'b'])->zip([10, 20])");
        List<?> pairs = r.values();
        assertEquals(2, pairs.size());
        Map<?, ?> outer = assertInstanceOf(Map.class, pairs.get(0));
        Map<?, ?> inner = assertInstanceOf(Map.class, outer.get("first"));
        assertEquals(1L, ((Number) inner.get("first")).longValue());
        assertEquals("a", inner.get("second"));
        assertEquals(10L, ((Number) outer.get("second")).longValue());
    }

    @Test
    @DisplayName("project over an instance literal: to-many paths CROSS-multiply")
    void projectInstanceCrossProduct() throws SQLException {
        var r = run("|^m::Holder(name='ok',"
                + " addresses=[^m::Addr(val='no'), ^m::Addr(val='other')],"
                + " values=[^m::Val(val=1), ^m::Val(val=2), ^m::Val(val=3)])"
                + "->project(~[one:x|$x.name, two:x|$x.addresses.val, three:x|$x.values.val])");
        assertEquals(com.legend.executionplan.ExecutionPlan.TdsResult.Shape.RELATION, r.shape());
        Lean.Result t = r;
        assertEquals(6, t.rows().size(), "2 addresses x 3 values = cross product");
        assertEquals("ok", t.rows().get(0).get(0));
    }

    @Test
    @DisplayName("project over an instance literal: an EMPTY array NULLs its column, keeps rows")
    void projectInstanceEmptyArray() throws SQLException {
        var r = run("|^m::Holder(name='ok',"
                + " addresses=[^m::Addr(val='no'), ^m::Addr(val='other')], values=[])"
                + "->project(~[one:x|$x.name, two:x|$x.addresses.val, three:x|$x.values.val])");
        assertEquals(com.legend.executionplan.ExecutionPlan.TdsResult.Shape.RELATION, r.shape());
        Lean.Result t = r;
        assertEquals(2, t.rows().size(), "empty array NULLs, never kills, the row");
        assertNull(t.rows().get(0).get(2));
    }

    @Test
    @DisplayName("two colspecs over the SAME to-many path share one iteration (no cross product)")
    void samePathColumnsShareIteration() throws SQLException {
        var r = run("|^m::Holder(name='ok',"
                + " addresses=[^m::Addr(val='A'), ^m::Addr(val='B')], values=[])"
                + "->project(~[a1:x|$x.addresses.val, a2:x|$x.addresses.val])");
        assertEquals(com.legend.executionplan.ExecutionPlan.TdsResult.Shape.RELATION, r.shape());
        Lean.Result t = r;
        assertEquals(2, t.rows().size(),
                "same collection iterated ONCE — (A,A),(B,B), never a 2x2 cross product");
        assertEquals(t.rows().get(0).get(0), t.rows().get(0).get(1));
    }

    @Test
    @DisplayName("contains: an instance IS found in a heterogeneous (Any) list; another alike is not it")
    void containsInstanceInMixedList() throws SQLException {
        var r = run("|let a = ^m::Addr(val='x'); [1, 'a', $a]->contains($a);");
        assertEquals(true, r.one(),
                "audit: cross-kind FALSE must not fire before the Any-variant branch");
        // m::Addr declares no key: equal() is identity (docs/OBJECT_IDENTITY_2026_10_20.md §1)
        var other = run("|[1, 'a', ^m::Addr(val='x')]->contains(^m::Addr(val='x'))");
        assertEquals(false, other.one());
    }

    @Test
    @DisplayName("collection arithmetic: plus sums, minus left-folds, indexOf searches string lists")
    void collectionArithmeticAndSearch() throws SQLException {
        assertEquals(6L, ((Number) run("|[1, 2, 3]->plus()").one()).longValue());
        assertEquals(5L, ((Number) run("|[10, 3, 2]->minus()").one()).longValue());
        assertEquals(24L, ((Number) run("|[2, 3, 4]->times()").one()).longValue());
        assertEquals(1L, ((Number) run("|['apple', 'banana']->indexOf('banana')").one()).longValue(),
                "a String[*] source is LIST search, not strpos");
    }

    @Test
    @DisplayName("two constructions of a keyless class are two objects, however alike (Pure's equal: identity)")
    void instanceEquality() throws SQLException {
        var r = run("|^m::Person(firstName='A', lastName='B') =="
                + " ^m::Person(firstName='A', lastName='B')");
        assertEquals(false, r.one());
        var same = run("|let p = ^m::Person(firstName='A', lastName='B'); $p == $p;");
        assertEquals(true, same.one());
    }
}
