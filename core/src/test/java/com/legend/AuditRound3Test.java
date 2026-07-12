package com.legend;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins for the third adversarial audit (post-ce8c70f range): every fix is a
 * verified silent-wrongness scenario — each pin holds the LOUD or the
 * corrected-value behavior the fix restored. See the audit commit message
 * for the finding-by-finding map.
 */
class AuditRound3Test {

    private static final String MODEL = """
            Class test::A { x: Integer[1]; }
            Class test::B { y: Integer[1]; }
            function test::repair(a: meta::pure::metamodel::type::Any[1], b: meta::pure::metamodel::type::Any[1]): String[1] { 'REAL' }
            """;

    private static Object scalar(String query) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            return Compiler.execute(MODEL, query, c).rows().get(0).get(0);
        }
    }

    private static Exception rejects(String query) {
        return assertThrows(Exception.class, () -> Compiler.compileQuery(MODEL, query));
    }

    // ---- kernel / checkers ----

    @Test
    @DisplayName("audit: [] never satisfies a required [1] slot (Nil is a type bottom, not a value)")
    void emptyIntoRequiredSlotIsLoud() {
        assertTrue(rejects("|abs([])").getMessage().contains("at least one"));
        // Function bodies compile at INLINE time (the execute path) — the
        // return-position check fires there; compileQuery types calls from
        // signatures alone.
        var ex = assertThrows(Exception.class, () -> {
            try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
                Compiler.execute(MODEL + "function test::f(): Integer[1] { [] }\n",
                        "|test::f()", c);
            }
        });
        assertTrue(ex.getMessage().contains("at least one"), ex.getMessage());
    }

    @Test
    @DisplayName("audit: multi-if consumes ONLY exact pair() — a user fn ending in 'pair' is not hijacked")
    void multiIfPairIsExactName() {
        var ex = rejects("|if([test::repair(|true, |'a')], |'b')");
        assertTrue(ex.getMessage().contains("pair(|cond, |value)"), ex.getMessage());
    }

    @Test
    @DisplayName("audit: multi-if condition must be Boolean[1] — a [0..1] condition is loud")
    void multiIfConditionMultiplicity() {
        var ex = rejects("|if([pair(|[true]->first(), |'a')], |'b')");
        assertTrue(ex.getMessage().contains("Boolean[1]"), ex.getMessage());
    }

    @Test
    @DisplayName("audit: distinct(~[]) is loud — a zero-column dedup has no coherent schema")
    void emptyDistinctIsLoud() {
        assertTrue(rejects("#TDS\n id\n 1\n#->distinct(~[])")
                .getMessage().contains("names no columns"));
    }

    @Test
    @DisplayName("audit: a join prefix that manufactures a duplicate column is a TYPED error")
    void joinPrefixCollisionIsTyped() {
        var ex = rejects("#TDS\n id, r_id\n 1, 2\n#->join(#TDS\n id\n 1\n#,"
                + " meta::pure::functions::relation::JoinKind.INNER,"
                + " {a,b|$a.id==$b.id}, 'r')");
        assertTrue(ex instanceof com.legend.compiler.spec.TypeInferenceException,
                ex.getClass().getName());
        assertTrue(ex.getMessage().contains("duplicate column 'r_id'"), ex.getMessage());
    }

    @Test
    @DisplayName("audit: eval over a literal lambda conforms args to DECLARED param types")
    void lambdaEvalArgConformance() {
        assertTrue(rejects("|{x: Integer[1]|$x}->eval('s')")
                .getMessage().contains("expected Integer, got String"));
        assertTrue(rejects("|{x: test::A[1]|$x}->eval(^test::B(y=1))")
                .getMessage().contains("expected test::A, got test::B"));
    }

    @Test
    @DisplayName("audit: a UNIQUE bare class name resolves (FQN stamped); ambiguity stays loud")
    void bareNameResolutionPinnedPositively() throws Exception {
        var typed = Compiler.compileQuery(MODEL, "A.all()");
        assertEquals("test::A", typed.info().type().typeName(),
                "the node carries the RESOLVED FQN");
        // Two same-simple-name classes: ambiguous — loud with the hint.
        var ex = assertThrows(Exception.class, () -> Compiler.compileQuery(
                MODEL + "Class other::A { z: Integer[1]; }\n", "A.all()"));
        assertTrue(ex.getMessage().contains("fully qualified"), ex.getMessage());
    }

    @Test
    @DisplayName("PCT slice: TDS inference is Deephaven's — full datetimes are DateTime, bare dates stay String")
    void tdsDateInference() throws Exception {
        // Real pure's TDS inference (TDSExtension -> Deephaven CSV): a full
        // ISO datetime cell infers DateTime (interval-RANGE windows work);
        // a DATE-ONLY cell stays String (parseDate over a date-shaped
        // string column is a real corpus fixture).
        Object v = scalar("#TDS\n d\n 2024-01-29T00:32:34.000000000+0000\n"
                + "#->extend(~y: x|$x.d->datePart())->select(~[y])");
        assertEquals("2024-01-29", v.toString().substring(0, 10));
        Object d = scalar("#TDS\n d\n 2024-01-29\n"
                + "#->extend(~p: x|$x.d->parseDate())->select(~[p])");
        assertEquals("2024-01-29", d.toString().substring(0, 10));
    }

    @Test
    @DisplayName("PCT slice 3a: JSON-shaped TDS cells infer Variant")
    void tdsVariantInference() throws Exception {
        // "[3,1,2]" cells pair with Variant-annotated lambdas in the PCT
        // fixtures; toMany(@Integer) over the inferred Variant column takes
        // the ARRAY cast path (a String inference sent list_sort a scalar).
        Object v = scalar("#TDS\n id, payload\n 1, \"[3,1,2]\"\n"
                + "#->extend(~s:x|$x.payload"
                + "->meta::pure::functions::variant::convert::toMany(@Integer)"
                + "->sort()->meta::pure::functions::variant::convert::toVariant())"
                + "->select(~[s])");
        assertEquals("[1,2,3]", v.toString());
    }

    // ---- lowering values ----

    @Test
    @DisplayName("audit: singleton list literals take the COLLECTION reductions, not the to-one identity")
    void singletonListReductions() throws Exception {
        assertEquals(5L, ((Number) scalar("|[5]->plus()")).longValue());
        assertEquals(Boolean.TRUE, scalar("|[true]->and()"));
        assertEquals(Boolean.FALSE, scalar("|[false]->or()"));
        assertEquals(1.5, ((Number) scalar("|[1.5]->minus()")).doubleValue(), 1e-9);
        assertEquals(6L, ((Number) scalar("|[1,2,3]->plus()")).longValue());
    }

    @Test
    @DisplayName("audit: HUGEINT widening targets the integer literal — float operands never cast")
    void hugeWidenSkipsFloats() throws Exception {
        // VALUE-level: the float operand never casts (2.5 * HUGEINT stays
        // 2.25e19); the print convention is the boundary's concern.
        assertEquals(2.25e19,
                ((Number) scalar("|2.5 * 9000000000000000000")).doubleValue(), 1e4);
        assertEquals("18446744073709551614",
                scalar("|2 * 9223372036854775807").toString());
    }

    @Test
    @DisplayName("audit: floatRepr — fraction-free magnitudes render exact plain digits via HUGEINT")
    void floatReprLargeBand() throws Exception {
        assertEquals("1000000000000000000.0",
                scalar("|1000000000000000000.0->toString()").toString());
        assertEquals("1.5", scalar("|1.5->toString()").toString());
        assertEquals("0.000000013421",
                scalar("|0.000000013421->toString()").toString());
    }

    @Test
    @DisplayName("audit: date() float-seconds carrier keeps MICROsecond precision")
    void dateCarrierMicros() throws Exception {
        assertEquals("2015-04-16T14:51:59.999999",
                scalar("|date(2015,4,16,14,51,59.999999)").toString());
    }

    @Test
    @DisplayName("audit: has* precision of a date() constructor answers from its ARITY")
    void hasPrecisionOfDateConstructor() throws Exception {
        assertEquals(0L, ((Number) scalar("|date(2015,4,16,14)->hasMinute()")).longValue());
        assertEquals(1L, ((Number) scalar("|date(2015,4,16,14)->hasHour()")).longValue());
    }

    @Test
    @DisplayName("audit: unknown Java date-pattern letters are LOUD, never literal passthrough")
    void unknownDatePatternTokenIsLoud() {
        var ex = assertThrows(Exception.class,
                () -> scalar("|format('%t{MMM}', [%2014-03-05])"));
        assertTrue(ex.getMessage().contains("unsupported date-format token"),
                ex.getMessage());
    }

    @Test
    @DisplayName("audit: add(set, index, val) errors on an out-of-range index — no silent clamp")
    void addOutOfBoundsIsLoud() throws Exception {
        var ex = assertThrows(Exception.class,
                () -> scalar("|[1,2]->meta::pure::functions::collection::add(9, 99)"));
        assertTrue(ex.getMessage().contains("index out of bounds"), ex.getMessage());
        assertEquals("a", scalar(
                "|['a','b']->meta::pure::functions::collection::add(1, 'c')").toString());
    }

    @Test
    @DisplayName("audit: corr over mismatched-length lists is LOUD — no silent NULL-pad zip")
    void corrMismatchedLengthsIsLoud() throws Exception {
        var ex = assertThrows(Exception.class,
                () -> scalar("|meta::pure::functions::math::corr([1.0,2.0,3.0],[2.0,4.0])"));
        assertTrue(ex.getMessage().contains("differ in length"), ex.getMessage());
        assertEquals(1.0, ((Number) scalar(
                "|meta::pure::functions::math::corr([1.0,2.0,3.0],[2.0,4.0,6.0])"))
                .doubleValue(), 1e-9);
    }
}
