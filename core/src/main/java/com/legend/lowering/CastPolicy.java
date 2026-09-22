package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;

/**
 * The one primitive-cast policy (extracted from the Lowerer, shared by
 * the scalar cast arm and the reduce/value channels): a CONVERTING
 * primitive cast emits SQL; widening/same-type/non-primitive is the
 * assertion no-op.
 */
final class CastPolicy {

    private CastPolicy() {
    }

    // T4 LEG 1 — THREE REFEREE VERDICTS (2026-08-24, recorded in
    // TYPED_SQL_IR.md): the mapping-read conformance (concrete
    // Float/String contracts convert IN SQL — the user ruling stands)
    // was applied at the PROJECTION boundary by OUTPUT-NAME lookup and
    // rejected three ways: (1) the flat class form pins the engine's
    // WIRE-typed plan metadata (TypedProject.wireForm now carries that
    // lane fact); (2) deep-join schemas collide output names — an
    // Integer column conformed to String off a same-named column;
    // (3) a conform cast on ONE union branch breaks branch-projection
    // identity and reorders the merge. The SOUND seam is the
    // PROPERTY-READ PAIRING (property meets its mapped column uniquely,
    // no name lookup) — the next attempt builds THERE. The Cast.conform
    // provenance + engine-text elision + rebuild transports stay: they
    // are the correct plumbing for that seam.

    /** The cast policy over an ALREADY-LOWERED source (scalar or window channel). */
    static SqlExpr lower(TypedCast c, SqlExpr value, boolean isMany, boolean engineText) {
        if (c.wire() && engineText) {
            // the mapping's WIRE coercion — the engine runtime converts on
            // the wire and its SQL/plan text never spells it; execution
            // (boundary inactive) keeps the SQL cast (DuckDB does not
            // wire-convert — audit 19 F7)
            return value;
        }
        boolean variantSource = c.source().info().type()
                instanceof Type.ClassType ct && PlatformTypes.isVariant(ct);
        if (!variantSource) {
            // burn lane (audit §4): impossible cross-kind casts raise
            // pure's Cast exception (CastPolicy.crossKindRaise — the
            // conversion contract and WIRE coercions are exempt there).
            if (!c.wire() && crossKindRaise(
                    c.source().info().type(), c.target())
                    instanceof SqlExpr raise) {
                return raise;
            }
            // A CONVERTING primitive cast (String->@Integer) must reach
            // SQL (bare return left VARCHAR arithmetic); a WIDENING cast
            // is a type ASSERTION — converting corrupts (42 -> 42.0).
            // DELIBERATE divergence: pure's cast never converts; the
            // corpus contract (engine-lite lineage) is SQL-style
            // conversion, so a NARROWING cast converts here.
            Type src = c.source().info().type();
            if (isSqlPrimitive(c.target()) && isSqlPrimitive(src)
                    && !isWidening(src, c.target())
                    && !PureSql.type(src).equals(PureSql.type(c.target()))) {
                // A converting cast over a COLLECTION is ELEMENT-WISE — the
                // scalar channel carries collections as LISTs and DuckDB has
                // no LIST->scalar cast (calendar DateRange family: row-var
                // .values is an ArrayLit even at bounded-1 multiplicity).
                if (value instanceof SqlExpr.ArrayLit lit) {
                    return new SqlExpr.ArrayLit(lit.elements().stream()
                            .map(e -> (SqlExpr) new SqlExpr.Cast(
                                    e, PureSql.type(c.target())))
                            .toList());
                }
                return isMany
                        ? SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, value,
                                new SqlExpr.Lambda(List.of("x"),
                                        new SqlExpr.Cast(
                                                SqlExpr.Column.derived(null, "x"),
                                                PureSql.type(c.target()))))
                        : new SqlExpr.Cast(value, PureSql.type(c.target()));
            }
            return value;
        }
        boolean many = isMany;
        // meta::json KIND casts (->cast(@JSONObject) over an element, over
        // a list of elements): the value IS the JSON value already —
        // identity in every multiplicity (the kind is a type assertion).
        if (c.source().info().type() instanceof Type.ClassType js
                && PlatformTypes.isJsonElement(js)
                && c.target() instanceof Type.ClassType jt
                && PlatformTypes.isVariant(jt)) {
            return value;
        }
        // THE NO-RE-WRAP DECISION (M4 §3.3): a LITERAL-marked value is
        // a self-describing Any carrier — an Any-conformance keeps the
        // mark unchanged. The carrier casts below would re-carrier
        // spelled texts as JSON (bare numbers silently re-kind, quoted
        // strings malform — the parked branch's gate-caught witness,
        // testUsingSameAggFunctionTwice). Labels distinguish carriers;
        // casts never re-carrier. Decided on the STORED fact — the
        // typed IR's clean read; the branch needed a node-shape sniff
        // (its judge) for the same decision. Dormant until the claim
        // lands: no live flow routes a LITERAL-marked value here today.
        if (value.type() instanceof com.legend.sql.TypeFact.Typed vt
                && (vt.type() == SqlType.Scalar.LITERAL
                        || (vt.type() instanceof SqlType.Array va
                                && va.element() == SqlType.Scalar.LITERAL))) {
            return value;
        }
        if (many) {
            boolean variantTarget = c.target() instanceof Type.ClassType t
                    && PlatformTypes.isVariant(t);
            return variantTarget
                    ? SqlExpr.Call.of(SqlFn.VARIANT_ELEMENTS, value)
                    // A to-many cast targets an ARRAY of the element type —
                    // expressed in the TYPE (SqlType.Array). JSON null stays
                    // SQL NULL (real relation-land pins toVariant(NULL) =
                    // 'null' vs toVariant([]) = '[]'); the list CONSUMERS
                    // (contains/isEmpty/joinStrings) are null-safe instead.
                    : new SqlExpr.Cast(value,
                            new SqlType.Array(PureSql.type(c.target())));
        }
        if (c.target() instanceof Type.ClassType tc
                && !PlatformTypes.isVariant(tc)
                && !PlatformTypes.isAny(tc)) {
            // to(@ModelClass) MATERIALIZED as a value: the real relation
            // runtime rejects class-typed columns — message verbatim
            // (property reads through the cast never come here; the
            // extraction arm in scalar() fields them).
            throw new ModelException(
                    LegendCompileException.Phase.LOWER,
                    "The type " + tc.fqn() + " is not supported yet!");
        }
        // The dialect may render this cast through its text-extraction idiom
        // (DuckDB ->>) — that is RENDERING knowledge; the IR keeps the access.
        // KIND/SCALE PRESERVATION (X-audit): engine cast-to-Decimal keeps
        // the VALUE's own scale — an INTEGER source is a scale-0 Decimal,
        // never the blanket (38,18) fabrication
        if ((c.target() == Type.Primitive.DECIMAL)
                && c.source().info().type() == Type.Primitive.INTEGER) {
            return new SqlExpr.Cast(value,
                    new com.legend.sql.SqlType.Decimal(38, 0));
        }
        return new SqlExpr.Cast(value, PureSql.type(c.target()));
    }

    static SqlExpr castByPolicy(SqlExpr e, Type src, Type target) {
        return castByPolicy(e, src, target, false);
    }

    /** {@code wire}: a mapping WIRE coercion — the engine converts on
     * the wire (string columns feeding Integer properties), so the
     * cross-kind raise never applies to it. */
    static SqlExpr castByPolicy(SqlExpr e, Type src, Type target,
            boolean wire) {
        if (!wire && crossKindRaise(src, target) instanceof SqlExpr raise) {
            return raise;
        }
        if (isSqlPrimitive(target) && isSqlPrimitive(src)
                && !isWidening(src, target)
                && !PureSql.type(src).equals(PureSql.type(target))) {
            return new SqlExpr.Cast(e, PureSql.type(target));
        }
        return e;
    }

    /** Pure's runtime raise for a cast that can NEVER succeed: both
     * sides concrete primitives of DIFFERENT kind families (the
     * existing {@link Type.Primitive.Family} lattice). The reference
     * raises "Cast exception: X cannot be cast to Y" (Cast.java:135) —
     * the audit's confirmed silent-wrong-answers were 1->cast(@String)
     * typed STRING and 1->cast(@Boolean) -> true. WITHIN-family
     * conversions keep the standing corpus-contract conversion arm
     * (per-lane adjudication territory, not this burn); null = not a
     * concrete primitive (Any/class casts flow). */
    static @com.legend.base.Nullable SqlExpr crossKindRaise(Type src, Type target) {
        Type.Primitive.Family sf = familyOf(src);
        Type.Primitive.Family tf = familyOf(target);
        if (sf == null || tf == null || sf == tf) {
            return null;
        }
        // STRING interconverts with NUMERIC and TEMPORAL — the standing
        // PRODUCT conversion contract, referee-pinned
        // (TypeConversionCheckerTest$StringToInteger IS the
        // String->Integer pin; the view family pins Integer->String;
        // temporal strings are the wire carrier). The remaining cross
        // pairs (boolean<->anything, temporal<->numeric) can never
        // succeed in any lane and raise pure's Cast exception.
        boolean contract = sf == Type.Primitive.Family.TEXT
                        && (tf == Type.Primitive.Family.NUMERIC
                                || tf == Type.Primitive.Family.TEMPORAL)
                || tf == Type.Primitive.Family.TEXT
                        && (sf == Type.Primitive.Family.NUMERIC
                                || sf == Type.Primitive.Family.TEMPORAL);
        if (contract) {
            return null;
        }
        return SqlExpr.Call.of(com.legend.sql.SqlFn.ERROR,
                new SqlExpr.StringLit("Cast exception: " + src.typeName()
                        + " cannot be cast to " + target.typeName()));
    }

    private static Type.Primitive.@com.legend.base.Nullable Family familyOf(Type t) {
        if (t instanceof Type.PrecisionDecimal) {
            return Type.Primitive.Family.NUMERIC;
        }
        return t instanceof Type.Primitive p ? p.family() : null;
    }

    /** Whether {@code tgt} is {@code src}'s primitive-lattice supertype (cast-as-assertion). */
    static boolean isWidening(Type src, Type tgt) {
        if (tgt == Type.Primitive.NUMBER) {
            return src == Type.Primitive.INTEGER || src == Type.Primitive.FLOAT
                    || src == Type.Primitive.DECIMAL || src instanceof Type.PrecisionDecimal;
        }
        if (tgt == Type.Primitive.DATE) {
            return src == Type.Primitive.STRICT_DATE || src == Type.Primitive.DATE_TIME;
        }
        return false;
    }

    static boolean isSqlPrimitive(Type t) {
        return (t instanceof Type.Primitive p
                        && p != Type.Primitive.BYTE && p != Type.Primitive.LATEST_DATE
                        && p != Type.Primitive.STRICT_TIME)
                || t instanceof Type.PrecisionDecimal;
    }

    /** In a comparison against a LITERAL side, a WIRE cast unwraps when
     * the literal speaks the cast SOURCE's type — engine predicates
     * compare the RAW mapped expression (testInWithDynaFunction golden:
     * case-string IN string-list, bare ID = 4); a literal matching the
     * cast TARGET keeps the cast ($i.active == true passes cast). */
    static SqlExpr comparisonWireOperand(TypedSpec typed,
            SqlExpr lowered, TypedSpec other) {
        TypedSpec t = typed;
        while (t instanceof TypedNativeCall nc && !nc.args().isEmpty()
                && com.legend.builtin.Pure.isToOneCall(nc.callee().qualifiedName())) {
            t = nc.args().get(0);
        }
        if (t instanceof TypedCast tc && tc.wire()
                && lowered instanceof SqlExpr.Cast sc
                && literalish(other)
                && other.info().type().equals(tc.source().info().type())
                && !other.info().type().equals(tc.target())) {
            return sc.value();
        }
        return lowered;
    }


    static boolean literalish(TypedSpec v) {
        return switch (v) {
            case TypedCString ignored -> true;
            case TypedCInteger ignored -> true;
            case TypedCBoolean ignored -> true;
            case TypedCDate ignored -> true;
            case com.legend.compiler.spec.typed.TypedCollection c ->
                    c.elements().stream().allMatch(CastPolicy::literalish);
            default -> false;
        };
    }

    static TypedSpec cellRootUnwrapWire(TypedSpec b) {
        if (b instanceof TypedCast tc && tc.wire()
                && tc.target() == Type.Primitive.STRING) {
            return cellRootUnwrapWire(tc.source());
        }
        if (b instanceof TypedNativeCall nc
                && com.legend.builtin.Pure.isToOneCall(nc.callee().qualifiedName())
                && !nc.args().isEmpty()) {
            TypedSpec inner = cellRootUnwrapWire(nc.args().get(0));
            if (inner != nc.args().get(0)) {
                List<TypedSpec> na = new ArrayList<>(nc.args());
                na.set(0, inner);
                return nc.withChildren(na);
            }
        }
        return b;
    }
}
