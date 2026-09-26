// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.compiler.RelationalKinds;

import com.legend.builtin.Pure;
import com.legend.compiler.ModelBuilder;
import com.legend.model.ClassDefinition;
import com.legend.model.DatabaseDefinition;
import com.legend.model.PropertyMapping;
import com.legend.model.RelationalOperation;
import com.legend.protocol.TypeExpression;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.TypeAnnotation;
import com.legend.protocol.spec.ValueSpecification;

import java.util.List;
import java.util.Set;

/**
 * The DECLARED-KIND coercion family &mdash; the property&harr;column
 * pairing seam of the legacy mapping lane: each read of a mapped column
 * conforms (or type-asserts) to the owning class property's DECLARED
 * platform kind. Split from {@link MappingNormalizer} at the shape
 * limit; this seam is also where {@link RequiredNullableCensus} watches
 * the pairing (each hook fires BEFORE the kind checks' early returns, so
 * the census sees every pairing, not just kind-mismatched ones).
 */
final class DeclaredCoercions {

    private DeclaredCoercions() {
    }

    /**
     * Engine semantics for EXPRESSION property mappings: the dynafunction's
     * result coerces to the property's declared type at the SQL boundary
     * (abs(...) types Number, the property says Float — the engine compiles
     * and the database delivers the declared kind; Boolean covers the
     * corpus's own {@code case(equal(col,'Y'), 'true', 'false')} idiom on a
     * Boolean[1] property). NUMERIC/temporal/Boolean declared types wrap in
     * cast(@Declared); everything else passes through untouched so genuine
     * kind errors stay loud.
     *
     * <p>DELIBERATE divergence (audit 19 F6): the engine's runtime rule for
     * declared-Boolean strings is {@code Boolean.parseBoolean} — 'Y' maps
     * to FALSE, silently. Our SQL cast ERRORS on such strings instead:
     * loud beats silently-different, and the corpus only ever feeds
     * 'true'/'false' (where the two agree).
     */
    static ValueSpecification coerceToDeclaredNumeric(ValueSpecification value,
            String propName, @com.legend.base.Nullable String ownerClassFqn, ModelBuilder model) {
        String simple = declaredPlatformKind(propName, ownerClassFqn, model);
        if (simple == null || !Set.of("Float", "Integer", "Decimal",
                "Number", "DateTime", "StrictDate", "Date", "Boolean")
                        .contains(simple)) {
            return value;
        }
        // a WIRE coercion (the engine runtime converts on the wire and
        // its SQL/plan text never spells it) — castAsDeclared casts at
        // execution, reads bare in the engine-text funnel
        return new AppliedFunction(Pure.Lite.CAST_AS_DECLARED, List.of(value,
                new TypeAnnotation.Named(
                        new TypeExpression.NameRef(simple))));
    }

    /**
     * The declared property type's PLATFORM primitive simple name, or null
     * for class/enum-typed and shadowed names. Identified exactly: the bare
     * spelling (not shadowed by a user class — {@code m::Number} must never
     * coerce) or the full platform FQN. Suffix-matching is the banned idiom.
     */
    private static @com.legend.base.Nullable String declaredPlatformKind(String propName,
            @com.legend.base.Nullable String ownerClassFqn, ModelBuilder model) {
        if (ownerClassFqn == null) {
            return null;
        }
        ClassDefinition owner = MissProbe.knownMiss(model.knowledge().hierarchyClass(ownerClassFqn));
        TypeExpression t = owner == null ? null
                : model.knowledge().propertyType(owner, propName);
        String name = t instanceof TypeExpression.NameRef nr ? nr.name() : null;
        if (name == null) {
            return null;
        }
        if (!name.contains("::") && model.knowledge().hierarchyClass(name).isEmpty()) {
            return name;
        }
        if (name.startsWith("meta::pure::metamodel::type::")) {
            return name.substring("meta::pure::metamodel::type::".length());
        }
        return null;
    }

    /** {@code typeAsDeclared(read, @Declared)} when the terminal column's
     * physical kind differs from the declared platform kind — the TYPE-ONLY
     * assertion (no SQL); matching kinds pass through untouched. */
    static ValueSpecification declaredAssertion(ValueSpecification read,
            PropertyMapping.JoinTerminalColumn jtc, String ownerClassFqn,
            ModelBuilder model, MappingLedger ledger) {
        String declared = declaredPlatformKind(jtc.propertyName(),
                ownerClassFqn, model);
        if (declared == null
                || !(jtc.terminalColumn()
                        instanceof RelationalOperation.ColumnRef cr)) {
            return read;
        }
        String db = cr.databaseName() != null ? cr.databaseName()
                : jtc.database();
        if (db == null) {
            return read;
        }
        DatabaseDefinition.ColumnDefinition cd = model.knowledge().column(db, cr.table(), cr.column()).orElseGet(MissProbe::miss);
        String colKind = cd == null ? null : RelationalKinds.pureKindOf(cd.dataType());
        if (colKind == null || colKind.equals(declared)) {
            return read;
        }
        return new AppliedFunction(Pure.Lite.TYPE_AS_DECLARED, List.of(read,
                new TypeAnnotation.Named(
                        new TypeExpression.NameRef(declared))));
    }

    /**
     * A plain column PM whose PHYSICAL kind disagrees with the declared
     * property type casts at the boundary ({@code id: String[1]} over an
     * INTEGER column — engine relational execution coerces to the property
     * type on the wire). Matching kinds and subsuming declarations
     * ({@code Number} over any numeric, {@code Date} over any temporal)
     * emit NO cast; unknown columns and non-primitive declarations pass
     * through (the type checker stays the loud arbiter).
     */
    static ValueSpecification coerceColumnToDeclared(ValueSpecification read,
            PropertyMapping.Column col,
            String ownerClassFqn, ModelBuilder model, MappingLedger ledger) {
        String declared = declaredPlatformKind(col.propertyName(), ownerClassFqn, model);
        if (declared == null) {
            return read;
        }
        // scope-block columns carry no [db] — skip (checker stays loud).
        // The column's kind resolves THROUGH a view (its column expression's
        // inferred SQL type — the engine's inferRelationalType), so a
        // Float property over `HIGHEST_RATE: max(DECIMAL col)` meets the
        // same Decimal → Float coercion a table column does (ledger F-AB).
        String db = col.database();
        String colKind = db == null ? null
                : model.knowledge().columnKind(db, col.table(), col.column());
        if (colKind == null || colKind.equals(declared)) {
            return read;
        }
        // Only conversions the engine's runtime transformer actually
        // performs (Boolean, Date-family) or that are lossless (*->String,
        // DATE widening to DateTime) may cast. NUMERIC declared-vs-column
        // mismatches are IDENTITY in the engine (SetImplTransformers passes
        // numerics through untouched; audit 19 F7) — they get the TYPE-ONLY
        // assertion (typeAsDeclared, no SQL cast): the declared kind drives
        // result typing, the database delivers the raw value. Anything else
        // falls through uncast and the type checker stays the loud arbiter.
        // String/Boolean-declared over a mismatched column is a WIRE
        // coercion — castAsDeclared casts at execution but the
        // engine-text funnel reads the expression bare (engine goldens
        // never spell wire coercions; the runtime converts on the wire)
        if ("String".equals(declared) || "Boolean".equals(declared)) {
            return new AppliedFunction(Pure.Lite.CAST_AS_DECLARED, List.of(read,
                    new TypeAnnotation.Named(
                            new TypeExpression.NameRef(declared))));
        }
        boolean cast = switch (declared) {
            case "DateTime" -> "StrictDate".equals(colKind);
            default -> false;
        };
        if (cast) {
            return new AppliedFunction("cast", List.of(read,
                    new TypeAnnotation.Named(
                            new TypeExpression.NameRef(declared))));
        }
        Set<String> numeric = Set.of("Float", "Decimal", "Integer", "Number");
        if (numeric.contains(declared) && numeric.contains(colKind)) {
            // T4 attempt 2 (charter §4bR/§4bZ): a CONCRETE Float contract
            // over a DECIMAL/NUMERIC column CONVERTS — and the conversion
            // lowers to SQL (tenet #1), never decode-by-label. Engine
            // receipt (§4bZ homework — a DESIGNED feature, the dataType
            // test names its properties decimalAsFloat/numericAsFloat):
            // BOTH engine runtimes flatten DECIMAL wires to Float AT THE
            // JDBC FETCH (interpreted ExecuteInDb:81 Types.DECIMAL->Float;
            // compiled ResultSetValueHandlers getBigDecimal().doubleValue())
            // — we lower that same conversion into the SQL instead of the
            // host. Float over an INTEGER-family column stays IDENTITY —
            // referee receipt (this slice's own first sweep): the
            // validation showcase golden prints the raw 'Quantity not in
            // range: 1000000' (Float-declared quantity over an INT column,
            // toString computed IN SQL); the engine has NO int->float
            // fetch rule, so emission there would invent semantics — those
            // rows belong to the carry-through tolerance (§4bZ plan).
            // Abstract Number (identity carrier — castErasure referee)
            // and Integer-declared likewise stay type-only assertions.
            // NUMERIC CHARTER Rule 1 + 2 (docs/NUMERIC_CHARTER_2026_09_17.md):
            // a Float property over a DECIMAL column is a TYPE assertion, never
            // a read-time cast — the engine reads the cell by its JDBC kind
            // and its numeric transformers are the identity; the value stays
            // DECIMAL in SQL, the declared kind converts ONCE at the root
            // select (Rule 2). (Retired: castAsDeclared → DOUBLE at the read,
            // fc2fe6bd3 — double arithmetic over the column:
            // 0.8499999999999943 for 0.85.)
            return new AppliedFunction(Pure.Lite.TYPE_AS_DECLARED, List.of(read,
                    new TypeAnnotation.Named(
                            new TypeExpression.NameRef(declared))));
        }
        // a NUMERIC property over a VARCHAR column parses by EMISSION —
        // the engine's H2 reader converts numeric strings at read
        // (multigrain ACCOUNT_NUM); DuckDB needs the explicit parse
        if (numeric.contains(declared) && "String".equals(colKind)) {
            String parseFn = switch (declared) {
                case "Integer" -> "parseInteger";
                case "Float", "Number" -> "parseFloat";
                case "Decimal" -> "parseDecimal";
                default -> null;
            };
            if (parseFn != null) {
                // parse natives are strict String[1]; the column read is
                // SQL-lane [0..1] — the toOne trust wrap (audit slice 2)
                return new AppliedFunction(parseFn, List.of(
                        new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(read))));
            }
        }
        return read;
    }
}
