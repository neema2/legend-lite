// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFold;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedRawSqlRelation;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The EXECUTION-BOUNDARY schema resolver (One-Platform Plan Phase 1c —
 * the dynamic-pivot rule): a raw-SQL grid's columns first exist where a
 * session exists, so the compiler types them LATE-BOUND
 * ({@link Type.RelationType#lateBound()}) and this pass resolves them
 * here — one LIMIT-0 metadata read per distinct grid (schema, never
 * values; the E1 probe discipline), exactly where pivot resolves its
 * data-derived names ({@code DynamicPivot.staticize}'s first-query
 * model). The compiler never probes.
 *
 * <p>TWO duties, both TYPE-directed (marker nodes the Typer left, never
 * user-spelling recognition):
 * <ol>
 * <li><b>STAMP</b> — every late-bound {@link TypedRawSqlRelation} gets
 * its real column names ({@code Any[0..1]} cells: the SQL layer needs
 * NAMES for its outputs invariant; cell types stay the database's own,
 * decoded at egress).</li>
 * <li><b>RESOLVE</b> — the reads that could not resolve statically
 * resolve against the now-known schema: {@code .columnNames} becomes
 * the string collection; {@code .values} over the grid becomes the
 * row-major cell stream ({@code map(_r | [cells...])}, the existing
 * collection-mapper flatten channel); {@code $binder.values} inside a
 * fold/map/filter over a stamped grid becomes the binder's cell
 * collection (the TDS row-var rule applied late); {@code at(cells, k)}
 * over a resolved cell collection picks statically.</li>
 * </ol>
 *
 * <p><strong>Staged compilation (Invariant 7):</strong> this pass is
 * COMPILER work — it mints and rewrites typed nodes — so it lives in
 * the resolver, parameterized by the {@link SchemaOracle}: the
 * runtime-discovered column roster is an INPUT to a compiler phase run
 * at a later stage. The executor's oracle is
 * {@code exec.GridProbe#probeNames}; the compiler never holds a
 * connection. F1.3 funnels the JDBC surface to the exec layer at
 * BYTECODE level — even the driver's exception type may not appear
 * here — so the oracle is UNCHECKED and the executor's implementation
 * wraps its checked failure (the {@code Frames.inlineExecute} idiom).
 */
public final class RawGridSchema {

    private RawGridSchema() {
    }

    /** The executor-supplied schema authority: the grid's projection
     * columns for the authored SQL (one LIMIT-0 metadata read —
     * schema, never values). TYPED since §4bZ-U's follow-on: the probe
     * carries the database's own column types, so a stamped grid's
     * cells read typed instead of Any (an unmapped SQL type falls back
     * to the trusted-Any column at the oracle — an upgrade, never a
     * new wall). */
    public interface SchemaOracle {
        List<Type.Column> columnsOf(String sql);
    }

    /** The tree with every late-bound raw grid stamped and its
     * late-bound reads resolved.
     *
     * <p>SINGLE-QUERY RULE (P3-2): the LIMIT-0 probe runs ONLY when the
     * tree DEMANDS the schema statically — a {@code columnNames} or
     * {@code values} read, whose resolution needs the column names at
     * compile time. An undemanded grid stays late-bound through lowering
     * (a zero-output star-select) and the ONE executed query is its own
     * schema authority: the egress adopts the result-set headers
     * ({@code Executor.resolveColumns}' late-bound arm, gated on
     * {@code schema.isLateBound()}). Two queries only when the second
     * is genuinely needed. */
    public static List<TypedSpec> stamp(List<TypedSpec> body,
            SchemaOracle oracle) {
        if (!demandsSchema(body)) {
            return body;
        }
        List<TypedSpec> out = new ArrayList<>(body.size());
        boolean changed = false;
        Map<String, Type.RelationType> binders = new HashMap<>();
        for (TypedSpec n : body) {
            TypedSpec s = resolve(n, oracle, binders);
            changed |= s != n;
            out.add(s);
        }
        return changed ? out : body;
    }

    /** Whether any node statically demands a late-bound grid's schema:
     * a {@code columnNames}/{@code values} property read anywhere in the
     * tree (lambda bodies included — children() recursion enters them).
     * Conservative on purpose: a values-read over a NON-grid source
     * still triggers the probe (old behavior, never wrong); absence is
     * PROOF no resolution needs names before execution. */
    private static boolean demandsSchema(List<TypedSpec> body) {
        for (TypedSpec n : body) {
            if (demands(n)) {
                return true;
            }
        }
        return false;
    }

    private static boolean demands(TypedSpec n) {
        if (n instanceof TypedPropertyAccess pa
                && (pa.property().equals("columnNames")
                        || pa.property().equals("values"))) {
            return true;
        }
        // a BY-NAME FIELD read over a LATE-BOUND source is a static
        // schema demand too (§4bZ-U ruling: burn the wire ledger — the
        // trust-name rule typed such cells Any/JSON, the probe's typed
        // columns replace that with the database's own type). The bare
        // {@code .rows} egress stays probe-free — no field is named,
        // the executed query's own headers are the authority (P3-2's
        // surviving case, still pinned by ExecuteInDbProbeCountTest).
        if (n instanceof TypedPropertyAccess pa2
                && !pa2.property().equals(PlatformTypes.ROWS_MARKER)
                && Type.schemaView(pa2.source().info().type())
                        instanceof Type.RelationType rt2
                && rt2.isLateBound()) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (demands(c)) {
                return true;
            }
        }
        return false;
    }

    private static TypedSpec resolve(TypedSpec n, SchemaOracle oracle,
            Map<String, Type.RelationType> binders) {
        // STAMP the grid leaf
        if (n instanceof TypedRawSqlRelation raw
                && Type.relationSchema(raw.info().type())
                        instanceof Type.RelationType rt
                && rt.isLateBound()) {
            return new TypedRawSqlRelation(raw.sql(),
                    new ExprType(Type.relation(new Type.RelationType(
                                    oracle.columnsOf(raw.sql()), List.of())),
                            raw.info().multiplicity()));
        }
        // BINDER SCOPE: a lambda over a (recursively stamped) grid binds
        // its row variable to the stamped schema for the body walk
        // (enter through the body — the shadow-stop discipline)
        TypedSpec scoped = resolveLambdaOwner(n, oracle, binders);
        if (scoped != null) {
            return scoped;
        }
        // generic recursion, children first
        n = recurse(n, oracle, binders);
        // RESOLVE the late-bound reads against known schema
        if (n instanceof TypedPropertyAccess pa) {
            Type.RelationType schema = stampedSchemaOf(pa.source(), binders);
            if (schema != null && pa.property().equals("columnNames")) {
                List<TypedSpec> lits = new ArrayList<>(schema.columns().size());
                ExprType s1 = ExprType.one(Type.Primitive.STRING);
                for (Type.Column c : schema.columns()) {
                    lits.add(new TypedCString(c.name(), s1));
                }
                return new TypedCollection(lits,
                        new ExprType(Type.Primitive.STRING,
                                new Multiplicity.Bounded(lits.size(),
                                        lits.size())));
            }
            if (schema != null && pa.property().equals("values")) {
                if (pa.source() instanceof TypedVariable v) {
                    // $binder.values — the row's cells (TDS row-var rule)
                    return cells(v.name(), schema);
                }
                // grid .values — the row-major cell stream: map each row
                // to its cell list; the collection-mapper channel
                // flattens one level (the existing rule)
                String b = "_gvr";
                TypedSpec mapper = cells(b, schema);
                Multiplicity one = Multiplicity.Bounded.ONE;
                TypedLambda lam = new TypedLambda(List.of(b),
                        List.of(mapper),
                        ExprType.one(new Type.FunctionType(
                                List.of(new Type.Param(schema, one)),
                                new Type.Param(new Type.ClassType(
                                        PlatformTypes.ANY),
                                        Multiplicity.Bounded.ZERO_MANY))));
                return new TypedMap(pa.source(), lam,
                        new ExprType(new Type.ClassType(PlatformTypes.ANY),
                                Multiplicity.Bounded.ZERO_MANY));
            }
        }
        return n;
    }

    /** The row's cells as a typed collection of per-column reads over
     * the named binder — the TDS row-var {@code .values} rule applied
     * at the boundary, where the schema first exists. */
    private static TypedSpec cells(String binder, Type.RelationType schema) {
        List<TypedSpec> reads = new ArrayList<>(schema.columns().size());
        for (Type.Column c : schema.columns()) {
            reads.add(new TypedPropertyAccess(
                    new TypedVariable(binder, ExprType.one(schema)),
                    c.name(), new ExprType(c.type(), c.multiplicity())));
        }
        return new TypedCollection(reads,
                new ExprType(new Type.ClassType(PlatformTypes.ANY),
                        new Multiplicity.Bounded(reads.size(), reads.size())));
    }

    /** Fold/map/filter over a stamped grid: resolve the source first,
     * bind the row binder to its schema, walk the lambda body under the
     * binding (restored after). Null = not such a node. */
    private static @com.legend.base.Nullable TypedSpec resolveLambdaOwner(
            TypedSpec n, SchemaOracle oracle,
            Map<String, Type.RelationType> binders) {
        TypedSpec src;
        TypedLambda lam;
        if (n instanceof TypedFold f) {
            src = f.source();
            lam = f.reducer();
        } else if (n instanceof TypedMap m) {
            src = m.source();
            lam = m.mapper();
        } else if (n instanceof TypedFilter fl) {
            src = fl.source();
            lam = fl.predicate();
        } else {
            return null;
        }
        TypedSpec src2 = resolve(src, oracle, binders);
        Type.RelationType schema = stampedSchemaOf(src2, binders);
        if (schema == null || lam.parameters().isEmpty()) {
            if (src2 == src) {
                return null;   // nothing to do — generic recursion handles
            }
            return n.withChildren(replaceFirst(n.children(), src, src2));
        }
        String rowVar = lam.parameters().get(0);
        Type.RelationType prev = binders.put(rowVar, schema);
        try {
            List<TypedSpec> body2 = new ArrayList<>(lam.body().size());
            boolean changed = false;
            for (TypedSpec st : lam.body()) {
                TypedSpec r = resolve(st, oracle, binders);
                changed |= r != st;
                body2.add(r);
            }
            if (!changed && src2 == src) {
                return n;
            }
            TypedLambda lam2 = changed
                    ? new TypedLambda(lam.parameters(), body2, lam.info())
                    : lam;
            List<TypedSpec> kids = new ArrayList<>(n.children());
            for (int i = 0; i < kids.size(); i++) {
                if (kids.get(i) == src) {
                    kids.set(i, src2);
                } else if (kids.get(i) == lam) {
                    kids.set(i, lam2);
                }
            }
            return n.withChildren(kids);
        } finally {
            if (prev == null) {
                binders.remove(rowVar);
            } else {
                binders.put(rowVar, prev);
            }
        }
    }

    /** The STAMPED schema behind an expression: a stamped raw grid
     * (directly, or under the {@code .rows} marker), or a bound lambda
     * row variable. Null = not a stamped-grid read. */
    private static Type.@com.legend.base.Nullable RelationType stampedSchemaOf(
            TypedSpec source, Map<String, Type.RelationType> binders) {
        TypedSpec s = source;
        while (s instanceof TypedPropertyAccess p
                && p.property().equals(PlatformTypes.ROWS_MARKER)) {
            s = p.source();
        }
        if (s instanceof TypedRawSqlRelation r
                && Type.relationSchema(r.info().type())
                        instanceof Type.RelationType rt
                && !rt.isLateBound()) {
            return rt;
        }
        if (s instanceof TypedVariable v) {
            return binders.get(v.name());
        }
        return null;
    }

    private static TypedSpec recurse(TypedSpec n, SchemaOracle oracle,
            Map<String, Type.RelationType> binders) {
        // SHADOWING (audit T1.1): a lambda's parameters hide any
        // same-named outer grid binder for its whole subtree — a nested
        // lambda over a NON-grid source must never see the outer row
        // schema (the substitution would be silently wrong data)
        if (n instanceof TypedLambda lam && !binders.isEmpty()) {
            Map<String, Type.RelationType> shadowed = null;
            for (String p : lam.parameters()) {
                if (binders.containsKey(p)) {
                    if (shadowed == null) {
                        shadowed = new HashMap<>(binders);
                    }
                    shadowed.remove(p);
                }
            }
            if (shadowed != null) {
                binders = shadowed;
            }
        }
        List<TypedSpec> kids = n.children();
        if (kids.isEmpty()) {
            return n;
        }
        List<TypedSpec> out = new ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec s = resolve(k, oracle, binders);
            changed |= s != k;
            out.add(s);
        }
        return changed ? n.withChildren(out) : n;
    }

    private static List<TypedSpec> replaceFirst(List<TypedSpec> kids,
            TypedSpec from, TypedSpec to) {
        List<TypedSpec> out = new ArrayList<>(kids);
        for (int i = 0; i < out.size(); i++) {
            if (out.get(i) == from) {
                out.set(i, to);
                break;
            }
        }
        return out;
    }
}
