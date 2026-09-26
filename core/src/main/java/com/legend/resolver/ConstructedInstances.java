// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.RelationalOpRows;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.RelationalOperation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * CONSTRUCTED metamodel instances as ROWS (group F burn, 2026-09-02 — the
 * ruling's "resolver side-output rows"): a query's {@code ^DynaFunction(...)}
 * / {@code ^Literal(...)} / {@code ^LiteralList(...)} tree over constants is
 * the same fact a mapping expression is, so it becomes {@code
 * relational_elements} / {@code data_types} rows (one builder, {@link
 * RelationalOpRows}) keyed by a content id, and the instance expression
 * becomes the member class's extent filtered on that id — every navigation
 * and function body over it is then the ordinary row path. THE ROWS RIDE
 * THE QUERY (user ruling 2026-09-02): the chain rooted at the instance
 * resolves under the tree's scope and its class sources read these rows as
 * inline relations ({@code ClassSources.scoped}); the system database is
 * never written for them.
 *
 * <p>A ROW-VALUED argument inside a constructed tree (a navigated element)
 * has no compile-time row: it is admitted only where the parent's type rule
 * ignores its arguments ({@link #ARG_FREE}, the engine's own fixed-type dyna
 * functions — {@code joinStrings} is VARCHAR(4000) whatever it joins) and
 * seeds no child row (named gap); anywhere else the tree stays an instance
 * and walls loudly downstream.
 */
final class ConstructedInstances {

    private static final String DYNA = "meta::relational::metamodel::DynaFunction";
    private static final String LITERAL = "meta::relational::metamodel::Literal";
    private static final String LITERAL_LIST = "meta::relational::metamodel::LiteralList";
    private static final String SQL_NULL = "meta::relational::metamodel::SQLNull";

    /** Dyna functions whose inferred type does not depend on their
     * arguments (RelationalTypeInference's fixed-result rules). */
    private static final Set<String> ARG_FREE = Set.of("joinstrings", "count",
            "sqlnull", "sqltrue", "sqlfalse", "and", "or", "not", "equal", "notequal",
            "in", "isnull", "isnotnull", "greaterthan", "lessthan", "greaterthanequal",
            "lessthanequal", "isempty", "isnotempty", "like", "startswith", "endswith",
            "contains", "position", "length", "charindex", "locate", "indexof");

    private final ModelContext ctx;
    private final ClassSources sources;
    /** tree id -> (store table -> its rows): THE QUERY'S OWN CONSTANTS,
     * read as inline rows under the tree's scope (ClassSources) — never
     * written to the system database. */
    private final Map<String, Map<String, List<List<String>>>> rowsById = new LinkedHashMap<>();
    private final Set<String> seen = new HashSet<>();

    ConstructedInstances(ModelContext ctx, ClassSources sources) {
        this.ctx = ctx;
        this.sources = sources;
    }

    /** Whether {@code ni} is a system-mapped relational-op instance the
     * store can carry as rows. */
    boolean convertible(TypedNewInstance ni) {
        return (ni.classFqn().equals(DYNA) || ni.classFqn().equals(LITERAL)
                || ni.classFqn().equals(LITERAL_LIST))
                && sources.binds(com.legend.builtin.SystemMetamodel.MAPPING_FQN, ni.classFqn())
                && convert(ni) != null;
    }

    /** The row id of a convertible instance (its rows recorded once), or
     * null when the instance is not one. */
    @com.legend.base.Nullable String rowId(TypedNewInstance ni) {
        if (!convertible(ni)) {
            return null;
        }
        RelationalOperation op = java.util.Objects.requireNonNull(convert(ni));
        String id = "q:" + Integer.toHexString(op.toString().hashCode())
                + ":" + op.toString().length();
        if (seen.add(id)) {
            RelationalOpRows rows = new RelationalOpRows(ctx);
            rows.node(op, id, null, null, null, null, null);
            Map<String, List<List<String>>> byTable = new LinkedHashMap<>();
            byTable.put("relational_elements", rows.ops);
            byTable.put("data_types", rows.dataTypes);
            rowsById.put(id, byTable);
        }
        return id;
    }

    /** The rows of one constructed tree per store table (the scope's
     * inline relations); empty for an unknown id. */
    /** Rows computed OUTSIDE the resolver (the executor's plan model as
     * rows — PlanRows): registered under their scope id, read like any
     * constructed tree's. */
    void register(String id, Map<String, List<List<String>>> rows) {
        if (rows instanceof com.legend.plan.LazyRows lazy) {
            pending.put(id, lazy);   // computed when first read
        } else {
            rowsById.put(id, rows);
        }
    }

    /** Registered rows not yet computed ({@link com.legend.plan.LazyRows}). */
    private final Map<String, com.legend.plan.LazyRows> pending = new LinkedHashMap<>();

    /** Moves {@code id}'s pending rows into place (none: not registered). */
    private void materialize(String id) {
        com.legend.plan.LazyRows lazy = pending.remove(id);
        if (lazy != null) {
            Map<String, List<List<String>>> rows = lazy.rows();
            if (rows != null) {
                rowsById.put(id, rows);
            }
        }
    }

    private java.util.function.@com.legend.base.Nullable Function<
            com.legend.compiler.spec.typed.TypedNativeCall,
            @com.legend.base.Nullable Map<String, List<List<String>>>> handleRegistrar;

    void setHandleRegistrar(java.util.function.Function<
            com.legend.compiler.spec.typed.TypedNativeCall,
            @com.legend.base.Nullable Map<String, List<List<String>>>> registrar) {
        this.handleRegistrar = registrar;
    }

    /** Whether a HANDLE call's rows are registered — registering them ON
     * DEMAND through the executor's registrar when the resolver first
     * meets an INLINE handle (an executionPlan(...) inside an inlined
     * helper: no let to register at). */
    boolean handleRows(com.legend.compiler.spec.typed.TypedNativeCall pn) {
        String scope = com.legend.plan.PlanRows.scopeId(pn);
        if (!has(scope) && handleRegistrar != null) {
            Map<String, List<List<String>>> rows = handleRegistrar.apply(pn);
            if (rows != null) {
                register(scope, rows);
            }
        }
        return has(scope);
    }

    boolean has(String id) {
        materialize(id);
        return rowsById.containsKey(id);
    }

    Map<String, List<List<String>>> rowsFor(String id) {
        materialize(id);
        Map<String, List<List<String>>> rows = rowsById.get(id);
        return rows == null ? Map.of() : rows;
    }

    /** The relational operation a constant instance tree denotes; null for
     * any other shape. */
    static @com.legend.base.Nullable RelationalOperation convert(TypedNewInstance ni) {
        switch (ni.classFqn()) {
            case DYNA -> {
                if (!(ni.properties().get("name") instanceof TypedCString cs)) {
                    return null;
                }
                List<RelationalOperation> args = new ArrayList<>();
                boolean opaque = false;
                for (TypedSpec e : elements(ni.properties().get("parameters"))) {
                    RelationalOperation a = argument(e);
                    if (a == null) {
                        opaque = true;
                    } else {
                        args.add(a);
                    }
                }
                if (opaque && !ARG_FREE.contains(cs.value().toLowerCase(java.util.Locale.ROOT))) {
                    return null;
                }
                return new RelationalOperation.FunctionCall(cs.value(), args);
            }
            case LITERAL -> {
                TypedSpec v = ni.properties().get("value");
                Object lit = switch (v) {
                    case null -> null;
                    case TypedCString c -> c.value();
                    case TypedCInteger i -> i.value();
                    case TypedCFloat f -> f.value();
                    case TypedCBoolean b -> b.value();
                    case TypedCDate d -> d.value();
                    default -> null;
                };
                if (lit == null && v instanceof TypedNewInstance sn
                        && sn.classFqn().equals(SQL_NULL)) {
                    // ^Literal(value=^SQLNull()) — the null marker rides as
                    // the sqlNull dynafunction (one downstream shape)
                    return new RelationalOperation.FunctionCall("sqlNull", List.of());
                }
                return lit == null ? null : new RelationalOperation.Literal(lit);
            }
            case LITERAL_LIST -> {
                List<RelationalOperation> els = new ArrayList<>();
                for (TypedSpec e : elements(ni.properties().get("values"))) {
                    RelationalOperation a = argument(e);
                    if (a == null) {
                        return null;
                    }
                    els.add(a);
                }
                return new RelationalOperation.ArrayLiteral(els);
            }
            default -> {
                return null;
            }
        }
    }

    private static @com.legend.base.Nullable RelationalOperation argument(TypedSpec e) {
        return e instanceof TypedNewInstance n ? convert(n) : null;
    }

    private static List<TypedSpec> elements(@com.legend.base.Nullable TypedSpec v) {
        return v == null ? List.of()
                : v instanceof TypedCollection c ? c.elements() : List.of(v);
    }
}
