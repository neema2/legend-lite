// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.sql.SqlExpr;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * legend-engine's plan-TEMPLATE parameters, the edge's own table (the audit's J31, 2026-11-18, the orchestrator's
 * ruling: no edge node in the product's MIR -- J28's pattern, product MIR plus edge-held data). Each parameter lowers
 * to a plain {@link SqlExpr.PlanParam} of its name, its type {@code UNKNOWN} (a template binds nothing); what only
 * the FreeMarker template knows is held here, by name, and read by the edge's printers ({@link EngineTextDialect#params})
 * and {@link PlanEnumForm}: an enumeration's mapping function ({@code enumMap_<mapping>_<enum>}), and the temp-table
 * protocol's wrapper variable, spliced bare ({@code ${inFilterClause_X}}, {@link InProtocol}). A field read through a
 * parameter ({@code $p.a.b}) is legend-engine's dotted placeholder {@code ${p.a.b}}: the edge resolves it before
 * lowering ({@link #resolveDotted}), so the product's lowering reads no template.
 */
public final class EngineParams {

    private final LinkedHashMap<String, SqlExpr.PlanParam> slots = new LinkedHashMap<>();
    private final Map<String, Kind> kinds = new HashMap<>();
    private final Map<String, String> enumMapFns = new HashMap<>();
    private final Set<String> splices = new HashSet<>();
    private final Set<String> tempTables = new HashSet<>();

    /** A table with no parameters. */
    public static EngineParams none() {
        return new EngineParams();
    }

    /** {@code Type[multiplicity]}, as legend-engine's plans and messages spell a parameter. */
    static String signature(Type type, com.legend.compiler.element.type.Multiplicity multiplicity) {
        return com.legend.plan.PureSpelling.typeName(type) + "["
                + sizeRange(multiplicity) + "]";
    }

    /** legend-engine's plan text's multiplicity spelling: {@code 1}, {@code 0..1}, {@code *}, {@code n..*} (inside the
     *  brackets; moved from plan.PureSpelling 2026-11-18, J40: only the edge read it). */
    static String sizeRange(
            com.legend.compiler.element.type.Multiplicity m) {
        if (m instanceof com.legend.compiler.element.type.Multiplicity
                .Bounded b) {
            if (b.upper() != null) {
                return b.lower() == b.upper() ? String.valueOf(b.lower())
                        : b.lower() + ".." + b.upper();
            }
            // unbounded: [*] (lower 0) or [n..*]
            return b.lower() == 0 ? "*" : b.lower() + "..*";
        }
        throw new com.legend.error.NotImplementedException(
                "plan: multiplicity spelling for " + m + " pending");
    }

    /** A template parameter: a plain plan parameter of {@code name}, its type unknown (a template binds nothing). */
    static SqlExpr.PlanParam template(String name, Kind kind, boolean optional) {
        return new SqlExpr.PlanParam(name, kind == Kind.ENUM, optional, com.legend.sql.SqlTyping.UNKNOWN, null);
    }

    /** A template placeholder's kind: how legend-engine's FreeMarker template spells its value (moved from
     *  sql.SqlExpr.PlanParam.Kind, L-B.PlanParam, the lens audit, 2026-11-18: only the edge's printers read it).
     *  {@code DATE} is pure Date (the h2New surface spells it with the TIMESTAMP keyword, like DATETIME);
     *  {@code STRICT_DATE} is pure StrictDate, spelled with the DATE keyword ({@code DATE'${startDate}'}). */
    public enum Kind { STRING, DATE, STRICT_DATE, DATETIME, FLOAT, BOOLEAN, ENUM, OTHER }

    /** The placeholder kind for a parameter's or a field's type (h2New types its date placeholders; an optional
     *  parameter picks the per-kind varPlaceHolderToString arguments; an enumeration's value is its NAME, quoted). Moved
     *  from lowering.Fold.planKindOf (L-B.PlanParam). */
    static Kind kindOf(Type t) {
        if (t == Type.Primitive.STRING) {
            return Kind.STRING;
        }
        if (t == Type.Primitive.DATE) {
            return Kind.DATE;
        }
        if (t == Type.Primitive.STRICT_DATE) {
            return Kind.STRICT_DATE;
        }
        if (t == Type.Primitive.DATE_TIME) {
            return Kind.DATETIME;
        }
        if (t == Type.Primitive.FLOAT) {
            return Kind.FLOAT;
        }
        if (t == Type.Primitive.BOOLEAN) {
            return Kind.BOOLEAN;
        }
        if (t instanceof Type.EnumType) {
            return Kind.ENUM;
        }
        return Kind.OTHER;
    }

    /** The template kind of the parameter {@code name}; refused by name for a parameter this table does not hold. */
    Kind kind(String name) {
        Kind k = kinds.get(name);
        if (k == null) {
            throw new IllegalStateException("plan parameter '" + name + "' is not one of the plan's template"
                    + " parameters: legend-engine's text spells only those");
        }
        return k;
    }

    /** {@code name} held as a template parameter of {@code kind}. */
    private SqlExpr.PlanParam hold(String name, Kind kind, boolean optional) {
        kinds.put(name, kind);
        SqlExpr.PlanParam p = template(name, kind, optional);
        slots.put(name, p);
        return p;
    }

    /** A declared parameter, {@code [0..1]} when {@code optional}; an enumeration's with its mapping function
     *  {@code enumMapFn}. */
    void declare(String name, Type type, boolean optional, @com.legend.base.Nullable String enumMapFn) {
        hold(name, kindOf(type), optional);
        if (enumMapFn != null) {
            enumMapFns.put(name, enumMapFn);
        }
    }

    /** A let's variable, of {@code type}. */
    void let(String name, Type type) {
        hold(name, kindOf(type), false);
    }

    /** The parameters a lowering binds. */
    Collection<SqlExpr.PlanParam> slots() {
        return slots.values();
    }

    /** The mapping function of the enumeration parameter {@code name}; null for another parameter. */
    @com.legend.base.Nullable String enumMapFn(String name) {
        return enumMapFns.get(name);
    }

    /** Whether {@code name} is the temp-table protocol's wrapper, spliced bare. */
    boolean isSplice(String name) {
        return splices.contains(name);
    }

    /** Whether {@code name} is the temp-table protocol's temp table, read back by legend-engine's temp-select template
     *  ({@code generateTempTableSelectSQLQuery}; J40, 2026-11-18: SqlExpr.TempTableInSplice until then). */
    boolean isTempTable(String name) {
        return tempTables.contains(name);
    }

    /** This table with {@code wrappers} spliced bare and {@code tables} read back as temp tables ({@link InProtocol}). */
    EngineParams withProtocol(Collection<String> wrappers, Collection<String> tables) {
        EngineParams out = copy();
        out.splices.addAll(wrappers);
        out.tempTables.addAll(tables);
        return out;
    }

    /** This table, its entries {@code other}'s (a printer's table, replaced per plan). */
    void replaceWith(EngineParams other) {
        if (other == this) {
            return;
        }
        slots.clear();
        kinds.clear();
        enumMapFns.clear();
        splices.clear();
        tempTables.clear();
        slots.putAll(other.slots);
        kinds.putAll(other.kinds);
        enumMapFns.putAll(other.enumMapFns);
        splices.addAll(other.splices);
        tempTables.addAll(other.tempTables);
    }

    private EngineParams copy() {
        EngineParams out = new EngineParams();
        out.slots.putAll(slots);
        out.kinds.putAll(kinds);
        out.enumMapFns.putAll(enumMapFns);
        out.splices.addAll(splices);
        out.tempTables.addAll(tempTables);
        return out;
    }

    /** A body with its dotted reads resolved, and the table that binds them. */
    record Resolved(List<TypedSpec> body, EngineParams params) {
    }

    /**
     * {@code body} with each field read through a parameter as legend-engine's dotted placeholder: {@code $p.a} is the
     * parameter {@code p.a} whatever the field's type; a deeper read {@code $p.a.b.c} is {@code p.a.b.c} where its
     * last field is not a class (a class-valued field stays a field read of the placeholder before it), and so is a
     * read through {@code $p->toOne()}. Its kind follows the field's type: the rule the product's lowering applied
     * until the audit's J31.
     */
    Resolved resolveDotted(List<TypedSpec> body) {
        if (slots.isEmpty()) {
            return new Resolved(body, this);
        }
        EngineParams out = copy();
        Set<String> roots = new HashSet<>(slots.keySet());
        Set<String> dotted = new HashSet<>();
        List<TypedSpec> resolved = body.stream().map(s -> dotted(s, roots, dotted, out)).toList();
        return new Resolved(resolved, out);
    }

    private static TypedSpec dotted(TypedSpec s, Set<String> roots, Set<String> dotted, EngineParams out) {
        TypedSpec m = s.mapChildren(c -> dotted(c, roots, dotted, out));
        if (!(m instanceof TypedPropertyAccess p)) {
            return m;
        }
        ArrayDeque<String> path = new ArrayDeque<>();
        TypedSpec cur = p;
        while (cur instanceof TypedPropertyAccess a) {
            path.addFirst(a.property());
            cur = a.source();
        }
        // a parameter read through toOne() is the parameter itself (its lowering's identity): a field under it reads
        // as a deeper read does -- the placeholder where the field is not a class
        boolean throughOne = false;
        if (cur instanceof com.legend.compiler.spec.typed.TypedNativeCall one && one.args().size() == 1
                && com.legend.builtin.Pure.isToOneCall(one.callee().qualifiedName())
                && one.args().get(0) instanceof TypedVariable under && roots.contains(under.name())) {
            cur = under;
            throughOne = true;
        }
        if (!(cur instanceof TypedVariable v)) {
            return m;
        }
        boolean read = !throughOne && path.size() == 1 && roots.contains(v.name())
                || (throughOne || dotted.contains(v.name())) && !(p.info().type() instanceof Type.ClassType);
        if (!read) {
            return m;
        }
        String name = v.name() + "." + String.join(".", path);
        dotted.add(name);
        out.hold(name, kindOf(p.info().type()), false);
        return new TypedVariable(name, p.info());
    }
}
