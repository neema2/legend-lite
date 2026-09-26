// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.model.ClassMapping;
import com.legend.model.LegacyMappingDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * The AGGREGATION-AWARE execution-activity channel: for a query whose
 * root class is mapped through an AggregationAware set, the engine
 * records ONE {@code AggregationAwareActivity} whose
 * {@code rewrittenQuery} is the ROUTED query's print — the query
 * re-pointed at the main set implementation
 * ({@code ' | [SCT_Main Class Wholesales].all() -> …;'}). The print
 * format follows the engine's routed ValueSpecification spelling:
 * lambda params {@code name:FQN[1]}, statements {@code ;}-terminated,
 * chain steps {@code ' -> '}-separated, string literals single-quoted,
 * automap hops as {@code map(v_automap:FQN[1] | …)} steps.
 *
 * <p>{@link #rewrittenQuery} returns null when the frame is not
 * aggregation-aware-routed or the chain has a shape this printer does
 * not know — the caller keeps its honest fallback.
 */
final class AggAwareActivities {

    private AggAwareActivities() {
    }

    /** The routed print for an aggregation-aware frame, else null. */
    static @com.legend.base.Nullable String rewrittenQuery(TypedSpec chain, ModelContext ctx,
            com.legend.compiler.spec.SpecCompiler specs) {
        String mappingFqn = null;
        TypedSpec cur = chain;
        List<TypedSpec> ops = new ArrayList<>();
        TypedGetAll root = null;
        while (cur != null) {
            switch (cur) {
                case TypedFrom f -> {
                    mappingFqn = f.mapping()
                            .map(com.legend.compiler.spec.typed
                                    .TypedPackageableRef::fullPath)
                            .orElse(mappingFqn);
                    cur = f.source();
                }
                case TypedGetAll g -> {
                    root = g;
                    cur = null;
                }
                default -> {
                    ops.add(0, cur);
                    List<TypedSpec> ch = cur.children();
                    cur = ch.isEmpty() ? null : ch.get(0);
                }
            }
        }
        if (root == null || mappingFqn == null) {
            return null;
        }
        String setId = com.legend.resolver.AggregationAwareRouting.mainSetId(ctx, mappingFqn, root.classFqn());
        if (setId == null) {
            return null;
        }
        String simple = root.classFqn()
                .substring(root.classFqn().lastIndexOf("::") + 2);
        // the router's choice (AggregationAwareRouting): the getAll's set
        // holder names the aggregate view the query rewrote to, else the
        // main set
        TypedSpec top = ops.isEmpty() ? null : ops.get(ops.size() - 1);
        String chosen = top == null ? null
                : com.legend.resolver.AggregationAwareRouting.chooseSet(ctx, specs,
                        mappingFqn, root, ops.subList(0, ops.size() - 1), top);
        StringBuilder sb = new StringBuilder(" | [")
                .append(chosen != null ? chosen : setId + "_Main")
                .append(" Class ").append(simple).append("].all()");
        for (TypedSpec op : ops) {
            if (!step(op, sb)) {
                return null;
            }
        }
        return sb.append(';').toString();
    }

    private static boolean step(TypedSpec op, StringBuilder sb) {
        switch (op) {
            case TypedFilter f -> {
                sb.append(" -> filter(");
                if (!lambda(f.predicate(), sb)) {
                    return false;
                }
                sb.append(')');
            }
            case TypedMap m -> {
                sb.append(" -> map(");
                if (!lambda(m.mapper(), sb)) {
                    return false;
                }
                sb.append(')');
            }
            // collection-position automap hop (.product over the class
            // collection): the engine routes it as a map step
            case TypedPropertyAccess pa
                    when pa.source().info().type()
                            instanceof Type.ClassType hc -> {
                sb.append(" -> map(v_automap:").append(hc.fqn())
                        .append("[1] | $v_automap.").append(pa.property())
                        .append(";)");
            }
            // restrict(['c']) types as the column-subset select
            case com.legend.compiler.spec.typed.TypedSelect sel -> {
                sb.append(" -> restrict(");
                boolean manyR = sel.columns().size() > 1;
                if (manyR) {
                    sb.append('[');
                }
                for (int i = 0; i < sel.columns().size(); i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append('\'').append(sel.columns().get(i)).append('\'');
                }
                if (manyR) {
                    sb.append(']');
                }
                sb.append(')');
            }
            case TypedProject p -> {
                sb.append(" -> project(");
                boolean many = p.columns().size() > 1;
                if (many) {
                    sb.append('[');
                }
                for (int i = 0; i < p.columns().size(); i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    if (!lambda(p.columns().get(i).fn(), sb)) {
                        return false;
                    }
                }
                if (many) {
                    sb.append(']');
                }
                sb.append(", ");
                if (many) {
                    sb.append('[');
                }
                for (int i = 0; i < p.columns().size(); i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append('\'').append(p.columns().get(i).name())
                            .append('\'');
                }
                if (many) {
                    sb.append(']');
                }
                sb.append(')');
            }
            default -> {
                return false;
            }
        }
        return true;
    }

    /** {@code name:FQN[1] | <body>;} — the routed lambda spelling. */
    private static boolean lambda(TypedLambda l, StringBuilder sb) {
        if (l.parameters().size() != 1 || l.body().size() != 1
                || !(com.legend.compiler.element.type.PlatformTypes
                        .functionTypeOf(l.info().type())
                        instanceof Type.FunctionType ft)
                || ft.params().size() != 1
                || !(ft.params().get(0).type() instanceof Type.ClassType pct)) {
            return false;
        }
        sb.append(l.parameters().get(0)).append(':').append(pct.fqn())
                .append("[1] | ");
        if (!expr(l.body().get(0), sb)) {
            return false;
        }
        sb.append(';');
        return true;
    }

    private static boolean expr(TypedSpec n, StringBuilder sb) {
        switch (n) {
            case TypedVariable v -> sb.append('$').append(v.name());
            case TypedPropertyAccess pa -> {
                if (!expr(pa.source(), sb)) {
                    return false;
                }
                // access THROUGH a property result routes as an automap
                // step ($x.product -> map(v_automap:Product[1] | …));
                // only the lambda param's own read prints inline
                if (!(pa.source() instanceof TypedVariable)
                        && pa.source().info().type()
                                instanceof Type.ClassType hc2) {
                    sb.append(" -> map(v_automap:").append(hc2.fqn())
                            .append("[1] | $v_automap.")
                            .append(pa.property()).append(";)");
                } else {
                    sb.append('.').append(pa.property());
                }
            }
            case TypedCString s ->
                    sb.append('\'').append(s.value()).append('\'');
            case TypedMap m -> {
                // automap hop in expression position:
                // $x.product -> map(v_automap:Product[1] | …;)
                if (!expr(m.source(), sb)) {
                    return false;
                }
                sb.append(" -> map(");
                if (!lambda(m.mapper(), sb)) {
                    return false;
                }
                sb.append(')');
            }
            case TypedNativeCall c when c.args().size() == 2
                    && "meta::pure::functions::boolean::equal"
                            .equals(c.callee().qualifiedName()) -> {
                if (!expr(c.args().get(0), sb)) {
                    return false;
                }
                sb.append(" == ");
                return expr(c.args().get(1), sb);
            }
            default -> {
                return false;
            }
        }
        return true;
    }
}
