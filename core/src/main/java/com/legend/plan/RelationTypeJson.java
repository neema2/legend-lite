// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.plan;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A query's result columns AS THE COMPILER TYPED THEM, as JSON: lite's own form of a relation's type, which a lite plan
 * export carries in its {@code type} field (the wasm boundary's plan, planJson, planHeld and planJsonHeld). Plain maps,
 * {@code _type} first, then alphabetical; each surface serialises them with its own writer. Today its bytes are those
 * of legend-engine's {@code lambdaRelationType} answer, which the compatibility edge writes for pure/v1's route
 * ({@code edge.UpstreamRelationType}); this is the product's own, and follows the compiler, not upstream (Phase 6,
 * 2026-11-17, the orchestrator's ruling (a): a product export reaches no edge class).
 */
public final class RelationTypeJson {

    private RelationTypeJson() {
    }

    public static Map<String, Object> of(ExprType root) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("_type", "relationType");
        List<Map<String, Object>> columns = new ArrayList<>();
        for (Type.Column c : PureSpelling.relationColumns(root)) {
            Map<String, Object> col = new LinkedHashMap<>();
            col.put("genericType", genericType(c.type()));
            col.put("multiplicity", multiplicity(c.multiplicity()));
            col.put("name", c.name());
            columns.add(col);
        }
        out.put("columns", columns);
        return out;
    }

    private static Map<String, Object> genericType(Type t) {
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put("_type", "packageableType");
        raw.put("fullPath", PureSpelling.typePath(t));
        List<Map<String, Object>> typeVariableValues = new ArrayList<>();
        if (t instanceof Type.PrecisionDecimal d) {
            typeVariableValues.add(integer(d.precision()));
            typeVariableValues.add(integer(d.scale()));
        }
        Map<String, Object> g = new LinkedHashMap<>();
        g.put("multiplicityArguments", List.of());
        g.put("rawType", raw);
        g.put("typeArguments", List.of());
        g.put("typeVariableValues", typeVariableValues);
        return g;
    }

    private static Map<String, Object> integer(int v) {
        Map<String, Object> i = new LinkedHashMap<>();
        i.put("_type", "integer");
        i.put("value", v);
        return i;
    }

    private static Map<String, Object> multiplicity(Multiplicity m) {
        Multiplicity.Bounded b = m.requireBounded("a result column");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("lowerBound", b.lower());
        if (b.upper() != null) {
            out.put("upperBound", b.upper());
        }
        return out;
    }
}
