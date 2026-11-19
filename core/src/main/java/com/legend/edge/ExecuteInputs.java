// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * LEGEND-ENGINE'S EXECUTE INPUT, READ AT THE EDGE (B7 of the separation census, 2026-11-17, D's split by content: a
 * piece whose shape upstream's protocol defines is the edge's): an {@code ExecuteInput}'s {@code parameterValues}
 * bound to its lambda's parameters, for the pure/v1 routes ({@link PureV1Routes}, {@link HostAnswers}, the test-data
 * routes). Moved unchanged from the product's API kit ({@code server.LiteV1Api}).
 */
public final class ExecuteInputs {

    private ExecuteInputs() {
    }

    /**
     * {@code ExecuteInput.parameterValues} ({@code [{name, value}]}, each value a protocol value
     * specification -- a literal, a collection, an enum value, a call such as {@code today()})
     * bound to the lambda's parameters: each becomes {@code let name = value;} ahead of the
     * body, in the lambda's declared order, and the lambda takes no parameters. The database
     * still computes every value; this only rearranges the query's shape. As legend-engine
     * 4.145.0 answers (measured 2026-09-30): a parameter with no value is refused, named with
     * its type and multiplicity; a value for no parameter of the lambda is ignored.
     */
    public static Json.Obj boundParameters(Json.Obj function, @com.legend.base.Nullable Json.Arr values) {
        List<Json.Node> declared = function.has("parameters") ? function.getArr("parameters").items() : List.of();
        Map<String, Json.Node> byName = new LinkedHashMap<>();
        if (values != null) {
            for (Json.Node n : values.items()) {
                Json.Obj pv = (Json.Obj) n;
                byName.put(pv.getString("name"), pv.get("value"));
            }
        }
        if (declared.isEmpty() && byName.isEmpty()) {
            return function;
        }
        List<Json.Node> body = new ArrayList<>();
        List<String> missing = new ArrayList<>();
        for (Json.Node p : declared) {
            String name = ((Json.Obj) p).getString("name");
            Json.Node value = byName.remove(name);
            if (value == null) {
                missing.add(name + ":" + signature((Json.Obj) p));
                continue;
            }
            LinkedHashMap<String, Json.Node> let = new LinkedHashMap<>();
            let.put("_type", Json.str("func"));
            let.put("function", Json.str("letFunction"));
            LinkedHashMap<String, Json.Node> letName = new LinkedHashMap<>();
            letName.put("_type", Json.str("string"));
            letName.put("value", Json.str(name));
            let.put("parameters", new Json.Arr(List.of(new Json.Obj(letName), value)));
            body.add(new Json.Obj(let));
        }
        if (!missing.isEmpty()) {
            throw new com.legend.error.MissingParameters(missing, null);
        }
        body.addAll(function.getArr("body").items());
        LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>(function.fields());
        out.put("parameters", new Json.Arr(List.of()));
        out.put("body", new Json.Arr(body));
        return new Json.Obj(out);
    }

    /** A lambda parameter's {@code Type[m]}, as the engine's refusal spells it. */
    private static String signature(Json.Obj parameter) {
        Json.Obj raw = parameter.getObj("genericType").getObj("rawType");
        Json.Obj m = parameter.getObj("multiplicity");
        long lower = m.getLong("lowerBound");
        String mult = !m.has("upperBound") || m.get("upperBound") instanceof Json.Null
                ? (lower == 0 ? "*" : lower + "..*")
                : lower == m.getLong("upperBound") ? String.valueOf(lower) : lower + ".." + m.getLong("upperBound");
        return raw.getString("fullPath") + "[" + mult + "]";
    }

    /** The runtime of an upstream ExecuteInput (moved from server.LiteV1Api, J40 of the edge audit, 2026-11-18: its
     *  callers are the edge's pure/v1 routes and host answers): the request's own, else the one the query's {@code ->from}s bind -- every door's one rule
     *  ({@link com.legend.TypedQuery#runtime}: several refused by name; the checkpoint tenets audit's 1, 2026-11-10:
     *  until then this door kept the first from()'s and ran the rest on it). */
    public static String runtimeOf(Json.Obj request, com.legend.TypedQuery query) {
        Json.Obj rt = request.getObjOr("runtime", null);
        if (rt != null && !rt.has("runtime")) {
            // a runtime written out in full (upstream's ExecuteInput.runtime: an engineRuntime, a legacyRuntime) is
            // refused by name, never dropped for the query's own ->from runtime (register S301, the user's ruling of
            // 2026-11-12: "It can also go to follow up"; until then this door read it as no runtime at all)
            throw new com.legend.error.NotImplementedException("a runtime written out in the request ("
                    + rt.getStringOr("_type", "?") + "): not planned; name a runtime (runtimePointer), register S301");
        }
        String runtime = query.runtime(rt != null ? rt.getString("runtime") : null);
        if (runtime == null) {
            throw new IllegalArgumentException(
                    "no runtime: the request names none and the query has no ->from(runtime)");
        }
        return runtime;
    }
}
