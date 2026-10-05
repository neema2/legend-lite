// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Protocol JSON without its {@code sourceInformation}, as the engine answers {@code returnSourceInformation=false}. */
public final class SourceInformation {

    private SourceInformation() {
    }

    /** {@code json} with every {@code sourceInformation} field removed. A whole model nests deep,
     *  so the parse is not held to a request's depth limit. */
    public static String strip(String json) {
        return Json.toCompact(strip(Json.parse(json, new Json.Config(4096))));
    }

    /** {@link #strip(String)} over a parsed node. */
    public static Object strip(Json.Node n) {
        return strip(n, false);
    }

    /**
     * {@code json} with EVERY span removed: {@code sourceInformation} and the named spans the protocol carries
     * beside it ({@code classSourceInformation}, {@code profileSourceInformation}, ...) -- the JSON of a model
     * parsed without source information, as the engine writes it ({@code returnSourceInformation=false}) and as
     * entity JSON arrives.
     */
    public static String stripAll(String json) {
        return Json.toCompact(strip(Json.parse(json, new Json.Config(4096)), true));
    }

    private static Object strip(Json.Node n, boolean named) {
        if (n instanceof Json.Obj o) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<String, Json.Node> e : o.fields().entrySet()) {
                String k = e.getKey();
                if (!k.equals("sourceInformation") && !(named && k.endsWith("SourceInformation"))) {
                    out.put(k, strip(e.getValue(), named));
                }
            }
            return out;
        }
        if (n instanceof Json.Arr a) {
            List<Object> out = new ArrayList<>();
            for (Json.Node x : a.items()) {
                out.add(strip(x, named));
            }
            return out;
        }
        return n;
    }
}
