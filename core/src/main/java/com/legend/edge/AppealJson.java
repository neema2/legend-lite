// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.base.Nullable;
import com.legend.executionplan.PlanJson;
import com.legend.json.Json;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testexecutionplan.Failure;
import com.legend.testexecutionplan.RecordJson;
import com.legend.testexecutionplan.SideShape;
import com.legend.testexecutionplan.TestPlan;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * AN APPEAL'S JSON (census C2, 2026-11-17: moved from the plan's own codec with the appeal), written and read by the
 * edge's codec ({@link EdgeCodec}); the product's records inside it written by the plan's codec ({@link RecordJson}).
 */
final class AppealJson {

    private AppealJson() {
    }

    private static <T> List<Object> list(List<T> values, Function<T, Object> each) {
        List<Object> out = new ArrayList<>(values.size());
        for (T v : values) {
            out.add(each.apply(v));
        }
        return out;
    }

    private static <T> Map<String, Object> map(Map<String, T> values, Function<T, Object> each) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, T> e : values.entrySet()) {
            out.put(e.getKey(), each.apply(e.getValue()));
        }
        return out;
    }

    private static <T> List<T> readList(Json.Node node, Function<Json.Node, T> each) {
        if (!(node instanceof Json.Arr a)) {
            throw new IllegalArgumentException("a test plan list is an array, not " + node);
        }
        List<T> out = new ArrayList<>(a.items().size());
        for (Json.Node n : a.items()) {
            out.add(each.apply(n));
        }
        return out;
    }

    private static <T> Map<String, T> readMap(Json.Node node, Function<Json.Node, T> each) {
        Map<String, T> out = new LinkedHashMap<>();
        for (Map.Entry<String, Json.Node> e : obj(node).fields().entrySet()) {
            out.put(e.getKey(), each.apply(e.getValue()));
        }
        return out;
    }

    private static Json.Obj obj(Json.Node node) {
        if (!(node instanceof Json.Obj o)) {
            throw new IllegalArgumentException("a test plan record is an object, not " + node);
        }
        return o;
    }

    private static String str(Json.Node node) {
        if (!(node instanceof Json.Str s)) {
            throw new IllegalArgumentException("a test plan text is a string, not " + node);
        }
        return s.value();
    }

    private static boolean bool(Json.Node node) {
        if (!(node instanceof Json.Bool b)) {
            throw new IllegalArgumentException("a test plan flag is a boolean, not " + node);
        }
        return b.value();
    }

    private static long num(Json.Node node) {
        if (!(node instanceof Json.Num n) || !n.isInteger()) {
            throw new IllegalArgumentException("a test plan number is an integer, not " + node);
        }
        return n.longValue();
    }

    private static Json.Node field(Json.Obj o, String key, String record) {
        if (!o.has(key)) {
            throw new IllegalArgumentException(record + " read without its '" + key + "'");
        }
        return o.get(key);
    }

    private static boolean absent(Json.Obj o, String key, String record) {
        return field(o, key, record) instanceof Json.Null;
    }

    static Map<String, Object> wAppeal(Appeal x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("number", x.number());
        o.put("name", x.name());
        o.put("kind", x.kind().name());
        o.put("contract", x.contract() == null ? null : x.contract());
        o.put("rows", x.rows() == null ? null : wAppealRows(x.rows()));
        o.put("batched", x.batched() == null ? null : wAppealBatched(x.batched()));
        o.put("joins", x.joins());
        return o;
    }

    static Appeal rAppeal(Json.Obj o) {
        int v_number = Math.toIntExact(num(field(o, "number", "Appeal")));
        String v_name = str(field(o, "name", "Appeal"));
        Appeal.Kind v_kind = Appeal.Kind.valueOf(str(field(o, "kind", "Appeal")));
        @Nullable String v_contract = absent(o, "contract", "Appeal") ? null : str(o.get("contract"));
        Appeal.@Nullable Rows v_rows = absent(o, "rows", "Appeal") ? null : rAppealRows(obj(o.get("rows")));
        Appeal.@Nullable Batched v_batched = absent(o, "batched", "Appeal") ? null : rAppealBatched(obj(o.get("batched")));
        int v_joins = Math.toIntExact(num(field(o, "joins", "Appeal")));
        return new Appeal(v_number, v_name, v_kind, v_contract, v_rows, v_batched, v_joins);
    }

    static Map<String, Object> wAppealRows(Appeal.Rows x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("rows", wAppealRead(x.rows()));
        o.put("population", wAppealRead(x.population()));
        o.put("replaySql", x.replaySql() == null ? null : x.replaySql());
        o.put("mappingFqn", x.mappingFqn() == null ? null : x.mappingFqn());
        o.put("classFqn", x.classFqn() == null ? null : x.classFqn());
        o.put("extentSubset", x.extentSubset());
        o.put("ordered", x.ordered());
        o.put("sortKeys", x.sortKeys() == null ? null : list(x.sortKeys(), e0 -> e0));
        o.put("goldenPlan", x.goldenPlan() == null ? null : x.goldenPlan());
        o.put("planBindings", map(x.planBindings(), e0 -> list(e0, e1 -> e1)));
        o.put("temps", list(x.temps(), e0 -> wAppealTemp(e0)));
        o.put("seed", x.seed() == null ? null : wAppealSeed(x.seed()));
        o.put("on", com.legend.executionplan.PlanJson.targetJson(x.on()));
        o.put("enums", wAppealEnums(x.enums()));
        return o;
    }

    static Map<String, Object> wAppealEnums(Appeal.Enums x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("enums", list(x.enums(), e0 -> e0));
        o.put("decodes", map(x.decodes(), e0 -> map(e0, e1 -> e1)));
        o.put("properties", map(x.properties(), e0 -> e0));
        return o;
    }

    static Appeal.Enums rAppealEnums(Json.Obj o) {
        List<String> v_enums = readList(field(o, "enums", "Enums"), n0 -> str(n0));
        Map<String, Map<String, String>> v_decodes = readMap(field(o, "decodes", "Enums"),
                n0 -> readMap(n0, n1 -> str(n1)));
        Map<String, String> v_properties = readMap(field(o, "properties", "Enums"), n0 -> str(n0));
        return new Appeal.Enums(v_enums, v_decodes, v_properties);
    }

    static Appeal.Rows rAppealRows(Json.Obj o) {
        Appeal.Read v_rows = rAppealRead(obj(field(o, "rows", "Rows")));
        Appeal.Read v_population = rAppealRead(obj(field(o, "population", "Rows")));
        @Nullable String v_replaySql = absent(o, "replaySql", "Rows") ? null : str(o.get("replaySql"));
        @Nullable String v_mappingFqn = absent(o, "mappingFqn", "Rows") ? null : str(o.get("mappingFqn"));
        @Nullable String v_classFqn = absent(o, "classFqn", "Rows") ? null : str(o.get("classFqn"));
        boolean v_extentSubset = bool(field(o, "extentSubset", "Rows"));
        boolean v_ordered = bool(field(o, "ordered", "Rows"));
        @Nullable List<String> v_sortKeys = absent(o, "sortKeys", "Rows") ? null : readList(o.get("sortKeys"), n0 -> str(n0));
        @Nullable String v_goldenPlan = absent(o, "goldenPlan", "Rows") ? null : str(o.get("goldenPlan"));
        Map<String, List<String>> v_planBindings = readMap(field(o, "planBindings", "Rows"), n0 -> readList(n0, n1 -> str(n1)));
        List<Appeal.TempTable> v_temps = readList(field(o, "temps", "Rows"), n0 -> rAppealTemp(obj(n0)));
        Appeal.@Nullable InListSeed v_seed = absent(o, "seed", "Rows") ? null : rAppealSeed(obj(o.get("seed")));
        ExecutionPlan.Target v_on = com.legend.executionplan.PlanJson.targetOf(obj(field(o, "on", "Rows")));
        Appeal.Enums v_enums = rAppealEnums(obj(field(o, "enums", "Rows")));
        return new Appeal.Rows(v_rows, v_population, v_replaySql, v_mappingFqn, v_classFqn, v_extentSubset, v_ordered, v_sortKeys, v_goldenPlan, v_planBindings, v_temps, v_seed, v_on, v_enums);
    }

    static Map<String, Object> wAppealRead(Appeal.Read x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("side", x.side() == null ? null : RecordJson.side(x.side()));
        o.put("shape", x.shape() == null ? null : RecordJson.sideShape(x.shape()));
        o.put("failure", x.failure() == null ? null : RecordJson.failure(x.failure()));
        o.put("unlisted", x.unlisted() == null ? null : wAppealUnlisted(x.unlisted()));
        return o;
    }

    static Map<String, Object> wAppealUnlisted(Appeal.Unlisted x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("simpleName", x.simpleName());
        o.put("message", x.message());
        return o;
    }

    static Appeal.Unlisted rAppealUnlisted(Json.Obj o) {
        String v_simpleName = str(field(o, "simpleName", "Unlisted"));
        @Nullable String v_message = absent(o, "message", "Unlisted") ? null : str(o.get("message"));
        return new Appeal.Unlisted(v_simpleName, v_message);
    }

    static Appeal.Read rAppealRead(Json.Obj o) {
        TestPlan.@Nullable Side v_side = absent(o, "side", "Read") ? null : RecordJson.readSide(obj(o.get("side")));
        @Nullable SideShape v_shape = absent(o, "shape", "Read") ? null : RecordJson.readSideShape(obj(o.get("shape")));
        @Nullable Failure v_failure = absent(o, "failure", "Read") ? null : RecordJson.readFailure(obj(o.get("failure")));
        Appeal.@Nullable Unlisted v_unlisted = absent(o, "unlisted", "Read") ? null
                : rAppealUnlisted(obj(o.get("unlisted")));
        return new Appeal.Read(v_side, v_shape, v_failure, v_unlisted);
    }

    static Map<String, Object> wAppealTemp(Appeal.TempTable x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", x.name());
        o.put("kind", x.kind());
        o.put("values", list(x.values(), e0 -> e0));
        return o;
    }

    static Appeal.TempTable rAppealTemp(Json.Obj o) {
        String v_name = str(field(o, "name", "TempTable"));
        String v_kind = str(field(o, "kind", "TempTable"));
        List<String> v_values = readList(field(o, "values", "TempTable"), n0 -> str(n0));
        return new Appeal.TempTable(v_name, v_kind, v_values);
    }

    static Map<String, Object> wAppealSeed(Appeal.InListSeed x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("kind", x.kind());
        o.put("values", list(x.values(), e0 -> e0));
        return o;
    }

    static Appeal.InListSeed rAppealSeed(Json.Obj o) {
        String v_kind = str(field(o, "kind", "InListSeed"));
        List<String> v_values = readList(field(o, "values", "InListSeed"), n0 -> str(n0));
        return new Appeal.InListSeed(v_kind, v_values);
    }

    static Map<String, Object> wAppealBatched(Appeal.Batched x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("golden", x.golden());
        o.put("ours", x.ours() == null ? null : x.ours());
        o.put("textEqual", x.textEqual());
        return o;
    }

    static Appeal.Batched rAppealBatched(Json.Obj o) {
        String v_golden = str(field(o, "golden", "Batched"));
        @Nullable String v_ours = absent(o, "ours", "Batched") ? null : str(o.get("ours"));
        boolean v_textEqual = bool(field(o, "textEqual", "Batched"));
        return new Appeal.Batched(v_golden, v_ours, v_textEqual);
    }

}
