// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import com.legend.base.Nullable;
import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * THE TEST PLAN'S OWN DATA AS JSON, WRITTEN OUT (the plan-as-data design's steps 4 and 5, 2026-11-08): a verdict's
 * host facts -- its assertion, its judgment (each of its kinds), its sides' shapes -- and the database judge's root
 * loop. A record is an object of its components by name, a sealed family's member tagged {@code _type} by its simple
 * name, an enum by its constant's name, a list or a set an array, a map of names an object, an absent value null. One
 * method each way per type, spelled out: no reflection (F1.11), so every type the plan holds is named here, and a type
 * this codec does not name cannot be written. The product's nodes and the steps keep {@link TestPlanJson}'s spelling.
 */
final class HostJson {

    private HostJson() {
    }

    private static final String TAG = "_type";

    // ---- the shared spellings ---------------------------------------------------------------------------------------

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

    private static Map<String, Object> multiplicity(ExecutionPlan.Multiplicity m) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("lower", m.lower());
        if (m.upper() != null) {
            o.put("upper", m.upper());
        }
        return o;
    }

    private static ExecutionPlan.Multiplicity readMultiplicity(Json.Obj o) {
        return new ExecutionPlan.Multiplicity(o.getInt("lower"), o.has("upper") ? o.getInt("upper") : null);
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

    /** {@code o}'s component {@code key}: present, perhaps null. */
    private static Json.Node field(Json.Obj o, String key, String record) {
        if (!o.has(key)) {
            throw new IllegalArgumentException(record + " read without its '" + key + "'");
        }
        return o.get(key);
    }

    private static boolean absent(Json.Obj o, String key, String record) {
        return field(o, key, record) instanceof Json.Null;
    }

    static Map<String, Object> wHost(TestPlan.Host x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("assertion", wAssertion(x.assertion()));
        o.put("judgment", wHostJudgment(x.judgment()));
        o.put("shapes", list(x.shapes(), each -> wSideShape(each)));
        return o;
    }

    static TestPlan.Host rHost(Json.Obj o) {
        Assertion v_assertion = rAssertion(obj(field(o, "assertion", "Host")));
        HostJudgment v_judgment = rHostJudgment(obj(field(o, "judgment", "Host")));
        List<SideShape> v_shapes = readList(field(o, "shapes", "Host"), each -> rSideShape(obj(each)));
        return new TestPlan.Host(v_assertion, v_judgment, v_shapes);
    }

    static Map<String, Object> wAssertion(Assertion x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("listenerName", x.listenerName());
        o.put("family", x.family() == null ? null : x.family());
        o.put("wantEqual", x.wantEqual());
        o.put("beforeTheArm", x.beforeTheArm() == null ? null : wFailure(x.beforeTheArm()));
        return o;
    }

    static Assertion rAssertion(Json.Obj o) {
        String v_listenerName = str(field(o, "listenerName", "Assertion"));
        @Nullable String v_family = absent(o, "family", "Assertion") ? null : str(o.get("family"));
        boolean v_wantEqual = bool(field(o, "wantEqual", "Assertion"));
        @Nullable Failure v_beforeTheArm = absent(o, "beforeTheArm", "Assertion") ? null : rFailure(obj(o.get("beforeTheArm")));
        return new Assertion(v_listenerName, v_family, v_wantEqual, v_beforeTheArm);
    }

    static Map<String, Object> wFailure(Failure x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("kind", x.kind().name());
        o.put("message", x.message() == null ? null : x.message());
        o.put("phase", x.phase() == null ? null : x.phase().name());
        o.put("element", x.element() == null ? null : x.element());
        o.put("unjudgedReason", x.unjudgedReason() == null ? null : x.unjudgedReason());
        o.put("where", x.where());
        return o;
    }

    static Failure rFailure(Json.Obj o) {
        Failure.Kind v_kind = Failure.Kind.valueOf(str(field(o, "kind", "Failure")));
        @Nullable String v_message = absent(o, "message", "Failure") ? null : str(o.get("message"));
        com.legend.error.LegendCompileException.@Nullable Phase v_phase = absent(o, "phase", "Failure") ? null : com.legend.error.LegendCompileException.Phase.valueOf(str(o.get("phase")));
        @Nullable String v_element = absent(o, "element", "Failure") ? null : str(o.get("element"));
        @Nullable String v_unjudgedReason = absent(o, "unjudgedReason", "Failure") ? null : str(o.get("unjudgedReason"));
        String v_where = str(field(o, "where", "Failure"));
        return new Failure(v_kind, v_message, v_phase, v_element, v_unjudgedReason, v_where);
    }

    static Map<String, Object> wSideShape(SideShape x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("kind", x.kind().name());
        o.put("typeName", x.typeName());
        o.put("columns", list(x.columns(), each -> wSideShapeColumn(each)));
        o.put("templates", list(x.templates(), each -> wSideShapeColumn(each)));
        o.put("oneCarrier", x.oneCarrier());
        o.put("scalar", x.scalar());
        o.put("lower", x.lower());
        o.put("multiplicity", x.multiplicity());
        o.put("tdsNullRoot", x.tdsNullRoot());
        o.put("nullableRoot", x.nullableRoot());
        o.put("listCarrier", x.listCarrier());
        return o;
    }

    static SideShape rSideShape(Json.Obj o) {
        SideShape.Kind v_kind = SideShape.Kind.valueOf(str(field(o, "kind", "SideShape")));
        String v_typeName = str(field(o, "typeName", "SideShape"));
        List<SideShape.Column> v_columns = readList(field(o, "columns", "SideShape"), each -> rSideShapeColumn(obj(each)));
        List<SideShape.Column> v_templates = readList(field(o, "templates", "SideShape"), each -> rSideShapeColumn(obj(each)));
        boolean v_oneCarrier = bool(field(o, "oneCarrier", "SideShape"));
        boolean v_scalar = bool(field(o, "scalar", "SideShape"));
        long v_lower = num(field(o, "lower", "SideShape"));
        String v_multiplicity = str(field(o, "multiplicity", "SideShape"));
        boolean v_tdsNullRoot = bool(field(o, "tdsNullRoot", "SideShape"));
        boolean v_nullableRoot = bool(field(o, "nullableRoot", "SideShape"));
        boolean v_listCarrier = bool(field(o, "listCarrier", "SideShape"));
        return new SideShape(v_kind, v_typeName, v_columns, v_templates, v_oneCarrier, v_scalar, v_lower, v_multiplicity, v_tdsNullRoot, v_nullableRoot, v_listCarrier);
    }

    static Map<String, Object> wSideShapeColumn(SideShape.Column x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", x.name());
        o.put("type", x.type());
        o.put("multiplicity", x.multiplicity() == null ? null : multiplicity(x.multiplicity()));
        return o;
    }

    static SideShape.Column rSideShapeColumn(Json.Obj o) {
        String v_name = str(field(o, "name", "Column"));
        String v_type = str(field(o, "type", "Column"));
        ExecutionPlan.@Nullable Multiplicity v_multiplicity = absent(o, "multiplicity", "Column") ? null : readMultiplicity(obj(o.get("multiplicity")));
        return new SideShape.Column(v_name, v_type, v_multiplicity);
    }

    static Map<String, Object> wKeyTree(KeyTree x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("classFqn", x.classFqn());
        o.put("keys", list(x.keys(), each -> wKeyTreeKey(each)));
        return o;
    }

    static KeyTree rKeyTree(Json.Obj o) {
        String v_classFqn = str(field(o, "classFqn", "KeyTree"));
        List<KeyTree.Key> v_keys = readList(field(o, "keys", "KeyTree"), each -> rKeyTreeKey(obj(each)));
        return new KeyTree(v_classFqn, v_keys);
    }

    static Map<String, Object> wKeyTreeKey(KeyTree.Key x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", x.name());
        o.put("many", x.many());
        o.put("nested", x.nested() == null ? null : wKeyTree(x.nested()));
        o.put("objectOnly", x.objectOnly());
        o.put("declaredClass", x.declaredClass() == null ? null : x.declaredClass());
        return o;
    }

    static KeyTree.Key rKeyTreeKey(Json.Obj o) {
        String v_name = str(field(o, "name", "Key"));
        boolean v_many = bool(field(o, "many", "Key"));
        @Nullable KeyTree v_nested = absent(o, "nested", "Key") ? null : rKeyTree(obj(o.get("nested")));
        boolean v_objectOnly = bool(field(o, "objectOnly", "Key"));
        @Nullable String v_declaredClass = absent(o, "declaredClass", "Key") ? null : str(o.get("declaredClass"));
        return new KeyTree.Key(v_name, v_many, v_nested, v_objectOnly, v_declaredClass);
    }

    static Map<String, Object> wKcEnum(KindClass.Enum x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("fqn", x.fqn());
        return o;
    }

    static KindClass.Enum rKcEnum(Json.Obj o) {
        String v_fqn = str(field(o, "fqn", "Enum"));
        return new KindClass.Enum(v_fqn);
    }

    static Map<String, Object> wKcInstance(KindClass.Instance x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("fqn", x.fqn());
        return o;
    }

    static KindClass.Instance rKcInstance(Json.Obj o) {
        String v_fqn = str(field(o, "fqn", "Instance"));
        return new KindClass.Instance(v_fqn);
    }

    static Map<String, Object> wKcElement(KindClass.Element x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("fqn", x.fqn());
        return o;
    }

    static KindClass.Element rKcElement(Json.Obj o) {
        String v_fqn = str(field(o, "fqn", "Element"));
        return new KindClass.Element(v_fqn);
    }

    static Map<String, Object> wHjAllRows(HostJudgment.AllRows x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", x.name());
        o.put("wantTrue", x.wantTrue());
        o.put("message", x.message());
        return o;
    }

    static HostJudgment.AllRows rHjAllRows(Json.Obj o) {
        String v_name = str(field(o, "name", "AllRows"));
        boolean v_wantTrue = bool(field(o, "wantTrue", "AllRows"));
        String v_message = str(field(o, "message", "AllRows"));
        return new HostJudgment.AllRows(v_name, v_wantTrue, v_message);
    }

    static Map<String, Object> wHjRows(HostJudgment.Rows x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", x.name());
        o.put("tolerance", x.tolerance());
        o.put("args", list(x.args(), each -> wRowArg(each)));
        o.put("expectedClass", x.expectedClass() == null ? null : x.expectedClass());
        o.put("actualClass", x.actualClass() == null ? null : x.actualClass());
        o.put("pairing", x.pairing() == null ? null : wPairing(x.pairing()));
        return o;
    }

    static HostJudgment.Rows rHjRows(Json.Obj o) {
        String v_name = str(field(o, "name", "Rows"));
        boolean v_tolerance = bool(field(o, "tolerance", "Rows"));
        List<HostJudgment.RowArg> v_args = readList(field(o, "args", "Rows"), each -> rRowArg(obj(each)));
        @Nullable String v_expectedClass = absent(o, "expectedClass", "Rows") ? null : str(o.get("expectedClass"));
        @Nullable String v_actualClass = absent(o, "actualClass", "Rows") ? null : str(o.get("actualClass"));
        HostJudgment.@Nullable Pairing v_pairing = absent(o, "pairing", "Rows") ? null : rPairing(obj(o.get("pairing")));
        return new HostJudgment.Rows(v_name, v_tolerance, v_args, v_expectedClass, v_actualClass, v_pairing);
    }

    static Map<String, Object> wHjUnrolled(HostJudgment.Unrolled x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("parts", list(x.parts(), each -> wPart(each)));
        return o;
    }

    static HostJudgment.Unrolled rHjUnrolled(Json.Obj o) {
        List<HostJudgment.Part> v_parts = readList(field(o, "parts", "Unrolled"), each -> rPart(obj(each)));
        return new HostJudgment.Unrolled(v_parts);
    }

    static Map<String, Object> wHjTdsEquivalent(HostJudgment.TdsEquivalent x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("hasTimeDelta", x.hasTimeDelta());
        return o;
    }

    static HostJudgment.TdsEquivalent rHjTdsEquivalent(Json.Obj o) {
        boolean v_hasTimeDelta = bool(field(o, "hasTimeDelta", "TdsEquivalent"));
        return new HostJudgment.TdsEquivalent(v_hasTimeDelta);
    }

    static Map<String, Object> wHjGridPair(HostJudgment.GridPair x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", x.name());
        o.put("wantEqual", x.wantEqual());
        o.put("sortedOrder", x.sortedOrder());
        return o;
    }

    static HostJudgment.GridPair rHjGridPair(Json.Obj o) {
        String v_name = str(field(o, "name", "GridPair"));
        boolean v_wantEqual = bool(field(o, "wantEqual", "GridPair"));
        boolean v_sortedOrder = bool(field(o, "sortedOrder", "GridPair"));
        return new HostJudgment.GridPair(v_name, v_wantEqual, v_sortedOrder);
    }

    static Map<String, Object> wHjSize(HostJudgment.Size x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("envelopeValuesRead", x.envelopeValuesRead());
        return o;
    }

    static HostJudgment.Size rHjSize(Json.Obj o) {
        boolean v_envelopeValuesRead = bool(field(o, "envelopeValuesRead", "Size"));
        return new HostJudgment.Size(v_envelopeValuesRead);
    }

    static Map<String, Object> wHjContains(HostJudgment.Contains x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("collection", wSideFacts(x.collection()));
        o.put("value", wSideFacts(x.value()));
        return o;
    }

    static HostJudgment.Contains rHjContains(Json.Obj o) {
        HostJudgment.SideFacts v_collection = rSideFacts(obj(field(o, "collection", "Contains")));
        HostJudgment.SideFacts v_value = rSideFacts(obj(field(o, "value", "Contains")));
        return new HostJudgment.Contains(v_collection, v_value);
    }

    static Map<String, Object> wHjTolerance(HostJudgment.Tolerance x) {
        Map<String, Object> o = new LinkedHashMap<>();
        return o;
    }

    static HostJudgment.Tolerance rHjTolerance(Json.Obj o) {
        return new HostJudgment.Tolerance();
    }

    static Map<String, Object> wHjCondition(HostJudgment.Condition x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", x.name());
        o.put("wantTrue", x.wantTrue());
        return o;
    }

    static HostJudgment.Condition rHjCondition(Json.Obj o) {
        String v_name = str(field(o, "name", "Condition"));
        boolean v_wantTrue = bool(field(o, "wantTrue", "Condition"));
        return new HostJudgment.Condition(v_name, v_wantTrue);
    }

    static Map<String, Object> wHjSubset(HostJudgment.Subset x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", x.name());
        o.put("wantTrue", x.wantTrue());
        return o;
    }

    static HostJudgment.Subset rHjSubset(Json.Obj o) {
        String v_name = str(field(o, "name", "Subset"));
        boolean v_wantTrue = bool(field(o, "wantTrue", "Subset"));
        return new HostJudgment.Subset(v_name, v_wantTrue);
    }

    static Map<String, Object> wHjEmpty(HostJudgment.Empty x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("wantEmpty", x.wantEmpty());
        return o;
    }

    static HostJudgment.Empty rHjEmpty(Json.Obj o) {
        boolean v_wantEmpty = bool(field(o, "wantEmpty", "Empty"));
        return new HostJudgment.Empty(v_wantEmpty);
    }

    static Map<String, Object> wHjEq(HostJudgment.Eq x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("e", wSideFacts(x.e()));
        o.put("a", wSideFacts(x.a()));
        o.put("keys", wKeys(x.keys()));
        return o;
    }

    static HostJudgment.Eq rHjEq(Json.Obj o) {
        HostJudgment.SideFacts v_e = rSideFacts(obj(field(o, "e", "Eq")));
        HostJudgment.SideFacts v_a = rSideFacts(obj(field(o, "a", "Eq")));
        HostJudgment.Keys v_keys = rKeys(obj(field(o, "keys", "Eq")));
        return new HostJudgment.Eq(v_e, v_a, v_keys);
    }

    static Map<String, Object> wHjInstanceOf(HostJudgment.InstanceOf x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("typeName", x.typeName() == null ? null : x.typeName());
        o.put("acceptedTypes", new ArrayList<Object>(x.acceptedTypes()));
        return o;
    }

    static HostJudgment.InstanceOf rHjInstanceOf(Json.Obj o) {
        @Nullable String v_typeName = absent(o, "typeName", "InstanceOf") ? null : str(o.get("typeName"));
        java.util.Set<String> v_acceptedTypes = new java.util.LinkedHashSet<>(readList(field(o, "acceptedTypes", "InstanceOf"), each -> str(each)));
        return new HostJudgment.InstanceOf(v_typeName, v_acceptedTypes);
    }

    static Map<String, Object> wHjIs(HostJudgment.Is x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", x.name());
        return o;
    }

    static HostJudgment.Is rHjIs(Json.Obj o) {
        String v_name = str(field(o, "name", "Is"));
        return new HostJudgment.Is(v_name);
    }

    static Map<String, Object> wHjDecided(HostJudgment.Decided x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("held", x.held());
        o.put("message", x.message() == null ? null : x.message());
        return o;
    }

    static HostJudgment.Decided rHjDecided(Json.Obj o) {
        boolean v_held = bool(field(o, "held", "Decided"));
        @Nullable String v_message = absent(o, "message", "Decided") ? null : str(o.get("message"));
        return new HostJudgment.Decided(v_held, v_message);
    }

    static Map<String, Object> wHjSameElements(HostJudgment.SameElements x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("e", wSideFacts(x.e()));
        o.put("a", wSideFacts(x.a()));
        o.put("gridPair", x.gridPair());
        o.put("keys", wKeys(x.keys()));
        return o;
    }

    static HostJudgment.SameElements rHjSameElements(Json.Obj o) {
        HostJudgment.SideFacts v_e = rSideFacts(obj(field(o, "e", "SameElements")));
        HostJudgment.SideFacts v_a = rSideFacts(obj(field(o, "a", "SameElements")));
        boolean v_gridPair = bool(field(o, "gridPair", "SameElements"));
        HostJudgment.Keys v_keys = rKeys(obj(field(o, "keys", "SameElements")));
        return new HostJudgment.SameElements(v_e, v_a, v_gridPair, v_keys);
    }

    static Map<String, Object> wHjEquals(HostJudgment.Equals x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", x.name());
        o.put("wantEqual", x.wantEqual());
        o.put("incidental", x.incidental());
        o.put("gridPair", x.gridPair());
        o.put("e", wSideFacts(x.e()));
        o.put("a", wSideFacts(x.a()));
        o.put("keys", wKeys(x.keys()));
        o.put("flat", wFlatFacts(x.flat()));
        return o;
    }

    static HostJudgment.Equals rHjEquals(Json.Obj o) {
        String v_name = str(field(o, "name", "Equals"));
        boolean v_wantEqual = bool(field(o, "wantEqual", "Equals"));
        boolean v_incidental = bool(field(o, "incidental", "Equals"));
        boolean v_gridPair = bool(field(o, "gridPair", "Equals"));
        HostJudgment.SideFacts v_e = rSideFacts(obj(field(o, "e", "Equals")));
        HostJudgment.SideFacts v_a = rSideFacts(obj(field(o, "a", "Equals")));
        HostJudgment.Keys v_keys = rKeys(obj(field(o, "keys", "Equals")));
        HostJudgment.FlatFacts v_flat = rFlatFacts(obj(field(o, "flat", "Equals")));
        return new HostJudgment.Equals(v_name, v_wantEqual, v_incidental, v_gridPair, v_e, v_a, v_keys, v_flat);
    }

    static Map<String, Object> wHjJson(HostJudgment.Json x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("incidentalRoot", x.incidentalRoot());
        return o;
    }

    static HostJudgment.Json rHjJson(Json.Obj o) {
        boolean v_incidentalRoot = bool(field(o, "incidentalRoot", "Json"));
        return new HostJudgment.Json(v_incidentalRoot);
    }

    static Map<String, Object> wHjRendered(HostJudgment.Rendered x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", x.name());
        o.put("wantEqual", x.wantEqual());
        o.put("form", x.form());
        o.put("eForm", x.eForm() == null ? null : x.eForm());
        o.put("aForm", x.aForm() == null ? null : x.aForm());
        o.put("sortedOrder", x.sortedOrder());
        o.put("counted", x.counted());
        return o;
    }

    static HostJudgment.Rendered rHjRendered(Json.Obj o) {
        String v_name = str(field(o, "name", "Rendered"));
        boolean v_wantEqual = bool(field(o, "wantEqual", "Rendered"));
        String v_form = str(field(o, "form", "Rendered"));
        @Nullable String v_eForm = absent(o, "eForm", "Rendered") ? null : str(o.get("eForm"));
        @Nullable String v_aForm = absent(o, "aForm", "Rendered") ? null : str(o.get("aForm"));
        boolean v_sortedOrder = bool(field(o, "sortedOrder", "Rendered"));
        boolean v_counted = bool(field(o, "counted", "Rendered"));
        return new HostJudgment.Rendered(v_name, v_wantEqual, v_form, v_eForm, v_aForm, v_sortedOrder, v_counted);
    }

    static Map<String, Object> wHjRaise(HostJudgment.Raise x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("failure", wFailure(x.failure()));
        return o;
    }

    static HostJudgment.Raise rHjRaise(Json.Obj o) {
        Failure v_failure = rFailure(obj(field(o, "failure", "Raise")));
        return new HostJudgment.Raise(v_failure);
    }

    static Map<String, Object> wHjEngineText(HostJudgment.EngineText x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", x.name());
        o.put("golden", x.golden() == null ? null : x.golden());
        o.put("ours", x.ours() == null ? null : x.ours());
        o.put("textEqual", x.textEqual());
        o.put("decline", x.decline() == null ? null : x.decline());
        o.put("oursOrigin", x.oursOrigin() == null ? null : x.oursOrigin());
        o.put("appeal", x.appeal());
        o.put("routed", x.routed());
        return o;
    }

    static HostJudgment.EngineText rHjEngineText(Json.Obj o) {
        String v_name = str(field(o, "name", "EngineText"));
        @Nullable String v_golden = absent(o, "golden", "EngineText") ? null : str(o.get("golden"));
        @Nullable String v_ours = absent(o, "ours", "EngineText") ? null : str(o.get("ours"));
        boolean v_textEqual = bool(field(o, "textEqual", "EngineText"));
        @Nullable String v_decline = absent(o, "decline", "EngineText") ? null : str(o.get("decline"));
        @Nullable String v_oursOrigin = absent(o, "oursOrigin", "EngineText") ? null : str(o.get("oursOrigin"));
        int v_appeal = Math.toIntExact(num(field(o, "appeal", "EngineText")));
        boolean v_routed = bool(field(o, "routed", "EngineText"));
        return new HostJudgment.EngineText(v_name, v_golden, v_ours, v_textEqual, v_decline, v_oursOrigin, v_appeal, v_routed);
    }

    static Map<String, Object> wHjAssertError(HostJudgment.AssertError x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("expected", x.expected());
        o.put("line", x.line() == null ? null : x.line());
        o.put("column", x.column() == null ? null : x.column());
        o.put("caught", x.caught() == null ? null : wFailure(x.caught()));
        o.put("inner", x.inner() == null ? null : wPart(x.inner()));
        return o;
    }

    static HostJudgment.AssertError rHjAssertError(Json.Obj o) {
        String v_expected = str(field(o, "expected", "AssertError"));
        @Nullable Long v_line = absent(o, "line", "AssertError") ? null : num(o.get("line"));
        @Nullable Long v_column = absent(o, "column", "AssertError") ? null : num(o.get("column"));
        @Nullable Failure v_caught = absent(o, "caught", "AssertError") ? null : rFailure(obj(o.get("caught")));
        HostJudgment.@Nullable Part v_inner = absent(o, "inner", "AssertError") ? null : rPart(obj(o.get("inner")));
        return new HostJudgment.AssertError(v_expected, v_line, v_column, v_caught, v_inner);
    }

    static Map<String, Object> wHjMessaged(HostJudgment.Messaged x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("inner", wHostJudgment(x.inner()));
        o.put("innerSides", x.innerSides());
        o.put("unplanned", x.unplanned() == null ? null : x.unplanned());
        return o;
    }

    static HostJudgment.Messaged rHjMessaged(Json.Obj o) {
        HostJudgment v_inner = rHostJudgment(obj(field(o, "inner", "Messaged")));
        int v_innerSides = Math.toIntExact(num(field(o, "innerSides", "Messaged")));
        @Nullable String v_unplanned = absent(o, "unplanned", "Messaged") ? null : str(o.get("unplanned"));
        return new HostJudgment.Messaged(v_inner, v_innerSides, v_unplanned);
    }

    static Map<String, Object> wSideFacts(HostJudgment.SideFacts x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("classFqn", x.classFqn() == null ? null : x.classFqn());
        o.put("kind", x.kind() == null ? null : wKindClass(x.kind()));
        o.put("anyStamped", x.anyStamped());
        o.put("nil", x.nil());
        o.put("mapCarrier", x.mapCarrier());
        o.put("typeName", x.typeName());
        o.put("declared", x.declared());
        return o;
    }

    static HostJudgment.SideFacts rSideFacts(Json.Obj o) {
        @Nullable String v_classFqn = absent(o, "classFqn", "SideFacts") ? null : str(o.get("classFqn"));
        @Nullable KindClass v_kind = absent(o, "kind", "SideFacts") ? null : rKindClass(obj(o.get("kind")));
        boolean v_anyStamped = bool(field(o, "anyStamped", "SideFacts"));
        boolean v_nil = bool(field(o, "nil", "SideFacts"));
        boolean v_mapCarrier = bool(field(o, "mapCarrier", "SideFacts"));
        String v_typeName = str(field(o, "typeName", "SideFacts"));
        String v_declared = str(field(o, "declared", "SideFacts"));
        return new HostJudgment.SideFacts(v_classFqn, v_kind, v_anyStamped, v_nil, v_mapCarrier, v_typeName, v_declared);
    }

    static Map<String, Object> wKeys(HostJudgment.Keys x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("e", x.e() == null ? null : wKeyTree(x.e()));
        o.put("a", x.a() == null ? null : wKeyTree(x.a()));
        o.put("eSubA", x.eSubA());
        o.put("aSubE", x.aSubE());
        o.put("byClass", map(x.byClass(), each -> wKeyTree(each)));
        return o;
    }

    static HostJudgment.Keys rKeys(Json.Obj o) {
        @Nullable KeyTree v_e = absent(o, "e", "Keys") ? null : rKeyTree(obj(o.get("e")));
        @Nullable KeyTree v_a = absent(o, "a", "Keys") ? null : rKeyTree(obj(o.get("a")));
        boolean v_eSubA = bool(field(o, "eSubA", "Keys"));
        boolean v_aSubE = bool(field(o, "aSubE", "Keys"));
        Map<String, KeyTree> v_byClass = readMap(field(o, "byClass", "Keys"), each -> rKeyTree(obj(each)));
        return new HostJudgment.Keys(v_e, v_a, v_eSubA, v_aSubE, v_byClass);
    }

    static Map<String, Object> wFlatFacts(HostJudgment.FlatFacts x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("bareRowStampE", x.bareRowStampE());
        o.put("bareRowStampA", x.bareRowStampA());
        o.put("wrappedRelationStampE", x.wrappedRelationStampE());
        o.put("wrappedRelationStampA", x.wrappedRelationStampA());
        return o;
    }

    static HostJudgment.FlatFacts rFlatFacts(Json.Obj o) {
        boolean v_bareRowStampE = bool(field(o, "bareRowStampE", "FlatFacts"));
        boolean v_bareRowStampA = bool(field(o, "bareRowStampA", "FlatFacts"));
        boolean v_wrappedRelationStampE = bool(field(o, "wrappedRelationStampE", "FlatFacts"));
        boolean v_wrappedRelationStampA = bool(field(o, "wrappedRelationStampA", "FlatFacts"));
        return new HostJudgment.FlatFacts(v_bareRowStampE, v_bareRowStampA, v_wrappedRelationStampE, v_wrappedRelationStampA);
    }

    static Map<String, Object> wRaWhole(HostJudgment.RowArg.Whole x) {
        Map<String, Object> o = new LinkedHashMap<>();
        return o;
    }

    static HostJudgment.RowArg.Whole rRaWhole(Json.Obj o) {
        return new HostJudgment.RowArg.Whole();
    }

    static Map<String, Object> wRaField(HostJudgment.RowArg.Field x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", x.name());
        return o;
    }

    static HostJudgment.RowArg.Field rRaField(Json.Obj o) {
        String v_name = str(field(o, "name", "Field"));
        return new HostJudgment.RowArg.Field(v_name);
    }

    static Map<String, Object> wRaLiteral(HostJudgment.RowArg.Literal x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("carrier", x.carrier());
        o.put("text", x.text());
        return o;
    }

    static HostJudgment.RowArg.Literal rRaLiteral(Json.Obj o) {
        String v_carrier = str(field(o, "carrier", "Literal"));
        String v_text = str(field(o, "text", "Literal"));
        return new HostJudgment.RowArg.Literal(v_carrier, v_text);
    }

    static Map<String, Object> wPaArm(HostJudgment.Pairing.Arm x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("index", x.index());
        return o;
    }

    static HostJudgment.Pairing.Arm rPaArm(Json.Obj o) {
        int v_index = Math.toIntExact(num(field(o, "index", "Arm")));
        return new HostJudgment.Pairing.Arm(v_index);
    }

    static Map<String, Object> wPaZip(HostJudgment.Pairing.Zip x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("left", wPairing(x.left()));
        o.put("right", wPairing(x.right()));
        return o;
    }

    static HostJudgment.Pairing.Zip rPaZip(Json.Obj o) {
        HostJudgment.Pairing v_left = rPairing(obj(field(o, "left", "Zip")));
        HostJudgment.Pairing v_right = rPairing(obj(field(o, "right", "Zip")));
        return new HostJudgment.Pairing.Zip(v_left, v_right);
    }

    static Map<String, Object> wPart(HostJudgment.Part x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("assertion", wAssertion(x.assertion()));
        o.put("judgment", wHostJudgment(x.judgment()));
        o.put("asks", x.asks());
        return o;
    }

    static HostJudgment.Part rPart(Json.Obj o) {
        Assertion v_assertion = rAssertion(obj(field(o, "assertion", "Part")));
        HostJudgment v_judgment = rHostJudgment(obj(field(o, "judgment", "Part")));
        int v_asks = Math.toIntExact(num(field(o, "asks", "Part")));
        return new HostJudgment.Part(v_assertion, v_judgment, v_asks);
    }

    static Map<String, Object> wRootLoop(RootLoop x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("roots", list(x.roots(), each -> wRlRoot(each)));
        o.put("fragments", map(x.fragments(), each -> each));
        return o;
    }

    static RootLoop rRootLoop(Json.Obj o) {
        List<RootLoop.Root> v_roots = readList(field(o, "roots", "RootLoop"), each -> rRlRoot(obj(each)));
        Map<String, String> v_fragments = readMap(field(o, "fragments", "RootLoop"), each -> str(each));
        return new RootLoop(v_roots, v_fragments);
    }

    static Map<String, Object> wRlRoot(RootLoop.Root x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("listenerName", x.listenerName());
        o.put("steps", list(x.steps(), each -> wRootStep(each)));
        o.put("message", x.message() == null ? null : wRlMessage(x.message()));
        return o;
    }

    static RootLoop.Root rRlRoot(Json.Obj o) {
        String v_listenerName = str(field(o, "listenerName", "Root"));
        List<RootLoop.Step> v_steps = readList(field(o, "steps", "Root"), each -> rRootStep(obj(each)));
        RootLoop.@Nullable Message v_message = absent(o, "message", "Root") ? null : rRlMessage(obj(o.get("message")));
        return new RootLoop.Root(v_listenerName, v_steps, v_message);
    }

    static Map<String, Object> wRlMessage(RootLoop.Message x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("side", x.side() == null ? null : TestPlanJson.side(x.side()));
        o.put("refusal", x.refusal() == null ? null : wFailure(x.refusal()));
        o.put("carried", x.carried());
        return o;
    }

    static RootLoop.Message rRlMessage(Json.Obj o) {
        TestPlan.@Nullable Side v_side = absent(o, "side", "Message") ? null : TestPlanJson.readSide(obj(o.get("side")));
        @Nullable Failure v_refusal = absent(o, "refusal", "Message") ? null : rFailure(obj(o.get("refusal")));
        boolean v_carried = bool(field(o, "carried", "Message"));
        return new RootLoop.Message(v_side, v_refusal, v_carried);
    }

    static Map<String, Object> wRsRow(RootLoop.Row x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", x.name());
        o.put("verdict", x.verdict());
        o.put("refusal", x.refusal() == null ? null : wFailure(x.refusal()));
        o.put("orElse", x.orElse() == null ? null : wFailure(x.orElse()));
        o.put("appeal", x.appeal());
        return o;
    }

    static RootLoop.Row rRsRow(Json.Obj o) {
        String v_name = str(field(o, "name", "Row"));
        int v_verdict = Math.toIntExact(num(field(o, "verdict", "Row")));
        @Nullable Failure v_refusal = absent(o, "refusal", "Row") ? null : rFailure(obj(o.get("refusal")));
        @Nullable Failure v_orElse = absent(o, "orElse", "Row") ? null : rFailure(obj(o.get("orElse")));
        int v_appeal = Math.toIntExact(num(field(o, "appeal", "Row")));
        return new RootLoop.Row(v_name, v_verdict, v_refusal, v_orElse, v_appeal);
    }

    static Map<String, Object> wRsDecided(RootLoop.Decided x) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("failure", wFailure(x.failure()));
        o.put("reported", x.reported());
        return o;
    }

    static RootLoop.Decided rRsDecided(Json.Obj o) {
        Failure v_failure = rFailure(obj(field(o, "failure", "Decided")));
        boolean v_reported = bool(field(o, "reported", "Decided"));
        return new RootLoop.Decided(v_failure, v_reported);
    }

    static Map<String, Object> wKindClass(KindClass x) {
        return switch (x) {
            case KindClass.Primitive e -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Primitive");
                o.put("value", e.name());
                yield o;
            }
            case KindClass.Enum m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Enum");
                o.putAll(wKcEnum(m));
                yield o;
            }
            case KindClass.Instance m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Instance");
                o.putAll(wKcInstance(m));
                yield o;
            }
            case KindClass.Element m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Element");
                o.putAll(wKcElement(m));
                yield o;
            }
        };
    }

    static KindClass rKindClass(Json.Obj o) {
        String tag = o.getString(TAG);
        return switch (tag) {
            case "Primitive" -> KindClass.Primitive.valueOf(o.getString("value"));
            case "Enum" -> rKcEnum(o);
            case "Instance" -> rKcInstance(o);
            case "Element" -> rKcElement(o);
            default -> throw new IllegalArgumentException("no member '" + tag + "' of KindClass");
        };
    }

    static Map<String, Object> wHostJudgment(HostJudgment x) {
        return switch (x) {
            case HostJudgment.AllRows m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "AllRows");
                o.putAll(wHjAllRows(m));
                yield o;
            }
            case HostJudgment.Rows m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Rows");
                o.putAll(wHjRows(m));
                yield o;
            }
            case HostJudgment.Unrolled m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Unrolled");
                o.putAll(wHjUnrolled(m));
                yield o;
            }
            case HostJudgment.TdsEquivalent m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "TdsEquivalent");
                o.putAll(wHjTdsEquivalent(m));
                yield o;
            }
            case HostJudgment.GridPair m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "GridPair");
                o.putAll(wHjGridPair(m));
                yield o;
            }
            case HostJudgment.Size m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Size");
                o.putAll(wHjSize(m));
                yield o;
            }
            case HostJudgment.Contains m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Contains");
                o.putAll(wHjContains(m));
                yield o;
            }
            case HostJudgment.Tolerance m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Tolerance");
                o.putAll(wHjTolerance(m));
                yield o;
            }
            case HostJudgment.Condition m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Condition");
                o.putAll(wHjCondition(m));
                yield o;
            }
            case HostJudgment.Subset m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Subset");
                o.putAll(wHjSubset(m));
                yield o;
            }
            case HostJudgment.Empty m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Empty");
                o.putAll(wHjEmpty(m));
                yield o;
            }
            case HostJudgment.Eq m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Eq");
                o.putAll(wHjEq(m));
                yield o;
            }
            case HostJudgment.InstanceOf m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "InstanceOf");
                o.putAll(wHjInstanceOf(m));
                yield o;
            }
            case HostJudgment.Is m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Is");
                o.putAll(wHjIs(m));
                yield o;
            }
            case HostJudgment.Decided m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Decided");
                o.putAll(wHjDecided(m));
                yield o;
            }
            case HostJudgment.SameElements m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "SameElements");
                o.putAll(wHjSameElements(m));
                yield o;
            }
            case HostJudgment.Equals m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Equals");
                o.putAll(wHjEquals(m));
                yield o;
            }
            case HostJudgment.Json m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Json");
                o.putAll(wHjJson(m));
                yield o;
            }
            case HostJudgment.Rendered m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Rendered");
                o.putAll(wHjRendered(m));
                yield o;
            }
            case HostJudgment.Raise m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Raise");
                o.putAll(wHjRaise(m));
                yield o;
            }
            case HostJudgment.EngineText m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "EngineText");
                o.putAll(wHjEngineText(m));
                yield o;
            }
            case HostJudgment.AssertError m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "AssertError");
                o.putAll(wHjAssertError(m));
                yield o;
            }
            case HostJudgment.Messaged m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Messaged");
                o.putAll(wHjMessaged(m));
                yield o;
            }
        };
    }

    static HostJudgment rHostJudgment(Json.Obj o) {
        String tag = o.getString(TAG);
        return switch (tag) {
            case "AllRows" -> rHjAllRows(o);
            case "Rows" -> rHjRows(o);
            case "Unrolled" -> rHjUnrolled(o);
            case "TdsEquivalent" -> rHjTdsEquivalent(o);
            case "GridPair" -> rHjGridPair(o);
            case "Size" -> rHjSize(o);
            case "Contains" -> rHjContains(o);
            case "Tolerance" -> rHjTolerance(o);
            case "Condition" -> rHjCondition(o);
            case "Subset" -> rHjSubset(o);
            case "Empty" -> rHjEmpty(o);
            case "Eq" -> rHjEq(o);
            case "InstanceOf" -> rHjInstanceOf(o);
            case "Is" -> rHjIs(o);
            case "Decided" -> rHjDecided(o);
            case "SameElements" -> rHjSameElements(o);
            case "Equals" -> rHjEquals(o);
            case "Json" -> rHjJson(o);
            case "Rendered" -> rHjRendered(o);
            case "Raise" -> rHjRaise(o);
            case "EngineText" -> rHjEngineText(o);
            case "AssertError" -> rHjAssertError(o);
            case "Messaged" -> rHjMessaged(o);
            default -> throw new IllegalArgumentException("no member '" + tag + "' of HostJudgment");
        };
    }

    static Map<String, Object> wRowArg(HostJudgment.RowArg x) {
        return switch (x) {
            case HostJudgment.RowArg.Whole m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Whole");
                o.putAll(wRaWhole(m));
                yield o;
            }
            case HostJudgment.RowArg.Field m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Field");
                o.putAll(wRaField(m));
                yield o;
            }
            case HostJudgment.RowArg.Literal m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Literal");
                o.putAll(wRaLiteral(m));
                yield o;
            }
        };
    }

    static HostJudgment.RowArg rRowArg(Json.Obj o) {
        String tag = o.getString(TAG);
        return switch (tag) {
            case "Whole" -> rRaWhole(o);
            case "Field" -> rRaField(o);
            case "Literal" -> rRaLiteral(o);
            default -> throw new IllegalArgumentException("no member '" + tag + "' of RowArg");
        };
    }

    static Map<String, Object> wPairing(HostJudgment.Pairing x) {
        return switch (x) {
            case HostJudgment.Pairing.Arm m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Arm");
                o.putAll(wPaArm(m));
                yield o;
            }
            case HostJudgment.Pairing.Zip m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Zip");
                o.putAll(wPaZip(m));
                yield o;
            }
        };
    }

    static HostJudgment.Pairing rPairing(Json.Obj o) {
        String tag = o.getString(TAG);
        return switch (tag) {
            case "Arm" -> rPaArm(o);
            case "Zip" -> rPaZip(o);
            default -> throw new IllegalArgumentException("no member '" + tag + "' of Pairing");
        };
    }

    static Map<String, Object> wRootStep(RootLoop.Step x) {
        return switch (x) {
            case RootLoop.Row m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Row");
                o.putAll(wRsRow(m));
                yield o;
            }
            case RootLoop.Decided m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                o.put(TAG, "Decided");
                o.putAll(wRsDecided(m));
                yield o;
            }
        };
    }

    static RootLoop.Step rRootStep(Json.Obj o) {
        String tag = o.getString(TAG);
        return switch (tag) {
            case "Row" -> rRsRow(o);
            case "Decided" -> rRsDecided(o);
            default -> throw new IllegalArgumentException("no member '" + tag + "' of Step");
        };
    }

}
