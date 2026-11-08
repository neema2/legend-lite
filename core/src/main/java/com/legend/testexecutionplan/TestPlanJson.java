// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.json.Json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A TEST PLAN AS JSON (docs/PLAN_AS_DATA_2026_11_07.md 2.1 and 2.10's guard 2, the design's step 3, 2026-11-08): the
 * format the product's plan has ({@link PlanJson}) around its steps -- each product node in the product's own
 * spelling, the steps only a test holds in this one. Written and read back whole: the goals of record's G11 for a
 * test body's plan ({@link #requireRoundTrip}).
 */
public final class TestPlanJson {

    public static final String FORMAT = "legend-lite-test-plan";

    /** 1 (2026-11-08): the verdict groups and branches out of the product's format (its version 27), the frame runs
     *  steps of their own, the judge the plan's; 2 the same day (step 4): a verdict's host facts -- its assertion, its
     *  judgment, its sides' shapes; 3 the same day (step 5): the database judge's root loop on a segment's last group;
     *  4 the same day (step 6): the agreement judge's pairing on an assert's last group; 5 the same day (step 8): the
     *  edge's records by step and by verdict, each through its codec. */
    public static final int VERSION = 5;

    private TestPlanJson() {
    }

    /** The {@code _type} tags of a test plan's steps and sides, one spelling for the writer and the reader. */
    enum StepTag { run, frameRun, verdicts, branch }

    enum SideTag { rows, keyedRows, constant }

    private static <E extends Enum<E>> E tag(Class<E> family, Json.Obj o, String what) {
        String name = o.getString("_type");
        for (E e : family.getEnumConstants()) {
            if (e.name().equals(name)) {
                return e;
            }
        }
        throw new IllegalArgumentException("unknown " + what + " _type '" + name + "'");
    }

    /** Why {@code plan} does not survive its JSON, or null where it does. */
    public static @com.legend.base.Nullable String roundTripDifference(TestPlan plan) {
        String json = write(plan);
        TestPlan back = read(json);
        if (back.equals(plan)) {
            return null;
        }
        return write(back).equals(json) ? "read back unequal, its JSON the same (a field the JSON does not carry)"
                : "read back with different JSON";
    }

    /** G11's guard for a test body's plan: it survives its JSON, or it is refused by name, never run. */
    public static void requireRoundTrip(TestPlan plan) {
        String why = roundTripDifference(plan);
        if (why != null) {
            throw new IllegalStateException("a test plan that is not portable data (G11): " + why);
        }
    }

    // ---- write ------------------------------------------------------------------------------------------------------

    public static String write(TestPlan plan) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("format", FORMAT);
        out.put("version", VERSION);
        out.put("productVersion", PlanJson.VERSION);
        out.put("judge", plan.judge().name());
        out.put("parameters", plan.parameters().stream().map(PlanJson::parameterJson).toList());
        out.put("steps", steps(plan.steps()));
        if (!plan.extensionsByStep().isEmpty() || !plan.extensionsByVerdict().isEmpty()) {
            Map<String, Object> ext = new LinkedHashMap<>();
            ext.put("byStep", extensions(plan.extensionsByStep()));
            ext.put("byVerdict", extensions(plan.extensionsByVerdict()));
            out.put("extensions", ext);
        }
        return Json.toCompact(out);
    }

    private static List<Object> steps(List<TestPlan.TestStep> steps) {
        List<Object> out = new ArrayList<>(steps.size());
        for (TestPlan.TestStep step : steps) {
            out.add(step(step));
        }
        return out;
    }

    private static Map<String, Object> step(TestPlan.TestStep step) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (step) {
            case TestPlan.Run r -> {
                o.put("_type", StepTag.run.name());
                o.put("node", PlanJson.nodeJson(r.node()));
            }
            case TestPlan.FrameRun f -> {
                o.put("_type", StepTag.frameRun.name());
                o.put("check", PlanJson.nodeJson(f.check()));
            }
            case TestPlan.Verdicts v -> {
                o.put("_type", StepTag.verdicts.name());
                List<Object> verdicts = new ArrayList<>();
                for (TestPlan.Verdict each : v.verdicts()) {
                    Map<String, Object> vo = new LinkedHashMap<>();
                    vo.put("id", each.id());
                    vo.put("name", each.name());
                    vo.put("arm", each.arm());
                    List<Object> base = new ArrayList<>();
                    for (TestPlan.Side side : each.base()) {
                        base.add(side(side));
                    }
                    vo.put("base", base);
                    vo.put("wrapper", each.wrapper() == null ? null : PlanJson.nodeJson(each.wrapper()));
                    if (each.message() != null) {
                        vo.put("message", side(each.message()));
                    }
                    if (each.raises()) {
                        vo.put("raises", true);
                    }
                    if (each.host() != null) {
                        // the host judge's facts, the judgment and the side shapes: the test plan's own codec
                        vo.put("host", HostJson.wHost(each.host()));
                    }
                    verdicts.add(vo);
                }
                o.put("verdicts", verdicts);
                o.put("fused", v.fused() == null ? null : PlanJson.nodeJson(v.fused()));
                if (v.savepoint() != null) {
                    Map<String, Object> sp = new LinkedHashMap<>();
                    sp.put("set", v.savepoint().set());
                    sp.put("rollbackTo", v.savepoint().rollbackTo());
                    sp.put("release", v.savepoint().release());
                    o.put("savepoint", sp);
                }
                o.put("rules", switch (v.rules()) {
                    case LITE -> "lite";
                    case COMPILED_PURE_COMPAT -> "compiledPureCompat";
                });
                if (v.roots() != null) {
                    o.put("roots", HostJson.wRootLoop(v.roots()));
                }
                if (v.agreement() != null) {
                    o.put("agreement", HostJson.wAgreement(v.agreement()));
                }
            }
            case TestPlan.Branch b -> {
                o.put("_type", StepTag.branch.name());
                o.put("condition", side(b.condition()));
                o.put("then", steps(b.then()));
                o.put("otherwise", steps(b.otherwise()));
            }
        }
        return o;
    }

    /** A verdict's side, or a branch's condition. */
    static Map<String, Object> side(TestPlan.Side side) {
        Map<String, Object> so = new LinkedHashMap<>();
        switch (side) {
            case TestPlan.Side.Rows r -> {
                so.put("_type", SideTag.rows.name());
                so.put("result", PlanJson.nodeJson(r.result()));
            }
            case TestPlan.Side.KeyedRows k -> {
                so.put("_type", SideTag.keyedRows.name());
                so.put("keyed", PlanJson.nodeJson(k.keyed()));
            }
            case TestPlan.Side.Constant c -> {
                so.put("_type", SideTag.constant.name());
                so.put("columns", PlanJson.columnsJson(c.columns()));
                so.put("cells", c.cells());
                so.put("folded", c.folded());
            }
        }
        return so;
    }

    // ---- read -------------------------------------------------------------------------------------------------------

    public static TestPlan read(String json) {
        if (!(Json.parse(json, new Json.Config(PlanJson.MAX_DEPTH)) instanceof Json.Obj o)) {
            throw new IllegalArgumentException("a test plan's JSON is an object");
        }
        String format = o.getStringOr("format", null);
        if (!FORMAT.equals(format) || o.getIntOr("version", -1) != VERSION
                || o.getIntOr("productVersion", -1) != PlanJson.VERSION) {
            throw new IllegalArgumentException("not a " + FORMAT + " v" + VERSION + " plan over the product's v"
                    + PlanJson.VERSION + " (format " + format + ", version " + o.getIntOr("version", -1)
                    + ", product version " + o.getIntOr("productVersion", -1) + ")");
        }
        List<ExecutionPlan.Parameter> parameters = new ArrayList<>();
        for (Json.Node n : o.getArr("parameters").items()) {
            parameters.add(PlanJson.parameterOf((Json.Obj) n));
        }
        String judge = o.getString("judge");
        TestPlan.Judge j = null;
        for (TestPlan.Judge each : TestPlan.Judge.values()) {
            if (each.name().equals(judge)) {
                j = each;
            }
        }
        if (j == null) {
            throw new IllegalArgumentException("a test plan's judge '" + judge + "'");
        }
        Json.Obj ext = o.getObjOr("extensions", null);
        return new TestPlan(parameters, readSteps(o.getArr("steps")), j,
                ext == null ? Map.of() : readExtensions(ext.getObj("byStep")),
                ext == null ? Map.of() : readExtensions(ext.getObj("byVerdict")));
    }

    // ---- the edge's records (the plan-as-data design's step 8, 2.7) ------------------------------------------------

    /** The codecs the classpath registers (the edge's, through the {@code ServiceLoader} seam): none under a run that
     *  hands no edge, whose plan holds no record of the edge's. */
    private static List<ExtensionCodec> codecs() {
        return Codecs.ALL;
    }

    /** The registered codecs, loaded once (the slice 2 audit, 2026-11-08: each write and read loaded them again). */
    private static final class Codecs {
        static final List<ExtensionCodec> ALL = load();

        private static List<ExtensionCodec> load() {
            List<ExtensionCodec> out = new ArrayList<>();
            for (ExtensionCodec c : java.util.ServiceLoader.load(ExtensionCodec.class,
                    ExtensionCodec.class.getClassLoader())) {
                out.add(c);
            }
            return List.copyOf(out);
        }
    }

    private static Map<String, Object> extensions(Map<Integer, List<Extension>> byKey) {
        List<ExtensionCodec> codecs = codecs();
        Map<String, Object> out = new LinkedHashMap<>();
        byKey.forEach((key, records) -> {
            List<Object> written = new ArrayList<>(records.size());
            for (Extension e : records) {
                Map<String, Object> o = null;
                if (e instanceof Appeal a) {
                    // the test plan's own record (an engine-text assert's appeal, step 8b): its own codec
                    o = new LinkedHashMap<>();
                    o.put("_type", "appeal");
                    o.putAll(HostJson.wAppeal(a));
                }
                for (ExtensionCodec c : o != null ? List.<ExtensionCodec>of() : codecs) {
                    o = c.write(e);
                    if (o != null) {
                        break;
                    }
                }
                if (o == null) {
                    throw new IllegalStateException("a test plan's extension no codec writes: "
                            + e.getClass().getSimpleName());
                }
                written.add(o);
            }
            out.put(Integer.toString(key), written);
        });
        return out;
    }

    private static Map<Integer, List<Extension>> readExtensions(Json.Obj byKey) {
        List<ExtensionCodec> codecs = codecs();
        Map<Integer, List<Extension>> out = new LinkedHashMap<>();
        for (Map.Entry<String, Json.Node> e : byKey.fields().entrySet()) {
            List<Extension> records = new ArrayList<>();
            for (Json.Node n : ((Json.Arr) e.getValue()).items()) {
                Json.Obj o = (Json.Obj) n;
                String tag = o.getString("_type");
                Extension read = "appeal".equals(tag) ? HostJson.rAppeal(o) : null;
                for (ExtensionCodec c : read != null ? List.<ExtensionCodec>of() : codecs) {
                    read = c.read(tag, o);
                    if (read != null) {
                        break;
                    }
                }
                if (read == null) {
                    throw new IllegalArgumentException("a test plan's extension tagged '" + tag + "' no codec reads");
                }
                records.add(read);
            }
            out.put(Integer.parseInt(e.getKey()), records);
        }
        return out;
    }

    private static List<TestPlan.TestStep> readSteps(Json.Arr steps) {
        List<TestPlan.TestStep> out = new ArrayList<>();
        for (Json.Node n : steps.items()) {
            out.add(readStep((Json.Obj) n));
        }
        return out;
    }

    private static TestPlan.TestStep readStep(Json.Obj o) {
        return switch (tag(StepTag.class, o, "test plan step")) {
            case run -> new TestPlan.Run(PlanJson.nodeOf(o.getObj("node")));
            case frameRun -> {
                if (!(PlanJson.nodeOf(o.getObj("check")) instanceof ExecutionPlan.Check c)) {
                    throw new IllegalArgumentException("a frame run's step is a check");
                }
                yield new TestPlan.FrameRun(c);
            }
            case verdicts -> {
                List<TestPlan.Verdict> verdicts = new ArrayList<>();
                for (Json.Node n : o.getArr("verdicts").items()) {
                    Json.Obj vo = (Json.Obj) n;
                    List<TestPlan.Side> base = new ArrayList<>();
                    for (Json.Node sn : vo.getArr("base").items()) {
                        base.add(readSide((Json.Obj) sn));
                    }
                    Json.Obj w = vo.getObjOr("wrapper", null);
                    Json.Obj m = vo.getObjOr("message", null);
                    Json.Obj h = vo.getObjOr("host", null);
                    verdicts.add(new TestPlan.Verdict(vo.getInt("id"), vo.getString("name"), vo.getString("arm"),
                            base, w == null ? null : PlanJson.nodeOf(w), m == null ? null : readSide(m),
                            vo.getBoolOr("raises", false),
                            h == null ? null : HostJson.rHost(h)));
                }
                Json.Obj f = o.getObjOr("fused", null);
                Json.Obj sp = o.getObjOr("savepoint", null);
                Json.Obj roots = o.getObjOr("roots", null);
                Json.Obj agreement = o.getObjOr("agreement", null);
                String rules = o.getString("rules");
                yield new TestPlan.Verdicts(verdicts, f == null ? null : typedResult(f, "a fused verdict statement"),
                        sp == null ? null : new TestPlan.Verdicts.Savepoint(sp.getString("set"),
                                sp.getString("rollbackTo"), sp.getString("release")),
                        switch (rules) {
                            case "lite" -> TestPlan.JudgingRules.LITE;
                            case "compiledPureCompat" -> TestPlan.JudgingRules.COMPILED_PURE_COMPAT;
                            default -> throw new IllegalArgumentException("a verdict group's rules '" + rules
                                    + "': lite or compiledPureCompat");
                        }, roots == null ? null : HostJson.rRootLoop(roots),
                        agreement == null ? null : HostJson.rAgreement(agreement));
            }
            case branch -> new TestPlan.Branch(readSide(o.getObj("condition")), readSteps(o.getArr("then")),
                    readSteps(o.getArr("otherwise")));
        };
    }

    private static ExecutionPlan.TdsResult typedResult(Json.Obj o, String what) {
        if (!(PlanJson.nodeOf(o) instanceof ExecutionPlan.TdsResult t)) {
            throw new IllegalArgumentException(what + " is a typed result, not a " + o.getString("_type"));
        }
        return t;
    }

    /** A verdict's side, or a branch's condition ({@code side}'s inverse). */
    static TestPlan.Side readSide(Json.Obj so) {
        return switch (tag(SideTag.class, so, "verdict side")) {
            case rows -> new TestPlan.Side.Rows(typedResult(so.getObj("result"), "a rows side"));
            case keyedRows -> {
                if (!(PlanJson.nodeOf(so.getObj("keyed")) instanceof ExecutionPlan.Keyed keyed)) {
                    throw new IllegalArgumentException("a keyed side holds a keyed result");
                }
                yield new TestPlan.Side.KeyedRows(keyed);
            }
            case constant -> new TestPlan.Side.Constant(PlanJson.columnsOf(so.getArr("columns")),
                    PlanJson.textCells(so.getArr("cells"), "a constant side"), so.getBool("folded"));
        };
    }
}
