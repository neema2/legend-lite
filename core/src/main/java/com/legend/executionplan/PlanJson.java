// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

import com.legend.executionplan.ExecutionPlan.Format;
import com.legend.executionplan.ExecutionPlan.Multiplicity;
import com.legend.executionplan.ExecutionPlan.Node;
import com.legend.executionplan.ExecutionPlan.Parameter;
import com.legend.executionplan.ExecutionPlan.Relation;
import com.legend.executionplan.ExecutionPlan.ResultType;
import com.legend.executionplan.ExecutionPlan.Sequence;
import com.legend.executionplan.ExecutionPlan.SetupStep;
import com.legend.executionplan.ExecutionPlan.Slot;
import com.legend.executionplan.ExecutionPlan.Sql;
import com.legend.executionplan.ExecutionPlan.Target;
import com.legend.executionplan.ExecutionPlan.TdsResult;
import com.legend.executionplan.ExecutionPlan.TextResult;
import com.legend.executionplan.ExecutionPlan.Value;
import com.legend.json.Json;
import com.legend.model.ConnectionWire;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE LITE PLAN FORMAT: an {@link ExecutionPlan} as JSON and back. Lite's own, clean shape — legend-engine's protocol is a
 * separate, later serialisation of the same plan (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §8). Every {@code _type} is an
 * enum constant shared by the writer and the reader, read by an exhaustive switch; an unknown tag is refused by name. The typed SQL tree is written too ({@link MirJson}, 2026-10-18): a plan read back is as typed as the planner's.
 */
public final class PlanJson {

    /** The format's name and version, the first two fields of every plan. */
    public static final String FORMAT = "legend-lite-plan";
    /** 2 since 2026-10-08: step 2's records (enum values by name, {@code textResult}, setup steps, no identity); 3 since
     *  2026-10-09: the planner's plans (step 2's landing 2) — a target's database (declared or the platform's), its
     *  server versions and session statements, and a text result's columns without a SQL type; 4 since 2026-10-09: the
     *  runner's plans (step 3) — each slot's binding (one value with its null's type and, where the statement names the
     *  value's type, its type hole; or a list as one array); 5 since 2026-10-17: a text result's column's three type
     *  facts -- its multiplicity, the SQL type of its slot, its store's declared type or null -- and the typed SQL
     *  tree, every MIR variant its own tag ({@link MirJson}; docs/PLAN_PATH_PROGRAM_2026_10_17.md §3); 6 since
     *  2026-10-18: a tabular result's columns carry the same three type facts (the plan path program's step 6: the
     *  runner decodes typed rows by them), where they carried a name, a type and a SQL type; 7 since 2026-10-19: a
     *  pivot carries the values a cast over it names ({@code named}, the plan path program's step 9) beside its IN; 8
     *  since 2026-10-20: the keyed result (a dynamic pivot's probe plan: a keys probe, a body with per-key regions, its
     *  fixed and per-key outputs, its snapshot), a statement's per-key regions, a key's cell as a slot's binding and a
     *  MIR leaf, a per-key name as an output origin; the row-as-JSON leaf gone; 9 since 2026-10-20: the keyed result's
     *  cells named as its statements write them (the probe's {@code keyFields}, each output's {@code field}: a session
     *  reading a cell by name, MongoDB's, reads it so); 10 since 2026-10-23: the tagged type (a mixed selection's
     *  winner as a tag and typed slots, docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a); 11 since 2026-10-26: main's write plans
     *  merged in (docs/WRITES_DESIGN_2026_10_18.md §8, leg 7: the {@code update} and {@code check} nodes, main's own
     *  format 5). */
    public static final int VERSION = 11;

    private PlanJson() {
    }

    /** The {@code _type} tags of the lite format, one spelling for the writer and the reader: an exhaustive switch over
     *  each family, an unknown tag refused by name ({@link #tag}). */
    enum NodeTag { sequence, tdsResult, textResult, keyed, update, check }

    enum OutputTag { fixed, perKey }

    enum ResultTypeTag { relation, value }

    enum SetupTag { statement, rows }

    enum BindingTag { one, array, key }

    enum DatabaseTag { declared, platform }

    enum ServersTag { every, versions }

    private static <E extends Enum<E>> E tag(Class<E> family, Json.Obj o, String what) {
        return named(family, o.getString("_type"), what + " _type");
    }

    private static <E extends Enum<E>> E named(Class<E> family, String name, String what) {
        for (E e : family.getEnumConstants()) {
            if (e.name().equals(name)) {
                return e;
            }
        }
        throw new IllegalArgumentException("unknown " + what + " '" + name + "'");
    }

    // ---- write ------------------------------------------------------------------------------------------------------

    public static String write(ExecutionPlan plan) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("format", FORMAT);
        out.put("version", VERSION);
        out.put("parameters", plan.parameters().stream().map(PlanJson::parameter).toList());
        out.put("root", node(plan.root()));
        return Json.toCompact(out);
    }

    private static Map<String, Object> parameter(Parameter p) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("name", p.name());
        o.put("type", p.type());
        o.put("multiplicity", multiplicity(p.multiplicity()));
        o.put("enumValues", p.enumValues());
        return o;
    }

    private static Map<String, Object> multiplicity(Multiplicity m) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("lower", m.lower());
        if (m.upper() != null) {
            o.put("upper", m.upper());
        }
        return o;
    }

    private static Map<String, Object> node(Node n) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (n) {
            case Sequence s -> {
                o.put("_type", NodeTag.sequence.name());
                o.put("steps", s.steps().stream().map(PlanJson::node).toList());
            }
            case TdsResult t -> {
                o.put("_type", NodeTag.tdsResult.name());
                o.put("columns", columns(t.columns()));
                o.put("sql", sql(t.sql()));
            }
            case TextResult x -> {
                o.put("_type", NodeTag.textResult.name());
                o.put("format", x.format().name());
                o.put("type", resultType(x.type()));
                o.put("sql", sql(x.sql()));
            }
            case ExecutionPlan.Keyed k -> {
                o.put("_type", NodeTag.keyed.name());
                o.put("keys", sql(k.keys()));
                o.put("keyFields", new ArrayList<Object>(k.keyFields()));
                o.put("format", k.format() == null ? null : k.format().name());
                List<Object> outputs = new ArrayList<>();
                for (ExecutionPlan.Keyed.Output out : k.outputs()) {
                    Map<String, Object> oo = new LinkedHashMap<>();
                    switch (out) {
                        case ExecutionPlan.Keyed.Fixed f -> {
                            oo.put("_type", OutputTag.fixed.name());
                            oo.put("name", f.name());
                        }
                        case ExecutionPlan.Keyed.PerKey p -> {
                            oo.put("_type", OutputTag.perKey.name());
                            oo.put("name", p.name());
                        }
                    }
                    oo.put("field", out.field());
                    oo.put("column", columns(List.of(out.column())).get(0));
                    outputs.add(oo);
                }
                o.put("outputs", outputs);
                o.put("body", sql(k.body()));
                o.put("snapshot", k.snapshot().name());
                o.put("maxColumns", k.maxColumns());
            }
            case ExecutionPlan.Update u -> {
                o.put("_type", NodeTag.update.name());
                o.put("sql", sql(u.sql()));
                if (u.count() != null) {
                    o.put("count", u.count());
                }
            }
            case ExecutionPlan.Check k -> {
                o.put("_type", NodeTag.check.name());
                o.put("sql", sql(k.sql()));
            }
        }
        return o;
    }

    /** The columns with their three type facts (format 5's relation columns; a tabular result's since format 6). */
    private static List<Object> columns(List<ExecutionPlan.Column> columns) {
        List<Object> cols = new ArrayList<>();
        for (ExecutionPlan.Column c : columns) {
            Map<String, Object> co = new LinkedHashMap<>();
            co.put("name", c.name());
            co.put("type", c.type());
            co.put("multiplicity", multiplicity(c.multiplicity()));
            co.put("slot", c.slot());
            co.put("store", c.store());   // null: computed, no declared type
            cols.add(co);
        }
        return cols;
    }

    private static Map<String, Object> resultType(ResultType type) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (type) {
            case Relation r -> {
                o.put("_type", ResultTypeTag.relation.name());
                o.put("columns", columns(r.columns()));
            }
            case Value v -> {
                o.put("_type", ResultTypeTag.value.name());
                o.put("type", v.type());
                o.put("multiplicity", multiplicity(v.multiplicity()));
            }
        }
        return o;
    }

    private static Map<String, Object> sql(Sql s) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("statement", s.statement());
        List<Object> slots = new ArrayList<>();
        for (Slot slot : s.slots()) {
            Map<String, Object> so = new LinkedHashMap<>();
            so.put("parameter", slot.parameter());
            so.put("binding", binding(slot.binding()));
            slots.add(so);
        }
        o.put("slots", slots);
        if (!s.regions().isEmpty()) {
            List<Object> regions = new ArrayList<>();
            for (ExecutionPlan.Region r : s.regions()) {
                Map<String, Object> ro = new LinkedHashMap<>();
                ro.put("start", r.start());
                ro.put("end", r.end());
                ro.put("lead", r.lead());
                ro.put("separator", r.separator());
                ro.put("empty", r.empty());
                ro.put("firstSlot", r.firstSlot());
                ro.put("slotCount", r.slotCount());
                ro.put("counters", r.counters());
                regions.add(ro);
            }
            o.put("regions", regions);
        }
        o.put("target", target(s.target()));
        // the typed SQL tree (MirJson; the plan path program's step 1b): a plan read back is as typed as the planner's
        o.put("tree", s.tree() == null ? null : MirJson.query(s.tree()));
        return o;
    }

    private static Map<String, Object> binding(ExecutionPlan.Binding binding) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (binding) {
            case ExecutionPlan.Binding.One one -> {
                o.put("_type", BindingTag.one.name());
                o.put("nullType", one.nullType());
                ExecutionPlan.TypeHole hole = one.hole();
                if (hole != null) {
                    Map<String, Object> h = new LinkedHashMap<>();
                    h.put("at", hole.at());
                    Map<String, Object> types = new LinkedHashMap<>();
                    for (ExecutionPlan.ValueKind kind : ExecutionPlan.ValueKind.values()) {
                        ExecutionPlan.TypeSpelling t = hole.types().get(kind);
                        if (t != null) {
                            Map<String, Object> spelling = new LinkedHashMap<>();
                            spelling.put("name", t.name());
                            spelling.put("digits", t.digits().name());
                            if (t.fraction() != null) {
                                spelling.put("fraction", t.fraction());
                            }
                            types.put(kind.name(), spelling);
                        }
                    }
                    h.put("types", types);
                    h.put("absent", hole.absent().name());
                    o.put("typeHole", h);
                }
            }
            case ExecutionPlan.Binding.Array array -> {
                o.put("_type", BindingTag.array.name());
                o.put("elementSqlType", array.elementSqlType());
            }
            case ExecutionPlan.Binding.Key key -> {
                o.put("_type", BindingTag.key.name());
                o.put("cell", key.cell());
            }
        }
        return o;
    }

    /** {@code target} in the lite format, alone: an unambiguous spelling of its whole content (a runner keys a shared
     *  database by it, decision A). */
    public static String writeTarget(Target target) {
        return Json.toCompact(target(target));
    }

    private static Map<String, Object> target(Target t) {
        Map<String, Object> to = new LinkedHashMap<>();
        to.put("database", database(t.database()));
        to.put("servers", servers(t.servers()));
        to.put("session", t.session());
        to.put("setup", t.setup().stream().map(PlanJson::setupStep).toList());
        return to;
    }

    private static Map<String, Object> database(ExecutionPlan.Database database) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (database) {
            case ExecutionPlan.Database.Declared d -> {
                o.put("_type", DatabaseTag.declared.name());
                o.put("connection", ConnectionWire.write(d.connection()));
            }
            case ExecutionPlan.Database.Platform p -> {
                o.put("_type", DatabaseTag.platform.name());
                o.put("databaseType", p.type().name());
            }
        }
        return o;
    }

    private static Map<String, Object> servers(ExecutionPlan.Servers servers) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (servers) {
            case ExecutionPlan.Servers.Every e -> o.put("_type", ServersTag.every.name());
            case ExecutionPlan.Servers.Versions v -> {
                o.put("_type", ServersTag.versions.name());
                o.put("prefixes", v.prefixes());
            }
        }
        return o;
    }

    private static Map<String, Object> setupStep(SetupStep step) {
        Map<String, Object> o = new LinkedHashMap<>();
        switch (step) {
            case SetupStep.Statement s -> {
                o.put("_type", SetupTag.statement.name());
                o.put("sql", s.sql());
            }
            case SetupStep.Rows r -> {
                o.put("_type", SetupTag.rows.name());
                o.put("stagingTable", r.stagingTable());
                o.put("createStaging", r.createStaging());
                o.put("copy", r.copy());
                o.put("dropStaging", r.dropStaging());
                o.put("rows", r.rows());
            }
        }
        return o;
    }

    // ---- read -------------------------------------------------------------------------------------------------------

    /** How deep a plan's JSON may nest: its typed SQL tree nests as the query does (a class instance's text, spelled
     *  over its layout, nests a CASE and a CONCAT per property per level), far past the reader's default of 64, which
     *  guards a server against hostile input; a plan is the planner's own text. */
    static final int MAX_DEPTH = 4096;

    public static ExecutionPlan read(String json) {
        if (!(Json.parse(json, new Json.Config(MAX_DEPTH)) instanceof Json.Obj o)) {
            throw new IllegalArgumentException("a plan's JSON is an object");
        }
        String format = o.getStringOr("format", null);
        if (!FORMAT.equals(format) || o.getIntOr("version", -1) != VERSION) {
            throw new IllegalArgumentException("not a " + FORMAT + " v" + VERSION + " plan (format " + format
                    + ", version " + o.getIntOr("version", -1) + ")");
        }
        List<Parameter> parameters = new ArrayList<>();
        for (Json.Node n : o.getArr("parameters").items()) {
            parameters.add(readParameter((Json.Obj) n));
        }
        return new ExecutionPlan(parameters, readNode(o.getObj("root")));
    }

    private static Parameter readParameter(Json.Obj o) {
        return new Parameter(o.getString("name"), o.getString("type"), readMultiplicity(o.getObj("multiplicity")),
                o.getStringArray("enumValues"));
    }

    private static Multiplicity readMultiplicity(Json.Obj o) {
        return new Multiplicity(o.getInt("lower"), o.has("upper") ? o.getInt("upper") : null);
    }

    private static Node readNode(Json.Obj o) {
        return switch (tag(NodeTag.class, o, "plan node")) {
            case sequence -> {
                List<Node> steps = new ArrayList<>();
                for (Json.Node n : o.getArr("steps").items()) {
                    steps.add(readNode((Json.Obj) n));
                }
                yield new Sequence(steps);
            }
            case tdsResult -> new TdsResult(readRelationColumns(o.getArr("columns")), readSql(o.getObj("sql")));
            case textResult -> new TextResult(named(Format.class, o.getString("format"), "text result format"),
                    readResultType(o.getObj("type")), readSql(o.getObj("sql")));
            case keyed -> {
                List<ExecutionPlan.Keyed.Output> outputs = new ArrayList<>();
                for (Json.Node n : o.getArr("outputs").items()) {
                    Json.Obj oo = (Json.Obj) n;
                    ExecutionPlan.Column column = readRelationColumns(new Json.Arr(List.of(oo.getObj("column")))).get(0);
                    outputs.add(switch (tag(OutputTag.class, oo, "keyed output")) {
                        case fixed -> new ExecutionPlan.Keyed.Fixed(column, oo.getString("name"),
                                oo.getString("field"));
                        case perKey -> new ExecutionPlan.Keyed.PerKey(column, oo.getInt("name"), oo.getString("field"));
                    });
                }
                List<String> keyFields = new ArrayList<>();
                for (Json.Node n : o.getArr("keyFields").items()) {
                    keyFields.add(((Json.Str) n).value());
                }
                String format = o.getStringOr("format", null);
                yield new ExecutionPlan.Keyed(readSql(o.getObj("keys")), keyFields,
                        format == null ? null : named(Format.class, format, "keyed result format"), outputs,
                        readSql(o.getObj("body")),
                        named(ExecutionPlan.Keyed.Snapshot.class, o.getString("snapshot"), "keyed snapshot"),
                        o.getOr("maxColumns", null) == null ? null : o.getInt("maxColumns"));
            }
            case update -> new ExecutionPlan.Update(readSql(o.getObj("sql")), o.has("count") ? o.getString("count")
                    : null);
            case check -> new ExecutionPlan.Check(readSql(o.getObj("sql")));
        };
    }

    private static List<ExecutionPlan.Column> readRelationColumns(Json.Arr columns) {
        List<ExecutionPlan.Column> cols = new ArrayList<>();
        for (Json.Node n : columns.items()) {
            Json.Obj c = (Json.Obj) n;
            cols.add(new ExecutionPlan.Column(c.getString("name"), c.getString("type"),
                    readMultiplicity(c.getObj("multiplicity")), c.getStringOr("slot", null), c.getStringOr("store", null)));
        }
        return cols;
    }

    private static ResultType readResultType(Json.Obj o) {
        return switch (tag(ResultTypeTag.class, o, "result type")) {
            case relation -> new Relation(readRelationColumns(o.getArr("columns")));
            case value -> new Value(o.getString("type"), readMultiplicity(o.getObj("multiplicity")));
        };
    }

    private static ExecutionPlan.Binding readBinding(Json.Obj o) {
        return switch (tag(BindingTag.class, o, "binding")) {
            case one -> {
                Json.Obj h = o.getObjOr("typeHole", null);
                ExecutionPlan.TypeHole hole = null;
                if (h != null) {
                    Map<ExecutionPlan.ValueKind, ExecutionPlan.TypeSpelling> types =
                            new java.util.EnumMap<>(ExecutionPlan.ValueKind.class);
                    for (Map.Entry<String, Json.Node> e : h.getObj("types").fields().entrySet()) {
                        Json.Obj t = (Json.Obj) e.getValue();
                        types.put(named(ExecutionPlan.ValueKind.class, e.getKey(), "value kind"),
                                new ExecutionPlan.TypeSpelling(t.getString("name"),
                                        named(ExecutionPlan.Digits.class, t.getString("digits"), "type digits"),
                                        t.getOr("fraction", null) == null ? null : t.getInt("fraction")));
                    }
                    hole = new ExecutionPlan.TypeHole(h.getInt("at"), types,
                            named(ExecutionPlan.ValueKind.class, h.getString("absent"), "absent value kind"));
                }
                yield new ExecutionPlan.Binding.One(o.getString("nullType"), hole);
            }
            case array -> new ExecutionPlan.Binding.Array(o.getString("elementSqlType"));
            case key -> new ExecutionPlan.Binding.Key(o.getInt("cell"));
        };
    }

    private static Sql readSql(Json.Obj o) {
        List<Slot> slots = new ArrayList<>();
        for (Json.Node n : o.getArr("slots").items()) {
            Json.Obj s = (Json.Obj) n;
            slots.add(new Slot(s.getString("parameter"), readBinding(s.getObj("binding"))));
        }
        Json.Obj t = o.getObj("target");
        List<SetupStep> setup = new ArrayList<>();
        for (Json.Node n : t.getArr("setup").items()) {
            setup.add(readSetupStep((Json.Obj) n));
        }
        Json.Node tree = o.getOr("tree", null);
        List<ExecutionPlan.Region> regions = new ArrayList<>();
        Json.Node rs = o.getOr("regions", null);
        if (rs != null) {
            for (Json.Node n : ((Json.Arr) rs).items()) {
                Json.Obj r = (Json.Obj) n;
                List<Integer> counters = new ArrayList<>();
                for (Json.Node c : r.getArr("counters").items()) {
                    counters.add(Math.toIntExact(((Json.Num) c).longValue()));
                }
                regions.add(new ExecutionPlan.Region(r.getInt("start"), r.getInt("end"), r.getString("lead"),
                        r.getString("separator"), r.getStringOr("empty", null), r.getInt("firstSlot"),
                        r.getInt("slotCount"), counters));
            }
        }
        return new Sql(o.getString("statement"), slots, new Target(readDatabase(t.getObj("database")),
                readServers(t.getObj("servers")), t.getStringArray("session"), setup),
                tree == null ? null : MirJson.readQuery((Json.Obj) tree), regions);
    }

    private static ExecutionPlan.Database readDatabase(Json.Obj o) {
        return switch (tag(DatabaseTag.class, o, "database")) {
            case declared -> new ExecutionPlan.Database.Declared(ConnectionWire.read(fields(o.getObj("connection"))));
            case platform -> new ExecutionPlan.Database.Platform(
                    ConnectionWire.databaseType(o.getString("databaseType")));
        };
    }

    private static ExecutionPlan.Servers readServers(Json.Obj o) {
        return switch (tag(ServersTag.class, o, "servers")) {
            case every -> new ExecutionPlan.Servers.Every();
            case versions -> new ExecutionPlan.Servers.Versions(o.getStringArray("prefixes"));
        };
    }

    private static SetupStep readSetupStep(Json.Obj o) {
        return switch (tag(SetupTag.class, o, "setup step")) {
            case statement -> new SetupStep.Statement(o.getString("sql"));
            case rows -> {
                List<List<String>> rows = new ArrayList<>();
                for (Json.Node r : o.getArr("rows").items()) {
                    if (!(r instanceof Json.Arr cells)) {
                        throw new IllegalArgumentException("rows for " + o.getString("stagingTable")
                                + ": a row is an array of cells");
                    }
                    List<String> row = new ArrayList<>();
                    for (Json.Node cell : cells.items()) {
                        row.add(switch (cell) {
                            case Json.Str s -> s.value();
                            case Json.Null x -> null;
                            case Json.Num x -> throw notTextCell(o);
                            case Json.Bool x -> throw notTextCell(o);
                            case Json.Obj x -> throw notTextCell(o);
                            case Json.Arr x -> throw notTextCell(o);
                        });
                    }
                    rows.add(row);
                }
                yield new SetupStep.Rows(o.getString("stagingTable"), o.getString("createStaging"), o.getString("copy"),
                        o.getString("dropStaging"), rows);
            }
        };
    }

    private static IllegalArgumentException notTextCell(Json.Obj rows) {
        return new IllegalArgumentException("rows for " + rows.getString("stagingTable")
                + ": a cell is text or null (the database types it)");
    }

    /** {@code o} as the fields a model record reads ({@link ConnectionWire}: the model depends on no JSON). */
    private static ConnectionWire.Fields fields(Json.Obj o) {
        return new ConnectionWire.Fields() {
            @Override
            public String string(String key) {
                return o.getString(key);
            }

            @Override
            public @com.legend.base.Nullable String optionalString(String key) {
                return o.getStringOr(key, null);
            }

            @Override
            public int integer(String key) {
                return o.getInt(key);
            }

            @Override
            public boolean bool(String key) {
                return o.getBool(key);
            }

            @Override
            public boolean has(String key) {
                return o.has(key);
            }

            @Override
            public java.util.List<String> strings(String key) {
                return o.getStringArray(key);
            }

            @Override
            public ConnectionWire.Fields object(String key) {
                return fields(o.getObj(key));
            }
        };
    }
}
