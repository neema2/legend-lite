// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.DialectCapability;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A DEPLOYMENT'S PLAN FOR A DEEPHAVEN SERVER, AS DATA (docs/DEPLOYMENT_DESIGN_2026_11_14.md §9; G4: the plan is data,
 * never code): each table the server lands -- its topic, the topic's brokers and format, its columns with the Java
 * types a Deephaven column holds them as (DH16's), and how it keeps its rows -- for lite's one fixed, versioned loader
 * ({@code core/src/main/deephaven/legend-loader.groovy}, {@link #VERSION}), which reads it and makes each table. It
 * replaced the generated Groovy startup program (the user, 2026-11-15: "putting groovy code in the plan which does not
 * seem like the right way forward").
 *
 * <p>What Deephaven can land is decided here, the dialect's part: a table name, a column name, a key and a delete
 * field are identifiers; a topic is a Kafka topic's name; the brokers a list of {@code host:port}; the format JSON (an
 * Avro topic is refused by name until production's registry lands); an appended table keeps its latest rows (a ring)
 * or all of them, a span of time refused by name; a milestoned table keeps its current version a key (each key's last
 * record, its milestoning columns filled: §5's live column), its delete indicator's values holding no quote. So the
 * loader never meets a name or a value it would have to check.
 */
public final class DeephavenLoad {

    private DeephavenLoad() {
    }

    /** The loader version a plan is read by: the loader refuses a plan of any other. */
    public static final int VERSION = 1;

    /** What a plan names itself, so the loader refuses any other document. */
    public static final String KIND = "legend.deephaven.load";

    /** A live batch id's partition prefix: {@code partition × 10^12 + offset}, one partition's offsets below it (§5). */
    public static final long PARTITION_PREFIX = 1_000_000_000_000L;

    /** A column of a table: its name and the Java type a Deephaven column holds it as (DH16's). */
    public record Column(String name, String type) {
    }

    /** How a table keeps its rows: an appended table's latest {@code rows} ({@link Rows}) or all of them
     *  ({@link All}); a milestoned table's current version a key ({@link Current}). */
    public sealed interface Keep permits Rows, All, Current {
    }

    /** {@code LastRows(n)}: a ring of n rows. */
    public record Rows(long rows) implements Keep {
    }

    /** {@code all}: every row appended. */
    public record All() implements Keep {
    }

    /** {@code current}: each key's latest record, its milestoning columns filled, its delete indicator's keys gone. */
    public record Current(List<String> key, HostOperation.Milestones milestones,
            HostOperation.@com.legend.base.Nullable DeleteWhen delete) implements Keep {
        public Current {
            key = List.copyOf(key);
        }
    }

    /** One table the server lands: its name, its topic, the brokers and format the topic is read with, its columns, how it
     *  keeps its rows. */
    public record Table(String name, String topic, String brokers, String format, List<Column> columns, Keep keep) {
        public Table {
            columns = List.copyOf(columns);
        }
    }

    /** A checked plan: the deployment it renders and its tables, in their order. */
    public record Plan(String deployment, List<Table> tables) {
        public Plan {
            tables = List.copyOf(tables);
        }

        /** The plan as the loader reads it: maps, lists, strings and numbers (the boundary writes it as JSON). */
        public Map<String, Object> data() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("kind", KIND);
            m.put("version", VERSION);
            m.put("deployment", deployment);
            List<Object> ts = new ArrayList<>();
            for (Table t : tables) {
                ts.add(table(t));
            }
            m.put("tables", ts);
            return m;
        }

        private static Map<String, Object> table(Table t) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", t.name());
            m.put("topic", t.topic());
            m.put("brokers", t.brokers());
            m.put("format", t.format());
            List<Object> cs = new ArrayList<>();
            for (Column c : t.columns()) {
                cs.add(Map.of("name", c.name(), "type", c.type()));
            }
            m.put("columns", cs);
            Map<String, Object> keep = new LinkedHashMap<>();
            switch (t.keep()) {
                case Rows r -> {
                    keep.put("kind", "rows");
                    keep.put("rows", r.rows());
                }
                case All a -> keep.put("kind", "all");
                case Current c -> {
                    keep.put("kind", "current");
                    keep.put("key", c.key());
                    HostOperation.Milestones ms = c.milestones();
                    Map<String, Object> mm = new LinkedHashMap<>();
                    mm.put("digest", ms.digest());
                    mm.put("batchIn", ms.batchIn());
                    mm.put("batchOut", ms.batchOut());
                    mm.put("openBatch", ms.openBatch());
                    mm.put("partitionPrefix", PARTITION_PREFIX);
                    keep.put("milestones", mm);
                    HostOperation.DeleteWhen d = c.delete();
                    if (d != null) {
                        keep.put("delete", Map.of("field", d.field(), "values", d.values()));
                    }
                }
            }
            m.put("keep", keep);
            return m;
        }
    }

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern TOPIC = Pattern.compile("[A-Za-z0-9._-]{1,249}");
    private static final Pattern SERVERS = Pattern.compile("[A-Za-z0-9._:,-]+");

    /** The Java types a topic's JSON is read as (the loader's {@code READ}): a day read as text, made a date there. */
    private static final java.util.Set<String> READ = java.util.Set.of("java.lang.String", "java.time.LocalDate",
            "double", "float", "int", "long", "short", "byte", "java.lang.Boolean", "java.time.Instant");

    /** {@code tables} checked as what Deephaven lands, as {@code deployment}'s plan; refused by name where it cannot. */
    public static Plan plan(String deployment, List<Table> tables) {
        java.util.Set<String> names = new java.util.HashSet<>();
        for (Table t : tables) {
            check(t);
            if (!names.add(t.name())) {
                throw new DialectCapability("the table " + t.name() + " is landed twice on one Deephaven server");
            }
        }
        return new Plan(deployment, tables);
    }

    private static void check(Table t) {
        String where = "the table " + t.name() + " (topic " + t.topic() + ")";
        name(t.name(), where);
        if (!TOPIC.matcher(t.topic()).matches()) {
            throw new DialectCapability(where + ": '" + t.topic() + "' is no Kafka topic's name");
        }
        if (!SERVERS.matcher(t.brokers()).matches()) {
            throw new DialectCapability(where + ": '" + t.brokers() + "' is no list of brokers (host:port, ...)");
        }
        if (!"JSON".equals(t.format())) {
            throw new DialectCapability(where + " is read from the " + t.format() + " topic: only a JSON topic is"
                    + " landed on Deephaven yet");
        }
        for (Column c : t.columns()) {
            name(c.name(), where + "'s column");
            if (!READ.contains(c.type())) {
                throw new DialectCapability(where + ": its column " + c.name() + " is held as " + c.type()
                        + ", which no topic is read as yet");
            }
        }
        if (t.keep() instanceof Current c) {
            for (String k : c.key()) {
                name(k, where + "'s key");
            }
            HostOperation.Milestones m = c.milestones();
            for (String n : List.of(m.digest(), m.batchIn(), m.batchOut())) {
                name(n, where + "'s milestoning column");
            }
            HostOperation.DeleteWhen d = c.delete();
            if (d != null) {
                name(d.field(), where + "'s delete indicator");
                for (String v : d.values()) {
                    if (v.indexOf('`') >= 0 || v.indexOf('\'') >= 0 || v.indexOf('"') >= 0 || v.indexOf('\\') >= 0) {
                        throw new DialectCapability(where + ": the delete value '" + v + "' holds a quote");
                    }
                }
            }
        }
        if (t.keep() instanceof Rows r && (r.rows() < 1 || r.rows() > Integer.MAX_VALUE)) {
            throw new DialectCapability(where + " keeps LastRows(" + r.rows() + "): a ring holds 1 to "
                    + Integer.MAX_VALUE + " rows");
        }
    }

    private static void name(String name, String where) {
        if (!IDENTIFIER.matcher(name).matches()) {
            throw new DialectCapability(where + ": '" + name + "' is no name a Deephaven table or column takes here");
        }
    }
}
