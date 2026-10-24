// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testcases;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.regex.Pattern;

/**
 * The model a dumped graph implies, and its tables (docs/CYPHER_STUDY_2026_10_15.md §8.2): a class per label, a
 * property per key typed by its values, a relationship type as an association between the two classes it joins
 * (its ends named by the type, {@code T} on the start class and {@code T_in} on the end class), realized as a
 * foreign key on the to-one side or a junction table, every node a row of its class's table. A relationship type
 * whose relationships carry properties is an edge class (the study's §5.6, the association form): a class
 * {@code E_T} marked {@code <<graph::graph.Edge>>} with the properties, its type spelled by a
 * {@code <<graph::graph.type>>} derived property as a value of the enumeration {@code RelationshipType}, its ends
 * two associations ({@code <<graph::graph.from>> from} with the start class's {@code T_out}, {@code <<graph::graph.to>>
 * to} with the end class's {@code T_in}), over a bridge table {@code r_t} (one row per relationship, its id the
 * key). The lanes seed the tables from the dump and never interpret a {@code CREATE}. What the inference cannot
 * model yet is said by name ({@link Unsupported}), and the case is SKIP in the ledger with that reason (a property
 * typed two ways across a label's nodes, a stored zoned or nanosecond value). A node of several labels is an
 * instance of an anonymous class per label set extending each label's class (the study's §12.61); a relationship
 * type between several class pairs is one edge class per pair (§5.6).
 */
public final class CypherGraph {

    private CypherGraph() {
    }

    public static final String DATABASE = "g::DB";
    public static final String MAPPING = "g::Map";
    public static final String RUNTIME = "g::RT";
    /** The graph a lane's island reads with {@code USE} (the study's §12.45): the DataSpace of every class the
     *  dump implies, marked {@code <<graph::graph.Open>>} (a Neo4j graph is open). */
    public static final String DATASPACE = "g::Graph";

    /** A graph the harness cannot model yet, and why. */
    public static final class Unsupported extends Exception {
        public Unsupported(String reason) {
            super(reason);
        }
    }

    /** The model's text (classes, associations, the database, the mapping; the lane appends the connection and
     *  the runtime), its tables with their rows, and a key over the dump (two cases with one graph seed once); for
     *  the native realization, the native store that holds the graph instead ({@link Native}). */
    public record Inferred(String model, List<SqlParity.Table> tables, String graphKey,
            @com.legend.base.Nullable Native nativeStore) {
        public Inferred(String model, List<SqlParity.Table> tables, String graphKey) {
            this(model, tables, graphKey, null);
        }
    }

    /** How the model's tables hold the graph (docs/WRITES_DESIGN_2026_10_18.md §3): a table per class, its columns
     *  the properties ({@link #DOMAIN}, graph A, a domain model's own tables), or the native store's fixed tables
     *  ({@link #NATIVE}, graph B: a node and its label set, a property per row, a relationship and its type, a
     *  relationship's property per row), the model's tables views over them. */
    public enum Realization { DOMAIN, NATIVE }

    /**
     * The native store (graph B) of a graph: its four tables with their rows and the views that read them as the
     * model's tables, so the model, its mapping and the island are the domain realization's own.
     * <ul>
     *   <li>{@code nodes (id, labels)}: a node and its label set, as one text ({@link #labelText});</li>
     *   <li>{@code node_props (node_id, key, slot, v_int, v_float, v_str, v_bool, v_date, v_ts, v_time, v_json)}: a
     *       property's value, one row per slot of it (a scalar's slot {@code ''}; a duration's, a zoned or a
     *       nanosecond value's, its parts), in the column of the slot's type, and a scalar's JSON beside it (what a
     *       property typed two ways reads);</li>
     *   <li>{@code rels (id, type, src_id, dst_id)} and {@code rel_props (rel_id, ...)}, alike.</li>
     * </ul>
     * Every relationship type is a junction (or an edge class's bridge): a foreign key holds one end, which writes
     * would outgrow.
     */
    public record Native(List<SqlParity.Table> tables, List<String> views, List<String> viewNames) {
    }

    /** The native store's label-set text: the labels sorted, each closed by {@code :} after an opening one
     *  ({@code :A:B:}); no label is {@code :}. */
    public static String labelText(java.util.Collection<String> labels) {
        java.util.TreeSet<String> sorted = new java.util.TreeSet<>(labels);
        return sorted.isEmpty() ? ":" : ":" + String.join(":", sorted) + ":";
    }

    /** The test that a native store's label-set text ({@code column}) is the set {@code labels}: each label's
     *  {@code :L:} in it and its length the set's text's, so a text the island wrote in another order (a label set
     *  appends, the writes design's §8 leg 6) is the same set; no label is {@code :}. */
    static String labelTest(String column, java.util.Collection<String> labels) {
        java.util.TreeSet<String> sorted = new java.util.TreeSet<>(labels);
        if (sorted.isEmpty()) {
            return column + " = ':'";
        }
        StringBuilder test = new StringBuilder();
        for (String l : sorted) {
            test.append(column).append(" LIKE '%:").append(l.replace("'", "''")).append(":%' AND ");
        }
        return test.append("length(").append(column).append(") = ").append(labelText(sorted).length()).toString();
    }

    /** The native store's tables, by their declared columns. */
    private static final List<SqlParity.Column> NODE_COLUMNS = List.of(new SqlParity.Column("id", "BIGINT"),
            new SqlParity.Column("labels", "VARCHAR(4000)"));
    private static final List<SqlParity.Column> REL_COLUMNS = List.of(new SqlParity.Column("id", "BIGINT"),
            new SqlParity.Column("type", "VARCHAR(4000)"), new SqlParity.Column("src_id", "BIGINT"),
            new SqlParity.Column("dst_id", "BIGINT"));
    /** A property row's value columns, by the SQL type of the slot they hold. */
    private static final List<String[]> VALUE_COLUMNS = List.of(new String[] {"v_int", "BIGINT"},
            new String[] {"v_float", "DOUBLE PRECISION"}, new String[] {"v_str", "VARCHAR(4000)"},
            new String[] {"v_bool", "BOOLEAN"}, new String[] {"v_date", "DATE"}, new String[] {"v_ts", "TIMESTAMP"},
            new String[] {"v_time", "TIME"}, new String[] {"v_json", "JSONB"});

    private static List<SqlParity.Column> propColumns(String owner) {
        List<SqlParity.Column> out = new ArrayList<>(List.of(new SqlParity.Column(owner, "BIGINT"),
                new SqlParity.Column("key", "VARCHAR(4000)"), new SqlParity.Column("slot", "VARCHAR(4000)")));
        VALUE_COLUMNS.forEach(v -> out.add(new SqlParity.Column(v[0], v[1])));
        return out;
    }

    /** The value column a slot of SQL type {@code sqlType} is held in. */
    private static String valueColumn(String sqlType) {
        for (String[] v : VALUE_COLUMNS) {
            if (v[1].equals(sqlType)) {
                return v[0];
            }
        }
        throw new IllegalStateException("no native value column for " + sqlType);
    }

    /** A column of a model table and the property slot it reads: its property's key, its slot ({@code ''} for a
     *  scalar), its SQL type. */
    private record Provenance(String column, String key, String slot, String sqlType) {
    }

    /** The native store's property rows of one element ({@code owner} its id) from its model row: each cell a row of
     *  its slot, in the column of the slot's type, a scalar's JSON beside it. */
    private static void propRows(long owner, List<Object> row, int first, List<Provenance> provenance,
            List<List<Object>> out) {
        for (int i = 0; i < provenance.size(); i++) {
            Object cell = row.get(first + i);
            if (cell == null) {
                continue;
            }
            Provenance pv = provenance.get(i);
            List<Object> prop = new ArrayList<>(java.util.Collections.nCopies(3 + VALUE_COLUMNS.size(), null));
            prop.set(0, owner);
            prop.set(1, pv.key());
            prop.set(2, pv.slot());
            String column = valueColumn(pv.sqlType());
            for (int v = 0; v < VALUE_COLUMNS.size(); v++) {
                if (VALUE_COLUMNS.get(v)[0].equals(column)) {
                    prop.set(3 + v, cell);
                }
            }
            if (pv.slot().isEmpty() && !column.equals("v_json")) {
                String json = switch (cell) {
                    case Long l -> l.toString();
                    case Double d -> Double.isNaN(d) || Double.isInfinite(d) ? null : Double.toString(d);
                    case Boolean b -> b.toString();
                    case String str when column.equals("v_str") -> com.legend.json.Json.toCompact(str);
                    default -> null;
                };
                prop.set(3 + VALUE_COLUMNS.size() - 1, json);
            }
            out.add(prop);
        }
    }

    /** The view over the native store that reads as the model table {@code table}: {@code from} the elements it
     *  holds, each provenance column a scalar read of its slot's row. */
    private static String view(String table, String head, String propTable, String owner,
            List<Provenance> provenance, String from) {
        StringBuilder sql = new StringBuilder("CREATE VIEW " + table + " AS SELECT " + head);
        for (Provenance pv : provenance) {
            sql.append(", (SELECT p.").append(valueColumn(pv.sqlType())).append(" FROM ").append(propTable)
                    .append(" p WHERE p.").append(owner).append(" = e.id AND p.key = '").append(pv.key())
                    .append("' AND p.slot = '").append(pv.slot()).append("') AS ").append(pv.column());
        }
        return sql.append(' ').append(from).toString();
    }

    /** The slots of an embedded value of {@code type}, as {@link #embeddedColumns} names its columns. */
    private static List<String> embeddedSlots(String type, boolean[] zonedNanos, boolean zonedRegion) {
        List<String> slotNames = new ArrayList<>(slotsOf(type, zonedNanos));
        if (type.equals(ZONED_DATETIME) && zonedRegion) {
            slotNames.add(ZONE_SLOT);
        }
        return slotNames;
    }

    /** A zoned time's slots in the native store, spelled apart from a zoned date-time's (the writes design's leg
     *  3 layout, 2026-10-20): the store says which a property's value is, as the oracle's graph does. */
    static final String TIME_SLOT_PREFIX = "time:";

    /** The native store's name of a value class's slot: a zoned time's prefixed ({@link #TIME_SLOT_PREFIX}), any
     *  other's its own. */
    private static String storeSlot(String type, String slot) {
        return type.equals(ZONED_TIME) ? TIME_SLOT_PREFIX + slot : slot;
    }

    private static String slotSqlType(String slot) {
        return slot.equals(ZONE_SLOT) ? "VARCHAR(4000)" : slot.equals("date") ? "DATE"
                : slot.equals("instant") ? "TIMESTAMP" : "BIGINT";
    }

    /** The provenance of {@code property}'s columns ({@code column} its scalar column, or each slot's). */
    private static void provenance(String property, String column, String type, boolean[] zonedNanos,
            boolean zonedRegion, List<Provenance> out) {
        if (embeddedValue(type)) {
            for (String slot : embeddedSlots(type, zonedNanos, zonedRegion)) {
                out.add(new Provenance(column + "_" + slot.toLowerCase(Locale.ROOT), property,
                        storeSlot(type, slot), slotSqlType(slot)));
            }
            return;
        }
        out.add(new Provenance(column, property, "", sqlType(type)));
    }

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** The class of the nodes without a label: anonymous (the study's §12.61), so the island reads no label on
     *  its instances. */
    private static final String UNLABELLED = "Unlabelled";

    private static final class Cls {
        final String name;
        final String table;
        /** Whether the class's name is no label ({@code <<graph.Anonymous>>}: the class of a label set, or of the
         *  unlabelled nodes). */
        final boolean anonymous;
        /** The label classes an anonymous label-set class extends (none for a label's own class). */
        final List<String> supers;
        final Map<String, String> propertyTypes = new TreeMap<>();
        /** A Variant property's graph profile list mark ({@link #listMark}), "" for none, by property. */
        final Map<String, String> propertyMarks = new TreeMap<>();
        final Set<String> keys = new HashSet<>();
        final List<CypherAnswers.GraphNode> nodes = new ArrayList<>();
        final Map<String, String> fkColumns = new LinkedHashMap<>();   // end name -> column
        final List<String> ends = new ArrayList<>();                   // "T: g::Dst[0..1];"

        Cls(String name, boolean anonymous, List<String> supers) {
            this.name = name;
            this.anonymous = anonymous;
            this.supers = List.copyOf(supers);
            this.table = "n_" + name.toLowerCase(Locale.ROOT);
        }
    }

    /** A relationship type between one class pair: a type joining several pairs is several of these, one edge class
     *  each (the study's §5.6: one Neo4j type may join several label pairs, and the endpoint labels choose), numbered
     *  in the order the pairs appear. */
    private static final class RelType {
        final String name;
        /** The name its tables and columns are spelled by: {@link #name} in lower case (the databases fold an unquoted
         *  name's case), with a suffix where another type's name differs from it only in case. */
        final String sql;
        final Cls src;
        final Cls dst;
        final List<CypherAnswers.GraphRel> rels = new ArrayList<>();
        /** The relationships' properties, typed by their values: an edge class when any. */
        final Map<String, String> propertyTypes = new TreeMap<>();
        /** A Variant property's graph profile list mark ({@link #listMark}), "" for none, by property. */
        final Map<String, String> propertyMarks = new TreeMap<>();
        /** 0 for the one pair of its type; 1, 2, ... for a type joining several pairs (always edge classes). */
        int pair;
        /** Held by the native store, where a relationship is a row of its own and so has an identity of its own:
         *  always an edge class (the writes design's §8, leg 6: a write names the relationship it writes). */
        boolean ownRow;

        RelType(String name, String sql, Cls src, Cls dst) {
            this.name = name;
            this.sql = sql;
            this.src = src;
            this.dst = dst;
        }

        boolean edge() {
            return !propertyTypes.isEmpty() || pair > 0 || ownRow;
        }

        /** The type's name with the pair's number: what the edge class, its table, its ends and joins are named by,
         *  apart from the classes and the other types (a label may be spelled like a relationship type). */
        String stem() {
            return pair == 0 ? name : name + "_" + pair;
        }

        String edgeClass() {
            return "g::E_" + stem();
        }

        String table() {
            return "r_" + (pair == 0 ? sql : sql + "_" + pair);
        }

        String outEnd() {
            return stem() + "_out";
        }

        String inEnd() {
            return stem() + "_in";
        }
    }

    /** The enumeration of the edge classes' relationship types (the study's §5.6: a typed constant). */
    private static final String RELATIONSHIP_TYPE = "g::RelationshipType";

    /** The property-graph profile, as projects/graph/model.pure declares it (inlined: the model is one text). */
    private static final String GRAPH_PROFILE = "Profile graph::graph\n{\n    stereotypes: [Anonymous, Edge, Open, Native, from,"
            + " to, type, nanoseconds, timeOfDay, listOfDate, listOfLocalTime, listOfZonedTime, listOfLocalDateTime,"
            + " listOfZonedDateTime, listOfDuration];\n}\n";

    /** The native store's tables as the model's database declares them (the island writes them: the writes
     *  design's §8, leg 6), in the model's column types. */
    private static final String NATIVE_DDL = "    Table nodes (id BIGINT PRIMARY KEY, labels VARCHAR(4000))\n"
            + "    Table node_props (node_id BIGINT, key VARCHAR(4000), slot VARCHAR(4000), v_int BIGINT, v_float DOUBLE,"
            + " v_str VARCHAR(4000), v_bool BIT, v_date DATE, v_ts TIMESTAMP, v_time TIME, v_json SEMISTRUCTURED)\n"
            + "    Table rels (id BIGINT PRIMARY KEY, type VARCHAR(4000), src_id BIGINT, dst_id BIGINT)\n"
            + "    Table rel_props (rel_id BIGINT, key VARCHAR(4000), slot VARCHAR(4000), v_int BIGINT, v_float DOUBLE,"
            + " v_str VARCHAR(4000), v_bool BIT, v_date DATE, v_ts TIMESTAMP, v_time TIME, v_json SEMISTRUCTURED)\n";

    public static Inferred infer(CypherAnswers.Graph g) throws Unsupported {
        return infer(g, Realization.DOMAIN);
    }

    /**
     * The domain model a write over {@code before} needs (the writes design's §8, leg 5): the schema that holds both
     * the graph before and the graph {@code after} it (their labels, label sets, properties and their types,
     * relationship types and how many each node holds), as a person's model would declare the classes the write
     * creates instances of; its tables seeded with the graph before's rows alone. The two graphs are read as one,
     * the graph after's ids past the graph before's, so its rows are told apart and left out. Every relationship type
     * is an edge class (a relationship's own row, as the native store's views hold it): a write creates and deletes
     * relationships one by one, parallel ones apart.
     */
    public static Inferred inferWrite(CypherAnswers.Graph before, CypherAnswers.Graph after) throws Unsupported {
        return inferWrite(before, after, null);
    }

    /** {@link #inferWrite(CypherAnswers.Graph, CypherAnswers.Graph)}, the schema also holding each property of a
     *  literal value the query {@code query} sets ({@link #written}): a branch of it Neo4j's run never took (a MERGE's
     *  ON MATCH where it created, ON CREATE where it matched) writes a property the model must declare. */
    public static Inferred inferWrite(CypherAnswers.Graph before, CypherAnswers.Graph after,
            @com.legend.base.Nullable String query) throws Unsupported {
        int offset = 1 + Math.max(before.nodes().stream().mapToInt(CypherAnswers.GraphNode::id).max().orElse(0),
                after.nodes().stream().mapToInt(CypherAnswers.GraphNode::id).max().orElse(0));
        int relOffset = 1 + Math.max(before.rels().stream().mapToInt(CypherAnswers.GraphRel::id).max().orElse(0),
                after.rels().stream().mapToInt(CypherAnswers.GraphRel::id).max().orElse(0));
        List<CypherAnswers.GraphNode> nodes = new ArrayList<>(before.nodes());
        after.nodes().forEach(n -> nodes.add(new CypherAnswers.GraphNode(n.id() + offset, n.labels(), n.props())));
        List<CypherAnswers.GraphRel> rels = new ArrayList<>(before.rels());
        after.rels().forEach(r -> rels.add(new CypherAnswers.GraphRel(r.id() + relOffset, r.start() + offset,
                r.end() + offset, r.type(), r.props())));
        // (the query's written properties: elements past the graph after's, never seeded)
        CypherAnswers.Graph phantoms = query == null ? new CypherAnswers.Graph(List.of(), List.of())
                : written(query, new CypherAnswers.Graph(nodes, rels), 2 * offset, 2 * relOffset);
        nodes.addAll(phantoms.nodes());
        rels.addAll(phantoms.rels());
        // (each relationship type an edge class: a relationship a write creates or deletes is its own row)
        Inferred both = infer(new CypherAnswers.Graph(nodes, rels), Realization.DOMAIN, true);
        List<SqlParity.Table> tables = new ArrayList<>();
        for (SqlParity.Table t : both.tables()) {
            // a class's table and an edge class's bridge: their id first; a junction: its two ends
            boolean junction = t.columns().size() == 2 && t.columns().get(0).name().equals("src_id");
            boolean bridge = !junction && t.columns().size() >= 3 && t.columns().get(1).name().equals("src_id");
            int limit = bridge ? relOffset : offset;
            List<List<Object>> rows = new ArrayList<>();
            for (List<Object> row : t.rows()) {
                if (((Long) java.util.Objects.requireNonNull(row.get(0))).intValue() < limit) {
                    rows.add(row);
                }
            }
            tables.add(new SqlParity.Table(t.name(), t.columns(), rows));
        }
        return new Inferred(both.model(), tables, graphKey(before) + "+" + graphKey(after) + "+" + graphKey(phantoms));
    }

    /** The properties of a literal value {@code query} sets (a SET's, an ON CREATE's or an ON MATCH's) on a node its
     *  patterns label or a relationship they type, as elements of {@code graph}'s schema (ids from {@code nodeId} and
     *  {@code relId}): a node of each label set of the graph holding the query's labels (else of those labels), a
     *  relationship of the type between nodes of its ends' (one of the graph's of the type where its ends hold them).
     *  A query lite's parser refuses has none. */
    static CypherAnswers.Graph written(String query, CypherAnswers.Graph graph, int nodeId, int relId) {
        com.legend.protocol.cypher.CypherAst.Statement st;
        try {
            st = com.legend.protocol.cypher.CypherParser.parse(query);
        } catch (RuntimeException unparsed) {
            return new CypherAnswers.Graph(List.of(), List.of());
        }
        Map<String, Set<String>> labelsOf = new HashMap<>();
        Map<String, String[]> relOf = new HashMap<>();
        List<String[]> sets = new ArrayList<>();
        // (the label sets of the nodes a MERGE would create, its free nodes': a MERGE that matched where Neo4j ran
        // still needs its creating branch's class)
        Set<List<String>> merged = new java.util.LinkedHashSet<>();
        Set<String> bound = new HashSet<>();
        for (com.legend.protocol.cypher.CypherAst.Clause c : st.clauses()) {
            if (c instanceof com.legend.protocol.cypher.CypherAst.Merge mg) {
                Set<String> inPattern = new HashSet<>();
                for (var e : mg.pattern().elements()) {
                    if (e instanceof com.legend.protocol.cypher.CypherAst.NodePattern n && (n.variable() == null
                            || !bound.contains(n.variable()) && inPattern.add(n.variable()))) {
                        merged.add(List.copyOf(new java.util.TreeSet<>(n.labels())));
                    }
                }
            }
            List<com.legend.protocol.cypher.CypherAst.PathPattern> patterns = switch (c) {
                case com.legend.protocol.cypher.CypherAst.Match m -> m.patterns();
                case com.legend.protocol.cypher.CypherAst.Create cr -> cr.patterns();
                case com.legend.protocol.cypher.CypherAst.Merge mg -> List.of(mg.pattern());
                default -> List.of();
            };
            for (com.legend.protocol.cypher.CypherAst.PathPattern p : patterns) {
                var els = p.elements();
                for (var e : els) {
                    if (e instanceof com.legend.protocol.cypher.CypherAst.NodePattern n && n.variable() != null) {
                        bound.add(n.variable());
                    }
                }
                for (int i = 0; i < els.size(); i++) {
                    if (els.get(i) instanceof com.legend.protocol.cypher.CypherAst.NodePattern n && n.variable() != null) {
                        labelsOf.computeIfAbsent(n.variable(), k -> new java.util.TreeSet<>()).addAll(n.labels());
                    } else if (els.get(i) instanceof com.legend.protocol.cypher.CypherAst.RelPattern r
                            && r.variable() != null && r.types().size() == 1 && i > 0 && i + 1 < els.size()
                            && els.get(i - 1) instanceof com.legend.protocol.cypher.CypherAst.NodePattern a
                            && els.get(i + 1) instanceof com.legend.protocol.cypher.CypherAst.NodePattern b
                            && a.variable() != null && b.variable() != null) {
                        boolean in = r.direction() == com.legend.protocol.cypher.CypherAst.Direction.IN;
                        relOf.put(r.variable(), new String[] {r.types().get(0), in ? b.variable() : a.variable(),
                                in ? a.variable() : b.variable()});
                    }
                }
            }
            List<com.legend.protocol.cypher.CypherAst.SetItem> items = switch (c) {
                case com.legend.protocol.cypher.CypherAst.SetClause sc -> sc.items();
                case com.legend.protocol.cypher.CypherAst.Merge mg -> {
                    List<com.legend.protocol.cypher.CypherAst.SetItem> both = new ArrayList<>(mg.onCreate());
                    both.addAll(mg.onMatch());
                    yield both;
                }
                default -> List.of();
            };
            for (com.legend.protocol.cypher.CypherAst.SetItem item : items) {
                if (item instanceof com.legend.protocol.cypher.CypherAst.SetProperty sp
                        && sp.target() instanceof com.legend.protocol.cypher.CypherAst.Var v) {
                    String text = literalText(sp.value());
                    if (text != null) {
                        sets.add(new String[] {v.name(), sp.key(), text});
                    }
                } else if (item instanceof com.legend.protocol.cypher.CypherAst.SetProperties sp
                        && sp.value() instanceof com.legend.protocol.cypher.CypherAst.MapLit map) {
                    for (var e : map.entries().entrySet()) {
                        String text = literalText(e.getValue());
                        if (text != null) {
                            sets.add(new String[] {sp.variable(), e.getKey(), text});
                        }
                    }
                }
            }
        }
        List<CypherAnswers.GraphNode> nodes = new ArrayList<>();
        List<CypherAnswers.GraphRel> rels = new ArrayList<>();
        int[] ids = {nodeId, relId};
        for (List<String> labels : merged) {
            boolean held = false;
            for (CypherAnswers.GraphNode n : graph.nodes()) {
                held |= new HashSet<>(n.labels()).equals(new HashSet<>(labels));
            }
            if (!held) {
                nodes.add(new CypherAnswers.GraphNode(ids[0]++, labels, Map.of()));
            }
        }
        for (String[] set : sets) {
            Map<String, String> props = Map.of(set[1], set[2]);
            String[] rel = relOf.get(set[0]);
            if (rel != null) {
                Set<String> from = labelsOf.getOrDefault(rel[1], Set.of());
                Set<String> to = labelsOf.getOrDefault(rel[2], Set.of());
                List<String> fromSet = null;
                List<String> toSet = null;
                for (CypherAnswers.GraphRel r : graph.rels()) {
                    List<String> a = labelsOfNode(graph, r.start());
                    List<String> b = labelsOfNode(graph, r.end());
                    if (r.type().equals(rel[0]) && a.containsAll(from) && b.containsAll(to)) {
                        fromSet = a;
                        toSet = b;
                        break;
                    }
                }
                if (fromSet == null) {
                    fromSet = List.copyOf(from);
                    toSet = List.copyOf(to);
                }
                int a = ids[0]++;
                int b = ids[0]++;
                nodes.add(new CypherAnswers.GraphNode(a, fromSet, Map.of()));
                nodes.add(new CypherAnswers.GraphNode(b, java.util.Objects.requireNonNull(toSet), Map.of()));
                rels.add(new CypherAnswers.GraphRel(ids[1]++, a, b, rel[0], props));
                continue;
            }
            Set<String> labels = labelsOf.get(set[0]);
            if (labels == null || labels.isEmpty()) {
                continue;
            }
            Set<List<String>> labelSets = new java.util.LinkedHashSet<>();
            for (CypherAnswers.GraphNode n : graph.nodes()) {
                if (n.labels().containsAll(labels)) {
                    labelSets.add(n.labels());
                }
            }
            if (labelSets.isEmpty()) {
                labelSets.add(List.copyOf(labels));
            }
            for (List<String> l : labelSets) {
                nodes.add(new CypherAnswers.GraphNode(ids[0]++, l, props));
            }
        }
        return new CypherAnswers.Graph(nodes, rels);
    }

    private static List<String> labelsOfNode(CypherAnswers.Graph g, int id) {
        for (CypherAnswers.GraphNode n : g.nodes()) {
            if (n.id() == id) {
                return n.labels();
            }
        }
        return List.of();
    }

    /** A literal's text as a dumped graph spells a property's value; null for any other expression. */
    private static @com.legend.base.Nullable String literalText(com.legend.protocol.cypher.CypherAst.Expr e) {
        return switch (e) {
            case com.legend.protocol.cypher.CypherAst.IntLit i -> Long.toString(i.value());
            case com.legend.protocol.cypher.CypherAst.FloatLit f -> Double.toString(f.value());
            case com.legend.protocol.cypher.CypherAst.StringLit str -> "'" + str.value().replace("\\", "\\\\")
                    .replace("'", "\\'") + "'";
            case com.legend.protocol.cypher.CypherAst.BoolLit b -> Boolean.toString(b.value());
            default -> null;
        };
    }

    public static Inferred infer(CypherAnswers.Graph g, Realization realization) throws Unsupported {
        return infer(g, realization, realization == Realization.NATIVE);
    }

    /** {@link #infer(CypherAnswers.Graph, Realization)}, every relationship type an edge class when
     *  {@code everyTypeAnEdge} (the native store's views; a domain model a write's relationships are created in and
     *  deleted from one by one, each its own identity, parallel ones apart). */
    private static Inferred infer(CypherAnswers.Graph g, Realization realization, boolean everyTypeAnEdge)
            throws Unsupported {
        boolean nativeStore = realization == Realization.NATIVE;
        // the native store's rows and views (the native realization's)
        List<List<Object>> nodePropRows = new ArrayList<>();
        List<List<Object>> relPropRows = new ArrayList<>();
        List<String> views = new ArrayList<>();
        List<String> viewNames = new ArrayList<>();
        Map<String, Cls> classes = new TreeMap<>();
        Map<Integer, Cls> classOfNode = new HashMap<>();
        Map<String, String> tableOwner = new HashMap<>();
        // a node's class is its label set's (the study's §12.61): the label's own class for a label no node carries
        // among others; else an anonymous class per set (named N_ and its labels, its name no label) extending each
        // label's class, which then holds no node of its own (a marker every class of its sets extends: so an
        // association is always a leaf class's, never inherited)
        Set<String> labels = new HashSet<>();
        Set<String> otherLabels = new HashSet<>();
        for (CypherAnswers.GraphNode n : g.nodes()) {
            labels.addAll(n.labels());
            if (n.labels().size() > 1) {
                otherLabels.addAll(n.labels());
            }
        }
        // a property name's precision is the finest the graph holds under it (the study's §12.64): a time of day or
        // date-time with sub-microsecond digits on any node or relationship makes every class's property of that
        // name a nanosecond one (an embedded value of its parts, the plan's W13 legs 5 and 6), so a label-less read
        // unites them
        Set<String> nanoNames = new HashSet<>();
        // (a zoned value class holds the instant's parts in the instant's stead when any zoned value of its kind in
        // the graph, on a node or a relationship, is sub-microsecond: one class per kind, the study's §12.65)
        boolean[] zonedNanos = new boolean[2];
        // (the zoned date-time class declares the region slot when a stored date-time of the graph is of a named
        // zone: the study's §12.75)
        boolean zonedRegion = false;
        List<Map<String, String>> propertySets = new ArrayList<>();
        g.nodes().forEach(n -> propertySets.add(n.props()));
        g.rels().forEach(r -> propertySets.add(r.props()));
        for (Map<String, String> props : propertySets) {
            for (Map.Entry<String, String> p : props.entrySet()) {
                if (subMicro(p.getValue())) {
                    nanoNames.add(p.getKey());
                }
                CypherValues.Value v = CypherValues.parse(p.getValue());
                if (v instanceof CypherValues.Opaque o && CypherValues.zonedParts(o.text()) != null
                        && !java.util.Objects.requireNonNull(CypherValues.zonedParts(o.text())).instant()
                        .substring(26).matches("0+")) {
                    zonedNanos[CypherValues.zonedTimeText(o.text()) ? 0 : 1] = true;
                }
                if (v instanceof CypherValues.Opaque o && CypherValues.zonedParts(o.text()) != null
                        && java.util.Objects.requireNonNull(CypherValues.zonedParts(o.text())).region() != null) {
                    zonedRegion = true;
                }
            }
        }
        for (CypherAnswers.GraphNode n : g.nodes()) {
            List<String> set = new ArrayList<>(new java.util.TreeSet<>(n.labels()));
            for (String label : set) {
                if (label.equals(UNLABELLED)) {
                    throw new Unsupported("harness: the label " + UNLABELLED + " is the class of the unlabelled"
                            + " nodes");
                }
                if (!IDENTIFIER.matcher(label).matches()) {
                    throw new Unsupported("harness: the label " + label + " is not a Pure identifier");
                }
                if (otherLabels.contains(label)) {
                    declare(classes, tableOwner, label, false, List.of());
                }
            }
            boolean own = set.size() == 1 && !otherLabels.contains(set.get(0));
            String name = set.isEmpty() ? UNLABELLED : own ? set.get(0) : "N_" + String.join("_", set);
            if (!own && labels.contains(name)) {
                throw new Unsupported("harness: the label " + name + " is the class the model makes up for the label"
                        + " set " + String.join(":", set));
            }
            Cls c = declare(classes, tableOwner, name, !own, own ? List.of() : set);
            c.nodes.add(n);
            classOfNode.put(n.id(), c);
            for (Map.Entry<String, String> p : n.props().entrySet()) {
                if (!IDENTIFIER.matcher(p.getKey()).matches()) {
                    throw new Unsupported("harness: the property " + p.getKey() + " is not a Pure identifier");
                }
                String type = typeOf(p.getValue(), p.getKey(), nanoNames.contains(p.getKey()));
                c.propertyTypes.put(p.getKey(), merged(c.propertyTypes.get(p.getKey()), type, c.name + "."
                        + p.getKey()));
                marked(c.propertyMarks, p.getKey(), p.getValue(), c.name + "." + p.getKey());
            }
        }
        for (Cls c : classes.values()) {
            Set<String> lower = new HashSet<>();
            for (String p : c.propertyTypes.keySet()) {
                if (!lower.add(p.toLowerCase(Locale.ROOT))) {
                    throw new Unsupported("harness: two properties of " + c.name + " differ only in case (" + p + ")");
                }
            }
        }
        Map<String, RelType> types = new TreeMap<>();
        // (a type's SQL spelling: its name in lower case; one differing from another's only in case, T2 and t2 of
        // Graph5/[2], two Pure names, takes a suffix no type's lower-case name has)
        Set<String> lowerNames = new HashSet<>();
        for (CypherAnswers.GraphRel r : g.rels()) {
            lowerNames.add(r.type().toLowerCase(Locale.ROOT));
        }
        Map<String, String> sqlOfType = new HashMap<>();
        Set<String> sqlTaken = new HashSet<>();
        for (CypherAnswers.GraphRel r : g.rels()) {
            if (!IDENTIFIER.matcher(r.type()).matches()) {
                throw new Unsupported("harness: the relationship type " + r.type() + " is not a Pure identifier");
            }
            String sql = sqlOfType.computeIfAbsent(r.type(), k -> {
                String lower = k.toLowerCase(Locale.ROOT);
                String name = lower;
                for (int n = 1; sqlTaken.contains(name) || !name.equals(lower) && lowerNames.contains(name); n++) {
                    name = lower + "_c" + n;
                }
                sqlTaken.add(name);
                return name;
            });
            Cls src = classOfNode.get(r.start());
            Cls dst = classOfNode.get(r.end());
            // (one entry per class pair the type joins, in the order the pairs appear)
            RelType t = types.computeIfAbsent(r.type() + "|" + src.name + "|" + dst.name,
                    k -> new RelType(r.type(), sql, src, dst));
            t.ownRow = everyTypeAnEdge;
            t.rels.add(r);
            for (Map.Entry<String, String> p : r.props().entrySet()) {
                if (!IDENTIFIER.matcher(p.getKey()).matches()) {
                    throw new Unsupported("harness: the property " + p.getKey() + " is not a Pure identifier");
                }
                if (p.getKey().equals("from") || p.getKey().equals("to") || p.getKey().equals("type")) {
                    throw new Unsupported("harness: the relationship property " + p.getKey() + " is named as the"
                            + " edge class's end or type");
                }
                // (a duration, a zoned value, a nanosecond one: an embedded value of the edge class, as a node's,
                // the plan's W13 leg 6)
                String type = typeOf(p.getValue(), p.getKey(), nanoNames.contains(p.getKey()));
                t.propertyTypes.put(p.getKey(), merged(t.propertyTypes.get(p.getKey()), type, r.type() + "."
                        + p.getKey()));
                marked(t.propertyMarks, p.getKey(), p.getValue(), r.type() + "." + p.getKey());
            }
        }
        // a type joining several pairs: its entries numbered (each an edge class of the type)
        Map<String, Integer> pairsOf = new HashMap<>();
        for (RelType t : types.values()) {
            pairsOf.merge(t.name, 1, Integer::sum);
        }
        Map<String, Integer> numbered = new HashMap<>();
        for (RelType t : types.values()) {
            if (pairsOf.get(t.name) > 1) {
                t.pair = numbered.merge(t.name, 1, Integer::sum);
            }
        }
        // the names the model makes up, apart from the graph's own: a label spelled like one of its elements, or two
        // of its tables spelled alike (a type named T_1 beside a type T joining two pairs), is refused by name
        Set<String> madeElements = new HashSet<>();
        Set<String> madeTables = new HashSet<>();
        for (Cls c : classes.values()) {
            madeTables.add(c.table);
        }
        if (classes.containsKey("RelationshipType")) {
            throw new Unsupported("harness: the label RelationshipType is a class the model makes up");
        }
        for (RelType t : types.values()) {
            Set<String> lower = new HashSet<>();
            for (String p : t.propertyTypes.keySet()) {
                if (!lower.add(p.toLowerCase(Locale.ROOT))) {
                    throw new Unsupported("harness: two properties of " + t.name + " differ only in case (" + p + ")");
                }
            }
            List<String> elements = t.edge()
                    ? List.of("E_" + t.stem(), "E_" + t.stem() + "_from", "E_" + t.stem() + "_to")
                    : List.of("R_" + t.name);
            for (String n : elements) {
                if (classes.containsKey(n) || !madeElements.add(n)) {
                    throw new Unsupported("harness: the element " + n + ", made up for the relationship type " + t.name
                            + ", is a label's or another type's");
                }
            }
            String table = t.edge() ? t.table() : "r_" + t.sql;
            if (!madeTables.add(table)) {
                throw new Unsupported("harness: the table " + table + ", made up for the relationship type " + t.name
                        + ", is another's");
            }
        }
        // the associations: one per type, realized by a foreign key on the to-one side, else a junction table; a
        // type with properties an edge class over a bridge table
        StringBuilder associations = new StringBuilder();
        StringBuilder joins = new StringBuilder();
        StringBuilder associationMappings = new StringBuilder();
        StringBuilder edgeClasses = new StringBuilder();
        List<String> scope = new ArrayList<>();
        StringBuilder edgeMappings = new StringBuilder();
        List<SqlParity.Table> junctions = new ArrayList<>();
        // (an edge class's table's DDL, by its name)
        Map<String, String> edgeDdl = new HashMap<>();
        List<String> edgeTypes = new ArrayList<>();
        for (RelType t : types.values()) {
            Cls src = java.util.Objects.requireNonNull(t.src);
            Cls dst = java.util.Objects.requireNonNull(t.dst);
            String in = t.edge() ? t.inEnd() : t.name + "_in";
            String outEnd = t.outEnd();
            for (Cls c : List.of(src, dst)) {
                if (c.propertyTypes.containsKey(t.name) || c.propertyTypes.containsKey(in)
                        || c.propertyTypes.containsKey(outEnd) || c.ends.stream().anyMatch(e -> e.startsWith(t.name
                        + ":") || e.startsWith(in + ":") || e.startsWith(outEnd + ":"))) {
                    throw new Unsupported("harness: the relationship type " + t.name + " collides with a property or"
                            + " another type's end on " + c.name);
                }
            }
            if (t.edge()) {
                if (!edgeTypes.contains(t.name)) {
                    edgeTypes.add(t.name);
                }
                String edge = t.edgeClass();
                scope.add(edge);
                edgeClasses.append("Class <<graph::graph.Edge>> ").append(edge).append("\n{\n")
                        .append("    <<graph::graph.type>> type() { ").append(RELATIONSHIP_TYPE).append('.')
                        .append(t.name).append(" }: ").append(RELATIONSHIP_TYPE).append("[1];\n");
                List<SqlParity.Column> columns = new ArrayList<>();
                columns.add(new SqlParity.Column("id", "BIGINT"));
                columns.add(new SqlParity.Column("src_id", "BIGINT"));
                columns.add(new SqlParity.Column("dst_id", "BIGINT"));
                List<String> propertyMappings = new ArrayList<>();
                StringBuilder ddl = new StringBuilder("    Table " + t.table()
                        + " (id BIGINT PRIMARY KEY, src_id BIGINT, dst_id BIGINT");
                for (Map.Entry<String, String> p : t.propertyTypes.entrySet()) {
                    String column = "p_" + p.getKey().toLowerCase(Locale.ROOT);
                    edgeClasses.append("    ").append(markText(t.propertyMarks, p.getKey())).append(p.getKey())
                            .append(": ").append(p.getValue()).append("[0..1];\n");
                    if (embeddedValue(p.getValue())) {
                        propertyMappings.add(embeddedColumns(p.getKey(), column, p.getValue(), t.table(), zonedNanos,
                                zonedRegion, columns, ddl));
                        continue;
                    }
                    columns.add(new SqlParity.Column(column, sqlType(p.getValue())));
                    ddl.append(", ").append(column).append(' ').append(relationalType(p.getValue()));
                    propertyMappings.add("        " + p.getKey() + ": [g::DB] " + t.table() + "." + column);
                }
                edgeClasses.append("}\n");
                edgeDdl.put(t.table(), ddl.append(")\n").toString());
                List<Provenance> edgeProvenance = new ArrayList<>();
                for (Map.Entry<String, String> p : t.propertyTypes.entrySet()) {
                    provenance(p.getKey(), "p_" + p.getKey().toLowerCase(Locale.ROOT), p.getValue(), zonedNanos,
                            zonedRegion, edgeProvenance);
                }
                List<List<Object>> rows = new ArrayList<>();
                for (CypherAnswers.GraphRel r : t.rels) {
                    List<Object> row = new ArrayList<>();
                    row.add((long) r.id());
                    row.add((long) r.start());
                    row.add((long) r.end());
                    for (Map.Entry<String, String> p : t.propertyTypes.entrySet()) {
                        String text = r.props().get(p.getKey());
                        if (embeddedValue(p.getValue())) {
                            embeddedCells(text, p.getValue(), zonedNanos, zonedRegion, row);
                            continue;
                        }
                        row.add(text == null ? null : p.getValue().equals(VARIANT)
                                ? jsonOf(CypherValues.parse(text), p.getKey(), text) : cell(text));
                    }
                    rows.add(row);
                    if (nativeStore) {
                        propRows((long) r.id(), row, 3, edgeProvenance, relPropRows);
                    }
                }
                junctions.add(new SqlParity.Table(t.table(), columns, rows));
                if (nativeStore) {
                    viewNames.add(t.table());
                    views.add(view(t.table(), "e.id AS id, e.src_id AS src_id, e.dst_id AS dst_id", "rel_props",
                            "rel_id", edgeProvenance, ends(t)));
                }
                edgeMappings.append("    ").append(edge).append(": Relational\n    {\n        ~mainTable [g::DB] ")
                        .append(t.table()).append('\n').append(String.join(",\n", propertyMappings))
                        .append(propertyMappings.isEmpty() ? "" : "\n").append("    }\n");
                // the two ends: the edge's from (the start class's T_out) and to (the end class's T_in), each an
                // association over its own join (a self-typed edge's two joins are two)
                String fromJoin = t.stem() + "_from";
                String toJoin = t.stem() + "_to";
                joins.append("    Join ").append(fromJoin).append('(').append(t.table()).append(".src_id = ")
                        .append(src.table).append(".id)\n");
                joins.append("    Join ").append(toJoin).append('(').append(t.table()).append(".dst_id = ")
                        .append(dst.table).append(".id)\n");
                src.ends.add(outEnd + ": " + edge + "[*];");
                dst.ends.add(in + ": " + edge + "[*];");
                associations.append("Association ").append(edge).append("_from\n{\n    <<graph::graph.from>> from: g::")
                        .append(src.name).append("[1];\n    ").append(outEnd).append(": ").append(edge)
                        .append("[*];\n}\n");
                associations.append("Association ").append(edge).append("_to\n{\n    <<graph::graph.to>> to: g::")
                        .append(dst.name).append("[1];\n    ").append(in).append(": ").append(edge)
                        .append("[*];\n}\n");
                associationMappings.append("    ").append(edge).append("_from: Relational { AssociationMapping ( from:"
                        + " [g::DB]@").append(fromJoin).append(", ").append(outEnd).append(": [g::DB]@")
                        .append(fromJoin).append(" ) }\n");
                associationMappings.append("    ").append(edge).append("_to: Relational { AssociationMapping ( to:"
                        + " [g::DB]@").append(toJoin).append(", ").append(in).append(": [g::DB]@").append(toJoin)
                        .append(" ) }\n");
                continue;
            }
            Map<Integer, Integer> out = new HashMap<>();
            Map<Integer, Integer> inDegree = new HashMap<>();
            for (CypherAnswers.GraphRel r : t.rels) {
                out.merge(r.start(), 1, Integer::sum);
                inDegree.merge(r.end(), 1, Integer::sum);
            }
            int maxOut = out.values().stream().mapToInt(Integer::intValue).max().orElse(0);
            int maxIn = inDegree.values().stream().mapToInt(Integer::intValue).max().orElse(0);
            // named apart from the classes: a label may be spelled like a relationship type
            String assoc = "g::R_" + t.name;
            String column = "r_" + t.sql;
            String target = src == dst ? "{target}." : dst.table + ".";
            if (maxOut <= 1 && !nativeStore) {
                // the start node holds at most one end node: a foreign key on the start class's table
                src.fkColumns.put(t.name, column);
                src.ends.add(t.name + ": g::" + dst.name + "[0..1];");
                dst.ends.add(in + ": g::" + src.name + "[*];");
                joins.append("    Join ").append(t.name).append('(').append(src.table).append('.').append(column)
                        .append(" = ").append(target).append("id)\n");
                // a self-association: the inverse end is its own join, read from the other side ({target} is the
                // node navigated to, so one join text cannot serve both ends)
                String inJoin = t.name;
                if (src == dst) {
                    inJoin = in;
                    joins.append("    Join ").append(in).append("({target}.").append(column).append(" = ")
                            .append(src.table).append(".id)\n");
                }
                associationMappings.append("    ").append(assoc).append(": Relational { AssociationMapping ( ")
                        .append(t.name).append(": [g::DB]@").append(t.name).append(", ").append(in)
                        .append(": [g::DB]@").append(inJoin).append(" ) }\n");
            } else if (maxIn <= 1 && !nativeStore) {
                // the end node holds at most one start node: a foreign key on the end class's table
                String col = column + "_in";
                dst.fkColumns.put(in, col);
                src.ends.add(t.name + ": g::" + dst.name + "[*];");
                dst.ends.add(in + ": g::" + src.name + "[0..1];");
                String inJoin = t.name;
                if (src == dst) {
                    // a self-association: the forward end reads the end node's key from the start ({target} is
                    // the node navigated to), the inverse end its own join from the other side
                    joins.append("    Join ").append(t.name).append("({target}.").append(col).append(" = ")
                            .append(src.table).append(".id)\n");
                    inJoin = in;
                    joins.append("    Join ").append(in).append('(').append(dst.table).append('.').append(col)
                            .append(" = {target}.id)\n");
                } else {
                    joins.append("    Join ").append(t.name).append('(').append(dst.table).append('.').append(col)
                            .append(" = ").append(src.table).append(".id)\n");
                }
                associationMappings.append("    ").append(assoc).append(": Relational { AssociationMapping ( ")
                        .append(t.name).append(": [g::DB]@").append(t.name).append(", ").append(in)
                        .append(": [g::DB]@").append(inJoin).append(" ) }\n");
            } else {
                // many to many: a junction table and a join chain
                String junction = column;
                List<List<Object>> rows = new ArrayList<>();
                for (CypherAnswers.GraphRel r : t.rels) {
                    rows.add(List.of((long) r.start(), (long) r.end()));
                }
                junctions.add(new SqlParity.Table(junction, List.of(new SqlParity.Column("src_id", "BIGINT"),
                        new SqlParity.Column("dst_id", "BIGINT")), rows));
                if (nativeStore) {
                    viewNames.add(junction);
                    views.add(view(junction, "e.src_id AS src_id, e.dst_id AS dst_id", "rel_props", "rel_id",
                            List.of(), ends(t)));
                }
                src.ends.add(t.name + ": g::" + dst.name + "[*];");
                dst.ends.add(in + ": g::" + src.name + "[*];");
                joins.append("    Join ").append(t.name).append("_a(").append(src.table).append(".id = ")
                        .append(junction).append(".src_id)\n");
                joins.append("    Join ").append(t.name).append("_b(").append(junction).append(".dst_id = ")
                        .append(dst.table).append(".id)\n");
                associationMappings.append("    ").append(assoc).append(": Relational { AssociationMapping ( ")
                        .append(t.name).append(": [g::DB]@").append(t.name).append("_a > @").append(t.name)
                        .append("_b, ").append(in).append(": [g::DB]@").append(t.name).append("_b > @")
                        .append(t.name).append("_a ) }\n");
            }
            associations.append("Association ").append(assoc).append("\n{\n");
            for (Cls c : src == dst ? List.of(src) : List.of(src, dst)) {
                for (String e : c.ends) {
                    if (e.startsWith(t.name + ":") || e.startsWith(in + ":")) {
                        associations.append("    ").append(e).append('\n');
                    }
                }
            }
            associations.append("}\n");
        }
        // the classes, the tables and the mapping; the Duration class the island carries a duration as (the study's
        // §12.9: a class of the model, not a platform type)
        StringBuilder pure = new StringBuilder("###Pure\n");
        pure.append("Class " + DURATION + "\n{\n    months: Integer[1];\n    days: Integer[1];\n    seconds: Integer[1];\n"
                + "    nanoseconds: Integer[1];\n}\n");
        // the zoned value classes (the study's §12.65): the instant (a time of day's on the epoch day) and the offset
        // in seconds (and, for a date-time of a named zone, the region's name: the study's §12.75); where any zoned
        // value of its kind is sub-microsecond, the instant's parts in the instant's stead (a zoned time's UTC
        // nanoseconds from the epoch day's midnight, a zoned date-time's UTC date and nanoseconds since midnight,
        // the plan's W13 leg 5)
        for (int i = 0; i < 2; i++) {
            pure.append("Class " + (i == 0 ? ZONED_TIME : ZONED_DATETIME) + "\n{\n"
                    + (!zonedNanos[i] ? "    instant: DateTime[1];\n" : i == 1 ? "    date: StrictDate[1];\n"
                    + "    nanosOfDay: Integer[1];\n" : "    nanosOfDay: Integer[1];\n")
                    + "    offsetSeconds: Integer[1];\n" + (i == 1 && zonedRegion ? "    zone: String[0..1];\n" : "")
                    + "}\n");
        }
        // the value classes of a local nanosecond value's parts (W13 leg 5): a date-time's date and nanoseconds
        // since midnight, a time of day's nanoseconds
        pure.append("Class " + LOCAL_DATETIME_PARTS + "\n{\n    date: StrictDate[1];\n    nanosOfDay: Integer[1];\n}\n");
        pure.append("Class " + LOCAL_TIME_PARTS + "\n{\n    nanosOfDay: Integer[1];\n}\n");
        StringBuilder tablesDdl = new StringBuilder();
        StringBuilder mapping = new StringBuilder();
        List<SqlParity.Table> tables = new ArrayList<>();
        for (Cls c : classes.values()) {
            pure.append("Class ").append(c.anonymous ? "<<graph::graph.Anonymous>> " : "").append("g::")
                    .append(c.name);
            if (!c.supers.isEmpty()) {
                pure.append(" extends ").append(c.supers.stream().map(s -> "g::" + s)
                        .collect(java.util.stream.Collectors.joining(", ")));
            }
            pure.append("\n{\n");
            scope.add("g::" + c.name);
            List<SqlParity.Column> columns = new ArrayList<>();
            columns.add(new SqlParity.Column("id", "BIGINT"));
            StringBuilder ddl = new StringBuilder("    Table " + c.table + " (id BIGINT PRIMARY KEY");
            mapping.append("    g::").append(c.name).append(": Relational\n    {\n        ~mainTable [g::DB] ")
                    .append(c.table).append('\n');
            List<String> propertyMappings = new ArrayList<>();
            List<Provenance> classProvenance = new ArrayList<>();
            for (Map.Entry<String, String> p : c.propertyTypes.entrySet()) {
                String column = "p_" + p.getKey().toLowerCase(Locale.ROOT);
                provenance(p.getKey(), column, p.getValue(), zonedNanos, zonedRegion, classProvenance);
                pure.append("    ").append(markText(c.propertyMarks, p.getKey())).append(p.getKey()).append(": ")
                        .append(p.getValue()).append("[0..1];\n");
                if (embeddedValue(p.getValue())) {
                    propertyMappings.add(embeddedColumns(p.getKey(), column, p.getValue(), c.table, zonedNanos,
                            zonedRegion, columns, ddl));
                    continue;
                }
                columns.add(new SqlParity.Column(column, sqlType(p.getValue())));
                ddl.append(", ").append(column).append(' ').append(relationalType(p.getValue()));
                propertyMappings.add("        " + p.getKey() + ": [g::DB] " + c.table + "." + column);
            }
            for (String column : c.fkColumns.values()) {
                columns.add(new SqlParity.Column(column, "BIGINT"));
                ddl.append(", ").append(column).append(" BIGINT");
            }
            pure.append("}\n");
            ddl.append(")\n");
            tablesDdl.append(ddl);
            mapping.append(String.join(",\n", propertyMappings)).append(propertyMappings.isEmpty() ? "" : "\n")
                    .append("    }\n");
            List<List<Object>> rows = new ArrayList<>();
            Map<Integer, Map<String, Long>> fks = new HashMap<>();
            for (RelType t : types.values()) {
                for (CypherAnswers.GraphRel r : t.rels) {
                    String col = java.util.Objects.requireNonNull(t.src).fkColumns.get(t.name);
                    if (col != null && t.src == c) {
                        fks.computeIfAbsent(r.start(), k -> new HashMap<>()).put(col, (long) r.end());
                    }
                    String colIn = java.util.Objects.requireNonNull(t.dst).fkColumns.get(t.name + "_in");
                    if (colIn != null && t.dst == c) {
                        fks.computeIfAbsent(r.end(), k -> new HashMap<>()).put(colIn, (long) r.start());
                    }
                }
            }
            for (CypherAnswers.GraphNode n : c.nodes) {
                List<Object> row = new ArrayList<>();
                row.add((long) n.id());
                for (Map.Entry<String, String> p : c.propertyTypes.entrySet()) {
                    String text = n.props().get(p.getKey());
                    if (embeddedValue(p.getValue())) {
                        embeddedCells(text, p.getValue(), zonedNanos, zonedRegion, row);
                        continue;
                    }
                    // (a Variant column, a list property's or a property typed two ways: the value's JSON)
                    row.add(text == null ? null : p.getValue().equals(VARIANT)
                            ? jsonOf(CypherValues.parse(text), p.getKey(), text) : cell(text));
                }
                Map<String, Long> fk = fks.getOrDefault(n.id(), Map.of());
                for (String column : c.fkColumns.values()) {
                    row.add(fk.get(column));
                }
                rows.add(row);
                if (nativeStore) {
                    propRows((long) n.id(), row, 1, classProvenance, nodePropRows);
                }
            }
            tables.add(new SqlParity.Table(c.table, columns, rows));
            if (nativeStore) {
                viewNames.add(c.table);
                // (a label among others is a marker class that holds no node of its own: a node of that label
                // alone is its set's anonymous class's, as the domain realization holds it)
                views.add(view(c.table, "e.id AS id", "node_props", "node_id", classProvenance,
                        !c.anonymous && otherLabels.contains(c.name) ? "FROM nodes e WHERE 1 = 0"
                        : "FROM nodes e WHERE " + labelTest("e.labels", c.anonymous ? c.supers : List.of(c.name))));
            }
        }
        for (SqlParity.Table j : junctions) {
            if (j.columns().size() == 2) {
                tablesDdl.append("    Table ").append(j.name()).append(" (src_id BIGINT, dst_id BIGINT)\n");
            } else {
                // a bridge table: its id the relationship's identity, its ends, its properties
                tablesDdl.append(java.util.Objects.requireNonNull(edgeDdl.get(j.name())));
            }
            tables.add(j);
        }
        // (the profile always: the DataSpace below carries its Open stereotype)
        pure.append(GRAPH_PROFILE);
        if (!edgeTypes.isEmpty()) {
            pure.append("Enum ").append(RELATIONSHIP_TYPE).append("\n{\n    ")
                    .append(String.join(", ", edgeTypes)).append("\n}\n").append(edgeClasses);
        }
        scope.add(DURATION);
        scope.add(ZONED_TIME);
        scope.add(ZONED_DATETIME);
        scope.add(LOCAL_DATETIME_PARTS);
        scope.add(LOCAL_TIME_PARTS);
        java.util.Collections.sort(scope);
        if (nativeStore) {
            // (the native store's own tables, which the island writes, and the graph marked native: its store among
            // its elements, the writes design's §3 B)
            tablesDdl.append(NATIVE_DDL);
            scope.add(DATABASE);
        }
        String model = pure + associations.toString() + "###Relational\nDatabase " + DATABASE + "\n(\n" + tablesDdl
                + joins + ")\n###Mapping\nMapping " + MAPPING + "\n(\n" + mapping + edgeMappings
                + associationMappings + ")\n"
                // the graph (the study's §12.45): a DataSpace of every class, open as a Neo4j graph is
                + "###DataSpace\nDataSpace <<graph::graph.Open" + (nativeStore ? ", graph::graph.Native" : "") + ">> "
                + DATASPACE + "\n{\n  executionContexts:\n  [\n"
                + "    {\n      name: 'default';\n      mapping: " + MAPPING + ";\n    }\n  ];\n"
                + "  defaultExecutionContext: 'default';\n  elements: [ " + String.join(", ", scope) + " ];\n}\n";
        if (!nativeStore) {
            return new Inferred(model, tables, graphKey(g));
        }
        List<List<Object>> nodeRows = new ArrayList<>();
        for (CypherAnswers.GraphNode n : g.nodes()) {
            nodeRows.add(List.of((long) n.id(), labelText(n.labels())));
        }
        List<List<Object>> relRows = new ArrayList<>();
        for (CypherAnswers.GraphRel r : g.rels()) {
            relRows.add(List.of((long) r.id(), r.type(), (long) r.start(), (long) r.end()));
        }
        return new Inferred(model, tables, graphKey(g), new Native(List.of(
                new SqlParity.Table("nodes", NODE_COLUMNS, nodeRows),
                new SqlParity.Table("node_props", propColumns("node_id"), nodePropRows),
                new SqlParity.Table("rels", REL_COLUMNS, relRows),
                new SqlParity.Table("rel_props", propColumns("rel_id"), relPropRows)), views, viewNames));
    }

    /** The relationships of {@code t} over the native store: its type, between nodes of its two classes' label
     *  sets (a type joining several pairs is one edge class per pair). */
    private static String ends(RelType t) {
        Cls src = java.util.Objects.requireNonNull(t.src);
        Cls dst = java.util.Objects.requireNonNull(t.dst);
        return "FROM rels e JOIN nodes s ON s.id = e.src_id JOIN nodes d ON d.id = e.dst_id WHERE e.type = '"
                + t.name + "' AND " + labelTest("s.labels", src.anonymous ? src.supers : List.of(src.name))
                + " AND " + labelTest("d.labels", dst.anonymous ? dst.supers : List.of(dst.name));
    }

    /** The class {@code name}, declared once: anonymous and extending {@code supers} as its first declaration says
     *  (a second one disagreeing, a label spelled as a class the model makes up, is refused); two names differing
     *  only in case are refused (one table). */
    private static Cls declare(Map<String, Cls> classes, Map<String, String> tableOwner, String name,
            boolean anonymous, List<String> supers) throws Unsupported {
        Cls c = classes.computeIfAbsent(name, k -> new Cls(k, anonymous, supers));
        if (c.anonymous != anonymous || !c.supers.equals(supers)) {
            throw new Unsupported("harness: the label " + name + " is the class the model makes up for the label set "
                    + String.join(":", c.anonymous ? c.supers : supers));
        }
        String owner = tableOwner.putIfAbsent(c.table, name);
        if (owner != null && !owner.equals(name)) {
            throw new Unsupported("harness: the labels " + owner + " and " + name + " differ only in case");
        }
        return c;
    }

    /** The type of a property across its nodes: one type where they agree; a Variant where they differ and each
     *  is one a Variant spells (Neo4j's property typed two ways, the study's §12.62); else refused by name. */
    private static String merged(@com.legend.base.Nullable String before, String type, String property)
            throws Unsupported {
        if (before == null || before.equals(type)) {
            return type;
        }
        if (VARIANT_SPELLED.contains(before) && VARIANT_SPELLED.contains(type)) {
            return VARIANT;
        }
        throw new Unsupported("harness: the property " + property + " is typed two ways (" + before + ", " + type
                + ")");
    }

    /** The model with its connection and runtime, as a lane runs it. */
    public static String model(Inferred inferred, String connection) {
        return inferred.model() + "###Connection\nRelationalDatabaseConnection g::Conn\n{\n    " + connection
                + "\n}\n###Runtime\nRuntime " + RUNTIME + "\n{\n    mappings: [ " + MAPPING + " ];\n    connections: [ "
                + DATABASE + ": [ environment: g::Conn ] ];\n}\n";
    }

    /** Whether a value is a local time or date-time with digits past the microsecond. */
    private static boolean subMicro(String valueText) {
        CypherValues.Value v = CypherValues.parse(valueText);
        if (!(v instanceof CypherValues.Opaque o)) {
            return false;
        }
        CypherValues.ZonedParts zoned = CypherValues.zonedParts(o.text());
        if (zoned != null) {
            return !zoned.instant().substring(26).matches("0+");
        }
        java.util.regex.Matcher m = LOCAL_DATE_TIME.matcher(o.text());
        if (!m.matches()) {
            m = LOCAL_TIME.matcher(o.text());
        }
        if (!m.matches()) {
            return false;
        }
        String fraction = m.group(2);
        return fraction.length() > 6 && !fraction.substring(6).matches("0+");
    }

    /** The harness's type of a property's value: a Pure type's name, the Variant, or a value class
     *  ({@code nanos}: the property name's precision across the graph). */
    private static String typeOf(String valueText, String key, boolean nanos) throws Unsupported {
        CypherValues.Value v = CypherValues.parse(valueText);
        return switch (v) {
            case CypherValues.Int i -> "Integer";
            case CypherValues.Flt f -> "Float";
            case CypherValues.Str s -> "String";
            case CypherValues.Bool b -> "Boolean";
            case CypherValues.Opaque o when o.text().startsWith("Date('") -> "StrictDate";
            // a list property (Neo4j's, of primitives): a Variant over a SEMISTRUCTURED column, seeded as its JSON
            // (the study's §12.37); a temporal, a node or a map inside it stays unsupported, the kind named
            case CypherValues.Lst l -> {
                jsonOf(l, key, valueText);
                yield VARIANT;
            }
            // a date-time: of nanosecond precision, an embedded instance of the value class of its parts (its date
            // and its nanoseconds since midnight, the plan's W13 legs 5 and 6: every database holds them, over every
            // year its DATE does), on a node or a relationship
            case CypherValues.Opaque o when LOCAL_DATE_TIME.matcher(o.text()).matches() -> {
                java.util.regex.Matcher m = LOCAL_DATE_TIME.matcher(o.text());
                m.matches();
                if (nanos) {
                    yield LOCAL_DATETIME_PARTS;
                }
                String fraction = m.group(2);
                if (fraction.length() > 6 && !fraction.substring(6).matches("0+")) {
                    throw new IllegalStateException("a sub-microsecond date-time under a microsecond name: " + key);
                }
                yield "DateTime";
            }
            // a time of day: lite's TIME store column (the relational grammar's extension, docs/SEMANTICS_REGISTER.md
            // S64 revised 2026-10-17), the property a StrictTime; its offset, where it has one, is a column beside
            // it (a zoned time: not yet)
            // (of nanosecond precision, the value class of its nanoseconds since midnight, W13 legs 5 and 6)
            case CypherValues.Opaque o when LOCAL_TIME.matcher(o.text()).matches() -> {
                if (nanos) {
                    yield LOCAL_TIME_PARTS;
                }
                String fraction = LOCAL_TIME.matcher(o.text()).replaceAll("$2");
                if (fraction.length() > 6 && !fraction.substring(6).matches("0+")) {
                    throw new IllegalStateException("a sub-microsecond time under a microsecond name: " + key);
                }
                yield "StrictTime";
            }
            // a duration: an embedded instance of the model's Duration class over four columns (its slots)
            case CypherValues.Opaque o when CypherValues.durationSlots(o.text()) != null -> DURATION;
            // a zoned time of day or date-time (the study's §12.65): an embedded instance of a value class over
            // two columns, its instant (on the epoch day for a time) and its offset in seconds (of nanosecond
            // precision, the instant's parts: the model's classes above)
            case CypherValues.Opaque o when CypherValues.zonedParts(o.text()) != null ->
                    CypherValues.zonedTimeText(o.text()) ? ZONED_TIME : ZONED_DATETIME;
            default -> throw new Unsupported("harness: the property " + key + " holds a " + v.getClass()
                    .getSimpleName().toLowerCase(Locale.ROOT) + " (" + valueText + "), a kind the model does not"
                    + " hold yet");
        };
    }

    /** The scenario's procedures as Pure functions (the study's §12.60): {@code test.my.proc} is the function
     *  {@code test::my::proc}, its inputs the parameters (each optional), its outputs the relation it returns; the
     *  body a typed TDS literal of the declared rows, filtered on the inputs (a null input matches a null cell) and
     *  cut to the outputs; a procedure of no output returns a Boolean (VOID). */
    public static String procedures(List<CypherTck.Procedure> procedures) {
        StringBuilder pure = new StringBuilder("###Pure\n");
        for (CypherTck.Procedure p : procedures) {
            String path = p.name().replace(".", "::");
            String params = p.inputs().stream().map(i -> i.name() + ": " + pureType(i.type()) + "[0..1]")
                    .collect(Collectors.joining(", "));
            if (p.outputs().isEmpty()) {
                pure.append("function ").append(path).append('(').append(params).append("): Boolean[1]\n{\n")
                        .append("    true\n}\n");
                continue;
            }
            String relation = p.outputs().stream().map(o -> o.name() + ": " + pureType(o.type()))
                    .collect(Collectors.joining(", "));
            pure.append("function ").append(path).append('(').append(params).append("): Relation<(")
                    .append(relation).append(")>[1]\n{\n    #TDS\n      ");
            List<CypherTck.Procedure.Param> columns = new ArrayList<>(p.inputs());
            columns.addAll(p.outputs());
            pure.append(columns.stream().map(c -> c.name() + ":" + pureType(c.type())).collect(
                    Collectors.joining(", "))).append('\n');
            for (List<String> row : p.rows()) {
                if (row.size() != columns.size()) {
                    throw new IllegalStateException("the procedure " + p.name() + " declares a row of " + row.size()
                            + " cells for " + columns.size() + " columns: " + row);
                }
                pure.append("      ").append(row.stream().map(CypherGraph::tdsCell).collect(Collectors.joining(", ")))
                        .append('\n');
            }
            pure.append("    #");
            if (!p.inputs().isEmpty()) {
                pure.append("->filter(r | ").append(p.inputs().stream().map(i -> "$r." + i.name() + " == $"
                        + i.name()).collect(Collectors.joining(" && "))).append(')');
            }
            pure.append("->select(~[").append(p.outputs().stream().map(CypherTck.Procedure.Param::name).collect(
                    Collectors.joining(", "))).append("])\n}\n");
        }
        return pure.toString();
    }

    /** The TCK's parameter types as Pure's (a NUMBER is a Float in the rows, a Number as the parameter). */
    private static String pureType(String tckType) {
        return switch (tckType) {
            case "INTEGER" -> "Integer";
            case "FLOAT" -> "Float";
            case "NUMBER" -> "Number";
            case "STRING" -> "String";
            case "BOOLEAN" -> "Boolean";
            default -> throw new IllegalStateException("a procedure type the harness does not declare: " + tckType);
        };
    }

    /** A TCK table cell as a TDS literal cell: a null empty, a string in its quotes (the grammar's spelling is
     *  the literal's). */
    private static String tdsCell(String cell) {
        return cell.equals("null") ? "" : cell;
    }

    /** The model's Duration value class (the island carries a duration as an instance of it): a stored duration
     *  is an embedded instance over four columns, one per slot. */
    private static final String DURATION = "g::values::Duration";
    /** The value classes of a local nanosecond value's parts (the plan's W13 leg 5): a date-time's date and
     *  nanoseconds since midnight, a time of day's nanoseconds. */
    private static final String LOCAL_DATETIME_PARTS = "g::values::LocalDateTime";
    private static final String LOCAL_TIME_PARTS = "g::values::LocalTime";

    /** A value class's slots, in its columns' order. */
    private static List<String> slotsOf(String type, boolean[] zonedNanos) {
        return switch (type) {
            case DURATION -> DURATION_SLOTS;
            case LOCAL_DATETIME_PARTS -> List.of("date", "nanosOfDay");
            case LOCAL_TIME_PARTS -> List.of("nanosOfDay");
            case ZONED_TIME -> zonedNanos[0] ? List.of("nanosOfDay", "offsetSeconds") : ZONED_SLOTS;
            case ZONED_DATETIME -> zonedNanos[1] ? List.of("date", "nanosOfDay", "offsetSeconds") : ZONED_SLOTS;
            default -> throw new IllegalStateException("not a value class: " + type);
        };
    }

    /** Whether a harness type is a value class a property holds as an embedded instance (a duration, a zoned
     *  value, a nanosecond value's parts): one column per slot, the property mapping's embedded block. */
    private static boolean embeddedValue(String type) {
        return type.equals(DURATION) || type.equals(ZONED_TIME) || type.equals(ZONED_DATETIME)
                || type.equals(LOCAL_DATETIME_PARTS) || type.equals(LOCAL_TIME_PARTS);
    }

    /** The columns of {@code property}'s embedded instance of {@code type} in {@code table}, one per slot (a
     *  duration's four BIGINTs; a zoned value's instant and offset, or of nanosecond precision the instant's parts,
     *  and a date-time's region where the graph names one; a local nanosecond value's parts), added to
     *  {@code columns} and {@code ddl}; the property mapping's embedded block. */
    private static String embeddedColumns(String property, String column, String type, String table,
            boolean[] zonedNanos, boolean zonedRegion, List<SqlParity.Column> columns, StringBuilder ddl) {
        List<String> slots = new ArrayList<>();
        List<String> slotNames = new ArrayList<>(slotsOf(type, zonedNanos));
        if (type.equals(ZONED_DATETIME) && zonedRegion) {
            slotNames.add(ZONE_SLOT);
        }
        for (String slot : slotNames) {
            String slotColumn = column + "_" + slot.toLowerCase(Locale.ROOT);
            String sql = slot.equals(ZONE_SLOT) ? "VARCHAR(4000)" : slot.equals("date") ? "DATE"
                    : slot.equals("instant") ? "TIMESTAMP" : "BIGINT";
            columns.add(new SqlParity.Column(slotColumn, sql));
            ddl.append(", ").append(slotColumn).append(' ').append(sql);
            slots.add("            " + slot + ": [g::DB] " + table + "." + slotColumn);
        }
        return "        " + property + "\n        (\n" + String.join(",\n", slots) + "\n        )";
    }

    /** The cells of an embedded instance of {@code type} the scenario's {@code text} spells (nulls for none), in
     *  {@link #embeddedColumns}' order: the seed's own reading of the scenario's text, the data the lane is seeded
     *  with. */
    private static void embeddedCells(@com.legend.base.Nullable String text, String type, boolean[] zonedNanos,
            boolean zonedRegion, List<Object> row) {
        if (type.equals(DURATION)) {
            long[] slots = text == null ? null : CypherValues.durationSlots(
                    CypherValues.parse(text) instanceof CypherValues.Opaque o ? o.text() : text);
            for (int i = 0; i < DURATION_SLOTS.size(); i++) {
                row.add(slots == null ? null : slots[i]);
            }
            return;
        }
        if (type.equals(LOCAL_DATETIME_PARTS) || type.equals(LOCAL_TIME_PARTS)) {
            // a local nanosecond value's parts: its date as written, its nanoseconds since midnight
            boolean time = type.equals(LOCAL_TIME_PARTS);
            java.util.regex.Matcher m = text == null ? null : (time ? LOCAL_TIME : LOCAL_DATE_TIME).matcher(
                    ((CypherValues.Opaque) CypherValues.parse(text)).text());
            boolean found = m != null && m.matches();
            if (!time) {
                row.add(found ? m.group(1).substring(0, 10) : null);
            }
            row.add(found ? nanosOfDay(time ? m.group(1) : m.group(1).substring(11), m.group(2)) : null);
            return;
        }
        CypherValues.ZonedParts parts = text == null ? null : CypherValues.zonedParts(
                ((CypherValues.Opaque) CypherValues.parse(text)).text());
        boolean time = type.equals(ZONED_TIME);
        if (zonedNanos[time ? 0 : 1]) {
            // of nanosecond precision, the instant's parts: a date-time's UTC date and nanoseconds since midnight, a
            // time's nanoseconds from the epoch day's midnight
            String instant = parts == null ? null : parts.instant();
            Long nanos = instant == null ? null : nanosOfDay(instant.substring(11, 19), instant.substring(20));
            if (!time) {
                row.add(instant == null ? null : instant.substring(0, 10));
                row.add(nanos);
            } else {
                row.add(instant == null ? null : java.time.LocalDate.parse(instant.substring(0, 10)).toEpochDay()
                        * 86_400_000_000_000L + java.util.Objects.requireNonNull(nanos));
            }
        } else {
            // (a TIMESTAMP column binds the text to the microsecond)
            row.add(parts == null ? null : parts.instant().substring(0, 26));
        }
        row.add(parts == null ? null : parts.offsetSeconds());
        if (type.equals(ZONED_DATETIME) && zonedRegion) {
            row.add(parts == null ? null : parts.region());
        }
    }

    /** The nanoseconds since midnight a clock {@code HH:mm:ss} and its fraction's digits spell (the seed's own
     *  arithmetic over the scenario's text: the data the lane is seeded with, not a value of the query's). */
    private static long nanosOfDay(String clock, String fraction) {
        long seconds = Long.parseLong(clock.substring(0, 2)) * 3600 + Long.parseLong(clock.substring(3, 5)) * 60
                + Long.parseLong(clock.substring(6, 8));
        return seconds * 1_000_000_000L + Long.parseLong((fraction + "000000000").substring(0, 9));
    }

    /** The zoned value classes (the study's §12.65): an embedded instance of the instant and its offset. */
    private static final String ZONED_TIME = "g::values::ZonedTime";
    private static final String ZONED_DATETIME = "g::values::ZonedDateTime";
    private static final List<String> ZONED_SLOTS = List.of("instant", "offsetSeconds");
    /** A zoned date-time's region slot, declared when a stored date-time names its zone (the study's §12.75). */
    private static final String ZONE_SLOT = "zone";
    private static final List<String> DURATION_SLOTS = List.of("months", "days", "seconds", "nanoseconds");
    /** Neo4j's {@code Time('HH:mm:ss.nnnnnnnnn')} with no offset: a local time of day. */
    private static final java.util.regex.Pattern LOCAL_TIME = java.util.regex.Pattern.compile(
            "Time\\('(\\d\\d:\\d\\d:\\d\\d)\\.(\\d+)'\\)");

    private static final String VARIANT = "meta::pure::metamodel::variant::Variant";
    /** The types a Variant column holds as JSON (an integer, a float, a string, a boolean, a list). */
    private static final Set<String> VARIANT_SPELLED = Set.of("Integer", "Float", "String", "Boolean", VARIANT);

    /** The graph profile's list mark of a list of temporals of one kind (2026-10-20,
     *  docs/WRITES_DESIGN_2026_10_18.md §5: a Variant property whose elements are each the text of a date, a local or
     *  zoned time or date-time, or a duration), null for any other value; a list mixing temporals with other values
     *  is refused by name. */
    private static @com.legend.base.Nullable String listMark(String valueText) throws Unsupported {
        if (!(CypherValues.parse(valueText) instanceof CypherValues.Lst l)) {
            return null;
        }
        Set<String> marks = new HashSet<>();
        for (CypherValues.Value item : l.items()) {
            marks.add(item instanceof CypherValues.Opaque o ? java.util.Objects.requireNonNullElse(temporalMark(
                    o.text()), "?") : "");
        }
        if (marks.size() > 1 && marks.stream().anyMatch(m -> !m.isEmpty() && !m.equals("?"))) {
            throw new Unsupported("harness: a list mixing temporal values of a kind with other values ("
                    + valueText + "): the graph profile marks a list of one kind");
        }
        String mark = marks.size() == 1 ? marks.iterator().next() : "";
        return mark.isEmpty() || mark.equals("?") ? null : mark;
    }

    /** The list mark of a temporal's text, null for another. */
    private static @com.legend.base.Nullable String temporalMark(String text) {
        if (text.startsWith("Date('")) {
            return "listOfDate";
        }
        if (LOCAL_TIME.matcher(text).matches()) {
            return "listOfLocalTime";
        }
        if (LOCAL_DATE_TIME.matcher(text).matches()) {
            return "listOfLocalDateTime";
        }
        if (CypherValues.zonedParts(text) != null) {
            return CypherValues.zonedTimeText(text) ? "listOfZonedTime" : "listOfZonedDateTime";
        }
        return CypherValues.durationSlots(text) != null ? "listOfDuration" : null;
    }

    /** Records a property's list mark over one more of its values ("" for none): the marks of all its values agree,
     *  or the property is refused by name (a list of dates under the name where another node holds a number). */
    private static void marked(Map<String, String> marks, String key, String valueText, String where)
            throws Unsupported {
        String mark = java.util.Objects.requireNonNullElse(listMark(valueText), "");
        String before = marks.putIfAbsent(key, mark);
        if (before != null && !before.equals(mark)) {
            throw new Unsupported("harness: the property " + where + " holds a list of temporals on one element and"
                    + " another value on another: the graph profile marks a list of one kind");
        }
    }

    /** A property's list mark as its declaration spells it, before its name; "" for none. */
    private static String markText(Map<String, String> marks, String key) {
        String mark = marks.getOrDefault(key, "");
        return mark.isEmpty() ? "" : "<<graph::graph." + mark + ">> ";
    }

    /** A list property's JSON text, as the seed binds it (DuckDB's JSON, Postgres's JSONB): its primitives spelled
     *  as JSON spells them, a temporal as the string of its text (the graph profile's list marks say what the list
     *  holds): a date's, a zoned value's and a duration's as the dump spells them, a local time's and date-time's to
     *  the microsecond as the island spells one (a finer one refused by name). */
    private static String jsonOf(CypherValues.Value v, String key, String valueText) throws Unsupported {
        return switch (v) {
            case CypherValues.Opaque o when temporalMark(o.text()) != null -> {
                java.util.regex.Matcher m = LOCAL_TIME.matcher(o.text());
                if (!m.matches()) {
                    m = LOCAL_DATE_TIME.matcher(o.text());
                }
                if (!m.matches()) {
                    yield com.legend.json.Json.toCompact(o.text());
                }
                String fraction = m.group(2);
                if (fraction.length() > 6 && !fraction.substring(6).matches("0+")) {
                    throw new Unsupported("harness: the property " + key + " holds a list with a value of"
                            + " nanosecond precision (" + valueText + "), which the island's list does not hold yet");
                }
                String micros = (fraction + "000000").substring(0, 6);
                yield com.legend.json.Json.toCompact((o.text().startsWith("Time('") ? "Time('" : "DateTime('")
                        + m.group(1) + "." + micros + "')");
            }
            case CypherValues.Null n -> "null";
            case CypherValues.Bool b -> String.valueOf(b.value());
            case CypherValues.Int i -> i.value().toString();
            case CypherValues.Flt f -> {
                if (Double.isNaN(f.value()) || Double.isInfinite(f.value())) {
                    throw new Unsupported("harness: the property " + key + " holds a list with " + f.value()
                            + " (" + valueText + "), which JSON does not spell");
                }
                yield Double.toString(f.value());
            }
            case CypherValues.Str str -> com.legend.json.Json.toCompact(str.value());
            case CypherValues.Lst l -> {
                StringBuilder out = new StringBuilder("[");
                for (int i = 0; i < l.items().size(); i++) {
                    out.append(i > 0 ? "," : "").append(jsonOf(l.items().get(i), key, valueText));
                }
                yield out.append(']').toString();
            }
            default -> throw new Unsupported("harness: the property " + key + " holds a list with a " + v.getClass()
                    .getSimpleName().toLowerCase(Locale.ROOT) + " (" + valueText + "), a kind the model does not hold"
                    + " in a list yet");
        };
    }

    /** A local date-time as the oracle spells it: no zone offset after the time. */
    private static final java.util.regex.Pattern LOCAL_DATE_TIME = java.util.regex.Pattern.compile(
            "DateTime\\('(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2})\\.(\\d{1,9})'\\)");

    /** A stored value as the seed binds it (a value class's cells are {@link #embeddedCells}'). */
    private static Object cell(String text) {
        CypherValues.Value v = CypherValues.parse(text);
        return switch (v) {
            case CypherValues.Int i -> i.value().longValueExact();
            case CypherValues.Flt f -> f.value();
            case CypherValues.Str s -> s.value();
            case CypherValues.Bool b -> b.value();
            // a date or date-time: the text the seed binds (java.sql.Date.valueOf, Timestamp.valueOf)
            case CypherValues.Opaque o when o.text().startsWith("Date('") -> o.text().substring(6,
                    o.text().length() - 2);
            // a time of day: its text to the microsecond (the seed binds a LocalTime)
            case CypherValues.Opaque o when LOCAL_TIME.matcher(o.text()).matches() -> {
                java.util.regex.Matcher m = LOCAL_TIME.matcher(o.text());
                m.matches();
                String fraction = m.group(2);
                yield m.group(1) + "." + (fraction.length() > 6 ? fraction.substring(0, 6) : fraction);
            }
            case CypherValues.Lst l -> {
                try {
                    yield jsonOf(l, "", text);
                } catch (Unsupported e) {
                    throw new IllegalStateException(e);
                }
            }
            case CypherValues.Opaque o when LOCAL_DATE_TIME.matcher(o.text()).matches() -> {
                java.util.regex.Matcher m = LOCAL_DATE_TIME.matcher(o.text());
                m.matches();
                String fraction = m.group(2);
                yield m.group(1).replace('T', ' ') + "." + (fraction.length() > 6 ? fraction.substring(0, 6)
                        : fraction);
            }
            default -> throw new IllegalStateException("a cell of a kind the inference refused: " + text);
        };
    }

    private static String sqlType(String pureType) {
        return switch (pureType) {
            case "Integer" -> "BIGINT";
            case "Float" -> "DOUBLE PRECISION";
            case "String" -> "VARCHAR(4000)";
            case "Boolean" -> "BOOLEAN";
            case "StrictDate" -> "DATE";
            case "DateTime" -> "TIMESTAMP";
            case "StrictTime" -> "TIME";
            case VARIANT -> "JSONB";
            default -> throw new IllegalStateException(pureType);
        };
    }

    private static String relationalType(String pureType) {
        return switch (pureType) {
            case "Integer" -> "BIGINT";
            case "Float" -> "DOUBLE";
            case "String" -> "VARCHAR(4000)";
            case "Boolean" -> "BIT";
            case "StrictDate" -> "DATE";
            case "DateTime" -> "TIMESTAMP";
            case "StrictTime" -> "TIME";
            case VARIANT -> "SEMISTRUCTURED";
            default -> throw new IllegalStateException(pureType);
        };
    }

    /** A key over the dump: two cases whose graphs are one graph seed the lane once. */
    private static String graphKey(CypherAnswers.Graph g) {
        StringBuilder text = new StringBuilder();
        for (CypherAnswers.GraphNode n : g.nodes()) {
            text.append(n.id()).append(' ').append(n.labels()).append(' ').append(n.props()).append('\n');
        }
        for (CypherAnswers.GraphRel r : g.rels()) {
            text.append(r.id()).append(' ').append(r.start()).append("->").append(r.end()).append(' ')
                    .append(r.type()).append(' ').append(r.props()).append('\n');
        }
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.toString().getBytes(
                    StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
