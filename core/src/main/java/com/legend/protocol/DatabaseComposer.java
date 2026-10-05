// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.elementPath;
import static com.legend.protocol.Composing.objOr;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###Relational}'s database and {@code ###RelationalMapper}'s mapper as upstream prints them
 * ({@code RelationalGrammarComposerExtension.renderDatabase}, {@code renderRelationalMapper} and
 * {@code HelperRelationalGrammarComposer}'s schemas, tables, views, columns and milestoning).
 */
final class DatabaseComposer {

    private static final String DEFAULT_SCHEMA = "default";

    /** A column type with no arguments: its keyword. */
    private static final Map<String, String> PLAIN_TYPES = Map.ofEntries(
            Map.entry("Float", "FLOAT"), Map.entry("Double", "DOUBLE"), Map.entry("Real", "REAL"),
            Map.entry("Integer", "INTEGER"), Map.entry("BigInt", "BIGINT"), Map.entry("SmallInt", "SMALLINT"),
            Map.entry("TinyInt", "TINYINT"), Map.entry("Date", "DATE"), Map.entry("Timestamp", "TIMESTAMP"),
            Map.entry("Bit", "BIT"), Map.entry("Other", "OTHER"), Map.entry("SemiStructured", "SEMISTRUCTURED"),
            Map.entry("Json", "JSON"));
    /** A column type with a size: its keyword. */
    private static final Map<String, String> SIZED_TYPES = Map.of(
            "Char", "CHAR", "Varchar", "VARCHAR", "Binary", "BINARY", "Varbinary", "VARBINARY");
    /** A column type with a precision and scale: its keyword. */
    private static final Map<String, String> SCALED_TYPES = Map.of("Numeric", "NUMERIC", "Decimal", "DECIMAL");

    private DatabaseComposer() {
    }

    static String database(Json.Obj db) {
        List<Json.Obj> schemas = objs(db, "schemas");
        List<Json.Obj> nonDefault = new ArrayList<>();
        Json.Obj defaultSchema = null;
        for (Json.Obj s : schemas) {
            if (DEFAULT_SCHEMA.equals(s.getString("name"))) {
                defaultSchema = defaultSchema == null ? s : defaultSchema;
            } else {
                nonDefault.add(s);
            }
        }
        RelationalOperations ops = new RelationalOperations("", elementPath(db), false);
        StringBuilder b = new StringBuilder(DomainComposer.declarationPrefix("Database", "", db)).append(elementPath(db)).append("\n(\n");
        boolean nonEmpty = false;
        List<String> includes = new ArrayList<>();
        for (Json.Node n : Composing.items(db, "includedStores")) {
            includes.add(TAB + "include " + Composing.convertPath(pointerPath(n)));
        }
        if (!includes.isEmpty()) {
            b.append(String.join("\n", includes)).append("\n");
            nonEmpty = true;
        }
        List<String> specs = new ArrayList<>();
        for (Json.Obj spec : objs(db, "includedStoreSpecifications")) {
            specs.add(TAB + "include " + spec.getString("storeType") + " " + spec.getObj("packageableElementPointer").getString("path"));
        }
        if (!specs.isEmpty()) {
            b.append(String.join("\n", specs)).append("\n");
            nonEmpty = true;
        }
        if (!nonDefault.isEmpty()) {
            b.append(nonEmpty ? "\n" : "");
            List<String> out = new ArrayList<>();
            for (Json.Obj s : nonDefault) {
                out.add(schema(s, ops));
            }
            b.append(String.join("\n", out)).append("\n");
            nonEmpty = true;
        }
        if (defaultSchema != null) {
            nonEmpty = section(b, nonEmpty, objs(defaultSchema, "tables"), t -> table(t, 1, ops));
            nonEmpty = section(b, nonEmpty, objs(defaultSchema, "tabularFunctions"), t -> tabularFunction(t, 1));
            nonEmpty = section(b, nonEmpty, objs(defaultSchema, "views"), v -> view(v, 1, ops));
        }
        List<String> joins = new ArrayList<>();
        for (Json.Obj j : objs(db, "joins")) {
            joins.add(TAB + "Join " + convertIdentifier(j.getString("name")) + "(" + ops.render(j.get("operation")) + ")");
        }
        if (!joins.isEmpty()) {
            b.append(nonEmpty ? "\n" : "").append(String.join("\n", joins)).append("\n");
            nonEmpty = true;
        }
        List<String> filters = new ArrayList<>();
        for (Json.Obj f : objs(db, "filters")) {
            filters.add(TAB + ("multigrain".equals(Composing.type(f)) ? "MultiGrainFilter " : "Filter ")
                    + convertIdentifier(f.getString("name")) + "(" + ops.render(f.get("operation")) + ")");
        }
        if (!filters.isEmpty()) {
            b.append(nonEmpty ? "\n" : "").append(String.join("\n", filters)).append("\n");
        }
        return b.append(")").toString();
    }

    /** Appends one block of a schema's members, a blank line before it when something precedes it. */
    private static boolean section(StringBuilder b, boolean nonEmpty, List<Json.Obj> members, java.util.function.Function<Json.Obj, String> print) {
        if (members.isEmpty()) {
            return nonEmpty;
        }
        List<String> out = new ArrayList<>();
        for (Json.Obj m : members) {
            out.add(print.apply(m));
        }
        b.append(nonEmpty ? "\n" : "").append(String.join("\n", out)).append("\n");
        return true;
    }

    /** A packageable element pointer's path: {@code {"path":...}}, or a bare string on an older wire. */
    static String pointerPath(Json.Node n) {
        return n instanceof Json.Str s ? s.value() : Composing.obj(n, "pointer").getString("path");
    }

    private static String schema(Json.Obj schema, RelationalOperations ops) {
        StringBuilder b = new StringBuilder(TAB).append(DomainComposer.declarationPrefix("Schema", TAB, schema))
                .append(schema.getString("name")).append("\n").append(TAB).append("(\n");
        boolean nonEmpty = false;
        List<String> tables = new ArrayList<>();
        for (Json.Obj t : objs(schema, "tables")) {
            tables.add(table(t, 2, ops));
        }
        if (!tables.isEmpty()) {
            b.append(String.join("\n", tables)).append("\n");
            nonEmpty = true;
        }
        List<String> views = new ArrayList<>();
        for (Json.Obj v : objs(schema, "views")) {
            views.add(view(v, 2, ops));
        }
        if (!views.isEmpty()) {
            b.append(nonEmpty ? "\n" : "").append(String.join("\n", views)).append("\n");
        }
        List<String> functions = new ArrayList<>();
        for (Json.Obj f : objs(schema, "tabularFunctions")) {
            functions.add(tabularFunction(f, 2));
        }
        if (!functions.isEmpty()) {
            b.append(nonEmpty ? "\n" : "").append(String.join("\n", functions)).append("\n");
        }
        return b.append(TAB).append(")").toString();
    }

    private static String table(Json.Obj table, int indent, RelationalOperations ops) {
        StringBuilder b = new StringBuilder(tab(indent)).append(DomainComposer.declarationPrefix("Table", tab(indent), table))
                .append(table.getString("name")).append("\n").append(tab(indent)).append("(\n");
        boolean nonEmpty = false;
        List<Json.Obj> milestoning = objs(table, "milestoning");
        if (!milestoning.isEmpty()) {
            List<String> ms = new ArrayList<>();
            for (Json.Obj m : milestoning) {
                ms.add(milestoning(m, indent + 2));
            }
            b.append(tab(indent + 1)).append("milestoning\n").append(tab(indent + 1)).append("(\n")
                    .append(String.join(",\n", ms)).append("\n").append(tab(indent + 1)).append(")\n");
            nonEmpty = true;
        }
        List<String> primaryKey = table.getStringArrayOr("primaryKey", List.of());
        List<String> columns = new ArrayList<>();
        for (Json.Obj c : objs(table, "columns")) {
            columns.add(column(c, primaryKey, indent + 1));
        }
        if (!columns.isEmpty()) {
            b.append(nonEmpty ? "\n" : "").append(String.join(",\n", columns)).append("\n");
        }
        return b.append(tab(indent)).append(")").toString();
    }

    private static String tabularFunction(Json.Obj function, int indent) {
        List<String> columns = new ArrayList<>();
        for (Json.Obj c : objs(function, "columns")) {
            columns.add(column(c, List.of(), indent + 1));
        }
        return tab(indent) + "TabularFunction " + function.getString("name") + "\n" + tab(indent) + "(\n"
                + (columns.isEmpty() ? "" : String.join(",\n", columns) + "\n") + tab(indent) + ")";
    }

    /** {@code renderDatabaseTableColumn}. */
    private static String column(Json.Obj column, List<String> primaryKey, int indent) {
        String name = column.getString("name");
        List<Json.Obj> taggedValues = objs(column, "taggedValues");
        StringBuilder b = new StringBuilder(tab(indent))
                .append(DomainComposer.documentationOnly(taggedValues, tab(indent)))
                .append(name.startsWith("\"") && name.endsWith("\"") ? name : convertIdentifierDoubleQuoted(name))
                .append(" ")
                .append(DomainComposer.annotations(objs(column, "stereotypes"), DomainComposer.withoutDocumentation(taggedValues)))
                .append(columnType(column.getObj("type")));
        if (primaryKey.contains(name)) {
            b.append(" PRIMARY KEY");
        } else if (!column.getBoolOr("nullable", true)) {
            b.append(" NOT NULL");
        }
        return b.toString();
    }

    private static String columnType(Json.Obj type) {
        String t = Composing.type(type);
        String plain = PLAIN_TYPES.get(t);
        if (plain != null) {
            return plain;
        }
        String sized = SIZED_TYPES.get(t);
        if (sized != null) {
            return sized + "(" + number(type, "size") + ")";
        }
        String scaled = SCALED_TYPES.get(t);
        if (scaled != null) {
            return scaled + "(" + number(type, "precision") + ", " + number(type, "scale") + ")";
        }
        throw Composing.refused("no composer rule for a column type of _type '" + t + "'");
    }

    private static String number(Json.Obj o, String key) {
        return Long.toString(((Json.Num) o.get(key)).longValue());
    }

    /** {@code convertIdentifier(val, true)}: bare when it is one, else double-quoted. */
    static String convertIdentifierDoubleQuoted(String val) {
        if (val.isEmpty()) {
            return "";
        }
        String bare = PureComposer.convertIdentifier(val);
        return bare.equals(val) ? val : "\"" + PureComposer.escapeJava(val).replace("'", "\\'") + "\"";
    }

    /** {@code visitMilestoning}. */
    private static String milestoning(Json.Obj m, int indent) {
        java.util.function.Function<Json.Obj, String> printer = MILESTONING.get(Composing.type(m));
        if (printer == null) {
            throw Composing.refused("no composer rule for milestoning of _type '" + Composing.type(m) + "'");
        }
        return tab(indent) + printer.apply(m);
    }

    /** Each milestoning {@code _type}'s printer. */
    private static final Map<String, java.util.function.Function<Json.Obj, String>> MILESTONING = Map.of(
            "businessMilestoning", m -> "business(BUS_FROM = " + m.getString("from") + ", BUS_THRU = " + m.getString("thru")
                    + (m.getBoolOr("thruIsInclusive", false) ? ", THRU_IS_INCLUSIVE = true" : "") + infinityDate(m) + ")",
            "businessSnapshotMilestoning", m -> "business(BUS_SNAPSHOT_DATE = " + m.getString("snapshotDate") + ")",
            "processingMilestoning", m -> "processing(PROCESSING_IN = " + m.getString("in") + ", PROCESSING_OUT = " + m.getString("out")
                    + (m.getBoolOr("outIsInclusive", false) ? ", OUT_IS_INCLUSIVE = true" : "") + infinityDate(m) + ")",
            "processingSnapshotMilestoning", m -> "processing(PROCESSING_SNAPSHOT_DATE = " + m.getString("snapshotDate") + ")");

    private static String infinityDate(Json.Obj m) {
        Json.Obj infinity = objOr(m, "infinityDate");
        return infinity == null ? "" : ", INFINITY_DATE = " + Composing.valueSpecification(infinity);
    }

    /** {@code renderDatabaseView}. */
    private static String view(Json.Obj view, int indent, RelationalOperations ops) {
        StringBuilder b = new StringBuilder(tab(indent)).append(DomainComposer.declarationPrefix("View", tab(indent), view))
                .append(view.getString("name")).append("\n").append(tab(indent)).append("(\n");
        Json.Obj filter = objOr(view, "filter");
        if (filter != null) {
            b.append(tab(indent + 1)).append(RelationalOperations.filterMapping(filter)).append("\n");
        }
        List<Json.Obj> groupBy = objs(view, "groupBy");
        if (!groupBy.isEmpty()) {
            List<String> gs = new ArrayList<>();
            for (Json.Obj g : groupBy) {
                gs.add(tab(indent + 2) + ops.render(g));
            }
            b.append(tab(indent + 1)).append("~groupBy\n").append(tab(indent + 1)).append("(\n")
                    .append(String.join(",\n", gs)).append("\n").append(tab(indent + 1)).append(")\n");
        }
        if (view.getBoolOr("distinct", false)) {
            b.append(tab(indent + 1)).append("~distinct\n");
        }
        List<String> primaryKey = view.getStringArrayOr("primaryKey", List.of());
        List<String> columns = new ArrayList<>();
        for (Json.Obj cm : objs(view, "columnMappings")) {
            columns.add(tab(indent + 1) + cm.getString("name") + ": " + ops.render(cm.get("operation"))
                    + (primaryKey.contains(cm.getString("name")) ? " PRIMARY KEY" : ""));
        }
        if (!columns.isEmpty()) {
            b.append(String.join(",\n", columns)).append("\n");
        }
        return b.append(tab(indent)).append(")").toString();
    }

    // ---------------------------------------------------------------------
    // ###RelationalMapper
    // ---------------------------------------------------------------------

    static String relationalMapper(Json.Obj mapper) {
        StringBuilder b = new StringBuilder("RelationalMapper ").append(elementPath(mapper)).append("\n(\n");
        List<String> databases = new ArrayList<>();
        for (Json.Obj d : objs(mapper, "databaseMappers")) {
            List<String> schemas = new ArrayList<>();
            for (Json.Obj s : objs(d, "schemas")) {
                schemas.add(schemaRef(s));
            }
            databases.add(tab(3) + "[" + String.join(", ", schemas) + "] -> '" + d.getString("databaseName") + "'");
        }
        mapperSection(b, "DatabaseMappers", databases);
        List<String> schemas = new ArrayList<>();
        for (Json.Obj s : objs(mapper, "schemaMappers")) {
            schemas.add(tab(3) + schemaRef(s.getObj("from")) + " -> '" + s.getString("to") + "'");
        }
        mapperSection(b, "SchemaMappers", schemas);
        List<String> tables = new ArrayList<>();
        for (Json.Obj t : objs(mapper, "tableMappers")) {
            Json.Obj from = t.getObj("from");
            tables.add(tab(3) + from.getString("database") + "." + from.getString("schema") + "." + from.getString("table")
                    + " -> '" + t.getString("to") + "'");
        }
        mapperSection(b, "TableMappers", tables);
        return b.append(")").toString();
    }

    private static void mapperSection(StringBuilder b, String name, List<String> lines) {
        if (!lines.isEmpty()) {
            b.append("   ").append(name).append(":\n   [\n").append(String.join(",\n", lines)).append("\n   ];\n");
        }
    }

    private static String schemaRef(Json.Obj s) {
        return str(s, "database") + "." + str(s, "schema");
    }
}
