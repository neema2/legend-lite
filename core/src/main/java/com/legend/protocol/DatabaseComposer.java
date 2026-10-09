// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###Relational}'s database and {@code ###RelationalMapper}'s mapper as upstream prints them
 * ({@code RelationalGrammarComposerExtension.renderDatabase}, {@code renderRelationalMapper} and
 * {@code HelperRelationalGrammarComposer}'s schemas, tables, views, columns and milestoning) -- over the records
 * ({@link Protocol.PDatabase}, {@link Protocol.PRelationalMapper}; the protocol program's leg 2, step 3).
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

    static String database(Protocol.PDatabase db, PureComposer.Style style) {
        List<Protocol.PDbSchema> nonDefault = new ArrayList<>();
        Protocol.PDbSchema defaultSchema = null;
        for (Protocol.PDbSchema s : db.schemas()) {
            if (DEFAULT_SCHEMA.equals(s.name())) {
                defaultSchema = defaultSchema == null ? s : defaultSchema;
            } else {
                nonDefault.add(s);
            }
        }
        String path = Composing.elementPath(db.pkg(), db.name());
        RelationalOperations ops = new RelationalOperations("", path, false, style);
        StringBuilder b = new StringBuilder(DomainComposer.declarationPrefix("Database", "", db.stereotypes(),
                db.taggedValues())).append(path).append("\n(\n");
        boolean nonEmpty = false;
        List<String> includes = new ArrayList<>();
        for (Protocol.PPointer p : db.includedStores()) {
            includes.add(TAB + "include " + Composing.convertPath(p.path()));
        }
        if (!includes.isEmpty()) {
            b.append(String.join("\n", includes)).append("\n");
            nonEmpty = true;
        }
        List<String> specs = new ArrayList<>();
        for (Protocol.PIncludedStoreSpec spec : db.includedStoreSpecifications()) {
            specs.add(TAB + "include " + spec.storeType() + " " + spec.path());
        }
        if (!specs.isEmpty()) {
            b.append(String.join("\n", specs)).append("\n");
            nonEmpty = true;
        }
        if (!nonDefault.isEmpty()) {
            b.append(nonEmpty ? "\n" : "");
            List<String> out = new ArrayList<>();
            for (Protocol.PDbSchema s : nonDefault) {
                out.add(schema(s, ops));
            }
            b.append(String.join("\n", out)).append("\n");
            nonEmpty = true;
        }
        if (defaultSchema != null) {
            nonEmpty = section(b, nonEmpty, defaultSchema.tables(), t -> table(t, 1, ops));
            nonEmpty = section(b, nonEmpty, defaultSchema.tabularFunctions(), t -> tabularFunction(t, 1));
            nonEmpty = section(b, nonEmpty, defaultSchema.views(), v -> view(v, 1, ops));
        }
        List<String> joins = new ArrayList<>();
        for (Protocol.PDbJoin j : db.joins()) {
            joins.add(TAB + "Join " + convertIdentifier(j.name()) + "(" + ops.render(j.operation()) + ")");
        }
        if (!joins.isEmpty()) {
            b.append(nonEmpty ? "\n" : "").append(String.join("\n", joins)).append("\n");
            nonEmpty = true;
        }
        List<String> filters = new ArrayList<>();
        for (Protocol.PDbFilter f : db.filters()) {
            filters.add(TAB + ("multigrain".equals(f.filterType()) ? "MultiGrainFilter " : "Filter ")
                    + convertIdentifier(f.name()) + "(" + ops.render(f.operation()) + ")");
        }
        if (!filters.isEmpty()) {
            b.append(nonEmpty ? "\n" : "").append(String.join("\n", filters)).append("\n");
        }
        return b.append(")").toString();
    }

    /** Appends one block of a schema's members, a blank line before it when something precedes it. */
    private static <T> boolean section(StringBuilder b, boolean nonEmpty, List<T> members,
            java.util.function.Function<T, String> print) {
        if (members.isEmpty()) {
            return nonEmpty;
        }
        List<String> out = new ArrayList<>();
        for (T m : members) {
            out.add(print.apply(m));
        }
        b.append(nonEmpty ? "\n" : "").append(String.join("\n", out)).append("\n");
        return true;
    }

    private static String schema(Protocol.PDbSchema schema, RelationalOperations ops) {
        StringBuilder b = new StringBuilder(TAB)
                .append(DomainComposer.declarationPrefix("Schema", TAB, schema.stereotypes(), schema.taggedValues()))
                .append(schema.name()).append("\n").append(TAB).append("(\n");
        boolean nonEmpty = false;
        List<String> tables = new ArrayList<>();
        for (Protocol.PDbTable t : schema.tables()) {
            tables.add(table(t, 2, ops));
        }
        if (!tables.isEmpty()) {
            b.append(String.join("\n", tables)).append("\n");
            nonEmpty = true;
        }
        List<String> views = new ArrayList<>();
        for (Protocol.PDbView v : schema.views()) {
            views.add(view(v, 2, ops));
        }
        if (!views.isEmpty()) {
            b.append(nonEmpty ? "\n" : "").append(String.join("\n", views)).append("\n");
        }
        List<String> functions = new ArrayList<>();
        for (Protocol.PDbTable f : schema.tabularFunctions()) {
            functions.add(tabularFunction(f, 2));
        }
        if (!functions.isEmpty()) {
            b.append(nonEmpty ? "\n" : "").append(String.join("\n", functions)).append("\n");
        }
        return b.append(TAB).append(")").toString();
    }

    private static String table(Protocol.PDbTable table, int indent, RelationalOperations ops) {
        StringBuilder b = new StringBuilder(tab(indent))
                .append(DomainComposer.declarationPrefix("Table", tab(indent), table.stereotypes(), table.taggedValues()))
                .append(table.name()).append("\n").append(tab(indent)).append("(\n");
        boolean nonEmpty = false;
        if (!table.milestoning().isEmpty()) {
            List<String> ms = new ArrayList<>();
            for (Protocol.PMilestoning m : table.milestoning()) {
                ms.add(tab(indent + 2) + milestoning(m));
            }
            b.append(tab(indent + 1)).append("milestoning\n").append(tab(indent + 1)).append("(\n")
                    .append(String.join(",\n", ms)).append("\n").append(tab(indent + 1)).append(")\n");
            nonEmpty = true;
        }
        List<String> columns = new ArrayList<>();
        for (Protocol.PDbColumn c : table.columns()) {
            columns.add(column(c, table.primaryKey(), indent + 1));
        }
        if (!columns.isEmpty()) {
            b.append(nonEmpty ? "\n" : "").append(String.join(",\n", columns)).append("\n");
        }
        return b.append(tab(indent)).append(")").toString();
    }

    private static String tabularFunction(Protocol.PDbTable function, int indent) {
        List<String> columns = new ArrayList<>();
        for (Protocol.PDbColumn c : function.columns()) {
            columns.add(column(c, List.of(), indent + 1));
        }
        return tab(indent) + "TabularFunction " + function.name() + "\n" + tab(indent) + "(\n"
                + (columns.isEmpty() ? "" : String.join(",\n", columns) + "\n") + tab(indent) + ")";
    }

    /** {@code renderDatabaseTableColumn}. */
    private static String column(Protocol.PDbColumn column, List<String> primaryKey, int indent) {
        String name = column.name();
        StringBuilder b = new StringBuilder(tab(indent))
                .append(DomainComposer.documentationOnly(column.taggedValues(), tab(indent)))
                .append(name.startsWith("\"") && name.endsWith("\"") ? name : convertIdentifierDoubleQuoted(name))
                .append(" ")
                .append(DomainComposer.annotations(column.stereotypes(),
                        DomainComposer.withoutDocumentation(column.taggedValues())))
                .append(columnType(column.type()));
        if (primaryKey.contains(name)) {
            b.append(" PRIMARY KEY");
        } else if (!column.nullable()) {
            b.append(" NOT NULL");
        }
        return b.toString();
    }

    private static String columnType(Protocol.PDbType type) {
        String t = type.kind();
        String plain = PLAIN_TYPES.get(t);
        if (plain != null) {
            return plain;
        }
        String sized = SIZED_TYPES.get(t);
        if (sized != null) {
            return sized + "(" + number(type.size(), t, "size") + ")";
        }
        String scaled = SCALED_TYPES.get(t);
        if (scaled != null) {
            return scaled + "(" + number(type.precision(), t, "precision") + ", " + number(type.scale(), t, "scale") + ")";
        }
        throw Composing.refused("no composer rule for a column type of _type '" + t + "'");
    }

    private static String number(@com.legend.base.Nullable Long n, String type, String what) {
        if (n == null) {
            throw Composing.refused("a column type '" + type + "' with no " + what);
        }
        return Long.toString(n);
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
    private static String milestoning(Protocol.PMilestoning m) {
        return switch (m) {
            case Protocol.PBusinessMilestoning b -> "business(BUS_FROM = " + b.from() + ", BUS_THRU = " + b.thru()
                    + (b.thruIsInclusive() ? ", THRU_IS_INCLUSIVE = true" : "") + infinityDate(b.infinityDate()) + ")";
            case Protocol.PBusinessSnapshotMilestoning b -> "business(BUS_SNAPSHOT_DATE = " + b.snapshotDate() + ")";
            case Protocol.PProcessingMilestoning p -> "processing(PROCESSING_IN = " + p.in() + ", PROCESSING_OUT = "
                    + p.out() + (p.outIsInclusive() ? ", OUT_IS_INCLUSIVE = true" : "") + infinityDate(p.infinityDate())
                    + ")";
            case Protocol.PProcessingSnapshotMilestoning p ->
                    "processing(PROCESSING_SNAPSHOT_DATE = " + p.snapshotDate() + ")";
        };
    }

    private static String infinityDate(Protocol.@com.legend.base.Nullable PDateTimeLit infinity) {
        return infinity == null ? "" : ", INFINITY_DATE = " + PureComposer.dateLiteral(infinity.value());
    }

    /** {@code renderDatabaseView}. */
    private static String view(Protocol.PDbView view, int indent, RelationalOperations ops) {
        StringBuilder b = new StringBuilder(tab(indent))
                .append(DomainComposer.declarationPrefix("View", tab(indent), view.stereotypes(), view.taggedValues()))
                .append(view.name()).append("\n").append(tab(indent)).append("(\n");
        Protocol.PViewFilter filter = view.filter();
        if (filter != null) {
            b.append(tab(indent + 1)).append(RelationalOperations.filterMapping(filter.db(), filter.name(),
                    filter.joins())).append("\n");
        }
        List<Protocol.PRelOp> groupBy = view.groupBy();
        if (groupBy != null && !groupBy.isEmpty()) {
            List<String> gs = new ArrayList<>();
            for (Protocol.PRelOp g : groupBy) {
                gs.add(tab(indent + 2) + ops.render(g));
            }
            b.append(tab(indent + 1)).append("~groupBy\n").append(tab(indent + 1)).append("(\n")
                    .append(String.join(",\n", gs)).append("\n").append(tab(indent + 1)).append(")\n");
        }
        if (view.distinct()) {
            b.append(tab(indent + 1)).append("~distinct\n");
        }
        List<String> columns = new ArrayList<>();
        for (Protocol.PViewColumnMapping cm : view.columnMappings()) {
            columns.add(tab(indent + 1) + cm.name() + ": " + ops.render(cm.operation())
                    + (view.primaryKey().contains(cm.name()) ? " PRIMARY KEY" : ""));
        }
        if (!columns.isEmpty()) {
            b.append(String.join(",\n", columns)).append("\n");
        }
        return b.append(tab(indent)).append(")").toString();
    }

    // ---------------------------------------------------------------------
    // ###RelationalMapper
    // ---------------------------------------------------------------------

    static String relationalMapper(Protocol.PRelationalMapper mapper) {
        StringBuilder b = new StringBuilder("RelationalMapper ").append(Composing.elementPath(mapper.pkg(), mapper.name()))
                .append("\n(\n");
        List<String> databases = new ArrayList<>();
        for (Protocol.PDatabaseMapper d : mapper.databaseMappers()) {
            List<String> schemas = new ArrayList<>();
            for (Protocol.PSchemaPointer s : d.schemas()) {
                schemas.add(s.database() + "." + s.schema());
            }
            databases.add(tab(3) + "[" + String.join(", ", schemas) + "] -> '" + d.databaseName() + "'");
        }
        mapperSection(b, "DatabaseMappers", databases);
        List<String> schemas = new ArrayList<>();
        for (Protocol.PSchemaMapper2 s : mapper.schemaMappers()) {
            schemas.add(tab(3) + s.from().database() + "." + s.from().schema() + " -> '" + s.to() + "'");
        }
        mapperSection(b, "SchemaMappers", schemas);
        List<String> tables = new ArrayList<>();
        for (Protocol.PTableMapper2 t : mapper.tableMappers()) {
            Protocol.PTablePointer2 from = t.from();
            tables.add(tab(3) + from.database() + "." + from.schema() + "." + from.table() + " -> '" + t.to() + "'");
        }
        mapperSection(b, "TableMappers", tables);
        return b.append(")").toString();
    }

    private static void mapperSection(StringBuilder b, String name, List<String> lines) {
        if (!lines.isEmpty()) {
            b.append("   ").append(name).append(":\n   [\n").append(String.join(",\n", lines)).append("\n   ];\n");
        }
    }
}
