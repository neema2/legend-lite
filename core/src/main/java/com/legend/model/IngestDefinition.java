// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.model;

import com.legend.protocol.Protocol;
import com.legend.protocol.TypeExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A {@code ###Lakehouse} Ingest as the model holds it (docs/DEEPHAVEN_TABLES_AND_SOURCES_2026_10_29.md §3a; the
 * declared extension LITE-DESIGN-lakehouse): where datasets land, how their rows arrive and how they land, kept as the
 * protocol record the parser or the reader made.
 *
 * <p>An ingest makes a database of its own, as Legend's does (legend-studio's round-trip fixtures call them
 * "ingest-generated databases"): {@link #database} -- one table for each dataset, in the schema the dataset group
 * names, else the default one. A store's {@code include Ingest x::Y} includes that database, so a mapping reads a
 * dataset as any table. A dataset whose columns are written out takes them as written; one made by a plain read of
 * another store's table ({@code |#>{db::Topics."tp.trades"}#}, every stream and pull of the desk) takes that table's
 * columns, so nothing is written twice. Every other function makes a materialized view, whose columns need its body
 * typed: refused by name until that lands (docs/DESK_DATA_ROADMAP_2026_10_29.md stage F).
 *
 * <p>The checks the design's §7 asks of an ingest run here, where its tables are made: a dataset's key is among its
 * columns, a batch milestoned dataset has a key, a {@code retention} is only on appended data, a pulled dataset is a
 * query of another store, a delete indicator's field is each of the ingest's datasets' and none is appended. A batch
 * milestoned dataset lands with upstream's three columns after its own -- {@code LAKE_DIGEST} VARCHAR(64), {@code
 * LAKE_IN_ID} and {@code LAKE_OUT_ID} BIGINT, none null (legend-studio's lakehouse Cube round trip) -- its latest rows
 * {@code LAKE_OUT_ID = 999999999} (docs/DEPLOYMENT_DESIGN_2026_11_14.md §5). Business temporal milestoning adds two more
 * lite does not make yet: refused by name.
 */
public record IngestDefinition(String qualifiedName, Protocol.PIngest ingest) implements PackageableElement {

    /** Where a dataset made by a read of another table finds that table: the store's tables through its includes. */
    @FunctionalInterface
    public interface TableLookup {
        /** {@code schema} null for a table named without one; both names bare (their quotes off). */
        Optional<DatabaseDefinition.TableDefinition> find(String database, @com.legend.base.Nullable String schema,
                String table);
    }

    /** The write mode a dataset lands by, on the wire: its own, else the ingest's; null when neither says. */
    public @com.legend.base.Nullable String writeModeOf(Protocol.PIngestDataset d) {
        return d.writeMode() != null ? d.writeMode() : ingest.writeMode();
    }

    /** A read of another store's table: the store, the schema if one is written, the table -- the names bare, as
     *  the store declares them ({@code "tp.trades"} is the table {@code tp.trades}). */
    public record TableRead(String database, @com.legend.base.Nullable String schema, String table) {
    }

    /** The table a dataset made by a plain read reads, or null for any other source. */
    public static @com.legend.base.Nullable TableRead tableRead(Protocol.PIngestDataset d) {
        if (!(d.source() instanceof Protocol.PIngestFunction f) || !f.function().parameters().isEmpty()
                || f.function().body().size() != 1
                || !(f.function().body().get(0) instanceof com.legend.protocol.spec.AppliedFunction call)
                || !call.isTableReference() || call.parameters().size() != 2
                || !(call.parameters().get(0) instanceof com.legend.protocol.spec.PackageableElementPtr db)
                || !(call.parameters().get(1) instanceof com.legend.protocol.spec.CString table)) {
            return null;
        }
        List<String> parts = RelationalIdentifier.parts(table.value());
        if (parts.size() > 2) {
            return null;
        }
        String last = RelationalIdentifier.bare(parts.get(parts.size() - 1));
        return new TableRead(db.fullPath(), parts.size() == 2 ? RelationalIdentifier.bare(parts.get(0)) : null, last);
    }

    /** The database this ingest makes, its tables' columns and keys; refused by name where the design's checks fail. A
     *  dataset that reads another store's table takes its key as it takes its columns, the table's, unless it writes
     *  its own ({@code pk=}; docs/DEPLOYMENT_DESIGN_2026_11_14.md §4). */
    public DatabaseDefinition database(TableLookup tables) {
        boolean pull = ingest.schedule() != null;
        Protocol.PIngestDeleteIndicator delete = ingest.readMode() == null ? null : ingest.readMode().deleteIndicator();
        List<DatabaseDefinition.TableDefinition> made = new ArrayList<>();
        for (Protocol.PIngestDataset d : ingest.datasets()) {
            String mode = writeModeOf(d);
            String where = "the ingest '" + qualifiedName + "''s dataset '" + d.name() + "'";
            if ("batch_milestoned_business_temporal".equals(mode)) {
                throw new IllegalArgumentException(where + " is batch milestoned business temporal: the LAKE_FROM and"
                        + " LAKE_THRU columns it lands with are not made yet");
            }
            boolean milestoned = "batch_milestoned".equals(mode);
            if (d.retention() != null && !"append_only".equals(mode)) {
                throw new IllegalArgumentException(where + " keeps a retention, which only appended data has"
                        + " (writeMode=AppendOnly)");
            }
            if (delete != null && !milestoned) {
                throw new IllegalArgumentException(where + " is not batch milestoned, so a delete indicator has no key"
                        + " to remove: an ingest with deletes lands its datasets BatchMilestoned");
            }
            List<String> key = d.primaryKey();
            List<DatabaseDefinition.ColumnDefinition> read = new ArrayList<>();
            switch (d.source()) {
                case Protocol.PIngestColumns written -> {
                    if (pull) {
                        throw new IllegalArgumentException(where + " is pulled, so it is a query of the source's"
                                + " store (|#>{db.table}#), not columns written out");
                    }
                    for (TypeExpression.Column c : written.columns()) {
                        read.add(new DatabaseDefinition.ColumnDefinition(c.name(), columnType(where, c), false,
                                notNull(where, c)));
                    }
                }
                case Protocol.PIngestFunction f -> {
                    TableRead source = tableRead(d);
                    if (source == null) {
                        throw new IllegalArgumentException(where + " is made by a function other than a read of a"
                                + " table (|#>{db.table}#): a materialized view, which is not typed yet");
                    }
                    DatabaseDefinition.TableDefinition table = tables.find(source.database(), source.schema(),
                                    source.table())
                            .orElseThrow(() -> new IllegalArgumentException(where + " reads the table '"
                                    + (source.schema() == null ? "" : source.schema() + ".") + source.table() + "' of '"
                                    + source.database() + "', which it does not declare"));
                    read.addAll(table.columns());
                    if (key.isEmpty()) {
                        key = table.columns().stream().filter(DatabaseDefinition.ColumnDefinition::primaryKey)
                                .map(DatabaseDefinition.ColumnDefinition::name).toList();
                    }
                }
            }
            List<String> keyOf = key;
            for (String k : keyOf) {
                if (read.stream().noneMatch(c -> c.name().equals(k))) {
                    throw new IllegalArgumentException(where + "'s key '" + k + "' is not one of its columns");
                }
            }
            if (milestoned && keyOf.isEmpty()) {
                throw new IllegalArgumentException(where + " is batch milestoned and names no key (pk=[ ... ], or the"
                        + " read table's primary key): its versions are each key's");
            }
            if (delete != null && read.stream().noneMatch(c -> c.name().equals(delete.deleteField()))) {
                throw new IllegalArgumentException(where + " has no column '" + delete.deleteField() + "', the field"
                        + " its ingest's delete indicator reads");
            }
            List<DatabaseDefinition.ColumnDefinition> columns = new ArrayList<>();
            for (DatabaseDefinition.ColumnDefinition c : read) {
                columns.add(new DatabaseDefinition.ColumnDefinition(c.name(), c.dataType(), keyOf.contains(c.name()),
                        c.notNull(), c.quoted()));
            }
            if (milestoned) {
                for (String lake : List.of(com.legend.protocol.IngestWords.LAKE_DIGEST,
                        com.legend.protocol.IngestWords.LAKE_IN_ID, com.legend.protocol.IngestWords.LAKE_OUT_ID)) {
                    if (columns.stream().anyMatch(c -> c.name().equals(lake))) {
                        throw new IllegalArgumentException(where + " declares the column '" + lake + "', which its"
                                + " batch milestoning adds");
                    }
                }
                columns.add(new DatabaseDefinition.ColumnDefinition(com.legend.protocol.IngestWords.LAKE_DIGEST,
                        new RelationalDataType.Varchar(64), false, true));
                columns.add(new DatabaseDefinition.ColumnDefinition(com.legend.protocol.IngestWords.LAKE_IN_ID,
                        new RelationalDataType.BigInt(), false, true));
                columns.add(new DatabaseDefinition.ColumnDefinition(com.legend.protocol.IngestWords.LAKE_OUT_ID,
                        new RelationalDataType.BigInt(), false, true));
            }
            made.add(new DatabaseDefinition.TableDefinition(d.name(), columns));
        }
        // the model's layout (FromProtocol.toDatabaseDefinition): a schema is recorded only when named -- the dataset
        // group -- and the flat list holds every table, the bare-name lookup's mirror
        List<DatabaseDefinition.SchemaDefinition> schemas = ingest.datasetGroup() == null ? List.of()
                : List.of(new DatabaseDefinition.SchemaDefinition(ingest.datasetGroup(), made, List.of(), List.of()));
        return new DatabaseDefinition(qualifiedName, List.of(), schemas, made, List.of(), List.of(), List.of(),
                List.of(), List.of());
    }

    private static boolean notNull(String where, TypeExpression.Column c) {
        if (!(c.multiplicity() instanceof com.legend.protocol.Multiplicity.Concrete m)
                || m.upperBound() == null || m.upperBound() != 1 || m.lowerBound() > 1) {
            throw new IllegalArgumentException(where + "'s column '" + c.name() + "' holds more than one value: a"
                    + " table's column holds one ([1] or [0..1])");
        }
        return m.lowerBound() == 1;
    }

    private static final String PRECISE = "meta::pure::precisePrimitives::";

    /**
     * A written column's table type: Legend's precise primitives (meta::pure::precisePrimitives) as their relational
     * types, and the Pure types that name one exactly -- {@code Boolean} a BIT, {@code StrictDate} a DATE, {@code
     * DateTime} a TIMESTAMP, {@code String} a VARCHAR of no bound (the model's own reading of a bare VARCHAR). A type
     * that names no width ({@code Integer}, {@code Float}, {@code Number}, {@code Decimal} without its precision) is
     * refused by name, with the precise types to write.
     */
    private static RelationalDataType columnType(String where, TypeExpression.Column c) {
        String name;
        List<com.legend.protocol.spec.ValueSpecification> args;
        switch (c.type()) {
            case TypeExpression.NameRef n -> {
                name = n.name();
                args = List.of();
            }
            case TypeExpression.Generic g when g.arguments().isEmpty() && g.multiplicityArguments().isEmpty() -> {
                name = g.name();
                args = g.typeVariableValues();
            }
            default -> throw new IllegalArgumentException(where + "'s column '" + c.name() + "' has a type a table's"
                    + " column cannot hold");
        }
        String bare = name.startsWith(PRECISE) ? name.substring(PRECISE.length()) : name;
        int[] n = new int[args.size()];
        for (int i = 0; i < n.length; i++) {
            if (!(args.get(i) instanceof com.legend.protocol.spec.CInteger ci)) {
                throw new IllegalArgumentException(where + "'s column '" + c.name() + "': " + bare
                        + " takes whole numbers");
            }
            n[i] = ci.value().intValue();
        }
        RelationalDataType t = switch (bare) {
            case "TinyInt" -> n.length == 0 ? new RelationalDataType.TinyInt() : null;
            case "SmallInt" -> n.length == 0 ? new RelationalDataType.SmallInt() : null;
            case "Int" -> n.length == 0 ? new RelationalDataType.Integer_() : null;
            case "BigInt" -> n.length == 0 ? new RelationalDataType.BigInt() : null;
            case "Float4" -> n.length == 0 ? new RelationalDataType.Float_() : null;
            case "Double" -> n.length == 0 ? new RelationalDataType.Double_() : null;
            case "Varchar" -> n.length == 1 ? new RelationalDataType.Varchar(n[0]) : null;
            case "Decimal" -> n.length == 2 ? new RelationalDataType.Decimal(n[0], n[1]) : null;
            case "Numeric" -> n.length == 2 ? new RelationalDataType.Numeric(n[0], n[1]) : null;
            case "Timestamp", "DateTime" -> n.length == 0 ? new RelationalDataType.Timestamp() : null;
            case "Date", "StrictDate" -> n.length == 0 ? new RelationalDataType.Date_() : null;
            case "Boolean" -> n.length == 0 ? new RelationalDataType.Bit() : null;
            case "String" -> n.length == 0 ? new RelationalDataType.Varchar(Integer.MAX_VALUE) : null;
            default -> throw new IllegalArgumentException(where + "'s column '" + c.name() + "' is typed " + bare
                    + ", which names no table type: write TinyInt, SmallInt, Int, BigInt, Float4, Double,"
                    + " Varchar(n), Decimal(p, s), Numeric(p, s), Timestamp, StrictDate, Boolean or String");
        };
        if (t == null) {
            throw new IllegalArgumentException(where + "'s column '" + c.name() + "' writes " + bare + " with "
                    + n.length + " value(s) in parentheses");
        }
        return t;
    }
}
