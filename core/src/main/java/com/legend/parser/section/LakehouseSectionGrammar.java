// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser.section;

import com.legend.lexer.TokenType;
import com.legend.parser.TokenStreamCursor;
import com.legend.protocol.Protocol;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * THE {@code ###Lakehouse} grammar: the {@code Ingest} element (docs/DEEPHAVEN_TABLES_AND_SOURCES_2026_10_29.md §3a),
 * a declared lite extension (docs/OWN_CORPUS_DECISIONS.md LITE-DESIGN-lakehouse). Legend's own grammar is closed --
 * legend-engine's public history never held it -- so the form Studio shows written is kept exactly,
 *
 * <pre>
 * Ingest test::IngestDefinition Snapshot&lt;CSV&gt; owner=AppDir(production='297484')
 * [
 *   TEST_TABLE( int_val: Int, varchar_val: Varchar(500) ) pk=[ int_val ];
 * ]
 * </pre>
 *
 * and every other field is spelled by that form's conventions, {@code key=value} on the header and each dataset, the
 * key the wire's field name: {@code datasetGroup=}, {@code writeMode=}, {@code deleteIndicator=}, {@code retention=},
 * {@code privacyClassification=} and the partition and cluster column lists. The read modes and formats are Legend's
 * ({@code Snapshot}, {@code Delta}; {@code CSV}, {@code Parquet}, {@code JSON}, {@code Avro}); an ingest of materialized
 * views alone writes none, as legend-studio's fixtures hold one. Lite's additions (docs/DEPLOYMENT_DESIGN_2026_11_14.md
 * §5): a pull's {@code schedule=}, a dataset's {@code retention}, and a dataset made by a read of another store's table
 * ({@code |#>{db.table}#}). Lite's retired words ({@code Stream}, {@code Pull<...>}, {@code LatestByKey}) are refused
 * by name with their replacements.
 *
 * <p>The engine's dialect never reaches here: it refuses the section as unknown, in its own words
 * ({@code PmcdParser}). The SPI's re-lex does (at the engine's dialect), and is refused the same way.
 */
public final class LakehouseSectionGrammar implements ElementwiseSectionGrammar {

    /** The one stateless instance the registry hands out. */
    public static final LakehouseSectionGrammar INSTANCE = new LakehouseSectionGrammar();

    private LakehouseSectionGrammar() {
    }

    @Override
    public String name() {
        return "Lakehouse";
    }

    @Override
    public String qualifiedNameOf(Protocol.Element e) {
        return Protocol.pathOf(e);
    }

    @Override
    public Protocol.Element parseOne(TokenStreamCursor c) {
        if (c.dialect().refusesLiteExtensions()) {
            throw c.error("'Lakehouse' is not a known section parser");
        }
        if (!c.isIdentifierToken(c.peek())) {
            throw c.error("unsupported ###Lakehouse element: " + c.safeText());
        }
        return switch (c.safeText()) {
            case "Ingest" -> parseIngest(c);
            case "Deployment" -> parseDeployment(c);
            default -> throw c.error("unsupported ###Lakehouse element: " + c.safeText() + " (Ingest, Deployment)");
        };
    }

    /**
     * {@code Deployment qn { deploys: [ ... ]; sources: [ conn: [ store, ... ], ... ]; live: conn keep [ ... ];
     * history: conn keep [ ... ]; }} -- lite's (docs/DEPLOYMENT_DESIGN_2026_11_14.md §8): every field once, the
     * deployed elements and a target required.
     */
    private static Protocol.PDeployment parseDeployment(TokenStreamCursor c) {
        SectionParse.Head h = SectionParse.head(c, "Deployment");
        c.expect(TokenType.BRACE_OPEN);
        List<String> deploys = null;
        List<Protocol.PDeploymentSource> sources = List.of();
        Protocol.PDeploymentTarget live = null;
        Protocol.PDeploymentTarget history = null;
        Set<String> seen = new HashSet<>();
        while (c.peek() != TokenType.BRACE_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seen, key, c);
            c.expect(TokenType.COLON);
            switch (key) {
                case "deploys" -> deploys = pathList(c);
                case "sources" -> {
                    c.expect(TokenType.BRACKET_OPEN);
                    List<Protocol.PDeploymentSource> out = new ArrayList<>();
                    while (c.peek() != TokenType.BRACKET_CLOSE) {
                        String connection = c.parseQualifiedName();
                        c.expect(TokenType.COLON);
                        out.add(new Protocol.PDeploymentSource(connection, pathList(c)));
                        if (c.peek() != TokenType.BRACKET_CLOSE) {
                            c.expect(TokenType.COMMA);
                        }
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                    sources = List.copyOf(out);
                }
                case "live" -> live = parseTarget(c);
                case "history" -> history = parseTarget(c);
                default -> throw c.error("unknown Deployment field '" + key + "' (deploys, sources, live, history)");
            }
            c.expect(TokenType.SEMI_COLON);
        }
        c.expect(TokenType.BRACE_CLOSE);
        if (deploys == null || deploys.isEmpty()) {
            throw c.error("the deployment '" + h.name() + "' deploys nothing (deploys: [ ... ])");
        }
        if (live == null && history == null) {
            throw c.error("the deployment '" + h.name() + "' names no target (live: ..., history: ...)");
        }
        return new Protocol.PDeployment(h.pkg(), h.name(), h.dec().stereotypes(), h.dec().taggedValues(), deploys,
                sources, live, history, c.spanOf(h.declStart(), c.pos() - 1));
    }

    /** {@code [ a::B, c::D ]}. */
    private static List<String> pathList(TokenStreamCursor c) {
        c.expect(TokenType.BRACKET_OPEN);
        List<String> out = new ArrayList<>();
        while (c.peek() != TokenType.BRACKET_CLOSE) {
            out.add(c.parseQualifiedName());
            if (c.peek() != TokenType.BRACKET_CLOSE) {
                c.expect(TokenType.COMMA);
            }
        }
        c.expect(TokenType.BRACKET_CLOSE);
        return List.copyOf(out);
    }

    /** {@code conn keep [ scope: rule, ... ]}, the keep list optional (an engine that keeps all it is sent). */
    private static Protocol.PDeploymentTarget parseTarget(TokenStreamCursor c) {
        String connection = c.parseQualifiedName();
        List<Protocol.PKeep> keep = new ArrayList<>();
        if (c.isIdentifierToken(c.peek()) && "keep".equals(c.safeText())) {
            c.advance();
            c.expect(TokenType.BRACKET_OPEN);
            Set<String> scopes = new HashSet<>();
            while (c.peek() != TokenType.BRACKET_CLOSE) {
                String scope = c.parseIdentifier();
                if (!scopes.add(scope)) {
                    throw c.error("the keep scope '" + scope + "' is given twice");
                }
                c.expect(TokenType.COLON);
                keep.add(new Protocol.PKeep(scope, parseKeepRule(c)));
                if (c.peek() != TokenType.BRACKET_CLOSE) {
                    c.expect(TokenType.COMMA);
                }
            }
            c.expect(TokenType.BRACKET_CLOSE);
        }
        return new Protocol.PDeploymentTarget(connection, keep);
    }

    /** {@code LastRows(n)}, {@code Last('PT8H')}, {@code all}, {@code current}, {@code versions}. */
    private static Protocol.PKeepRule parseKeepRule(TokenStreamCursor c) {
        String kind = c.parseIdentifier();
        return switch (kind) {
            case "all" -> new Protocol.PKeepAll();
            case "current" -> new Protocol.PKeepCurrent();
            case "versions" -> new Protocol.PKeepVersions();
            case "LastRows" -> {
                c.expect(TokenType.PAREN_OPEN);
                long rows = c.consumeLong();
                if (rows <= 0) {
                    throw c.error("a keep holds at least one row, not " + rows);
                }
                c.expect(TokenType.PAREN_CLOSE);
                yield new Protocol.PKeepRows(rows);
            }
            case "Last" -> {
                c.expect(TokenType.PAREN_OPEN);
                String d = duration(c, "a keep");
                c.expect(TokenType.PAREN_CLOSE);
                yield new Protocol.PKeepFor(d);
            }
            default -> throw c.error("unknown keep '" + kind + "' (LastRows(n), Last('PT8H'), all, current, versions)");
        };
    }

    /** {@code Ingest qn [<read mode>] [key=value ...] [ <datasets> ]}. */
    private static Protocol.PIngest parseIngest(TokenStreamCursor c) {
        SectionParse.Head h = SectionParse.head(c, "Ingest");
        // a read mode is a word not followed by '=' (a header field is); none for an ingest of views alone
        String kind = null;
        String format = null;
        if (c.peek() != TokenType.BRACKET_OPEN && c.peek(1) != TokenType.EQUAL) {
            kind = c.parseIdentifier();
            String retired = com.legend.protocol.IngestWords.RETIRED_READ_MODES.get(kind);
            if (retired != null) {
                throw c.error(retired);
            }
            if (!com.legend.protocol.IngestWords.READ_MODES.contains(kind)) {
                throw c.error("unknown read mode '" + kind + "' (Snapshot, Delta)");
            }
            if (c.match(TokenType.LESS_THAN)) {
                format = c.parseIdentifier();
                if (!com.legend.protocol.IngestWords.FORMATS.contains(format)) {
                    throw c.error("unknown format '" + format + "' (CSV, Parquet, JSON, Avro)");
                }
                c.expect(TokenType.GREATER_THAN);
            }
        }
        Protocol.PIngestDeleteIndicator deleteIndicator = null;
        Protocol.PIngestSchedule schedule = null;
        Protocol.PIngestOwner owner = null;
        String datasetGroup = null;
        String writeMode = null;
        Set<String> seen = new HashSet<>();
        while (c.peek() != TokenType.BRACKET_OPEN) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seen, key, c);
            c.expect(TokenType.EQUAL);
            switch (key) {
                case "owner" -> owner = parseOwner(c);
                case "datasetGroup" -> datasetGroup = c.parseIdentifier();
                case "writeMode" -> writeMode = parseWriteMode(c);
                case "deleteIndicator" -> deleteIndicator = parseDeleteIndicator(c);
                case "schedule" -> schedule = parseSchedule(c);
                default -> throw c.error("unknown Ingest field '" + key
                        + "' (owner, datasetGroup, writeMode, deleteIndicator, schedule)");
            }
        }
        if (deleteIndicator != null && !"Delta".equals(kind)) {
            throw c.error("a delete indicator marks a delta's deletes: the ingest '" + h.name() + "' is "
                    + (kind == null ? "no read" : kind) + ", not Delta");
        }
        if (schedule != null && !"Snapshot".equals(kind)) {
            throw c.error("a pull's schedule runs a query for the whole set each time: the ingest '" + h.name()
                    + "' is " + (kind == null ? "no read" : kind) + ", not Snapshot");
        }
        Protocol.PIngestReadMode readMode = kind == null ? null
                : new Protocol.PIngestReadMode(kind, format, deleteIndicator);
        c.expect(TokenType.BRACKET_OPEN);
        List<Protocol.PIngestDataset> datasets = new ArrayList<>();
        Set<String> names = new HashSet<>();
        while (c.peek() != TokenType.BRACKET_CLOSE) {
            Protocol.PIngestDataset d = parseDataset(c);
            if (!names.add(d.name())) {
                throw c.error("the dataset '" + d.name() + "' is declared twice");
            }
            datasets.add(d);
        }
        c.expect(TokenType.BRACKET_CLOSE);
        if (datasets.isEmpty()) {
            throw c.error("the ingest '" + h.name() + "' declares no dataset");
        }
        return new Protocol.PIngest(h.pkg(), h.name(), h.dec().stereotypes(), h.dec().taggedValues(), readMode,
                schedule, owner, datasetGroup, writeMode, datasets, c.spanOf(h.declStart(), c.pos() - 1));
    }

    /** {@code DeleteIndicator(deleteField='DELETED', deleteValues=['true', 'Y'])}. */
    private static Protocol.PIngestDeleteIndicator parseDeleteIndicator(TokenStreamCursor c) {
        String kind = c.parseIdentifier();
        if (!kind.equals("DeleteIndicator")) {
            throw c.error("unknown delete indicator '" + kind + "' (DeleteIndicator)");
        }
        c.expect(TokenType.PAREN_OPEN);
        String field = null;
        List<String> values = null;
        Set<String> seen = new HashSet<>();
        while (c.peek() != TokenType.PAREN_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seen, key, c);
            c.expect(TokenType.EQUAL);
            switch (key) {
                case "deleteField" -> field = SectionParse.stringValue(c);
                case "deleteValues" -> {
                    c.expect(TokenType.BRACKET_OPEN);
                    values = new ArrayList<>();
                    while (c.peek() != TokenType.BRACKET_CLOSE) {
                        values.add(SectionParse.stringValue(c));
                        if (c.peek() != TokenType.BRACKET_CLOSE) {
                            c.expect(TokenType.COMMA);
                        }
                    }
                    c.expect(TokenType.BRACKET_CLOSE);
                }
                default -> throw c.error("unknown DeleteIndicator field '" + key + "' (deleteField, deleteValues)");
            }
            if (c.peek() != TokenType.PAREN_CLOSE) {
                c.expect(TokenType.COMMA);
            }
        }
        c.expect(TokenType.PAREN_CLOSE);
        if (field == null || values == null || values.isEmpty()) {
            throw c.error("a delete indicator names its field and at least one value (deleteField='...',"
                    + " deleteValues=['...'])");
        }
        return new Protocol.PIngestDeleteIndicator(field, values);
    }

    /** {@code Daily('06:00', 'America/New_York')} or {@code Every('PT15M')}. */
    private static Protocol.PIngestSchedule parseSchedule(TokenStreamCursor c) {
        String schedule = c.parseIdentifier();
        c.expect(TokenType.PAREN_OPEN);
        Protocol.PIngestSchedule s = switch (schedule) {
            case "Daily" -> {
                String time = SectionParse.stringValue(c);
                if (!com.legend.protocol.IngestWords.isTimeOfDay(time)) {
                    throw c.error("a pull's time of day is HH:MM, not '" + time + "'");
                }
                c.expect(TokenType.COMMA);
                String zone = SectionParse.stringValue(c);
                if (!com.legend.protocol.IngestWords.isZone(zone)) {
                    throw c.error("a pull's time zone is a named zone (America/New_York, UTC), not '" + zone + "'");
                }
                yield new Protocol.PIngestDaily(time, zone);
            }
            case "Every" -> new Protocol.PIngestEvery(duration(c, "a pull's interval"));
            default -> throw c.error("unknown pull schedule '" + schedule + "' (Daily, Every)");
        };
        c.expect(TokenType.PAREN_CLOSE);
        return s;
    }

    /** {@code AppDir(production='111111', prodParallel='222222')}. */
    private static Protocol.PIngestOwner parseOwner(TokenStreamCursor c) {
        String kind = c.parseIdentifier();
        if (!kind.equals("AppDir")) {
            throw c.error("unknown owner '" + kind + "' (AppDir)");
        }
        c.expect(TokenType.PAREN_OPEN);
        String production = null;
        String prodParallel = null;
        Set<String> seen = new HashSet<>();
        while (c.peek() != TokenType.PAREN_CLOSE) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seen, key, c);
            c.expect(TokenType.EQUAL);
            String id = SectionParse.stringValue(c);
            if (!com.legend.protocol.IngestWords.isAppDirId(id)) {
                throw c.error("an application directory id is digits, not '" + id + "'");
            }
            switch (key) {
                case "production" -> production = id;
                case "prodParallel" -> prodParallel = id;
                default -> throw c.error("unknown AppDir field '" + key + "' (production, prodParallel)");
            }
            if (c.peek() != TokenType.PAREN_CLOSE) {
                c.expect(TokenType.COMMA);
            }
        }
        c.expect(TokenType.PAREN_CLOSE);
        if (production == null) {
            throw c.error("an AppDir owner names its production deployment");
        }
        return new Protocol.PIngestOwner(production, prodParallel);
    }

    private static String parseWriteMode(TokenStreamCursor c) {
        String word = c.parseIdentifier();
        if (word.equals("LatestByKey")) {
            throw c.error(com.legend.protocol.IngestWords.RETIRED_LATEST_BY_KEY);
        }
        String wire = com.legend.protocol.IngestWords.writeModeWire(word);
        if (wire == null) {
            throw c.error("unknown write mode '" + word + "' (AppendOnly, BatchMilestoned, BatchMilestonedBusinessTemporal)");
        }
        return wire;
    }

    /** {@code NAME( col: Type, ... ) key=value ... ;} or {@code NAME( |<lambda> ) key=value ... ;}. */
    private static Protocol.PIngestDataset parseDataset(TokenStreamCursor c) {
        int start = c.pos();
        String name = c.parseIdentifier();
        if (c.peek() != TokenType.PAREN_OPEN) {
            throw c.error("expected '(' after the dataset name '" + name + "'");
        }
        Protocol.PIngestSource source;
        if (c.peek(1) == TokenType.PIPE) {
            c.advance();                                // '('
            int from = c.pos();
            int depth = 0;
            while (!(c.peek() == TokenType.PAREN_CLOSE && depth == 0)) {
                if (c.atEnd()) {
                    throw c.error("the dataset '" + name + "''s function is not closed");
                }
                switch (c.peek()) {
                    case PAREN_OPEN, BRACE_OPEN, BRACKET_OPEN -> depth++;
                    case PAREN_CLOSE, BRACE_CLOSE, BRACKET_CLOSE -> depth--;
                    default -> { }
                }
                c.advance();
            }
            var spec = com.legend.parser.SpecParser.parse(c.tokens().slice(from, c.pos()), c.dialect());
            if (!(spec instanceof com.legend.protocol.spec.LambdaFunction lambda)) {
                throw c.error("the dataset '" + name + "''s source is a lambda, |<expression>");
            }
            c.expect(TokenType.PAREN_CLOSE);
            source = new Protocol.PIngestFunction(lambda);
        } else {
            com.legend.protocol.TypeExpression t = c.parseType();
            if (!(t instanceof com.legend.protocol.TypeExpression.RelationType rt) || rt.columns().isEmpty()) {
                throw c.error("the dataset '" + name + "' declares no column");
            }
            Set<String> cols = new HashSet<>();
            for (var col : rt.columns()) {
                if (!cols.add(col.name())) {
                    throw c.error("the dataset '" + name + "' declares the column '" + col.name() + "' twice");
                }
            }
            source = new Protocol.PIngestColumns(rt.columns());
        }
        List<String> pk = List.of();
        String writeMode = null;
        Protocol.PIngestRetention retention = null;
        String privacy = null;
        List<String> ingestPartitions = List.of();
        List<String> storagePartitions = List.of();
        List<String> storageClusters = List.of();
        Set<String> seen = new HashSet<>();
        while (c.peek() != TokenType.SEMI_COLON) {
            String key = c.parseIdentifier();
            TokenStreamCursor.once(seen, key, c);
            c.expect(TokenType.EQUAL);
            switch (key) {
                case "pk" -> pk = columnList(c);
                case "writeMode" -> writeMode = parseWriteMode(c);
                case "retention" -> retention = parseRetention(c);
                case "privacyClassification" -> privacy = c.parseIdentifier();
                case "ingestPartitionColumns" -> ingestPartitions = columnList(c);
                case "storageLayoutPartitionColumns" -> storagePartitions = columnList(c);
                case "storageLayoutClusterColumns" -> storageClusters = columnList(c);
                default -> throw c.error("unknown dataset field '" + key + "' (pk, writeMode, retention,"
                        + " privacyClassification, ingestPartitionColumns, storageLayoutPartitionColumns,"
                        + " storageLayoutClusterColumns)");
            }
        }
        c.expect(TokenType.SEMI_COLON);
        return new Protocol.PIngestDataset(name, pk, source, writeMode, retention, privacy, ingestPartitions,
                storagePartitions, storageClusters, c.spanOf(start, c.pos() - 1));
    }

    /** {@code [ a, b ]}. */
    private static List<String> columnList(TokenStreamCursor c) {
        c.expect(TokenType.BRACKET_OPEN);
        List<String> out = new ArrayList<>();
        while (c.peek() != TokenType.BRACKET_CLOSE) {
            out.add(c.parseIdentifier());
            if (c.peek() != TokenType.BRACKET_CLOSE) {
                c.expect(TokenType.COMMA);
            }
        }
        c.expect(TokenType.BRACKET_CLOSE);
        return List.copyOf(out);
    }

    /** {@code Last('PT8H')} or {@code LastRows(200000)}. */
    private static Protocol.PIngestRetention parseRetention(TokenStreamCursor c) {
        String kind = c.parseIdentifier();
        c.expect(TokenType.PAREN_OPEN);
        Protocol.PIngestRetention r = switch (kind) {
            case "Last" -> new Protocol.PIngestLast(duration(c, "a retention"));
            case "LastRows" -> {
                long rows = c.consumeLong();
                if (rows <= 0) {
                    throw c.error("a retention keeps at least one row, not " + rows);
                }
                yield new Protocol.PIngestLastRows(rows);
            }
            default -> throw c.error("unknown retention '" + kind + "' (Last, LastRows)");
        };
        c.expect(TokenType.PAREN_CLOSE);
        return r;
    }

    private static String duration(TokenStreamCursor c, String what) {
        String d = SectionParse.stringValue(c);
        if (!com.legend.protocol.IngestWords.isDuration(d)) {
            throw c.error(what + " is an ISO 8601 duration (PT15M, PT8H, P1D), not '" + d + "'");
        }
        return d;
    }
}
