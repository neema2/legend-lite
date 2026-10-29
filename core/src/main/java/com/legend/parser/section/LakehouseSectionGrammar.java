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
 * key the wire's field name: {@code datasetGroup=}, {@code writeMode=}, {@code retention=}, {@code
 * privacyClassification=} and the partition and cluster column lists. Lite's additions are the read modes {@code
 * Stream} and {@code Pull<schedule>}, the write mode {@code LatestByKey}, a dataset's {@code retention}, and a dataset
 * made by a function, {@code NAME( |<lambda> )}.
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
        return ((Protocol.PIngest) e).qualifiedName();
    }

    @Override
    public Protocol.Element parseOne(TokenStreamCursor c) {
        if (c.dialect().refusesLiteExtensions()) {
            throw c.error("'Lakehouse' is not a known section parser");
        }
        return parseIngest(c);
    }

    /** {@code Ingest qn <read mode> [key=value ...] [ <datasets> ]}. */
    private static Protocol.PIngest parseIngest(TokenStreamCursor c) {
        SectionParse.Head h = SectionParse.head(c, "Ingest");
        Protocol.PIngestReadMode readMode = parseReadMode(c);
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
                default -> throw c.error("unknown Ingest field '" + key + "' (owner, datasetGroup, writeMode)");
            }
        }
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
                owner, datasetGroup, writeMode, datasets, c.spanOf(h.declStart(), c.pos() - 1));
    }

    /** {@code Snapshot<CSV>}, {@code Delta<Parquet>}, {@code Stream}, {@code Stream<Json>},
     *  {@code Pull<Daily('06:00', 'America/New_York')>}, {@code Pull<Every('PT15M')>}. */
    private static Protocol.PIngestReadMode parseReadMode(TokenStreamCursor c) {
        String kind = c.parseIdentifier();
        return switch (kind) {
            case "Snapshot", "Delta" -> {
                c.expect(TokenType.LESS_THAN);
                String format = c.parseIdentifier();
                if (!format.equals("CSV") && !format.equals("Parquet")) {
                    throw c.error("unknown file format '" + format + "' (CSV, Parquet)");
                }
                c.expect(TokenType.GREATER_THAN);
                yield new Protocol.PIngestFiles(kind, format);
            }
            case "Stream" -> {
                String format = null;
                if (c.match(TokenType.LESS_THAN)) {
                    format = c.parseIdentifier();
                    if (!format.equals("Json") && !format.equals("Avro")) {
                        throw c.error("unknown stream format '" + format + "' (Json, Avro)");
                    }
                    c.expect(TokenType.GREATER_THAN);
                }
                yield new Protocol.PIngestStream(format);
            }
            case "Pull" -> {
                c.expect(TokenType.LESS_THAN);
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
                            throw c.error("a pull's time zone is a named zone (America/New_York, UTC), not '"
                                    + zone + "'");
                        }
                        yield new Protocol.PIngestDaily(time, zone);
                    }
                    case "Every" -> new Protocol.PIngestEvery(duration(c, "a pull's interval"));
                    default -> throw c.error("unknown pull schedule '" + schedule + "' (Daily, Every)");
                };
                c.expect(TokenType.PAREN_CLOSE);
                c.expect(TokenType.GREATER_THAN);
                yield new Protocol.PIngestPull(s);
            }
            default -> throw c.error("unknown read mode '" + kind + "' (Snapshot, Delta, Stream, Pull)");
        };
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
        String wire = com.legend.protocol.IngestWords.writeModeWire(word);
        if (wire == null) {
            throw c.error("unknown write mode '" + word
                    + "' (AppendOnly, BatchMilestoned, BatchMilestonedBusinessTemporal, LatestByKey)");
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
