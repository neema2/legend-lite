// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import com.legend.lexer.TokenStream;
import com.legend.lexer.TokenType;
import com.legend.protocol.Protocol;
import com.legend.protocol.SourceInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * PROTOCOL-path {@code ###Relational} Database parse (section-parity leg 3):
 * full span fidelity against ZRelationalProbe's oracle shapes. The
 * execution-path {@link RelationalGrammarParser} stays until execution
 * derives from protocol (Phase D settled the direction: {@code
 * com.legend.model} carries zero source positions, so a model&rarr;protocol
 * emitter is impossible — protocol is parsed, execution derives later).
 *
 * <p>Span rules probed, not guessed: a column node's span runs from ITS
 * table token to the end of the enclosing comparison's right operand (the
 * engine's ANTLR context spans); the implicit default schema takes the
 * whole database's span; {@code {target}} spells both table and alias of a
 * self-join column.
 */
public final class DatabaseProtocolParser implements TokenStreamCursor {

    private final TokenStream tokens;
    private int pos;
    private final String dbFqn;

    /** An active {@code scope([db]seg(.seg)?)} header (probe scope-forms):
     *  bare idents inside are columns of the HEADER table, whose pointer
     *  span points at the header tokens — the engine flattens scope at
     *  parse. Only headers WITH a table segment build one; db-only scopes
     *  just re-anchor {@code dbFqn}. */
    record ScopeCtx(String db, String schema, String table,
            SourceInfo tableSpan, boolean singleSeg) { }

    private final @com.legend.Nullable ScopeCtx scope;

    /** The CURRENT named schema's declaration-name span — nav ELEMENTS
     *  inside a non-default schema stretch their table pointer span from
     *  the SCHEMA DECLARATION to the local table token (probe
     *  cross-schema-nav). Set by parseSchema. */
    private @com.legend.Nullable SourceInfo currentSchemaDeclSpan;

    /** Stretch anchor handed to the NESTED nav-element parser. */
    private @com.legend.Nullable SourceInfo schemaStretchSpan;

    /** The caller's level, threaded for UNIFORMITY — every grammar takes
     *  its caller's dialect and reports it truthfully. No lever inside
     *  ###Relational consults it today (the last one, json-column-get,
     *  was retired to the engine's extractFromSemiStructured —
     *  OWN_CORPUS_DECISIONS §7); it exists so this parser has the same
     *  shape as every other one, and a future lever threads free. */
    private final Dialect dialect;

    @Override
    public Dialect dialect() {
        return dialect;
    }

    private DatabaseProtocolParser(TokenStream tokens, int pos, String dbFqn,
            @com.legend.Nullable ScopeCtx scope, Dialect dialect) {
        this.scope = scope;
        this.tokens = tokens;
        this.pos = pos;
        this.dbFqn = dbFqn;
        this.dialect = dialect;
    }

    @Override
    public TokenStream tokens() {
        return tokens;
    }

    @Override
    public int pos() {
        return pos;
    }

    @Override
    public void setPos(int pos) {
        this.pos = pos;
    }

    /** Relational identifiers admit QUOTED spellings, which KEEP their
     *  quotes as the wire name (probe: '"quoted col"'). */
    @Override
    public String parseIdentifier() {
        if (peek() == TokenType.QUOTED_STRING) {
            String raw = text();
            advance();
            return raw;
        }
        return TokenStreamCursor.super.parseIdentifier();
    }

    /** Package entry for OTHER section grammars (Mapping property
     *  mappings) that embed relational operations: parse one operation at
     *  {@code start}; the advanced position lands in {@code posOut[0]}. */
    static Protocol.PRelOp operationAt(TokenStream ts, int start,
            String dbFqn, String schemaCtx, int[] posOut, Dialect dialect) {
        DatabaseProtocolParser p = new DatabaseProtocolParser(ts, start,
                dbFqn, null, dialect);
        Protocol.PRelOp op = p.parseOperation(schemaCtx);
        posOut[0] = p.pos;
        return op;
    }

    /** Parse one {@code Database qn ( ... )} at {@code tokenIndex}. */
    public static Protocol.PDatabase parse(TokenStream ts, int tokenIndex,
            Dialect dialect) {
        return parse(ts, tokenIndex, null, dialect);
    }

    /** As {@link #parse(TokenStream, int)}, but reports where the element
     *  ENDED in {@code endOut[0]} so a caller iterating elements can resume.
     *  Needed by {@code ElementParser} now that the ###Relational model is
     *  built from protocol rather than by a second parser
     *  (PARSER_COMPLETENESS_PLAN.md §1). */
    public static Protocol.PDatabase parse(TokenStream ts, int tokenIndex,
            int @com.legend.Nullable [] endOut, Dialect dialect) {
        DatabaseProtocolParser p = new DatabaseProtocolParser(ts, tokenIndex,
                "", null, dialect);
        Protocol.PDatabase db = p.parseDatabase();
        if (endOut != null) {
            endOut[0] = p.pos;
        }
        return db;
    }

    private Protocol.PDatabase parseDatabase() {
        int declStart = pos;
        // database: documentation? DATABASE stereotypes? taggedValues? ...
        // (4.145.0; the walker folds it into the tagged values, first)
        Documentation doc = parseDocumentation();
        expect(TokenType.DATABASE);
        TokenStreamCursor.Decorations dbDec = withDocumentation(doc, parseDecorations());
        String qn = Protocol.unquotePath(parseQualifiedName());
        int cut = qn.lastIndexOf("::");
        String pkg = cut < 0 ? "" : qn.substring(0, cut);
        String name = cut < 0 ? qn : qn.substring(cut + 2);
        DatabaseProtocolParser inner =
                new DatabaseProtocolParser(tokens, pos, qn, null, dialect);
        Protocol.PDatabase db = inner.parseBody(declStart, pkg, name,
                dbDec.stereotypes(), dbDec.taggedValues());
        this.pos = inner.pos;
        return db;
    }

    private Protocol.PDatabase parseBody(int declStart, String pkg, String name,
            List<Protocol.PStereotype> dbStereotypes,
            List<Protocol.PTaggedValue> dbTaggedValues) {
        expect(TokenType.PAREN_OPEN);
        List<Protocol.PPointer> includes = new ArrayList<>();
        List<Protocol.PDbTable> defaultTables = new ArrayList<>();
        List<Protocol.PDbView> defaultViews = new ArrayList<>();
        List<Protocol.PDbTable> defaultTabFns = new ArrayList<>();
        List<Protocol.PDbSchema> schemas = new ArrayList<>();
        List<Protocol.PIncludedStoreSpec> includeSpecs = new ArrayList<>();
        List<Protocol.PDbJoin> joins = new ArrayList<>();
        List<Protocol.PDbFilter> filters = new ArrayList<>();
        while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
            // a member's documentation stands before its keyword (4.145.0:
            // schema / table / view); the member parser reads it
            switch (peek() == TokenType.DOC_STRING ? peek(1) : peek()) {
                case INCLUDE -> {
                    int s = pos;
                    advance();
                    String first = Protocol.unquotePath(parseQualifiedName());
                    if (!first.contains("::") && isIdentifierToken(peek())
                            && peek(1)
                                    == TokenType.PATH_SEPARATOR) {
                        // include <storeType> <path> — a TYPED include:
                        // bare single-word type followed by a QUALIFIED
                        // path (harvest testDatabaseIncludeStoreOrder);
                        // `include db::X` and `include X` stay plain
                        String path = Protocol.unquotePath(
                                parseQualifiedName());
                        includeSpecs.add(new Protocol.PIncludedStoreSpec(
                                path, first, spanOf(s, pos - 1)));
                    } else {
                        includes.add(new Protocol.PPointer("STORE", first,
                                spanOf(s, pos - 1)));
                    }
                }
                case SCHEMA -> schemas.add(parseSchema());
                case TABLE -> defaultTables.add(parseTable());
                case VIEW -> defaultViews.add(parseView("default"));
                case VALID_STRING -> {
                    if (!"TabularFunction".equals(text())) {
                        throw error("unsupported Database member: '"
                                + safeText() + "'");
                    }
                    defaultTabFns.add(parseTabularFunction());
                }
                case JOIN -> {
                    int s = pos;
                    advance();
                    String jn = parseIdentifier();
                    expect(TokenType.PAREN_OPEN);
                    Protocol.PRelOp op = parseOperation("default");
                    expect(TokenType.PAREN_CLOSE);
                    joins.add(new Protocol.PDbJoin(jn, op, spanOf(s, pos - 1)));
                }
                case FILTER, MULTIGRAIN_FILTER -> {
                    int s = pos;
                    String ft = peek() == TokenType.MULTIGRAIN_FILTER
                            ? "multigrain" : "filter";
                    advance();
                    String fn = parseIdentifier();
                    expect(TokenType.PAREN_OPEN);
                    Protocol.PRelOp op = parseOperation("default");
                    expect(TokenType.PAREN_CLOSE);
                    filters.add(new Protocol.PDbFilter(ft, fn, op,
                            spanOf(s, pos - 1)));
                }
                default -> throw error("unsupported Database member: '"
                        + safeText() + "'");
            }
        }
        int close = pos;
        expect(TokenType.PAREN_CLOSE);
        SourceInfo dbSpan = spanOf(declStart, close);
        if (!defaultTables.isEmpty() || !defaultViews.isEmpty()
                || !defaultTabFns.isEmpty()) {
            // the implicit default schema takes the WHOLE database span and
            // sorts AFTER named schemas (engine walker append order)
            schemas.add(new Protocol.PDbSchema("default", defaultTables,
                    defaultViews, defaultTabFns, List.of(), List.of(), dbSpan));
        }
        return new Protocol.PDatabase(pkg, name, includes, includeSpecs,
                schemas, joins,
                filters, dbStereotypes, dbTaggedValues, dbSpan);
    }


    private Protocol.PDbSchema parseSchema() {
        int s = pos;
        Documentation doc = parseDocumentation();
        expect(TokenType.SCHEMA);
        TokenStreamCursor.Decorations dec = withDocumentation(doc, parseDecorations());
        int nameTok = pos;
        // a QUOTED schema name keeps its single quotes in the wire name
        // ("'\"test\"'" — harvest testRelationalSchemaSingleAndDoubleQuoted;
        // the engine walker stores the raw token text here)
        String name;
        if (peek() == TokenType.STRING) {
            name = text();
            advance();
        } else {
            name = parseIdentifier();
        }
        currentSchemaDeclSpan = spanOf(nameTok, nameTok);
        expect(TokenType.PAREN_OPEN);
        List<Protocol.PDbTable> tables = new ArrayList<>();
        List<Protocol.PDbView> views = new ArrayList<>();
        List<Protocol.PDbTable> tabFns = new ArrayList<>();
        while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
            // a member's documentation stands before its keyword (4.145.0)
            TokenType head = peek() == TokenType.DOC_STRING ? peek(1) : peek();
            if (head == TokenType.TABLE) {
                tables.add(parseTable());
            } else if (head == TokenType.VIEW) {
                views.add(parseView(name));
            } else if (peek() == TokenType.VALID_STRING
                    && "TabularFunction".equals(text())) {
                tabFns.add(parseTabularFunction());
            } else {
                throw error("unsupported Schema member: '" + safeText() + "'");
            }
        }
        expect(TokenType.PAREN_CLOSE);
        return new Protocol.PDbSchema(name, tables, views, tabFns,
                dec.stereotypes(), dec.taggedValues(), spanOf(s, pos - 1));
    }

    /** {@code TabularFunction TF ( cols )} — slim table shape on the wire
     *  (columns + name + span; probe snapshot-quoted-tabfunc). */
    private Protocol.PDbTable parseTabularFunction() {
        int s = pos;
        advance();                                  // 'TabularFunction'
        String name = parseIdentifier();
        expect(TokenType.PAREN_OPEN);
        List<Protocol.PDbColumn> columns = new ArrayList<>();
        while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
            int cS = pos;
            String colName = parseIdentifier();
            Protocol.PDbType type = parseDbType(colName);
            boolean nullable = true;
            // (PRIMARY_KEY | NOT_NULL)? — AT MOST ONE constraint in the
            // .g4 (sibling negative neg-relational-primarykey-and-notnull)
            if (peek() == TokenType.PRIMARY_KEY
                    || peek() == TokenType.NOT_NULL) {
                nullable = false;
                advance();
            }
            columns.add(new Protocol.PDbColumn(colName, nullable, type,
                    List.of(), List.of(), spanOf(cS, pos - 1)));
            // columnDefinition (COMMA columnDefinition)* — a next column
            // WITHOUT the comma refuses (invention audit family 3)
            if (!match(TokenType.COMMA) && peek() != TokenType.PAREN_CLOSE) {
                throw error("Unexpected token '" + safeText() + "'");
            }
        }
        expect(TokenType.PAREN_CLOSE);
        return new Protocol.PDbTable(name, columns, List.of(), List.of(),
                List.of(), List.of(), spanOf(s, pos - 1));
    }

    private Protocol.PDbTable parseTable() {
        int s = pos;
        Documentation doc = parseDocumentation();
        expect(TokenType.TABLE);
        TokenStreamCursor.Decorations dec = withDocumentation(doc, parseDecorations());
        String name = parseIdentifier();
        expect(TokenType.PAREN_OPEN);
        List<Protocol.PDbColumn> columns = new ArrayList<>();
        List<Protocol.PMilestoning> milestoning = new ArrayList<>();
        List<String> primaryKey = new ArrayList<>();
        while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
            if (peek() == TokenType.VALID_STRING
                    && "milestoning".equals(text())) {
                advance();
                parseMilestoning(milestoning);
                continue;
            }
            int cS = pos;
            // columnDefinition: documentation? relationalIdentifier
            // stereotypes? taggedValues? identifier ... (4.145.0)
            Documentation colDoc = parseDocumentation();
            String colName = parseIdentifier();
            // columns take stereotypes AND tagged values before the type
            // (probes column-stereotypes + db-and-column-decorations)
            Decorations colDec = withDocumentation(colDoc, parseDecorations());
            Protocol.PDbType type = parseDbType(colName);
            boolean nullable = true;
            // PRIMARY KEY / NOT NULL lex as single tokens; the .g4 allows
            // AT MOST ONE constraint per column ((PRIMARY_KEY|NOT_NULL)? —
            // sibling negative neg-relational-primarykey-and-notnull)
            if (peek() == TokenType.PRIMARY_KEY
                    || peek() == TokenType.NOT_NULL) {
                if (peek() == TokenType.PRIMARY_KEY) {
                    primaryKey.add(colName);
                }
                nullable = false;
                advance();
            }
            columns.add(new Protocol.PDbColumn(colName, nullable,
                    type, colDec.stereotypes(), colDec.taggedValues(),
                    spanOf(cS, pos - 1)));
            // columnDefinition (COMMA columnDefinition)* — a next column
            // WITHOUT the comma refuses (invention audit family 3); the
            // milestoning block may follow the last column comma-free
            if (!match(TokenType.COMMA) && peek() != TokenType.PAREN_CLOSE
                    && !(peek() == TokenType.VALID_STRING
                            && "milestoning".equals(text()))) {
                throw error("Unexpected token '" + safeText() + "'");
            }
        }
        expect(TokenType.PAREN_CLOSE);
        return new Protocol.PDbTable(name, columns, milestoning, primaryKey,
                dec.stereotypes(), dec.taggedValues(), spanOf(s, pos - 1));
    }

    /** Datatype spelling &rarr; wire {@code _type} (probe TYPES): sized,
     *  precision/scale and plain kinds; unknown kinds refuse loudly. */
    private Protocol.PDbType parseDbType(String colName) {
        int typeStart = pos;
        String kindWord = parseIdentifier().toUpperCase(java.util.Locale.ROOT);
        Long size = null;
        Long precision = null;
        Long scale = null;
        String kind = switch (kindWord) {
            case "INTEGER", "INT" -> "Integer";
            case "BIGINT" -> "BigInt";
            case "SMALLINT" -> "SmallInt";
            case "TINYINT" -> "TinyInt";
            case "DOUBLE" -> "Double";
            case "FLOAT" -> "Float";
            case "REAL" -> "Real";
            case "BIT" -> "Bit";
            case "DATE" -> "Date";
            case "TIMESTAMP" -> "Timestamp";
            case "SEMISTRUCTURED" -> "SemiStructured";
            case "JSON" -> "Json";
            // engine has no Array wire type: the ARRAY keyword walks to
            // Other (RelationalParseTreeWalker.java:465). We emitted
            // "Array", which no corpus file exercises — so gate 8 never
            // saw it. Found by R3, when the model was built FROM protocol.
            case "OTHER", "ARRAY" -> "Other";
            case "VARCHAR" -> "Varchar";
            case "CHAR" -> "Char";
            case "BINARY" -> "Binary";
            case "VARBINARY" -> "Varbinary";
            case "DECIMAL" -> "Decimal";
            case "NUMERIC" -> "Numeric";
            default -> throw TokenStreamCursor.throwAt(tokens, typeStart,
                    "unsupported column datatype: " + kindWord);
        };
        boolean sized = kind.equals("Varchar") || kind.equals("Char")
                || kind.equals("Binary") || kind.equals("Varbinary");
        boolean precise = kind.equals("Decimal") || kind.equals("Numeric");
        if ((sized || precise) && peek() == TokenType.PAREN_OPEN) {
            advance();
            List<Long> args = new ArrayList<>();
            args.add(consumeLong());
            while (match(TokenType.COMMA)) {
                args.add(consumeLong());
            }
            expect(TokenType.PAREN_CLOSE);
            if (precise && args.size() == 2) {
                precision = args.get(0);
                scale = args.get(1);
            } else if (sized && args.size() == 1) {
                size = args.get(0);
            } else {
                throw dbTypeArityError(colName, kindWord, sized, typeStart);
            }
        } else if (sized || precise) {
            // the engine REQUIRES the parameter list on these kinds
            // (RelationalParseTreeWalker per-type arms)
            throw dbTypeArityError(colName, kindWord, sized, typeStart);
        }
        return new Protocol.PDbType(kind, size, precision, scale);
    }

    /** Engine-verbatim arity refusal: the quoted declaration is ANTLR's
     *  ctx.getText() — column name + type tokens with no whitespace. */
    private RuntimeException dbTypeArityError(String colName,
            String kindWord, boolean sized, int typeStart) {
        StringBuilder decl = new StringBuilder(colName);
        for (int i = typeStart; i < pos; i++) {
            decl.append(tokens.text(i));
        }
        return TokenStreamCursor.throwAt(tokens, typeStart,
                "Column data type " + kindWord + (sized
                ? " requires 1 parameter (size) in declaration '"
                : " requires 2 parameters (precision, scale) in"
                        + " declaration '")
                + decl + "'");
    }

    /** ENGINE QUIRK (probed + walker-read): each dimension reparses through
     *  an offset anchored at ITSELF, and the engine double-adds the 1-based
     *  anchor column — net {@code +1} on BOTH span columns. Business
     *  (fromThru) spans its ARGS context; processing spans the WHOLE
     *  dimension (MilestoningParseTreeWalker branch difference). */
    private void parseMilestoning(List<Protocol.PMilestoning> out) {
        expect(TokenType.PAREN_OPEN);
        while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
            int s = pos;
            String kind = parseIdentifier();
            expect(TokenType.PAREN_OPEN);
            int argsStart = pos;
            java.util.Map<String, String> kv = new java.util.LinkedHashMap<>();
            Protocol.PDateTimeLit infinity = null;
            // CLOSED, ORDERED key grammar (RelationalParserGrammar
            // businessMilestoning/processingMilestoning; sectioned negative
            // pins #110/#123 + deep-audit: a typo'd key was SILENTLY
            // ignored here): FROM,THRU (,IS_INCLUSIVE)? (,INFINITY_DATE)?
            // or SNAPSHOT_DATE alone; INFINITY_DATE may not open.
            java.util.List<String> keyOrder = new java.util.ArrayList<>();
            while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
                String key = parseIdentifier();
                keyOrder.add(key);
                expect(TokenType.EQUAL);
                if (peek() == TokenType.DATE) {
                    int dS = pos;
                    String raw = text();
                    advance();
                    infinity = new Protocol.PDateTimeLit(
                            raw.substring(1), plusOneCols(spanOf(dS, dS)));
                } else {
                    kv.put(key, safeText());
                    advance();
                }
                match(TokenType.COMMA);
            }
            validateMilestoningKeys(kind, keyOrder);
            int argsEnd = pos - 1;                  // last token inside ( )
            expect(TokenType.PAREN_CLOSE);
            // business: ARGS span; processing: WHOLE dimension span —
            // both with the +1-column reparse quirk
            boolean snapshot = kv.containsKey("BUS_SNAPSHOT_DATE")
                    || kv.containsKey("PROCESSING_SNAPSHOT_DATE");
            // fromThru-business and BOTH snapshot variants span their ARGS
            // context; plain processing spans the whole dimension
            SourceInfo span = "business".equals(kind) || snapshot
                    ? plusOneCols(spanOf(argsStart, argsEnd))
                    : plusOneCols(spanOf(s, pos - 1));
            match(TokenType.COMMA);
            switch (kind) {
                case "business" -> {
                    if (kv.containsKey("BUS_SNAPSHOT_DATE")) {
                        out.add(new Protocol.PBusinessSnapshotMilestoning(
                                req(kv, "BUS_SNAPSHOT_DATE"), span));
                        continue;
                    }
                    out.add(new Protocol.PBusinessMilestoning(
                        req(kv, "BUS_FROM"), req(kv, "BUS_THRU"),
                        Boolean.parseBoolean(kv.getOrDefault(
                                "THRU_IS_INCLUSIVE", "false")),
                        infinity, span));
                }
                case "processing" -> {
                    if (kv.containsKey("PROCESSING_SNAPSHOT_DATE")) {
                        out.add(new Protocol.PProcessingSnapshotMilestoning(
                                req(kv, "PROCESSING_SNAPSHOT_DATE"), span));
                        continue;
                    }
                    out.add(new Protocol.PProcessingMilestoning(
                        req(kv, "PROCESSING_IN"), req(kv, "PROCESSING_OUT"),
                        Boolean.parseBoolean(kv.getOrDefault(
                                "OUT_IS_INCLUSIVE", "false")),
                        infinity, span));
                }
                default -> throw error("unsupported milestoning kind: " + kind);
            }
        }
        expect(TokenType.PAREN_CLOSE);
    }

    /** The engine's ordered milestoning key grammar, enforced — an
     *  unknown, repeated, or misplaced key refuses with the engine's
     *  alternatives shape (its ANTLR message names what may OPEN). */
    private void validateMilestoningKeys(String kind, java.util.List<String> keys) {
        boolean business = "business".equals(kind);
        String from = business ? "BUS_FROM" : "PROCESSING_IN";
        String thru = business ? "BUS_THRU" : "PROCESSING_OUT";
        String incl = business ? "THRU_IS_INCLUSIVE" : "OUT_IS_INCLUSIVE";
        String snap = business ? "BUS_SNAPSHOT_DATE" : "PROCESSING_SNAPSHOT_DATE";
        if (keys.size() == 1 && keys.get(0).equals(snap)) {
            return;
        }
        java.util.List<String> expected = new java.util.ArrayList<>(
                java.util.List.of(from, thru));
        int i = 0;
        for (String k : keys) {
            if (i < 2) {
                if (!k.equals(expected.get(i))) {
                    throw error("Unexpected token '" + k
                            + "'. Valid alternatives: ['" + (i == 0
                                    ? from + "', '" + snap
                                    : thru) + "']");
                }
                i++;
                continue;
            }
            if (k.equals(incl) && i == 2) {
                i++;
                continue;
            }
            if (k.equals("INFINITY_DATE") && i >= 2 && i <= 3) {
                i = 4;
                continue;
            }
            throw error("Unexpected token '" + k + "'");
        }
        if (i < 2) {
            throw error("milestoning '" + kind + "' needs " + from
                    + " and " + thru + " (or " + snap + ")");
        }
    }

    private static SourceInfo plusOneCols(SourceInfo sp) {
        return new SourceInfo("", sp.startLine(), sp.startColumn() + 1,
                sp.endLine(), sp.endColumn() + 1);
    }

    private String req(java.util.Map<String, String> kv, String key) {
        String v = kv.get(key);
        if (v == null) {
            throw error("milestoning needs " + key);
        }
        return v;
    }

    private Protocol.PDbView parseView(String schemaCtx) {
        int s = pos;
        Documentation doc = parseDocumentation();
        expect(TokenType.VIEW);
        TokenStreamCursor.Decorations dec = withDocumentation(doc, parseDecorations());
        String name = parseIdentifier();
        expect(TokenType.PAREN_OPEN);
        boolean distinct = false;
        Protocol.PViewFilter filter = null;
        List<Protocol.PRelOp> groupBy = null;
        List<Protocol.PViewColumnMapping> mappings = new ArrayList<>();
        List<String> primaryKey = new ArrayList<>();
        while (!atEnd() && peek() != TokenType.PAREN_CLOSE) {
            if (peek() == TokenType.DISTINCT_CMD) {
                advance();
                distinct = true;
                continue;
            }
            if (peek() == TokenType.FILTER_CMD) {
                int tS = pos;
                advance();
                // viewFilterMapping: FILTER_CMD (viewFilterMappingJoin |
                //                                databasePointer)? identifier
                // viewFilterMappingJoin: databasePointer joinSequence PIPE
                //                        databasePointer
                // so a leading [db] is EITHER the filter's own database or the
                // start of a join chain — the '@' after it decides (grammar
                // RelationalParserGrammar.g4:141-143, probe ZViewFilterProbe)
                String fdb = null;
                List<Protocol.PJoinPtr> fjoins = new ArrayList<>();
                if (peek() == TokenType.AT) {
                    throw error("join-mediated ~filter needs a source [DB]"
                            + " pointer before the join sequence"
                            + " (~filter [DB] @Join | [DB] FilterName)");
                }
                if (peek() == TokenType.BRACKET_OPEN) {
                    advance();
                    String firstDb = Protocol.unquotePath(parseQualifiedName());
                    expect(TokenType.BRACKET_CLOSE);
                    if (peek() == TokenType.AT) {
                        // join-mediated: the joins carry the FIRST db, and the
                        // filter's own db comes from the pointer after the pipe
                        while (peek() == TokenType.AT) {
                            int jS = pos;
                            advance();
                            String jn = parseIdentifier();
                            fjoins.add(new Protocol.PJoinPtr(firstDb, null, jn,
                                    spanOf(jS, pos - 1)));
                            if (peek() == TokenType.GREATER_THAN) {
                                advance();          // chain separator '>'
                            }
                        }
                        expect(TokenType.PIPE);
                        if (peek() != TokenType.BRACKET_OPEN) {
                            // viewFilterMappingJoin ENDS in a databasePointer:
                            // the target filter is always [DB]-qualified, even
                            // when it lives in the enclosing database
                            throw error("join-mediated ~filter needs a target"
                                    + " [DB] pointer after the '|'"
                                    + " (~filter [DB] @Join | [DB] FilterName)");
                        }
                        expect(TokenType.BRACKET_OPEN);
                        fdb = Protocol.unquotePath(parseQualifiedName());
                        expect(TokenType.BRACKET_CLOSE);
                    } else {
                        fdb = firstDb;
                    }
                }
                String fn = parseIdentifier();
                filter = new Protocol.PViewFilter(fdb, fn, fjoins,
                        spanOf(tS, pos - 1));
                continue;
            }
            if (peek() == TokenType.GROUP_BY_CMD) {
                advance();
                groupBy = new ArrayList<>();
                expect(TokenType.PAREN_OPEN);
                while (peek() != TokenType.PAREN_CLOSE && !atEnd()) {
                    groupBy.add(parseOperation(schemaCtx));
                    match(TokenType.COMMA);
                }
                expect(TokenType.PAREN_CLOSE);
                continue;
            }
            int mS = pos;
            String colName = parseIdentifier();
            expect(TokenType.COLON);
            Protocol.PRelOp op = parseOperation(schemaCtx);
            if (peek() == TokenType.PRIMARY_KEY) {
                advance();
                primaryKey.add(colName);
            }
            mappings.add(new Protocol.PViewColumnMapping(colName, op,
                    spanOf(mS, pos - 1)));
            match(TokenType.COMMA);
        }
        expect(TokenType.PAREN_CLOSE);
        return new Protocol.PDbView(name, mappings, dec.stereotypes(),
                dec.taggedValues(), distinct, filter, groupBy,
                primaryKey, spanOf(s, pos - 1));
    }

    // ------------------------------------------------------------------
    // Operation expressions: or > and > comparison > atom
    // ------------------------------------------------------------------

    /** OPERATOR-node spans run the OPERATOR TOKEN to the right operand's
     *  end. and/or carry NO precedence (engine booleanOperation is
     *  RIGHT-recursive): {@code x and y or z} is {@code and(x, or(y,z))};
     *  CONSECUTIVE same-ops flatten n-ary with the FIRST operator's
     *  anchor. Parens wrap as a {@code group} dynaFunc. */
    Protocol.PRelOp parseOperation(String schemaCtx) {
        Protocol.PRelOp left = parseComparison(schemaCtx);
        String op = boolOpHere();
        if (op == null) {
            return left;
        }
        int opTok = pos;
        advance();
        Protocol.PRelOp rest = parseOperation(schemaCtx);
        SourceInfo span = spanOf(opTok, pos - 1);
        if (rest instanceof Protocol.PDynaFunc df && df.funcName().equals(op)) {
            List<Protocol.PRelOp> params = new ArrayList<>();
            params.add(left);
            params.addAll(df.parameters());
            return new Protocol.PDynaFunc(op, params, span);
        }
        if (rest instanceof Protocol.PDynaFunc df
                && ("and".equals(df.funcName()) || "or".equals(df.funcName()))) {
            // a DIFFERENT-op boolean right side wraps in group (engine's
            // right-recursion goes through the group rule)
            rest = new Protocol.PDynaFunc("group", List.of(rest),
                    df.sourceInformation());
        }
        return new Protocol.PDynaFunc(op, List.of(left, rest), span);
    }

    private @com.legend.Nullable String boolOpHere() {
        if (peek() == TokenType.RELATIONAL_AND
                || (peek() == TokenType.VALID_STRING && "and".equals(text()))) {
            return "and";
        }
        if (peek() == TokenType.RELATIONAL_OR
                || (peek() == TokenType.VALID_STRING && "or".equals(text()))) {
            return "or";
        }
        return null;
    }

    private Protocol.PRelOp parseComparison(String schemaCtx) {
        Protocol.PRelOp left = parseAtom(schemaCtx);
        // postfix null tests bind tighter than comparisons
        while (peek() == TokenType.IS_NULL || peek() == TokenType.IS_NOT_NULL) {
            String nf = peek() == TokenType.IS_NULL ? "isNull" : "isNotNull";
            int nTok = pos;
            advance();
            SourceInfo opSpan = spanOf(nTok, nTok);
            // the operand's context swallows the postfix operator (probe
            // null-postfix: column 14..24 under 'is null' at 18..24)
            left = new Protocol.PDynaFunc(nf,
                    List.of(withSpanEnd(left, opSpan)), opSpan);
        }
        String fn = switch (peek()) {
            case EQUAL -> "equal";
            case NOT_EQUAL -> "notEqualAnsi";   // <> is the ANSI spelling
            case TEST_NOT_EQUAL -> "notEqual";
            case GREATER_THAN -> "greaterThan";
            case GREATER_OR_EQUAL -> "greaterThanEqual";
            case LESS_THAN -> "lessThan";
            case LESS_OR_EQUAL -> "lessThanEqual";
            default -> null;
        };
        if (fn == null) {
            return left;
        }
        int opTok = pos;
        advance();
        // comparisons CHAIN right-recursively (probe chained-eq:
        // a = b = c is equal(a, equal(b, c)) with the same swallow)
        Protocol.PRelOp right = parseComparison(schemaCtx);
        SourceInfo compSpan = spanOf(opTok, pos - 1);
        // ENGINE QUIRK (probed): the LEFT operand's context swallows the
        // operator and right operand — its span END is the comparison's
        // end; the comparison NODE spans operator..right
        left = withSpanEnd(left, compSpan);
        return new Protocol.PDynaFunc(fn, List.of(left, right), compSpan);
    }

    /** One operation under an active {@code scope(...)} header with a
     *  table segment (probe scope-forms). */
    static Protocol.PRelOp scopedOperationAt(TokenStream ts, int start,
            ScopeCtx scope, int[] posOut, Dialect dialect) {
        DatabaseProtocolParser p = new DatabaseProtocolParser(ts, start,
                scope.db(), scope, dialect);
        Protocol.PRelOp op = p.parseOperation("default");
        posOut[0] = p.pos;
        return op;
    }

    /** {@code null} when parsing OUTSIDE any database (mapping-embedded
     *  bare refs) — the wire then omits both db keys (probe bare-no-db). */
    private @com.legend.Nullable String dbOrNull() {
        return dbFqn.isEmpty() ? null : dbFqn;
    }

    /** A WHOLE operation resolved under {@code db} — the post-pipe
     *  terminal of a join navigation (engine: {@code booleanOperation}). */
    private Protocol.PRelOp operationUnder(String db, String schemaCtx) {
        return under(db, schemaCtx, true);
    }

    private Protocol.PRelOp under(String db, String schemaCtx,
            boolean whole) {
        SourceInfo stretch = "default".equals(schemaCtx) ? null
                : currentSchemaDeclSpan;
        if (db.equals(dbFqn) && stretch == null) {
            return whole ? parseOperation(schemaCtx) : parseAtom(schemaCtx);
        }
        DatabaseProtocolParser p =
                new DatabaseProtocolParser(tokens, pos, db, scope, dialect);
        p.schemaStretchSpan = stretch;
        p.currentSchemaDeclSpan = currentSchemaDeclSpan;
        Protocol.PRelOp e = whole ? p.parseOperation(schemaCtx)
                : p.parseAtom(schemaCtx);
        this.pos = p.pos;
        return e;
    }

    /** {@code true} when the {@code [} opens a db pointer followed by an
     *  {@code @}-navigation ({@code [db]@J} — association sides). */
    private boolean bracketLeadsToAt() {
        int i = pos + 1;
        int depth = 1;
        while (i < tokens.count() && depth > 0) {
            TokenType t = tokens.type(i);
            if (t == TokenType.BRACKET_OPEN) {
                depth++;
            } else if (t == TokenType.BRACKET_CLOSE) {
                depth--;
            }
            i++;
        }
        if (i + 3 < tokens.count() && tokens.type(i) == TokenType.PAREN_OPEN
                && tokens.type(i + 2) == TokenType.PAREN_CLOSE
                && tokens.type(i + 3) == TokenType.AT) {
            // [db] (INNER) @Join — the FIRST pointer may carry a type
            // (harvest testInnerJoinReferenceInRelationalMapping)
            return true;
        }
        return i < tokens.count() && tokens.type(i) == TokenType.AT;
    }

    /** {@code @J [> (TYPE) @K]* [| element]} — a join-only nav
     *  (association sides) has NO element. {@code firstType} is a
     *  {@code (INNER)}/{@code (OUTER)} spelled BEFORE the first '@'. */
    private Protocol.PRelOp parseJoinNav(String db, String schemaCtx,
            int s, @com.legend.Nullable String firstType) {
        List<Protocol.PJoinPtr> joinPtrs = new ArrayList<>();
        String curDb = db;
        String pendingType = firstType;
        while (peek() == TokenType.AT) {
            int jS = pos;                           // span INCLUDES the '@'
            advance();
            String jn = parseIdentifier();
            joinPtrs.add(new Protocol.PJoinPtr(
                    curDb.isEmpty() ? null : curDb, pendingType, jn,
                    spanOf(jS, pos - 1)));
            pendingType = null;
            if (peek() == TokenType.GREATER_THAN) {
                advance();
                if (peek() == TokenType.PAREN_OPEN) {
                    // > (INNER) @Next — the TYPE rides the NEXT pointer
                    advance();
                    pendingType = parseIdentifier();
                    validateJoinType(pendingType);
                    expect(TokenType.PAREN_CLOSE);
                }
                if (peek() == TokenType.BRACKET_OPEN) {
                    // > [db2] @Next — re-anchors the NEXT pointer's db;
                    // spans stay AT the '@' (probe nav-chain-db-each-step)
                    advance();
                    curDb = Protocol.unquotePath(parseQualifiedName());
                    expect(TokenType.BRACKET_CLOSE);
                }
            } else {
                break;
            }
        }
        Protocol.PRelOp elem = null;
        if (peek() == TokenType.PIPE) {
            advance();
            // joinOperation: ... (PIPE (booleanOperation |
            // tableAliasColumnOperation))? — the terminal is a WHOLE
            // boolean operation, so `@J | T.X = 1` puts the comparison
            // INSIDE the nav rather than wrapping it
            // (RelationalParserGrammar.g4:227; visitJoinOperation calls
            // visitBooleanOperation on it).
            // The element resolves under the LAST join's (re-anchored)
            // db; inside a NAMED schema its table span stretches from the
            // SCHEMA DECLARATION (probe cross-schema-nav)
            elem = operationUnder(curDb, schemaCtx);
        }
        return new Protocol.PElemtWithJoins(joinPtrs, elem,
                spanOf(s, pos - 1));
    }

    /** The node with its span's END replaced by {@code end}'s end. */
    private static Protocol.PRelOp withSpanEnd(Protocol.PRelOp op,
            SourceInfo end) {
        return switch (op) {
            case Protocol.PDynaFunc f -> new Protocol.PDynaFunc(f.funcName(),
                    f.parameters(), stretch(f.sourceInformation(), end));
            case Protocol.PColumnRef c -> new Protocol.PColumnRef(c.column(),
                    c.table(), c.tableAlias(),
                    stretch(c.sourceInformation(), end));
            case Protocol.PRelLiteral l -> new Protocol.PRelLiteral(l.value(),
                    stretch(l.sourceInformation(), end));
            case Protocol.PRelLambda lam -> new Protocol.PRelLambda(lam.parameterNames(),
                    lam.body(), stretch(lam.sourceInformation(), end));
            case Protocol.PLambdaParam lp -> new Protocol.PLambdaParam(lp.name(),
                    stretch(lp.sourceInformation(), end));
            case Protocol.PElemtWithJoins ej -> ej;   // nav spans stay put
            case Protocol.PRelLiteralList ll -> new Protocol.PRelLiteralList(
                    ll.values(), stretch(ll.sourceInformation(), end));
        };
    }

    private static SourceInfo stretch(SourceInfo base, SourceInfo end) {
        return new SourceInfo("", base.startLine(), base.startColumn(),
                end.endLine(), end.endColumn());
    }

    /** {@code [} opens either a db-qualifier or a literal list — a literal
     *  or string first token means list. */
    private boolean looksLikeLiteralList() {
        TokenType n = peek(1);
        return n == TokenType.INTEGER || n == TokenType.FLOAT
                || n == TokenType.STRING;
    }

    /** One function-operation argument: an operation, or a LAMBDA (4.145.0
     *  {@code functionOperationLambda: identifier PIPE operation |
     *  PAREN_OPEN identifier (COMMA identifier)* PIPE operation PAREN_CLOSE})
     *  — a filter/map/fold over an array take, its body an ordinary
     *  operation over {@code $x} parameters. Parentheses are required for
     *  more than one parameter. The lambda's span covers its whole rule. */
    private Protocol.PRelOp parseFunctionArgument(String schemaCtx) {
        int s = pos;
        if (isIdentifierToken(peek()) && peek(1) == TokenType.PIPE) {
            String param = parseIdentifier();
            expect(TokenType.PIPE);
            Protocol.PRelOp body = parseOperation(schemaCtx);
            return new Protocol.PRelLambda(List.of(param), body, spanOf(s, pos - 1));
        }
        if (peek() == TokenType.PAREN_OPEN && isIdentifierToken(peek(1))
                && (peek(2) == TokenType.PIPE || peek(2) == TokenType.COMMA)) {
            int save = pos;
            advance();
            List<String> params = new ArrayList<>();
            params.add(parseIdentifier());
            while (match(TokenType.COMMA)) {
                if (!isIdentifierToken(peek())) {
                    pos = save;
                    return parseOperation(schemaCtx);
                }
                params.add(parseIdentifier());
            }
            if (peek() != TokenType.PIPE) {
                // `(a, b)` was not a parameter list after all
                pos = save;
                return parseOperation(schemaCtx);
            }
            advance();
            Protocol.PRelOp body = parseOperation(schemaCtx);
            expect(TokenType.PAREN_CLOSE);
            return new Protocol.PRelLambda(params, body, spanOf(s, pos - 1));
        }
        return parseOperation(schemaCtx);
    }

    private Protocol.PRelOp parseAtom(String schemaCtx) {
        int s = pos;
        if (peek() == TokenType.PAREN_OPEN) {
            int gS = pos;
            advance();
            Protocol.PRelOp inner = parseOperation(schemaCtx);
            expect(TokenType.PAREN_CLOSE);
            // parenthesized subexpressions WRAP as a group dynaFunc
            return new Protocol.PDynaFunc("group", List.of(inner),
                    spanOf(gS, pos - 1));
        }
        if (peek() == TokenType.STRING) {
            String quoted = text();
            advance();
            return new Protocol.PRelLiteral(
                    TokenStreamCursor.unquoteAndUnescape(quoted, this),
                    spanOf(s, s));
        }
        if (peek() == TokenType.MINUS) {
            advance();
            int numTok = pos;
            if (peek() == TokenType.INTEGER) {
                long v = -consumeLong();
                return new Protocol.PRelLiteral(v, spanOf(s, numTok));
            }
            double v = -Double.parseDouble(text());
            expect(TokenType.FLOAT);
            return new Protocol.PRelLiteral(v, spanOf(s, numTok));
        }
        if (peek() == TokenType.INTEGER) {
            long v = consumeLong();
            return new Protocol.PRelLiteral(v, spanOf(s, s));
        }
        if (peek() == TokenType.FLOAT) {
            double v = Double.parseDouble(text());
            advance();
            return new Protocol.PRelLiteral(v, spanOf(s, s));
        }
        if (peek() == TokenType.DOLLAR) {
            // $x — a LAMBDA PARAMETER inside a function-operation lambda's
            // body (4.145.0 `lambdaParameter: DOLLAR identifier`); its own
            // sigil keeps it apart from a bare column reference
            advance();
            String name = parseIdentifier();
            return new Protocol.PLambdaParam(name, spanOf(s, pos - 1));
        }
        if (peek() == TokenType.TARGET) {
            // {target} — lexes as ONE token; spells BOTH table and alias
            advance();
            expect(TokenType.DOT);
            String col = parseIdentifier();
            SourceInfo span = spanOf(s, pos - 1);
            return new Protocol.PColumnRef(col, new Protocol.PTablePtr(
                    dbOrNull(), dbOrNull(), schemaCtx, "{target}",
                    spanOf(s, s)), "{target}",
                    span);
        }
        if (peek() == TokenType.BRACKET_OPEN && !looksLikeLiteralList()
                && bracketLeadsToAt()) {
            // [db]@Join — bracket-qualified join nav; span INCLUDES the db
            // pointer (probe assoc-spans E=16 at the '[')
            int bS = pos;
            advance();
            String db = Protocol.unquotePath(parseQualifiedName());
            expect(TokenType.BRACKET_CLOSE);
            String firstType = null;
            if (peek() == TokenType.PAREN_OPEN) {
                advance();
                firstType = parseIdentifier();
                validateJoinType(firstType);
                expect(TokenType.PAREN_CLOSE);
            }
            return parseJoinNav(db, schemaCtx, bS, firstType);
        }
        if (peek() == TokenType.BRACKET_OPEN && !looksLikeLiteralList()) {
            // [db]table.col — bracket-qualified ref: the bracket db becomes
            // the pointer's database; ptr srcInfo = TABLE token only,
            // column span starts at the BRACKET (probe db-qualified)
            advance();
            String db = Protocol.unquotePath(parseQualifiedName());
            expect(TokenType.BRACKET_CLOSE);
            if (peek() == TokenType.VALID_STRING
                    && peek(1) == TokenType.PAREN_OPEN) {
                // [db] add(...) — a dynaFunc under the bracket db; the
                // OUTER op's span starts at the bracket (probe
                // db-dynafunc)
                DatabaseProtocolParser p =
                        new DatabaseProtocolParser(tokens, pos, db, scope, dialect);
                p.currentSchemaDeclSpan = currentSchemaDeclSpan;
                p.schemaStretchSpan = schemaStretchSpan;
                Protocol.PDynaFunc f =
                        (Protocol.PDynaFunc) p.parseAtom(schemaCtx);
                this.pos = p.pos;
                SourceInfo br = spanOf(s, s);
                return new Protocol.PDynaFunc(f.funcName(), f.parameters(),
                        new SourceInfo("", br.startLine(),
                                br.startColumn(),
                                f.sourceInformation().endLine(),
                                f.sourceInformation().endColumn()));
            }
            if (scope != null && peek() == TokenType.VALID_STRING
                    && peek(1) != TokenType.DOT) {
                // [db] BARECOL under a scope header — column of the
                // HEADER table; span bracket..ident (probe db-dynafunc)
                String col = parseIdentifier();
                return new Protocol.PColumnRef(col, new Protocol.PTablePtr(
                        db, db, scope.schema(), scope.table(),
                        scope.tableSpan()), scope.table(),
                        spanOf(s, pos - 1));
            }
            int firstTok = pos;                     // FIRST ident anchors
            int tblEndTok = pos;
            String schema = "default";
            String table = parseIdentifier();
            expect(TokenType.DOT);
            String column = parseIdentifier();
            if (peek() == TokenType.DOT) {
                advance();                          // [db]schema.table.col
                schema = table;
                table = column;
                tblEndTok = pos - 2;
                column = parseIdentifier();
            }
            return new Protocol.PColumnRef(column, new Protocol.PTablePtr(db,
                    db, schema, table, spanOf(firstTok, tblEndTok)), table,
                    spanOf(s, pos - 1));
        }
        if (peek() == TokenType.BRACKET_OPEN && looksLikeLiteralList()) {
            int lS = pos;
            advance();
            List<Protocol.PRelLiteral> items = new ArrayList<>();
            while (peek() != TokenType.BRACKET_CLOSE && !atEnd()) {
                int iS = pos;
                Object v;
                if (peek() == TokenType.STRING) {
                    v = TokenStreamCursor.unquoteAndUnescape(text(), this);
                } else if (peek() == TokenType.INTEGER) {
                    v = Long.parseLong(text());
                } else if (peek() == TokenType.FLOAT) {
                    v = Double.parseDouble(text());
                } else {
                    throw error("unsupported literal-list element: "
                            + safeText());
                }
                advance();
                items.add(new Protocol.PRelLiteral(v, spanOf(iS, iS)));
                match(TokenType.COMMA);
            }
            expect(TokenType.BRACKET_CLOSE);
            return new Protocol.PRelLiteralList(items, spanOf(lS, pos - 1));
        }
        if (peek() == TokenType.AT) {
            return parseJoinNav(dbFqn, schemaCtx, pos, null);
        }
        // identifier: function call, or dotted column ref
        String first = parseIdentifier();
        if (scope != null && peek() != TokenType.PAREN_OPEN
                && peek() != TokenType.DOT) {
            // bare ident under scope = column of the HEADER table; the
            // pointer span points AT THE HEADER tokens (probe scope-forms)
            return new Protocol.PColumnRef(first, new Protocol.PTablePtr(
                    dbFqn, dbFqn, scope.schema(), scope.table(),
                    scope.tableSpan()), scope.table(), spanOf(s, s));
        }
        if (peek() == TokenType.PAREN_OPEN) {
            advance();
            List<Protocol.PRelOp> args = new ArrayList<>();
            while (peek() != TokenType.PAREN_CLOSE && !atEnd()) {
                args.add(parseFunctionArgument(schemaCtx));
                match(TokenType.COMMA);
            }
            expect(TokenType.PAREN_CLOSE);
            return new Protocol.PDynaFunc(first, args, spanOf(s, pos - 1));
        }
        // A.col | S.T.col — schema-qualified when two dots; the TABLE
        // pointer's span runs through the TABLE token (probe: 'S.T1' 11-14)
        if (peek() != TokenType.DOT) {
            // engine takes the bare identifier at the GRAMMAR level
            // (tableAliasColumnOperationWithScopeInfo: relationalIdentifier
            // (DOT scopeInfo)?) and refuses it in the walker
            // (RelationalParseTreeWalker.java:993). Same rejection, same
            // words — there is no implicit-table column ref in either AST.
            throw error("Missing table or alias for column '" + first + "'");
        }
        expect(TokenType.DOT);
        int tblEnd = pos - 2;                       // the first identifier
        String second = parseIdentifier();
        String schema = schemaCtx;
        String table = first;
        String column = second;
        boolean explicitSchema = false;
        if (peek() == TokenType.DOT) {
            advance();
            schema = first;
            table = second;
            tblEnd = pos - 2;                       // through the table ident
            column = parseIdentifier();
            explicitSchema = true;
        }
        if (scope != null && scope.singleSeg() && !explicitSchema) {
            // a SINGLE-segment header is a SCHEMA prefix for dotted refs
            // (probe single-seg-scope-dotted): schema = the header seg,
            // span stretches header..local table
            schema = scope.table();
        }
        SourceInfo span = spanOf(s, pos - 1);
        SourceInfo tblSpan = spanOf(s, tblEnd);
        SourceInfo declAnchor = schemaStretchSpan != null
                ? schemaStretchSpan
                : (!"default".equals(schemaCtx) ? currentSchemaDeclSpan
                        : null);
        if (declAnchor != null && !explicitSchema) {
            // dotted refs inside a NAMED schema: span = schema
            // declaration name .. local table end (probes
            // cross-schema-nav + testGrammarSerializationExtension)
            tblSpan = new SourceInfo("", declAnchor.startLine(),
                    declAnchor.startColumn(), tblSpan.endLine(),
                    tblSpan.endColumn());
        } else if (scope != null && !explicitSchema) {
            // under a schema.table scope header, a dotted ref's TABLE
            // pointer span STRETCHES from the header's schema through the
            // LOCAL table token (probe scope-dotted w; chainedJoinsInner)
            tblSpan = new SourceInfo("", scope.tableSpan().startLine(),
                    scope.tableSpan().startColumn(), tblSpan.endLine(),
                    tblSpan.endColumn());
        }
        return new Protocol.PColumnRef(column, new Protocol.PTablePtr(
                dbOrNull(), dbOrNull(), schema, table, tblSpan),
                table, span);
    }
}
