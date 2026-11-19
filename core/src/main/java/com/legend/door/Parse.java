// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.door;

import com.legend.model.ModelFromProtocol;
import com.legend.model.ParsedModel;
import com.legend.parser.GrammarLevel;
import com.legend.parser.PmcdParser;
import com.legend.parser.SpecParser;
import com.legend.protocol.Protocol;
import com.legend.protocol.cypher.CypherAst;
import com.legend.protocol.cypher.CypherParser;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.InputDialect;
import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.ts.TsAst;
import com.legend.protocol.ts.TsParser;


/**
 * THE PARSE DOORS (C5-J item 1, 2026-11-15: a wider Java witness). The lexer (phase A) is JDK-only (invariant 7a) and
 * the parser (the parser half of B, and C) depends on nothing above it (7c), {@code RunScope} included; the model's own
 * door ({@code ModelFromProtocol}) is pure data (6j). None of them can ask the run's wall. So every caller outside them
 * comes here -- or to {@link ModelFromText}, or to the IDE's document door ({@code ide.DocumentParse}: a token stream
 * is the parser's and the IDE's alone, invariant 6f), each asking the same way -- and each door asks FIRST, then makes
 * the one call: inside a run each is refused by its phase's name, as phases D to J are. A door holds no lambda, no reference and
 * no state, so its question is asked when the parse runs, never when a callback is made
 * ({@code ArchitectureTest.everyParseIsReachedThroughADoorThatAsks}); the rule that keeps every other class away is
 * {@code ArchitectureTest.PARSE_BY_THE_DOORS}, and {@code PhaseEntriesRefusedInsideARunTest} pins each door's refusal.
 *
 * <p>The embedded languages' readers come here too (C5-J's last round, A1, 2026-11-15): an island's text -- a
 * {@code #SQL} or {@code #IQ} statement read by its dialect, a {@code #TS} query or item, a {@code #Cypher} statement --
 * is read by a reader in the protocol layer ({@code protocol.sql}, {@code protocol.ts}, {@code protocol.cypher}), which
 * depends on nothing but values and the JSON module (invariant 7b) and so cannot ask. Its translators take the tree,
 * never the text: the compiler's island seams and the server's routes read the text here.
 */
public final class Parse {

    private Parse() {
    }

    /** A model's text as its records (phases A and B) at {@code level}, the door's mode's grammar (20:10-R7, the
     *  user's ruling of 2026-11-18: the mode chosen at the door decides the grammar; until S1m of the presentation
     *  program this was {@code engineRecords}, at legend-engine's level): the edge's pure/v1 model input passes
     *  legend-engine's ({@code edge.Modes.ENGINE}), where the edge reads legend-engine's protocol (J21 of the edge
     *  audit, 2026-11-17; {@code ArchitectureTest.legendEnginesGrammarIsTheEdgesAlone}). */
    public static Protocol.PureModelContextData records(String source, GrammarLevel level) {
        com.legend.base.RunScope.planning("parse a model (phases A and B)");
        return PmcdParser.parse(source, level).model();
    }

    /** A model's text as its protocol JSON (phases A and B): the server's grammar routes and model changes, at the
     *  product's one grammar (J21, 2026-11-17). */
    public static String documentJson(String source, boolean sourceInformation) {
        return documentJson(source, sourceInformation, GrammarLevel.LEGEND_LITE);
    }

    /** {@link #documentJson(String, boolean)} at {@code level}, the door's mode's grammar (20:10-R7, 2026-11-18; until
     *  S1m, {@code engineDocumentJson} at legend-engine's): without source information EVERY span gone (J35 of the edge
     *  audit, 2026-11-18: legend-engine 4.145.0's kept spans, edge.EngineSpans, are the edge's, which asks with them). */
    public static String documentJson(String source, boolean sourceInformation, GrammarLevel level) {
        com.legend.base.RunScope.planning("parse a model (phases A and B)");
        String json = PmcdParser.parseDocument(source, true, level);
        return sourceInformation ? json : com.legend.protocol.SourceInformation.stripAll(json);
    }

    /** A query's text as its value specification (phase C). */
    public static ValueSpecification query(String source, GrammarLevel dialect) {
        com.legend.base.RunScope.planning("parse a query (phase C)");
        return SpecParser.parse(source, dialect);
    }

    /** A lambda's text as its protocol JSON (phase C): the grammar route and the boundary's programs, at the
     *  product's one grammar (J21, 2026-11-17). */
    public static String lambdaJson(String source, boolean sourceInformation) {
        return lambdaJson(source, sourceInformation, GrammarLevel.LEGEND_LITE);
    }

    /** {@link #lambdaJson(String, boolean)} at {@code level}, the door's mode's grammar (20:10-R7, 2026-11-18; until
     *  S1m, {@code engineLambdaJson} at legend-engine's): without source information every span gone (J35,
     *  2026-11-18; legend-engine's kept spans are the edge's, which asks with them). */
    public static String lambdaJson(String source, boolean sourceInformation, GrammarLevel level) {
        com.legend.base.RunScope.planning("parse a lambda (phase C)");
        String json = SpecParser.lambdaJson(source, true, level);
        return sourceInformation ? json : com.legend.protocol.SourceInformation.stripAll(json);
    }

    /** A model's records as the compiler's parsed model (phase B): the records' door. Its refusal of a shape the model
     *  cannot represent ({@code ModelFromProtocol.Unrepresentable}) is the caller's to word. */
    public static ParsedModel model(Protocol.PureModelContextData records) {
        com.legend.base.RunScope.planning("read a model's records (phase B)");
        return ModelFromProtocol.of(records);
    }

    /** An island's SQL as its statement (an island's text, phase C), read by the island's dialect -- Postgres's for
     *  {@code #SQL}, SAP IQ's under its profile for {@code #IQ}: what its translator ({@code SqlToPure}) reads, by the
     *  same dialect, which keeps what the reading found about the statement. */
    public static SqlAst.Statement sqlIsland(String sql, InputDialect dialect) {
        com.legend.base.RunScope.planning("parse a SQL island (phase C)");
        return dialect.parse(sql);
    }

    /** A {@code #TS} island's query as its tree (an island's text, phase C): what {@code TsToPure} translates and the
     *  server's {@code jsonToTs} shows. */
    public static TsAst.Query tsIsland(String text) {
        com.legend.base.RunScope.planning("parse a timeseries island (phase C)");
        return TsParser.parse(text);
    }

    /** One {@code #TS} item as a formula box holds a series, as its tree (phase C): the server's {@code tsItem}. */
    public static TsAst.Line tsItem(String text) {
        com.legend.base.RunScope.planning("parse a timeseries item (phase C)");
        return TsParser.item(text);
    }

    /** A {@code #Cypher} island's statement as its tree (an island's text, phase C): what {@code CypherToPure}
     *  translates. */
    public static CypherAst.Statement cypherIsland(String text) {
        com.legend.base.RunScope.planning("parse a Cypher island (phase C)");
        return CypherParser.parse(text);
    }
}
