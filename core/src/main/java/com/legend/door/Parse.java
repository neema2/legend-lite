// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.door;

import com.legend.model.ModelFromProtocol;
import com.legend.model.ParsedModel;
import com.legend.parser.Dialect;
import com.legend.parser.PmcdParser;
import com.legend.parser.SpecParser;
import com.legend.protocol.Protocol;
import com.legend.protocol.spec.ValueSpecification;


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
 */
public final class Parse {

    private Parse() {
    }

    /** A model's text as its records (phases A and B): the server's model input. */
    public static Protocol.PureModelContextData records(String source) {
        com.legend.base.RunScope.planning("parse a model (phases A and B)");
        return PmcdParser.parseModel(source);
    }

    /** A model's text as its protocol JSON (phases A and B): the server's grammar routes and model changes. */
    public static String documentJson(String source, boolean sourceInformation) {
        com.legend.base.RunScope.planning("parse a model (phases A and B)");
        return PmcdParser.parseDocument(source, sourceInformation);
    }

    /** A query's text as its value specification (phase C). */
    public static ValueSpecification query(String source, Dialect dialect) {
        com.legend.base.RunScope.planning("parse a query (phase C)");
        return SpecParser.parse(source, dialect);
    }

    /** A lambda's text as its protocol JSON (phase C): the grammar route and the boundary's programs. */
    public static String lambdaJson(String source, boolean sourceInformation) {
        com.legend.base.RunScope.planning("parse a lambda (phase C)");
        return SpecParser.lambdaJson(source, sourceInformation);
    }

    /** A model's records as the compiler's parsed model (phase B): the records' door. Its refusal of a shape the model
     *  cannot represent ({@code ModelFromProtocol.Unrepresentable}) is the caller's to word. */
    public static ParsedModel model(Protocol.PureModelContextData records) {
        com.legend.base.RunScope.planning("read a model's records (phase B)");
        return ModelFromProtocol.of(records);
    }
}
