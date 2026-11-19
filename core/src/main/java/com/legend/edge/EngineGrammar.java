// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * LEGEND-ENGINE'S GRAMMAR, AT THE EDGE (J21 of the edge audit, 2026-11-17, the user's ruling "yes to the rule"): the
 * product has one grammar, legend-lite's; where the edge reads legend-engine's protocol -- pure/v1's
 * {@code grammar/grammarToJson} routes and its model input's records -- it parses at legend-engine's level, its
 * refusals and its shapes, byte for byte, through the parse door's engine entries, which only the edge reaches
 * ({@code ArchitectureTest.legendEnginesGrammarIsTheEdgesAlone}).
 */
public final class EngineGrammar {

    private EngineGrammar() {
    }

    /** A model's text as its protocol JSON, as legend-engine's {@code grammarToJson/model} answers it. */
    public static String documentJson(String text, boolean sourceInformation) {
        String json = com.legend.door.Parse.engineDocumentJson(text);
        return sourceInformation ? json : EngineSpans.withoutSpans(json);
    }

    /** A lambda's text as its protocol JSON, as legend-engine's {@code grammarToJson/lambda} answers it. */
    public static String lambdaJson(String text, boolean sourceInformation) {
        String json = com.legend.door.Parse.engineLambdaJson(text);
        return sourceInformation ? json : EngineSpans.withoutSpans(json);
    }

    /** A request's model as its records: a {@code PureModelContextText}'s as legend-engine reads it, a
     *  {@code PureModelContextData}'s as read. */
    public static com.legend.protocol.Protocol.PureModelContextData records(ModelInput model) {
        return switch (model) {
            case ModelInput.Text t -> com.legend.door.Parse.engineRecords(t.code());
            case ModelInput.Data d -> d.model();
        };
    }
}
