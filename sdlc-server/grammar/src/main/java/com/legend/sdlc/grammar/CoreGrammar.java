package com.legend.sdlc.grammar;

import com.legend.sdlc.Sdlc;

import java.util.List;

/**
 * The SDLC's two Pure questions answered by legend-lite's own compiler, in process
 * (docs/PROTOCOL_PROGRAM_2026_10_05.md, leg 7): the one place the SDLC reaches the compiler, so its rules
 * ({@code //sdlc-server:rules}) depend on no Pure code and the build refuses them any. {@link #modelJson} is the
 * text-to-JSON conversion without source information at lite's dialect ({@code PmcdParser.parseLiteDocument(text,
 * false)}: legend-engine's protocol JSON, lite's own words as lite's JSON) -- what lite's {@code grammarToJson/model}
 * answers; {@link #compile} is the whole-model
 * compile both {@code compilation/compile} routes answer from ({@code Compiler.compileErrors}, leg 6). The SDLC's
 * server and its page wire it in.
 */
public final class CoreGrammar implements Sdlc.Grammar {
    @Override
    public String modelJson(String text) {
        // at lite's dialect (docs/ONE_CONNECTION_DESK_2026_10_28.md D1, D5): a file holding lite's own words (a Deephaven
        // connection, a SQLite datasource, a pre-shared key) is an entity like any other, written as lite's own JSON;
        // every other element as legend-engine writes it
        return com.legend.parser.PmcdParser.parseLiteDocument(text, false);
    }

    /** {@link #modelJson} with each element's span (its first character to its last, in lines and columns from 1). */
    @Override
    public String modelJsonWithSpans(String text) {
        return com.legend.parser.PmcdParser.parseLiteDocument(text, true);
    }

    /** The packages every element resolves through without importing them ({@link com.legend.compiler.CoreImports}). */
    @Override
    public List<String> importedByItself() {
        return com.legend.compiler.CoreImports.SEQUENCE;
    }

    @Override
    public List<String> compile(String model) {
        try {
            return com.legend.Compiler.compileErrors(model);
        } catch (RuntimeException | StackOverflowError e) {
            // a failure that is not the model's (a compiler bug) still keeps the tree from landing
            return List.of(e.getMessage() == null ? e.getClass().getName() : e.getMessage());
        }
    }
}
