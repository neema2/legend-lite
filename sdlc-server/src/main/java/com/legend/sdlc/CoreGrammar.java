package com.legend.sdlc;

import java.util.List;

/**
 * The SDLC's compiler: legend-lite's own. {@link #modelJson} is the text-to-JSON conversion without source
 * information ({@code PmcdParser.parseDocument(text, false)}, the engine-exact protocol JSON) -- the one the
 * server's {@code grammar/grammarToJson/model} calls (docs/PROTOCOL_PROGRAM_2026_10_05.md, invariant 5; leg 7 moves
 * this class out of SDLC's rules); {@link #compile} is the whole-model compile both {@code compilation/compile}
 * routes answer from ({@code Compiler.compileErrors}, leg 6).
 */
public final class CoreGrammar implements Sdlc.Grammar {
    @Override
    public String modelJson(String text) {
        return com.legend.parser.PmcdParser.parseDocument(text, false);
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
