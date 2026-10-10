package com.legend.sdlc.grammar;

import com.legend.sdlc.Sdlc;

import java.util.List;

/**
 * The SDLC's two Pure questions answered by legend-lite's own compiler, in process
 * (docs/PROTOCOL_PROGRAM_2026_10_05.md, leg 7): the one place the SDLC reaches the compiler, so its rules
 * ({@code //sdlc-server:rules}) depend on no Pure code and the build refuses them any. {@link #modelJson} is the
 * text-to-JSON conversion without source information ({@code PmcdParser.parseDocument(text, false)}, the engine-exact
 * protocol JSON) -- the one the server's {@code grammar/grammarToJson/model} calls; {@link #compile} is the whole-model
 * compile both {@code compilation/compile} routes answer from ({@code Compiler.compileErrors}, leg 6). The SDLC's
 * server and its page wire it in.
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
