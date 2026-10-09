package com.legend.sdlc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The SDLC's compiler: legend-lite's own. {@link #modelJson} is the text-to-JSON conversion without source
 * information ({@code PmcdParser.parseDocument(text, false)}, the engine-exact protocol JSON) -- the one the
 * server's {@code grammar/grammarToJson/model} calls (docs/PROTOCOL_PROGRAM_2026_10_05.md, invariant 5; leg 7 moves
 * this class out of SDLC's rules); {@link #compile} is the server's {@code compilation/compile}
 * ({@code Compiler.compileModel}, then {@code Compiler.compileAllBodies}).
 */
public final class CoreGrammar implements Sdlc.Grammar {
    @Override
    public String modelJson(String text) {
        return com.legend.parser.PmcdParser.parseDocument(text, false);
    }

    @Override
    public List<String> compile(String model) {
        try {
            Map<String, String> walls = com.legend.Compiler.compileAllBodies(com.legend.Compiler.compileModel(model));
            return new ArrayList<>(walls.values());
        } catch (RuntimeException | StackOverflowError e) {
            return List.of(e.getMessage() == null ? e.getClass().getName() : e.getMessage());
        }
    }
}
