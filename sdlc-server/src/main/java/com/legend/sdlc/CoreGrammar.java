package com.legend.sdlc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The SDLC's compiler: legend-lite's own. {@link #modelJson} is {@code grammarToJson}
 * ({@code PmcdParser.parseDocument}, the engine-exact protocol JSON) without source information -- the
 * same call the server's {@code grammar/grammarToJson/model} and the planner's {@code modelJsonOrError}
 * make; {@link #compile} is the server's {@code compilation/compile} ({@code Compiler.compileModel}, then
 * {@code Compiler.compileAllBodies}).
 */
public final class CoreGrammar implements Sdlc.Grammar {
    @Override
    public String modelJson(String text) {
        return com.legend.protocol.SourceInformation.strip(com.legend.parser.PmcdParser.parseDocument(text));
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
