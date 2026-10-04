package com.legend.sdlc;

/**
 * The SDLC's grammar: legend-lite's own {@code grammarToJson} ({@code PmcdParser.parseDocument}, the
 * engine-exact protocol JSON), without source information -- the same call the server's
 * {@code grammar/grammarToJson/model} and the WebAssembly planner's {@code modelJsonOrError} make.
 */
public final class CoreGrammar implements Sdlc.Grammar {
    @Override
    public String modelJson(String text) {
        return com.legend.protocol.SourceInformation.strip(com.legend.parser.PmcdParser.parseDocument(text));
    }
}
