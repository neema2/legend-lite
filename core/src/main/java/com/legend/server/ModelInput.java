package com.legend.server;

import com.legend.json.Json;
import com.legend.parser.PmcdParser;
import com.legend.protocol.ModelReader;
import com.legend.protocol.Protocol;

import java.util.List;
import java.util.Objects;

/**
 * A request's model context, as legend-engine's compile, type and plan routes take one (docs/PROTOCOL_PROGRAM_2026_10_05.md
 * leg 8, step 3): a {@code PureModelContextText} ({@code {"_type":"text","code":...}}), compiled from its text, or a
 * {@code PureModelContextData} ({@code {"_type":"data","elements":[...]}}, as upstream Studio, an SDLC or Depot sends
 * one), read into the protocol records ({@link ModelReader}) and compiled from them through the one door
 * ({@code ModelFromProtocol}) -- never printed and parsed again. A context of another kind (a pointer the engine loads
 * from an SDLC) is refused by name.
 */
sealed interface ModelInput {

    /** A {@code PureModelContextText}: the model's code. */
    record Text(String code) implements ModelInput {
        public Text {
            Objects.requireNonNull(code, "code");
        }

        @Override
        public com.legend.compiler.element.ModelContext compile() {
            return com.legend.Compiler.compileModel(code);
        }

        @Override
        public <T> T with(PureV1Api.Models models, java.util.function.Function<com.legend.compiler.element.ModelContext, T> use) {
            return models.with(code, use);
        }

        @Override
        public List<String> compileErrors() {
            return com.legend.Compiler.compileErrors(code);
        }

        @Override
        public Protocol.PureModelContextData records() {
            return PmcdParser.parseModel(code);
        }
    }

    /** A {@code PureModelContextData}: the model's records, as read from its JSON. */
    record Data(Protocol.PureModelContextData model) implements ModelInput {
        public Data {
            Objects.requireNonNull(model, "model");
        }

        @Override
        public com.legend.compiler.element.ModelContext compile() {
            return com.legend.Compiler.compileModel(model);
        }

        @Override
        public <T> T with(PureV1Api.Models models, java.util.function.Function<com.legend.compiler.element.ModelContext, T> use) {
            // records hold no text to name a held model by: compiled for this use
            return use.apply(compile());
        }

        @Override
        public List<String> compileErrors() {
            return com.legend.Compiler.compileErrors(model);
        }

        @Override
        public Protocol.PureModelContextData records() {
            return model;
        }
    }

    /** The model compiled: its elements, a body only when asked (as {@code Compiler.compileModel}). */
    com.legend.compiler.element.ModelContext compile();

    /** {@code use} of the model compiled -- a text's by {@code models} (a host's held model for that text, when it holds
     *  one), a JSON model's for this use alone. */
    <T> T with(PureV1Api.Models models, java.util.function.Function<com.legend.compiler.element.ModelContext, T> use);

    /** The model compiled whole, every body included: its errors ({@code Compiler.compileErrors}). */
    List<String> compileErrors();

    /** The model's records: a text's as legend-engine reads it (the engine's dialect), a JSON model's as read. */
    Protocol.PureModelContextData records();

    /** A request's {@code model}: text or data; any other context is refused, naming it. */
    static ModelInput of(Json.Obj model) {
        String type = model.getStringOr("_type", "");
        if ("text".equals(type)) {
            return new Text(model.getString("code"));
        }
        if ("data".equals(type)) {
            return new Data(ModelReader.read(model));
        }
        throw new IllegalArgumentException("a model of _type '" + type + "': legend-lite reads "
                + "PureModelContextText ({\"_type\":\"text\",\"code\":...}) and PureModelContextData "
                + "({\"_type\":\"data\",\"elements\":[...]})");
    }
}
