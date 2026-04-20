package com.gs.legend.model.m3;

import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.model.def.StereotypeApplication;
import com.gs.legend.model.def.TaggedValue;

import java.util.List;
import java.util.Objects;

/**
 * Compiled-metamodel representation of a Pure user function — the typed downstream form of
 * {@link com.gs.legend.model.def.FunctionDefinition} (parse layer). Mirrors the
 * {@link PureClass} / {@link PureEnum} pattern: parse artifact stays in {@code model.def}
 * with raw strings, typed metamodel lives in {@code model.m3}.
 *
 * <p>Fields are all resolved — every {@link Type} reference is a canonical variant of the
 * sealed {@link Type} hierarchy ({@link Primitive}, {@link Type.ClassType}, {@link Type.EnumType},
 * {@link Type.FunctionType}, ...). Consumers dispatch via pattern-match / identity rather than
 * string comparison; there is no dual-carriage (no {@code parsedType} alongside a raw
 * {@code String type}).
 *
 * <p>{@code body} is the parsed + name-resolved AST of the function body (list of statements
 * matching {@link com.gs.legend.parser.PureParser#parseCodeBlock}). It is <em>not</em>
 * type-checked yet — type-checking produces {@link com.gs.legend.compiled.CompiledFunction}.
 *
 * <p>Phase 6 of the PureFunction split plan introduces {@code FunctionBody} as a sealed
 * wrapper ({@code Resolved | Lazy}) so late-binding / REPL / cross-project demand-driven
 * compilation become typed and thread-safe. Until then, the body is eagerly populated at
 * {@link com.gs.legend.model.PureModelBuilder} assemble time.
 *
 * @param qualifiedName        Fully qualified function name (e.g., {@code "my::utils::greet"})
 * @param typeParams           Generic type parameter names (empty for ordinary user functions;
 *                             populated for native-function signatures and future user generics)
 * @param parameters           Function parameters in declared order
 * @param returnType           Resolved return type
 * @param returnMultiplicity   Return multiplicity
 * @param body                 Parsed + name-resolved body statements (not yet type-checked)
 * @param stereotypes          Stereotype annotations
 * @param taggedValues         Tagged-value annotations
 */
public record PureFunction(
        String qualifiedName,
        List<String> typeParams,
        List<Parameter> parameters,
        Type returnType,
        Multiplicity returnMultiplicity,
        List<ValueSpecification> body,
        List<StereotypeApplication> stereotypes,
        List<TaggedValue> taggedValues) {

    public PureFunction {
        Objects.requireNonNull(qualifiedName, "PureFunction qualifiedName cannot be null");
        Objects.requireNonNull(returnType, "PureFunction returnType cannot be null");
        Objects.requireNonNull(returnMultiplicity, "PureFunction returnMultiplicity cannot be null");
        Objects.requireNonNull(body, "PureFunction body cannot be null");
        if (qualifiedName.isBlank()) {
            throw new IllegalArgumentException("PureFunction qualifiedName cannot be blank");
        }
        typeParams    = typeParams    == null ? List.of() : List.copyOf(typeParams);
        parameters    = parameters    == null ? List.of() : List.copyOf(parameters);
        body          = List.copyOf(body);
        stereotypes   = stereotypes   == null ? List.of() : List.copyOf(stereotypes);
        taggedValues  = taggedValues  == null ? List.of() : List.copyOf(taggedValues);
    }
}
