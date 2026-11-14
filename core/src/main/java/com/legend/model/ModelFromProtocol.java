package com.legend.model;

import com.legend.protocol.Protocol;
import com.legend.protocol.SourceInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE ONE DOOR from protocol records to the compiler's model (docs/PROTOCOL_PROGRAM_2026_10_05.md, leg 8): a
 * {@code PureModelContextData} -- read from a client's JSON ({@code ModelReader}), or parsed -- to the
 * {@link ParsedModel} the compiler builds from. Each element goes through its kind's converter, the same function the
 * parser calls for that kind ({@link FromProtocol}, {@link MappingFromProtocol}, and the section kinds' conversions
 * here, which the parser's section grammars delegate to), so a model given as JSON and one given as text reach the
 * compiler identically; nothing is printed and parsed again.
 *
 * <p>A section index's imports apply to the elements its section lists, as legend-engine compiles them; a model
 * without one (an SDLC's entities) has no imports. An element's position is its record's source information, kept
 * beside the model ({@link ParsedModel#elementSpans}) as a parse keeps its offsets.
 */
public final class ModelFromProtocol {

    private ModelFromProtocol() {
    }

    /** A model given as records, as the compiler's parsed model. A shape the model cannot represent is refused by
     *  name ({@link Unrepresentable}), never dropped. */
    public static ParsedModel of(Protocol.PureModelContextData model) {
        return of(model, null, List.of());
    }

    /** {@link #of(Protocol.PureModelContextData)} for a model parsed from {@code source}: the text kept on the model,
     *  and the sections no grammar claimed (the platform dialect records them). */
    public static ParsedModel of(Protocol.PureModelContextData model, @com.legend.base.Nullable String source,
            List<ParsedModel.UnclaimedSection> unclaimed) {
        Map<String, Protocol.PSection> sectionOf = new HashMap<>();
        // the model's whole import scope: every section's, a section with no element included
        ImportScope.Builder all = new ImportScope.Builder();
        boolean indexed = false;
        for (Protocol.Element e : model.elements()) {
            if (e instanceof Protocol.PSectionIndex index) {
                indexed = true;
                for (Protocol.PSection s : index.sections()) {
                    for (String pkg : s.imports()) {
                        all.add(pkg + "::*");
                    }
                    for (String path : s.elements()) {
                        if (path != null) {
                            sectionOf.putIfAbsent(path, s);
                        }
                    }
                }
            }
        }
        List<PackageableElement> elements = new ArrayList<>();
        Map<String, ImportScope> elementImports = new LinkedHashMap<>();
        Map<String, ParsedModel.Position> spans = new LinkedHashMap<>();
        for (Protocol.Element e : model.elements()) {
            if (e instanceof Protocol.PSectionIndex) {
                continue;
            }
            PackageableElement el = element(e);
            elements.add(el);
            String key = ParsedModel.keyOf(el);
            Protocol.PSection section = sectionOf.get(pathOf(e));
            if (section == null && indexed) {
                // a model with a section index lists each element in the section whose imports it reads; one it
                // does not list would fall back to every section's imports, the leak the one parser removed --
                // refused by name (a path spelled otherwise than lite's, e.g. another mangling of a function's)
                throw unrepresentable(e, "the model's section index lists no section for it");
            }
            if (section != null) {
                ImportScope.Builder own = new ImportScope.Builder();
                for (String pkg : section.imports()) {
                    // the section index lists an import as its package (`a::b`), the grammar writes `a::b::*`
                    own.add(pkg + "::*");
                }
                elementImports.putIfAbsent(key, own.build());
            }
            SourceInfo span = spanOf(e);
            if (span != null) {
                spans.putIfAbsent(key, new ParsedModel.Position(span.startLine(), span.startColumn()));
            }
        }
        if (source != null) {
            // a model parsed from a text keeps where each element starts as an offset into that text, as it always
            // has (what reads a model's text by offset goes on working); one kind of position per model
            Map<String, Integer> offsets = new LinkedHashMap<>();
            int[] lineStarts = lineStarts(source);
            for (Map.Entry<String, ParsedModel.Position> p : spans.entrySet()) {
                int line = p.getValue().line();
                int offset = line >= 1 && line <= lineStarts.length
                        ? lineStarts[line - 1] + p.getValue().column() - 1 : -1;
                if (offset < 0 || offset > source.length()) {
                    // the spans were parsed from this text: one outside it is the parser's bug, never clamped
                    throw new IllegalStateException(p.getKey() + "'s span " + line + ":" + p.getValue().column()
                            + " is outside the text it was parsed from");
                }
                offsets.put(p.getKey(), offset);
            }
            return new ParsedModel(elements, all.build(), source, offsets, elementImports, Map.of(), unclaimed,
                    Map.of());
        }
        return new ParsedModel(elements, all.build(), null, Map.of(), elementImports, Map.of(), unclaimed, spans);
    }

    /** The char offset each line of {@code text} starts at (line 1 at index 0). */
    private static int[] lineStarts(String text) {
        List<Integer> starts = new ArrayList<>();
        starts.add(0);
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) == '\n') {
                starts.add(i + 1);
            }
        }
        int[] out = new int[starts.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = starts.get(i);
        }
        return out;
    }

    /**
     * A shape the compiler's model cannot represent (a converter's refusal), named: the element's path, the reason,
     * and where the element starts when its record says. The parser answers it as a parse error at that place, as it
     * always has; a model given as JSON is answered with a construct legend-lite does not compile.
     */
    public static final class Unrepresentable extends RuntimeException {
        private final String path;
        private final String reason;
        private final ParsedModel.@com.legend.base.Nullable Position at;

        Unrepresentable(String path, String reason, ParsedModel.@com.legend.base.Nullable Position at) {
            super(path + ": " + reason);
            this.path = path;
            this.reason = reason;
            this.at = at;
        }

        public String path() {
            return path;
        }

        public String reason() {
            return reason;
        }

        public ParsedModel.@com.legend.base.Nullable Position at() {
            return at;
        }
    }

    /**
     * One element record as the compiler's model: its kind's converter, a shape the model cannot represent refused as
     * {@link Unrepresentable}. A section index is not an element. The text's door ({@code door.ModelFromText}) calls
     * this for an element the IDE parses alone.
     */
    public static PackageableElement element(Protocol.Element e) {
        try {
            return converted(e);
        } catch (FromProtocol.UnsupportedConnectionShape u) {
            throw unrepresentable(e, u.reason());
        } catch (MappingFromProtocol.UnsupportedMappingShape u) {
            throw unrepresentable(e, u.reason());
        }
    }

    private static Unrepresentable unrepresentable(Protocol.Element e, String reason) {
        SourceInfo span = spanOf(e);
        return new Unrepresentable(pathOf(e), reason,
                span == null ? null : new ParsedModel.Position(span.startLine(), span.startColumn()));
    }

    /** One element record as the compiler's model: its kind's converter, whose refusals {@link #element} names. */
    private static PackageableElement converted(Protocol.Element e) {
        return switch (e) {
            case Protocol.PClass c -> FromProtocol.toClassDefinition(c);
            case Protocol.PAssociation a -> FromProtocol.toAssociationDefinition(a);
            case Protocol.PEnumeration en -> FromProtocol.toEnumDefinition(en);
            case Protocol.PFunction f -> FromProtocol.toFunctionDefinition(f);
            case Protocol.PProfile p -> FromProtocol.toProfileDefinition(p);
            case Protocol.PMeasure m -> FromProtocol.toMeasureDefinition(m);
            case Protocol.PRuntime r -> FromProtocol.toRuntimeElement(r);
            case Protocol.PConnection c -> FromProtocol.toConnectionElement(c);
            case Protocol.PDatabase d -> FromProtocol.toDatabaseDefinition(d);
            case Protocol.PMapping m -> MappingFromProtocol.toMappingElement(m);
            case Protocol.PService s -> FromProtocol.toServiceSectionElement(s);
            case Protocol.PExecutionEnvironment x -> FromProtocol.toServiceSectionElement(x);
            case Protocol.PDataSpace d -> FromProtocol.toDataSpaceDefinition(d);
            case Protocol.PPersistence p -> FromProtocol.toPersistenceElement(p);
            case Protocol.PPersistenceContext p -> FromProtocol.toPersistenceElement(p);
            case Protocol.PFunctionActivator a -> activator(a);
            case Protocol.PDataElement d -> new DataDefinition(d.qualifiedName(), d);
            // the kinds no compiler phase opens: a named carrier, by section and kind
            case Protocol.PDiagram d -> carried("Diagram", "Diagram", d.qualifiedName());
            case Protocol.PText t -> carried("Text", "Text", t.qualifiedName());
            case Protocol.PGenerationSpecification g ->
                    carried("GenerationSpecification", "GenerationSpecification", g.qualifiedName());
            case Protocol.PFileGeneration f -> carried("FileGeneration", f.type(), f.qualifiedName());
            case Protocol.PDeephavenDatabase d -> carried("Deephaven", "Deephaven", d.qualifiedName());
            case Protocol.PElasticsearch7Cluster s ->
                    carried("Elasticsearch", "Elasticsearch7Cluster", s.qualifiedName());
            // 2026-10-16 (docs/MONGODB_PLAN_2026_10_16.md, D3 and D6): a store whose collections are tables, their
            // columns the declared JSON schema's (until then carried opaque)
            case Protocol.PMongoDatabase m -> MongoDeclarations.toDatabaseDefinition(m);
            case Protocol.PDataQualityValidation v ->
                    carried("DataQualityValidation", "DataQualityValidation", v.qualifiedName());
            case Protocol.PDataQualityRelationValidation v ->
                    carried("DataQualityValidation", "DataQualityRelationValidation", v.qualifiedName());
            case Protocol.PDataQualityRelationComparison v ->
                    carried("DataQualityValidation", "DataQualityRelationComparison", v.qualifiedName());
            case Protocol.PSchemaSet s -> carried("ExternalFormat", "SchemaSet", s.qualifiedName());
            case Protocol.PBinding b -> carried("ExternalFormat", "Binding", b.qualifiedName());
            case Protocol.PServiceStoreDefinition s -> carried("ServiceStore", "ServiceStore", s.qualifiedName());
            case Protocol.PRelationalMapper m -> carried("QueryPostProcessor", "RelationalMapper", m.qualifiedName());
            // legend-lite's own records (no wire carries them; only its parser makes them)
            case Protocol.PNativeFunction n -> new NativeFunctionDefinition(n.qualifiedName(), n.typeParams(),
                    n.multParams(), FromProtocol.toFunctionParams(n.parameters()), n.returnType(), n.returnMultiplicity(),
                    FromProtocol.stereotypes(n.stereotypes()), FromProtocol.taggedValues(n.taggedValues()));
            case Protocol.PPrimitive p -> new PrimitiveExtensionDefinition(p.qualifiedName(), p.baseTypeName(),
                    p.typeVariables());
            case Protocol.POpaqueElement o -> new OpaqueElementDefinition(o.qualifiedName(), o.sectionName(),
                    o.protocolJson());
            // a projection association registers its name only, as an empty class (as lite's compiler always has)
            case Protocol.PAssociationProjection a -> new ClassDefinition(a.qualifiedName(), List.of(), List.of(),
                    List.of(), List.of(), List.of(), List.of(), List.of(), false);
            case Protocol.PSectionIndex i ->
                    throw new IllegalArgumentException("a section index is not an element: its sections are read by of");
        };
    }

    /** A function activator (Snowflake's, Deephaven's, ...): its fields as the activator definition keeps them. */
    private static PackageableElement activator(Protocol.PFunctionActivator a) {
        Map<String, String> fields = new LinkedHashMap<>(a.scalars());
        fields.put("function", a.functionPath());
        if (a.ownerId() != null) {
            fields.put("ownership", "Deployment " + a.ownerId());
        }
        if (a.userListUsers() != null) {
            fields.put("ownership", "UserList " + String.join(",", a.userListUsers()));
        }
        if (a.activationConnection() != null) {
            fields.put("activationConfiguration", a.activationConnection());
        }
        return new FunctionActivatorDefinition(a.qualifiedName(), a.kind(), fields);
    }

    private static PackageableElement carried(String section, String kind, String qualifiedName) {
        return new GenericSectionElementDefinition(section, kind, qualifiedName, Map.of(), null);
    }

    /** The path an element is listed under in its section index ({@code Protocol.pathOf}, the parser's rule). */
    private static String pathOf(Protocol.Element e) {
        return Protocol.pathOf(e);
    }

    /** An element record's source information, when it carries one (a model read from JSON without it carries none). */
    private static @com.legend.base.Nullable SourceInfo spanOf(Protocol.Element e) {
        return switch (e) {
            case Protocol.PClass c -> c.sourceInformation();
            case Protocol.PAssociation a -> a.sourceInformation();
            case Protocol.PEnumeration en -> en.sourceInformation();
            case Protocol.PFunction f -> f.sourceInformation();
            case Protocol.PProfile p -> p.sourceInformation();
            case Protocol.PMeasure m -> m.sourceInformation();
            case Protocol.PRuntime r -> r.sourceInformation();
            case Protocol.PConnection c -> c.sourceInformation();
            case Protocol.PDatabase d -> d.sourceInformation();
            case Protocol.PMapping m -> m.sourceInformation();
            case Protocol.PService s -> s.sourceInformation();
            case Protocol.PExecutionEnvironment x -> x.sourceInformation();
            case Protocol.PDataSpace d -> d.sourceInformation();
            case Protocol.PPersistence p -> p.sourceInformation();
            case Protocol.PPersistenceContext p -> p.sourceInformation();
            case Protocol.PFunctionActivator a -> a.sourceInformation();
            case Protocol.PDataElement d -> d.sourceInformation();
            case Protocol.PDiagram d -> d.sourceInformation();
            case Protocol.PText t -> t.sourceInformation();
            case Protocol.PGenerationSpecification g -> g.sourceInformation();
            case Protocol.PFileGeneration f -> f.sourceInformation();
            case Protocol.PDeephavenDatabase d -> d.sourceInformation();
            case Protocol.PElasticsearch7Cluster s -> s.sourceInformation();
            case Protocol.PMongoDatabase m -> m.sourceInformation();
            case Protocol.PDataQualityValidation v -> v.sourceInformation();
            case Protocol.PDataQualityRelationValidation v -> v.sourceInformation();
            case Protocol.PDataQualityRelationComparison v -> v.sourceInformation();
            case Protocol.PSchemaSet s -> s.sourceInformation();
            case Protocol.PBinding b -> b.sourceInformation();
            case Protocol.PServiceStoreDefinition s -> s.sourceInformation();
            case Protocol.PRelationalMapper m -> m.sourceInformation();
            case Protocol.PNativeFunction n -> n.sourceInformation();
            case Protocol.PPrimitive p -> p.sourceInformation();
            case Protocol.POpaqueElement o -> o.sourceInformation();
            case Protocol.PAssociationProjection a -> a.sourceInformation();
            case Protocol.PSectionIndex i -> null;
        };
    }
}
