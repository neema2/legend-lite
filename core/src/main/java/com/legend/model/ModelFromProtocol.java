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
     *  name (the converter's own refusal, the element's path before its reason), never dropped. */
    public static ParsedModel of(Protocol.PureModelContextData model) {
        Map<String, Protocol.PSection> sectionOf = new HashMap<>();
        for (Protocol.Element e : model.elements()) {
            if (e instanceof Protocol.PSectionIndex index) {
                for (Protocol.PSection s : index.sections()) {
                    for (String path : s.elements()) {
                        if (path != null) {
                            sectionOf.putIfAbsent(path, s);
                        }
                    }
                }
            }
        }
        List<PackageableElement> elements = new ArrayList<>();
        ImportScope.Builder all = new ImportScope.Builder();
        Map<String, ImportScope> elementImports = new LinkedHashMap<>();
        Map<String, ParsedModel.Position> spans = new LinkedHashMap<>();
        for (Protocol.Element e : model.elements()) {
            if (e instanceof Protocol.PSectionIndex) {
                continue;
            }
            PackageableElement el = refusingUnsupported(e);
            elements.add(el);
            String key = ParsedModel.keyOf(el);
            Protocol.PSection section = sectionOf.get(pathOf(e));
            if (section != null) {
                ImportScope.Builder own = new ImportScope.Builder();
                for (String pkg : section.imports()) {
                    // the section index lists an import as its package (`a::b`), the grammar writes `a::b::*`
                    own.add(pkg + "::*");
                    all.add(pkg + "::*");
                }
                elementImports.putIfAbsent(key, own.build());
            }
            SourceInfo span = spanOf(e);
            if (span != null) {
                spans.putIfAbsent(key, new ParsedModel.Position(span.startLine(), span.startColumn()));
            }
        }
        return new ParsedModel(elements, all.build(), null, Map.of(), elementImports, Map.of(), List.of(), spans);
    }

    /** {@link #element}, a shape the model cannot represent refused with the element's path (the compiler's driver
     *  answers it as a construct legend-lite does not compile). */
    private static PackageableElement refusingUnsupported(Protocol.Element e) {
        try {
            return element(e);
        } catch (FromProtocol.UnsupportedConnectionShape u) {
            throw new FromProtocol.UnsupportedConnectionShape(pathOf(e) + ": " + u.reason());
        } catch (MappingFromProtocol.UnsupportedMappingShape u) {
            throw new MappingFromProtocol.UnsupportedMappingShape(pathOf(e) + ": " + u.reason());
        }
    }

    /**
     * One element record as the compiler's model: its kind's converter. The parser calls this same function for
     * every kind (directly, or through its section grammars' {@code toModel}). A section index is not an element.
     */
    public static PackageableElement element(Protocol.Element e) {
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
            case Protocol.PMongoDatabase m -> carried("MongoDB", "Database", m.qualifiedName());
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
        return new SnowflakeActivatorDefinition(a.qualifiedName(), a.kind(), fields);
    }

    private static PackageableElement carried(String section, String kind, String qualifiedName) {
        return new GenericSectionElementDefinition(section, kind, qualifiedName, Map.of(), null);
    }

    /** The path an element is listed under in its section index (its package and name, as the JSON writes them). */
    private static String pathOf(Protocol.Element e) {
        return switch (e) {
            case Protocol.PClass c -> c.qualifiedName();
            case Protocol.PAssociation a -> a.qualifiedName();
            case Protocol.PEnumeration en -> en.qualifiedName();
            // a function is listed by its name with its signature, as the engine names it
            case Protocol.PFunction f -> f.pkg().isEmpty() ? f.mangledName() : f.pkg() + "::" + f.mangledName();
            case Protocol.PProfile p -> p.qualifiedName();
            case Protocol.PMeasure m -> m.qualifiedName();
            case Protocol.PRuntime r -> r.qualifiedName();
            case Protocol.PConnection c -> c.qualifiedName();
            case Protocol.PDatabase d -> d.qualifiedName();
            case Protocol.PMapping m -> m.qualifiedName();
            case Protocol.PService s -> s.qualifiedName();
            case Protocol.PExecutionEnvironment x -> x.qualifiedName();
            case Protocol.PDataSpace d -> d.qualifiedName();
            case Protocol.PPersistence p -> p.qualifiedName();
            case Protocol.PPersistenceContext p -> p.qualifiedName();
            case Protocol.PFunctionActivator a -> a.qualifiedName();
            case Protocol.PDataElement d -> d.qualifiedName();
            case Protocol.PDiagram d -> d.qualifiedName();
            case Protocol.PText t -> t.qualifiedName();
            case Protocol.PGenerationSpecification g -> g.qualifiedName();
            case Protocol.PFileGeneration f -> f.qualifiedName();
            case Protocol.PDeephavenDatabase d -> d.qualifiedName();
            case Protocol.PElasticsearch7Cluster s -> s.qualifiedName();
            case Protocol.PMongoDatabase m -> m.qualifiedName();
            case Protocol.PDataQualityValidation v -> v.qualifiedName();
            case Protocol.PDataQualityRelationValidation v -> v.qualifiedName();
            case Protocol.PDataQualityRelationComparison v -> v.qualifiedName();
            case Protocol.PSchemaSet s -> s.qualifiedName();
            case Protocol.PBinding b -> b.qualifiedName();
            case Protocol.PServiceStoreDefinition s -> s.qualifiedName();
            case Protocol.PRelationalMapper m -> m.qualifiedName();
            case Protocol.PSectionIndex i -> i.pkg() + "::" + i.name();
        };
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
            case Protocol.PSectionIndex i -> null;
        };
    }
}
