// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Protocol to Pure text for a whole MODEL: upstream's {@code pure/v1/grammar/jsonToGrammar/model}
 * ({@code PureGrammarComposer.renderPureModelContextData} and the element composers it calls, core's
 * and every extension's), ported rule for rule with the reference checkout as spec
 * (docs/STUDIO_FULL_PLAN_2026_10_04.md, B1). Lambdas and value specifications inside elements print
 * through {@link PureComposer}, the one value-specification printer.
 *
 * <p>The printers work on the protocol records ({@link Protocol.PureModelContextData}; the protocol program's leg 2,
 * step 3): a model's JSON ({@code {"_type":"data","elements":[...]}}) is read by {@link ModelReader} first, so the
 * reader is the one place JSON is understood. With a section index (a parsed text's own sections) each section
 * prints in order under its {@code ###} header with its imports; without one (entity JSON from an SDLC carries
 * none) the elements group by kind in upstream's fixed order. An element kind lite cannot print yet is REFUSED,
 * naming its {@code _type}: never skipped, never printed approximately. Byte parity with upstream's composer is
 * pinned by the parser-equivalence oracle ({@code ModelComposerParityTest}) over the reference corpus.
 */
public final class ModelComposer {

    private static final String DEFAULT_SECTION = "Pure";

    private ModelComposer() {
    }

    /** One element as Pure text, as its section prints it. */
    public static String element(Protocol.Element element) {
        return family(element).print().apply(element);
    }

    /** {@link #element(Protocol.Element)} of the JSON, read first. */
    public static String element(Json.Obj element) {
        return element(ModelReader.readElement(element));
    }

    /** {@link #model(Protocol.PureModelContextData)} of the JSON, read first. */
    public static String model(Json.Obj pmcd) {
        return model(ModelReader.read(pmcd));
    }

    /** A whole model as Pure text. */
    public static String model(Protocol.PureModelContextData pmcd) {
        List<Protocol.Element> elements = pmcd.elements();
        // identity, not equality: two equal elements are two elements to print
        Set<Protocol.Element> toCompose = Collections.newSetFromMap(new IdentityHashMap<>());
        toCompose.addAll(elements);
        List<String> composed = new ArrayList<>();
        Index index = new Index(elements);
        for (Protocol.Element e : elements) {
            if (e instanceof Protocol.PSectionIndex si) {
                for (Protocol.PSection s : si.sections()) {
                    composed.add(section(s, index, toCompose, composed.isEmpty()));
                }
            }
        }
        for (ElementFamilies.Family free : ElementFamilies.EXTENSIONS) {
            List<Protocol.Element> mine = new ArrayList<>();
            for (Protocol.Element e : elements) {
                if (toCompose.contains(e) && !(e instanceof Protocol.PSectionIndex) && family(e) == free
                        && !ElementFamilies.NOT_IN_FREE_SECTIONS.contains(ElementFamilies.typeOf(e))) {
                    mine.add(e);
                }
            }
            if (!mine.isEmpty()) {
                composed.add("###" + free.parser() + "\n" + joinPrinted(free.freeSectionOrder(mine)) + "\n");
                mine.forEach(toCompose::remove);
            }
        }
        for (ElementFamilies.Family core : ElementFamilies.CORE) {
            List<Protocol.Element> mine = new ArrayList<>();
            for (Protocol.Element e : elements) {
                if (toCompose.contains(e) && !(e instanceof Protocol.PSectionIndex) && family(e) == core) {
                    mine.add(e);
                }
            }
            if (!mine.isEmpty()) {
                mine.forEach(toCompose::remove);
                String header = !composed.isEmpty() || !DEFAULT_SECTION.equals(core.parser()) ? "###" + core.parser() + "\n" : "";
                composed.add(header + joinPrinted(mine) + "\n");
            }
        }
        for (Protocol.Element e : toCompose) {
            if (!(e instanceof Protocol.PSectionIndex)) {
                // upstream drops an element no section claims; every kind lite knows has a section
                throw Composing.refused("no section prints an element of _type '" + ElementFamilies.typeOf(e) + "'");
            }
        }
        List<String> out = new ArrayList<>();
        for (String s : composed) {
            if (!s.isEmpty()) {
                out.add(s);
            }
        }
        return String.join("\n\n", out);
    }

    /**
     * The elements by the path a section index names them by: the path written on the wire, the first element
     * of a path winning. A function's is its signature-mangled name; older JSON wrote a function's name without the
     * mangling, and its section names it so, which the declared name matches.
     */
    private static final class Index {
        private final Map<String, Protocol.Element> byPath = new LinkedHashMap<>();
        private final Map<String, Protocol.Element> functionsByDeclaredPath = new LinkedHashMap<>();

        Index(List<Protocol.Element> elements) {
            for (Protocol.Element e : elements) {
                if (e instanceof Protocol.PFunction f) {
                    byPath.putIfAbsent(path(f.pkg(), f.mangledName()), e);
                    functionsByDeclaredPath.putIfAbsent(f.qualifiedName(), e);
                } else if (!(e instanceof Protocol.PSectionIndex)) {
                    byPath.putIfAbsent(path(e), e);
                }
            }
        }

        @com.legend.base.Nullable Protocol.Element get(String path) {
            Protocol.Element e = byPath.get(path);
            return e != null ? e : functionsByDeclaredPath.get(path);
        }
    }

    private static String path(String pkg, String name) {
        return pkg.isEmpty() ? name : pkg + "::" + name;
    }

    /** {@code PackageableElement.getPath} of every element a section can name (a function's is set apart). */
    private static String path(Protocol.Element e) {
        return switch (e) {
            case Protocol.PClass x -> x.qualifiedName();
            case Protocol.PAssociation x -> x.qualifiedName();
            case Protocol.PEnumeration x -> x.qualifiedName();
            case Protocol.PFunction x -> path(x.pkg(), x.mangledName());
            case Protocol.PProfile x -> x.qualifiedName();
            case Protocol.PSectionIndex x -> path(x.pkg(), x.name());
            case Protocol.PMeasure x -> x.qualifiedName();
            case Protocol.PRuntime x -> x.qualifiedName();
            case Protocol.PConnection x -> x.qualifiedName();
            case Protocol.PDatabase x -> x.qualifiedName();
            case Protocol.PService x -> x.qualifiedName();
            case Protocol.PExecutionEnvironment x -> x.qualifiedName();
            case Protocol.PDataSpace x -> x.qualifiedName();
            case Protocol.PPersistence x -> x.qualifiedName();
            case Protocol.PPersistenceContext x -> x.qualifiedName();
            case Protocol.PFunctionActivator x -> path(x.pkg(), x.name());
            case Protocol.PDiagram x -> x.qualifiedName();
            case Protocol.PText x -> x.qualifiedName();
            case Protocol.PGenerationSpecification x -> x.qualifiedName();
            case Protocol.PFileGeneration x -> x.qualifiedName();
            case Protocol.PDeephavenDatabase x -> x.qualifiedName();
            case Protocol.PElasticsearch7Cluster x -> x.qualifiedName();
            case Protocol.PMongoDatabase x -> x.qualifiedName();
            case Protocol.PDataQualityValidation x -> x.qualifiedName();
            case Protocol.PDataQualityRelationValidation x -> x.qualifiedName();
            case Protocol.PDataQualityRelationComparison x -> x.qualifiedName();
            case Protocol.PSchemaSet x -> x.qualifiedName();
            case Protocol.PBinding x -> x.qualifiedName();
            case Protocol.PServiceStoreDefinition x -> x.qualifiedName();
            case Protocol.PMapping x -> x.qualifiedName();
            case Protocol.PDataElement x -> x.qualifiedName();
            case Protocol.PRelationalMapper x -> x.qualifiedName();
        };
    }

    /** {@code PureGrammarComposer.renderSectionIndex}, one section. */
    private static String section(Protocol.PSection section, Index index, Set<Protocol.Element> toCompose, boolean first) {
        String parser = section.parserName();
        StringBuilder b = new StringBuilder(!first || !DEFAULT_SECTION.equals(parser) ? "###" + parser + "\n" : "");
        List<String> imports = section.importAware() ? new ArrayList<>(new LinkedHashSet<>(section.imports())) : List.of();
        if (!imports.isEmpty()) {
            List<String> lines = new ArrayList<>();
            for (String i : imports) {
                lines.add("import " + PureComposer.convertPath(i) + "::*;");
            }
            b.append(String.join("\n", lines)).append("\n");
        }
        List<Protocol.Element> mine = new ArrayList<>();
        for (String path : new LinkedHashSet<>(section.elements())) {
            // a section may list a literal null (the BigQuery deployment-configuration walker registers one)
            Protocol.Element e = path == null ? null : index.get(path);
            if (e != null) {
                toCompose.remove(e);
                mine.add(e);
            }
        }
        if (!mine.isEmpty()) {
            for (Protocol.Element e : mine) {
                if (!family(e).printsIn(parser)) {
                    throw Composing.refused("an element of _type '" + ElementFamilies.typeOf(e) + "' in a ###" + parser
                            + " section (upstream prints its can't-transform comment)");
                }
            }
            b.append(joinPrinted(mine)).append("\n");
        }
        return b.toString();
    }

    private static String joinPrinted(List<Protocol.Element> elements) {
        List<String> out = new ArrayList<>();
        for (Protocol.Element e : elements) {
            out.add(element(e));
        }
        return String.join("\n\n", out);
    }

    private static ElementFamilies.Family family(Protocol.Element element) {
        String type = ElementFamilies.typeOf(element);
        ElementFamilies.Family f = ElementFamilies.BY_TYPE.get(type);
        if (f == null) {
            throw Composing.refused("no model composer rule for an element of _type '" + type
                    + "' -- add the rule, do not drop it");
        }
        return f;
    }
}
