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
 * Protocol JSON to Pure text for a whole MODEL: upstream's {@code pure/v1/grammar/jsonToGrammar/model}
 * ({@code PureGrammarComposer.renderPureModelContextData} and the element composers it calls, core's
 * and every extension's), ported rule for rule with the reference checkout as spec
 * (docs/STUDIO_FULL_PLAN_2026_10_04.md, B1). Lambdas and value specifications inside elements print
 * through {@link PureComposer}, the one value-specification printer.
 *
 * <p>Input is a {@code PureModelContextData} ({@code {"_type":"data","elements":[...]}}). With a
 * section index (a parsed text's own sections) each section prints in order under its {@code ###}
 * header with its imports; without one (entity JSON from an SDLC carries none) the elements group by
 * kind in upstream's fixed order. An element kind lite cannot print yet is REFUSED, naming its
 * {@code _type}: never skipped, never printed approximately. Byte parity with upstream's composer is
 * pinned by the parser-equivalence oracle ({@code ModelComposerParityTest}) over the reference corpus.
 */
public final class ModelComposer {

    private static final String DEFAULT_SECTION = "Pure";
    private static final String SECTION_INDEX = "sectionIndex";

    private ModelComposer() {
    }

    /** One element as Pure text, as its section prints it. */
    public static String element(Json.Obj element) {
        return family(element).print().apply(element);
    }

    /** A whole model, {@code {"_type":"data","elements":[...]}}, as Pure text. */
    public static String model(Json.Obj pmcd) {
        List<Json.Obj> elements = new ArrayList<>();
        for (Json.Node n : Composing.items(pmcd, "elements")) {
            elements.add(Composing.obj(n, "element"));
        }
        Set<Json.Obj> toCompose = Collections.newSetFromMap(new IdentityHashMap<>());
        toCompose.addAll(elements);
        List<String> composed = new ArrayList<>();
        boolean indexed = false;
        for (Json.Obj e : elements) {
            indexed |= SECTION_INDEX.equals(Composing.type(e));
        }
        if (indexed) {
            Map<String, Json.Obj> byPath = new LinkedHashMap<>();
            for (Json.Obj e : elements) {
                byPath.putIfAbsent(Composing.path(e), e);
            }
            for (Json.Obj e : elements) {
                if (SECTION_INDEX.equals(Composing.type(e))) {
                    for (Json.Node s : Composing.items(e, "sections")) {
                        composed.add(section(Composing.obj(s, "section"), byPath, toCompose, composed.isEmpty()));
                    }
                }
            }
        }
        for (ElementFamilies.Family free : ElementFamilies.EXTENSIONS) {
            List<Json.Obj> mine = new ArrayList<>();
            for (Json.Obj e : elements) {
                if (toCompose.contains(e) && !SECTION_INDEX.equals(Composing.type(e)) && family(e) == free) {
                    mine.add(e);
                }
            }
            if (!mine.isEmpty()) {
                composed.add("###" + free.parser() + "\n" + joinPrinted(free.freeSectionOrder(mine)) + "\n");
                toCompose.removeAll(mine);
            }
        }
        for (ElementFamilies.Family core : ElementFamilies.CORE) {
            List<Json.Obj> mine = new ArrayList<>();
            for (Json.Obj e : elements) {
                if (toCompose.contains(e) && !SECTION_INDEX.equals(Composing.type(e)) && family(e) == core) {
                    mine.add(e);
                }
            }
            if (!mine.isEmpty()) {
                toCompose.removeAll(mine);
                String header = !composed.isEmpty() || !DEFAULT_SECTION.equals(core.parser()) ? "###" + core.parser() + "\n" : "";
                composed.add(header + joinPrinted(mine) + "\n");
            }
        }
        for (Json.Obj e : toCompose) {
            if (!SECTION_INDEX.equals(Composing.type(e))) {
                // upstream drops an element no section claims; every kind lite knows has a section
                throw Composing.refused("no section prints an element of _type '" + Composing.type(e) + "'");
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

    /** {@code PureGrammarComposer.renderSectionIndex}, one section. */
    private static String section(Json.Obj section, Map<String, Json.Obj> byPath, Set<Json.Obj> toCompose, boolean first) {
        String parser = section.getString("parserName");
        StringBuilder b = new StringBuilder(!first || !DEFAULT_SECTION.equals(parser) ? "###" + parser + "\n" : "");
        List<String> imports = "importAware".equals(Composing.type(section))
                ? new ArrayList<>(new LinkedHashSet<>(section.getStringArrayOr("imports", List.of()))) : List.of();
        if (!imports.isEmpty()) {
            List<String> lines = new ArrayList<>();
            for (String i : imports) {
                lines.add("import " + PureComposer.convertPath(i) + "::*;");
            }
            b.append(String.join("\n", lines)).append("\n");
        }
        List<Json.Obj> mine = new ArrayList<>();
        for (String path : new LinkedHashSet<>(section.getStringArrayOr("elements", List.of()))) {
            Json.Obj e = byPath.get(path);
            if (e != null) {
                toCompose.remove(e);
                mine.add(e);
            }
        }
        if (!mine.isEmpty()) {
            for (Json.Obj e : mine) {
                if (!family(e).printsIn(parser)) {
                    throw Composing.refused("an element of _type '" + Composing.type(e) + "' in a ###" + parser
                            + " section (upstream prints its can't-transform comment)");
                }
            }
            b.append(joinPrinted(mine)).append("\n");
        }
        return b.toString();
    }

    private static String joinPrinted(List<Json.Obj> elements) {
        List<String> out = new ArrayList<>();
        for (Json.Obj e : elements) {
            out.add(element(e));
        }
        return String.join("\n\n", out);
    }

    private static ElementFamilies.Family family(Json.Obj element) {
        String type = Composing.type(element);
        ElementFamilies.Family f = ElementFamilies.BY_TYPE.get(type);
        if (f == null) {
            throw Composing.refused("no model composer rule for an element of _type '" + type
                    + "' -- add the rule, do not drop it");
        }
        return f;
    }

}
