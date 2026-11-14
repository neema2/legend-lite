// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static com.legend.server.ModelChanges.arrOf;
import static com.legend.server.ModelChanges.element;
import static com.legend.server.ModelChanges.isIdentifier;
import static com.legend.server.ModelChanges.literal;
import static com.legend.server.ModelChanges.refused;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;

/**
 * CREATE A DATA SPACE (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.1, §15.6a; an action of {@link ModelChanges}): the
 * page's model as one front door, as Legend Query lists models -- {@code <pkg>::<Name>}, its one execution context the
 * place's (the model's mapping and the one runtime that names it; named after the runtime, {@code Browser}), its
 * classes the ones named or, none named, every class of the package's domain; a title and a description when given.
 */
final class CreateDataSpace {

    private CreateDataSpace() {
    }

    /** {@code spec}: {@code {"name", "title"?, "description"?, "classes"?}}; the data space's path. */
    static String apply(ModelChanges m, String pkg, Json.Obj spec) {
        ModelChanges.only(spec, "a data space to make", "name", "title", "description", "classes");
        String name = spec.getString("name");
        if (!isIdentifier(name)) {
            throw refused("a data space's name is a Pure name: '" + name + "'");
        }
        String path = pkg + "::" + name;
        String mapping = pkg + "::mapping::Model";
        if (m.get(mapping) == null) {
            throw refused("the model has no mapping yet (" + mapping + "): make its classes first");
        }
        // one there already over another mapping -- written in Studio, say -- is not written over
        Json.Obj existing = m.get(path);
        if (existing != null && "dataSpace".equals(existing.getStringOr("_type", "")) && !over(existing, mapping)) {
            throw refused("'" + path + "' is already in the project, not made here: name this data space otherwise");
        }
        String runtime = m.runtimeNaming(mapping);
        Json.Obj rt = m.get(runtime);
        String context = rt == null ? runtime : rt.getString("name");
        List<String> classes = new ArrayList<>();
        for (Json.Node n : arrOf(spec, "classes").items()) {
            String cls = ((Json.Str) n).value();
            Json.Obj c = m.get(cls);
            if (c == null || !"class".equals(c.getStringOr("_type", ""))) {
                throw refused("the project has no class " + cls);
            }
            classes.add(cls);
        }
        if (classes.isEmpty()) {
            classes.addAll(m.classesOf(pkg));
        }
        if (classes.isEmpty()) {
            throw refused("a data space shows a model's classes, and " + pkg + " has none yet");
        }
        StringBuilder text = new StringBuilder("###DataSpace\nDataSpace " + path + "\n{\n  executionContexts:\n  [\n"
                + "    {\n      name: " + literal(context) + ";\n      mapping: " + mapping + ";\n      defaultRuntime: "
                + runtime + ";\n    }\n  ];\n  defaultExecutionContext: " + literal(context) + ";\n");
        String title = spec.getStringOr("title", null);
        if (title != null && !title.strip().isEmpty()) {
            text.append("  title: ").append(literal(title.strip())).append(";\n");
        }
        String description = spec.getStringOr("description", null);
        if (description != null && !description.strip().isEmpty()) {
            text.append("  description: ").append(literal(description.strip())).append(";\n");
        }
        text.append("  elements:\n  [\n    ").append(String.join(",\n    ", classes)).append("\n  ];\n}\n");
        m.put(element(text.toString()));
        return path;
    }

    /** Whether each execution context of the data space {@code e} is over {@code mapping}. */
    private static boolean over(Json.Obj e, String mapping) {
        List<Json.Node> contexts = arrOf(e, "executionContexts").items();
        for (Json.Node n : contexts) {
            if (!mapping.equals(ModelChanges.objOf((Json.Obj) n, "mapping").getStringOr("path", ""))) {
                return false;
            }
        }
        return !contexts.isEmpty();
    }
}
