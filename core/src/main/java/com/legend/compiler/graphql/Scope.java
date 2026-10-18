// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.graphql;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedClass;
import com.legend.model.DataSpaceDefinition;

import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * The classes a schema is generated for (docs/GRAPHQL_STUDY_2026_10_18.md §5.2): every user class of the model, or
 * the data space's element scope ({@code elements: [demo, -demo::Internal]}: an entry includes the element it names
 * or every element under it as a package; a {@code -} entry excludes the same). Platform classes are never in
 * scope. The enumerations a class in scope refers to follow it (the generator adds them as it meets them).
 */
public record Scope(SortedSet<String> classFqns) {

    public Scope {
        classFqns = Collections.unmodifiableSortedSet(new TreeSet<>(classFqns));
    }

    /** Every user class of the model. */
    public static Scope all(ModelContext ctx) {
        return new Scope(userClasses(ctx));
    }

    /** The data space's element scope over the model's user classes; everything when it names nothing. */
    public static Scope of(ModelContext ctx, DataSpaceDefinition dataSpace) {
        SortedSet<String> all = userClasses(ctx);
        if (dataSpace.elements().isEmpty()) {
            return new Scope(all);
        }
        SortedSet<String> in = new TreeSet<>();
        for (String entry : dataSpace.elements()) {
            if (!entry.startsWith("-")) {
                for (String fqn : all) {
                    if (covers(entry, fqn)) {
                        in.add(fqn);
                    }
                }
            }
        }
        for (String entry : dataSpace.elements()) {
            if (entry.startsWith("-")) {
                String path = entry.substring(1);
                in.removeIf(fqn -> covers(path, fqn));
            }
        }
        return new Scope(in);
    }

    /** Whether an element-scope path names {@code fqn} or a package above it. */
    static boolean covers(String path, String fqn) {
        return fqn.equals(path) || fqn.startsWith(path + "::");
    }

    private static SortedSet<String> userClasses(ModelContext ctx) {
        SortedSet<String> out = new TreeSet<>();
        for (String fqn : ctx.elementFqns()) {
            TypedClass c = ctx.findClass(fqn).orElse(null);
            if (c != null && !c.isNative() && !fqn.startsWith("meta::")) {
                out.add(fqn);
            }
        }
        return out;
    }

    public List<String> asList() {
        return List.copyOf(classFqns);
    }
}
