// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.graphql;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedClass;
import com.legend.model.DataSpaceDefinition;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * The classes a schema is generated for (docs/GRAPHQL_STUDY_2026_10_18.md §5.2): every user class of the model, or
 * the data space's element scope ({@code elements: [demo, -demo::Internal]}: an entry includes the element it names
 * or every element under it as a package; a {@code -} entry excludes the same). Platform classes are never in
 * scope. The enumerations a class in scope refers to follow it (the generator adds them as it meets them).
 */
public record Scope(List<String> classFqns) {

    /** Sorted, without duplicates. (A sorted copy, not an unmodifiable sorted set: TeaVM, which compiles the
     *  planner to WebAssembly, lacks {@code Collections.unmodifiableSortedSet}.) */
    public Scope {
        classFqns = List.copyOf(new TreeSet<>(classFqns));
    }

    public static Scope of(Collection<String> classFqns) {
        return new Scope(List.copyOf(classFqns));
    }

    public boolean contains(String fqn) {
        return classFqns.contains(fqn);
    }

    /** Every user class of the model. */
    public static Scope all(ModelContext ctx) {
        return of(userClasses(ctx));
    }

    /** The data space's element scope over the model's user classes; everything when it names nothing. */
    public static Scope of(ModelContext ctx, DataSpaceDefinition dataSpace) {
        Set<String> all = userClasses(ctx);
        if (dataSpace.elements().isEmpty()) {
            return of(all);
        }
        Set<String> in = new TreeSet<>();
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
        return of(in);
    }

    /** Whether an element-scope path names {@code fqn} or a package above it. */
    static boolean covers(String path, String fqn) {
        return fqn.equals(path) || fqn.startsWith(path + "::");
    }

    private static Set<String> userClasses(ModelContext ctx) {
        Set<String> out = new TreeSet<>();
        for (String fqn : ctx.elementFqns()) {
            TypedClass c = ctx.findClass(fqn).orElse(null);
            if (c != null && !c.isNative() && !fqn.startsWith("meta::")) {
                out.add(fqn);
            }
        }
        return out;
    }

    public List<String> asList() {
        return classFqns;
    }
}
