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

    /** The system metamodel's own classes (lite's views over its system tables), which every model carries. */
    private static final Set<String> SYSTEM = systemFqns();

    private static Set<String> systemFqns() {
        Set<String> out = new TreeSet<>();
        for (com.legend.model.PackageableElement e : com.legend.builtin.SystemMetamodel.elements()) {
            out.add(e.qualifiedName());
        }
        return java.util.Collections.unmodifiableSet(out);
    }

    /** The model's classes less the platform's: the natives, the prelude's declarations, which the resolver
     *  lists ({@code NameResolver.platformFqns()}), and the system metamodel's; each a set the element is looked
     *  up in, never a package prefix tested as text. */
    private static Set<String> userClasses(ModelContext ctx) {
        Set<String> platform = com.legend.compiler.NameResolver.platformFqns();
        Set<String> out = new TreeSet<>();
        for (String fqn : ctx.elementFqns()) {
            TypedClass c = ctx.findClass(fqn).orElse(null);
            if (c != null && !c.isNative() && !platform.contains(fqn) && !SYSTEM.contains(fqn)
                    && !companion(ctx, fqn)) {
                out.add(fqn);
            }
        }
        return out;
    }

    /** A class the companion generated ({@code <<meta::legend::lite::graphql.companion>>}, PR4): never a root,
     *  never a type of the schema; the schema spells its shape itself. */
    private static boolean companion(ModelContext ctx, String fqn) {
        var cd = ctx.findClassDefinition(fqn).orElse(null);
        if (cd == null) {
            return false;
        }
        for (var st : cd.stereotypes()) {
            if (com.legend.compiler.element.type.PlatformTypes.isProfile(st.profileName(),
                    com.legend.compiler.element.type.PlatformTypes.GRAPHQL_PROFILE)
                    && st.stereotypeName().equals(com.legend.compiler.element.type.PlatformTypes.GRAPHQL_COMPANION)) {
                return true;
            }
        }
        return false;
    }

    public List<String> asList() {
        return classFqns;
    }
}
