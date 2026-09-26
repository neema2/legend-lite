// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.platform;
import com.legend.model.FunctionId;

import com.legend.model.Function;
import com.legend.model.FunctionDefinition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * EVERY FUNCTION DECLARATION the platform knows, one per {@link FunctionId}
 * (platform architecture untangle, step 2). Its sources are the standard
 * library declared upstream, the catalog, and the platform's own modules; the
 * table does not care which, only that one id is one declaration.
 *
 * <p>The same id met twice is the same entity. The catalog restates upstream's
 * declarations id for id (CatalogUpstreamDiffTest pins DIVERGENT at zero), so a
 * catalog native and upstream's declaration of that id are one declaration:
 * the one WITH a body is kept, since it is the upstream reference and the
 * signature is identical. Two DIFFERENT bodies under one id cannot both be kept:
 * the first is, and the id is reported in {@link #duplicates()} — upstream
 * refuses a duplicate definition at compile; here the model builder still
 * admits one (a harness loading the prelude's copy beside upstream's original),
 * so the table reports rather than refuses, and a census pins the count.
 */
public final class DeclarationTable {

    private final Map<FunctionId, Function> byId;
    private final Map<String, List<Function>> byFqn;
    private final List<String> duplicates;

    private DeclarationTable(Map<FunctionId, Function> byId, List<String> duplicates) {
        this.byId = Collections.unmodifiableMap(byId);
        this.duplicates = List.copyOf(duplicates);
        Map<String, List<Function>> fqns = new LinkedHashMap<>();
        for (Function f : byId.values()) {
            fqns.computeIfAbsent(f.qualifiedName(), k -> new ArrayList<>()).add(f);
        }
        fqns.replaceAll((k, v) -> List.copyOf(v));
        this.byFqn = Collections.unmodifiableMap(fqns);
    }

    /** The table over {@code declarations}, merged by id. */
    public static DeclarationTable of(Collection<? extends Function> declarations) {
        Map<FunctionId, Function> byId = new LinkedHashMap<>();
        List<String> duplicates = new ArrayList<>();
        for (Function f : declarations) {
            FunctionId id = FunctionId.of(f);
            Function prior = byId.get(id);
            if (prior == null) {
                byId.put(id, f);
            } else if (f instanceof FunctionDefinition && prior instanceof FunctionDefinition
                    && !f.equals(prior)) {
                duplicates.add(id.qualified());
            } else if (f instanceof FunctionDefinition) {
                byId.put(id, f);   // the bodied declaration is upstream's reference
            }
        }

        return new DeclarationTable(byId, duplicates);
    }

    /** The declaration with exactly this id, or null. */
    public @com.legend.base.Nullable Function get(FunctionId id) {
        return byId.get(id);
    }

    /** Every declaration at {@code fqn} (its overloads), in insertion order. */
    public List<Function> at(String fqn) {
        return byFqn.getOrDefault(fqn, List.of());
    }

    /** Ids declared by two different bodies (the first kept). */
    public List<String> duplicates() {
        return duplicates;
    }

    /** Every id, in insertion order. */
    public java.util.Set<FunctionId> ids() {
        return byId.keySet();
    }

    public int size() {
        return byId.size();
    }
}
