// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.NameResolver;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.Property;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.IslandLanguage;
import com.legend.protocol.spec.SqlIsland;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.InputLanguages;
import com.legend.protocol.sql.SqlToPure;
import com.legend.protocol.sql.SqlTranslationException;
import com.legend.protocol.ts.TsToPure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * THE {@code #TS{...}#} ISLAND'S SEAM (docs/TS_LANGUAGE_BUILD_2026_10_30.md P2): the protocol layer's translator
 * ({@link TsToPure}) builds the Pure the query means; this seam answers what it asks of the model -- a relation's
 * columns (as the {@code #SQL{...}#} island's seam types a FROM source, {@link SqlIslands}), the classes a name names,
 * a class's properties, an enumeration's values -- refuses a model that does not depend on the timeseries library,
 * and resolves the result's names as the resolver resolves any query; the Typer then types it once. A refusal becomes
 * the compile error its kind says, placed in the enclosing source.
 */
final class TsIslands {

    private TsIslands() {
    }

    /** The relation expression {@code island}, a {@code #TS{...}#} query, means, names resolved; {@code typeSource}
     *  types a resolved relation in {@code env}. */
    static ValueSpecification desugar(SqlIsland island, Function<ValueSpecification, TypedSpec> typeSource,
            ModelContext model, Env env) {
        if (model.findFunction(TsToPure.LIBRARY + "mean").isEmpty()) {
            // the library is a project a model depends on (docs/TS_LANGUAGE_BUILD_2026_10_30.md §1, open: a dependency
            // until the user rules on the platform loading it)
            throw new TypeInferenceException(island.opening() + "...}# at " + SqlIslands.position(island, 1, 1)
                    + ": #TS uses the timeseries library: add the project timeseries (projects/timeseries) to the"
                    + " model's dependencies");
        }
        SqlToPure.Sources sources = SqlIslands.sources(island, typeSource, model, env, new java.util.TreeSet<>());
        try {
            return NameResolver.resolveQuery(TsToPure.translate(island.sql(), new Facts(sources, model),
                    java.util.Objects.requireNonNull(InputLanguages.of(IslandLanguage.SQL, null))));
        } catch (SqlTranslationException e) {
            throw SqlIslands.refusal(island, e);
        }
    }

    /** The model's classes by their simple names (R13), computed once per model. */
    record Classes(Map<String, List<String>> bySimpleName) {

        static Classes of(ModelContext model) {
            Map<String, List<String>> out = new TreeMap<>();
            for (String fqn : new java.util.TreeSet<>(model.elementFqns())) {
                if (model.findClass(fqn).isPresent()) {
                    out.computeIfAbsent(CypherIslands.Labels.simpleName(fqn), k -> new ArrayList<>()).add(fqn);
                }
            }
            return new Classes(java.util.Collections.unmodifiableMap(out));
        }
    }

    /** What the translator asks: a relation's columns and the island's other facts ({@code sources}), and the
     *  model's classes, properties and enumerations. */
    private record Facts(SqlToPure.Sources sources, ModelContext model) implements TsToPure.Model {

        @Override
        public void columns(ValueSpecification source, SqlToPure.Column column) {
            sources.columns(source, column);
        }

        @Override
        public java.util.@com.legend.base.Nullable List<String> primaryKey(String database, String table) {
            return sources.primaryKey(database, table);
        }

        @Override
        public void tables(SqlToPure.Table table) {
            sources.tables(table);
        }

        @Override
        public boolean variable(String name, SqlToPure.Scoped type) {
            return sources.variable(name, type);
        }

        @Override
        public void variables(java.util.function.Consumer<String> names) {
            sources.variables(names);
        }

        @Override
        public ValueSpecification inlineView(ValueSpecification relation) {
            return sources.inlineView(relation);
        }

        @Override
        public List<String> classesNamed(String name) {
            if (model.findClass(name).isPresent()) {
                return List.of(name);
            }
            return model.derived(Classes.class, Classes::of).bySimpleName().getOrDefault(name, List.of());
        }

        @Override
        public void property(String cls, String name, TsToPure.PropertySink sink) {
            Property p = model.findProperty(cls, name).orElse(null);
            if (p == null) {
                return;
            }
            boolean one = p.multiplicity() instanceof Multiplicity.Bounded b && b.upper() != null && b.upper() <= 1;
            sink.property(p.name(), p.type().typeName(), p.type() instanceof Type.ClassType ct ? ct.fqn() : null, one,
                    p instanceof Property.Derived d && !d.parameters().isEmpty());
        }

        @Override
        public List<String> properties(String cls) {
            List<String> out = new ArrayList<>();
            java.util.Deque<String> todo = new java.util.ArrayDeque<>(List.of(cls));
            java.util.Set<String> seen = new java.util.HashSet<>();
            while (!todo.isEmpty()) {
                String c = todo.poll();
                if (!seen.add(c)) {
                    continue;
                }
                com.legend.compiler.element.TypedClass tc = model.findClass(c).orElse(null);
                if (tc == null) {
                    continue;
                }
                tc.properties().forEach(p -> out.add(p.name()));
                todo.addAll(tc.superClassFqns());
            }
            return out;
        }

        @Override
        public java.util.@com.legend.base.Nullable List<String> enumeration(String type) {
            return model.findEnum(type).map(com.legend.compiler.element.TypedEnum::values).orElse(null);
        }
    }
}
