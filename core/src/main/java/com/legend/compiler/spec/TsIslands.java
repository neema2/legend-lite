// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.NameResolver;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.Property;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.protocol.spec.IslandLanguage;
import com.legend.protocol.spec.SqlIsland;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.InputLanguages;
import com.legend.protocol.sql.SqlToPure;
import com.legend.protocol.sql.SqlTranslationException;
import com.legend.protocol.ts.TsCatalog;
import com.legend.protocol.ts.TsToPure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * THE {@code #TS{...}#} ISLAND'S SEAM (docs/TS_LANGUAGE_BUILD_2026_10_30.md P2): the protocol layer's translator
 * ({@link TsToPure}) builds the Pure the query means; this seam answers what it asks of the model -- a relation's
 * columns (as the {@code #SQL{...}#} island's seam types a FROM source, {@link SqlIslands}), the classes a name names,
 * a class's properties, an enumeration's values -- refuses a model that does not depend on the timeseries library,
 * and resolves the result's names as the resolver resolves any query; the Typer then types it once. A refusal becomes
 * the compile error its kind says, placed in the enclosing source.
 */
public final class TsIslands {

    private TsIslands() {
    }

    /** THE CATALOG (docs/TS_LIBRARY_DEFINES_THE_LANGUAGE_2026_10_31.md M3): the words over one line {@code model}'s
     *  timeseries library defines, read from their overloads -- what a formula box completes, a help page says and the
     *  Python DSL's stubs are made of ({@link TsCatalog}). */
    public static List<TsCatalog.Word> catalog(ModelContext model) {
        return TsCatalog.words(new Facts((source, column) -> {
            throw new IllegalStateException("the catalog reads no relation");
        }, model));
    }

    /** The relation expression {@code island}, a {@code #TS{...}#} query, means, names resolved; {@code typer} types
     *  the relations it reads in {@code env}. */
    static ValueSpecification desugar(SqlIsland island, Typer typer, ModelContext model, Env env) {
        return NameResolver.resolveQuery(translated(island, typer, model, env));
    }

    /** {@code island}'s translation, its names not yet resolved: one node wherever the query reads it again (the
     *  translator builds a relation once and reads it at each use). */
    private static ValueSpecification translated(SqlIsland island, Typer typer, ModelContext model, Env env) {
        if (model.findFunction(TsToPure.LIBRARY + "mean").isEmpty()) {
            // the library is a project a model depends on (docs/TS_LANGUAGE_BUILD_2026_10_30.md §1, open: a dependency
            // until the user rules on the platform loading it)
            throw new TypeInferenceException(island.opening() + "...}# at " + SqlIslands.position(island, 1, 1)
                    + ": #TS uses the timeseries library: add the project timeseries (projects/timeseries) to the"
                    + " model's dependencies");
        }
        SqlToPure.Sources sources = SqlIslands.sources(island, source -> typer.synth(source, env), model, env,
                new java.util.TreeSet<>());
        try {
            return TsToPure.translate(island.sql(), new Facts(new TypedOnce(sources, island, typer, env), model),
                    java.util.Objects.requireNonNull(InputLanguages.of(IslandLanguage.SQL, null)));
        } catch (SqlTranslationException e) {
            throw SqlIslands.refusal(island, e);
        }
    }

    /** A query statement and the lets it now reads ({@link #lifted}). */
    record Lifted(List<ValueSpecification> lets, ValueSpecification statement) {
    }

    /** Whether {@code statement} holds a {@code #TS{...}#} island outside every lambda. */
    static boolean holdsIsland(ValueSpecification statement) {
        return switch (statement) {
            case SqlIsland si -> si.language() == IslandLanguage.TS;
            case com.legend.protocol.spec.LambdaFunction ignored -> false;
            default -> statement.children().stream().anyMatch(TsIslands::holdsIsland);
        };
    }

    /**
     * A RELATION THE QUERY READS AGAIN IS BOUND ONCE (2026-11-04, the user: "can we do this for real"): each
     * {@code #TS{...}#} island of {@code statement} outside every lambda translated, and each relation its translation
     * reads at more than one place -- a line an item reads, an item a later item reads -- bound by a {@code let} ahead
     * of the statement, its every read the let's name. The translator builds such a relation once and reads it at
     * each use; typed where it stands it was typed, inlined and lowered again at each, and the SQL wrote it again at
     * each (spec-13: the prices table read 36 times, one regression computed 13 times). Bound by a let it is typed
     * once, and the inliner splices the one value at each read (UserCallInliner, query lets), which the lowering
     * writes once, as a common table expression ({@code lowering.SharedRelations}). Bound only where its value reads
     * no name but the query's own (its parameters, the lets before it): a relation that reads a lambda's parameter
     * stays where it is. {@code scope} is the statement's: its names, and the sources typed in it.
     */
    static Lifted lifted(ValueSpecification statement, Typer typer, ModelContext model, Env scope,
            java.util.Set<String> taken) {
        List<ValueSpecification> lets = new ArrayList<>();
        java.util.IdentityHashMap<ValueSpecification, ValueSpecification> islands = new java.util.IdentityHashMap<>();
        collect(statement, island -> {
            TsToPure.Bound bound = TsToPure.bindShared(translated(island, typer, model, scope), scope.names(), taken);
            lets.addAll(bound.lets());
            islands.put(island, NameResolver.resolveQuery(bound.answer()));
        });
        return new Lifted(lets.stream().map(NameResolver::resolveQuery).toList(), replaced(statement, islands));
    }

    /** Each {@code #TS{...}#} island of {@code v} outside every lambda, in order. */
    private static void collect(ValueSpecification v, java.util.function.Consumer<SqlIsland> each) {
        switch (v) {
            case SqlIsland si -> {
                if (si.language() == IslandLanguage.TS) {
                    each.accept(si);
                }
            }
            case com.legend.protocol.spec.LambdaFunction ignored -> {
            }
            default -> v.children().forEach(c -> collect(c, each));
        }
    }

    /** {@code v} with each island of {@code islands} replaced by its translation, outside every lambda. */
    private static ValueSpecification replaced(ValueSpecification v,
            java.util.IdentityHashMap<ValueSpecification, ValueSpecification> islands) {
        ValueSpecification done = islands.get(v);
        if (done != null) {
            return done;
        }
        if (v instanceof com.legend.protocol.spec.LambdaFunction || v.children().isEmpty()) {
            return v;
        }
        List<ValueSpecification> kids = v.children();
        List<ValueSpecification> out = new ArrayList<>(kids.size());
        boolean changed = false;
        for (ValueSpecification k : kids) {
            ValueSpecification r = replaced(k, islands);
            changed |= r != k;
            out.add(r);
        }
        return changed ? v.withChildren(out) : v;
    }

    /**
     * A RELATION THE TRANSLATOR ASKS THE COLUMNS OF, TYPED AS THE QUERY WILL BE (2026-11-04, spec-15's planning time):
     * the translator asks the columns of the relation it is building, which reads at several places the relations it
     * built before -- one node, read again ({@link TsToPure#bindShared}). Its names resolved as one expression, each
     * read is a copy, typed again at each place: spec-15's last relation, 1,102 nodes, took 3.0 of the query's 3.7
     * seconds of planning. So each relation it reads at more than one place is bound by a let, typed once, in the
     * order the lets read each other, and the relation typed with them in scope -- as {@link #lifted} binds them and
     * the query's typing types them. A relation read at one place only is {@code of}'s, as any source.
     */
    private record TypedOnce(SqlToPure.Sources of, SqlIsland island, Typer typer, Env env)
            implements SqlToPure.Sources {

        @Override
        public void columns(ValueSpecification source, SqlToPure.Column column) {
            // the lets' names are this typing's own: none taken from the query's, none left to it
            java.util.Set<String> taken = new java.util.HashSet<>(env.names());
            TsToPure.Bound bound = TsToPure.bindShared(source, env.names(), taken);
            if (bound.lets().isEmpty()) {
                of.columns(source, column);
                return;
            }
            Env at = env;
            for (ValueSpecification let : bound.lets()) {
                com.legend.protocol.spec.AppliedFunction bind =
                        (com.legend.protocol.spec.AppliedFunction) NameResolver.resolveQuery(let);
                String name = ((com.legend.protocol.spec.CString) bind.parameters().get(0)).value();
                ValueSpecification value = bind.parameters().get(1);
                at = at.withLet(name, typer.synth(value, at).info(), value);
            }
            SqlIslands.columns(typer.synth(NameResolver.resolveQuery(bound.answer()), at), island, column);
        }

        @Override
        public java.util.@com.legend.base.Nullable List<String> primaryKey(String database, String table) {
            return of.primaryKey(database, table);
        }

        @Override
        public void tables(SqlToPure.Table table) {
            of.tables(table);
        }

        @Override
        public boolean variable(String name, SqlToPure.Scoped type) {
            return of.variable(name, type);
        }

        @Override
        public void variables(java.util.function.Consumer<String> names) {
            of.variables(names);
        }

        @Override
        public boolean isEnumeration(String type) {
            return of.isEnumeration(type);
        }

        @Override
        public ValueSpecification inlineView(ValueSpecification relation) {
            return of.inlineView(relation);
        }
    }

    /** The enumerations directly in the package {@code pkg} ({@code timeseries}), by their simple names, each its
     *  values in their declared order: an option's one source, exported for the clients (TsCatalogExport). */
    public static Map<String, List<String>> enumerations(ModelContext model, String pkg) {
        Map<String, List<String>> out = new java.util.LinkedHashMap<>();
        for (String fqn : model.elementFqns().stream().sorted().toList()) {
            String name = memberOf(fqn, pkg);
            if (name != null) {
                model.findEnum(fqn).ifPresent(e -> out.put(name, e.values()));
            }
        }
        return java.util.Collections.unmodifiableMap(out);
    }

    /** {@code fqn}'s simple name where it is an element directly in the package {@code pkg}, else null: the model
     *  lists its elements by path alone, so a package's members are read by their paths' prefix (the one site,
     *  2026-11-01: the library's words and its enumerations, the catalog's two readers). */
    private static @com.legend.base.Nullable String memberOf(String fqn, String pkg) {
        String prefix = pkg + "::";
        return fqn.startsWith(prefix) && !fqn.substring(prefix.length()).contains("::")
                ? fqn.substring(prefix.length()) : null;
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

    /**
     * The library's words as the translator reads them, an index of the model built where it is first read (Knowledge,
     * docs/TENETS.md): each function's overloads by path, each package's functions, and the function each alias names.
     * Before it, a lookup of an alias (avg, mean's) walked every function of the package and read each one's overloads
     * again, at each of a word's four lookups (the compile-time homework, 2026-11-03: 259 reads of 86 functions for a
     * query of one item, 2,072 for eight).
     */
    private static final class Words {
        private final ModelContext model;
        private final java.util.concurrent.ConcurrentHashMap<String, List<TsToPure.Overload>> overloads =
                new java.util.concurrent.ConcurrentHashMap<>();
        private final java.util.concurrent.ConcurrentHashMap<String, List<String>> packages =
                new java.util.concurrent.ConcurrentHashMap<>();
        private final java.util.concurrent.ConcurrentHashMap<String, Map<String, String>> aliases =
                new java.util.concurrent.ConcurrentHashMap<>();

        Words(ModelContext model) {
            this.model = model;
        }

        List<TsToPure.Overload> overloads(String fqn) {
            return overloads.computeIfAbsent(fqn, f -> Facts.read(model, f));
        }

        List<String> functionsIn(String pkg) {
            return packages.computeIfAbsent(pkg, p -> model.functionFqns().stream().map(f -> memberOf(f, p))
                    .filter(java.util.Objects::nonNull).distinct().sorted().toList());
        }

        @com.legend.base.Nullable String aliased(String pkg, String word) {
            return aliases.computeIfAbsent(pkg, p -> {
                Map<String, String> out = new TreeMap<>();
                for (String fn : functionsIn(p)) {
                    for (TsToPure.Overload o : overloads(p + "::" + fn)) {
                        o.aliases().forEach(a -> out.putIfAbsent(a, fn));
                    }
                }
                return java.util.Collections.unmodifiableMap(out);
            }).get(word);
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
        public boolean isEnumeration(String type) {
            return sources.isEnumeration(type);
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

        @Override
        public List<TsToPure.Overload> overloads(String fqn) {
            return model.derived(Words.class, Words::new).overloads(fqn);
        }

        @Override
        public @com.legend.base.Nullable String aliased(String pkg, String word) {
            return model.derived(Words.class, Words::new).aliased(pkg, word);
        }

        /** The arguments of the one call {@code body} is, where it calls {@code f}'s own function -- the declarations
         *  its name finds hold {@code f} -- of its parameters and literals alone (a default: W5, 2026-11-06); else
         *  null. */
        private static @com.legend.base.Nullable List<ValueSpecification> delegate(ModelContext model,
                com.legend.compiler.element.TypedFunction f, List<ValueSpecification> body) {
            if (body.size() != 1 || !(body.get(0) instanceof com.legend.protocol.spec.AppliedFunction a)
                    || !model.findFunction(a.function()).contains(f)) {
                return null;
            }
            for (ValueSpecification p : a.parameters()) {
                if (!(p instanceof com.legend.protocol.spec.Variable || p instanceof com.legend.protocol.spec.CInteger
                        || p instanceof com.legend.protocol.spec.CFloat || p instanceof com.legend.protocol.spec.CString
                        || p instanceof com.legend.protocol.spec.CBoolean)) {
                    return null;
                }
            }
            return a.parameters();
        }

        /** {@code fqn}'s overloads as the words read them, from the model's functions. */
        private static List<TsToPure.Overload> read(ModelContext model, String fqn) {
            List<TsToPure.Overload> out = new ArrayList<>();
            for (com.legend.compiler.element.TypedFunction f : model.findFunction(fqn)) {
                List<TsToPure.Parameter> ps = new ArrayList<>();
                for (com.legend.compiler.element.TypedParameter p : f.parameters()) {
                    boolean optional = p.multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                            && b.lower() == 0 && Integer.valueOf(1).equals(b.upper());
                    ps.add(new TsToPure.Parameter(p.name(), typeOf(p.type()), optional));
                }
                List<String> gives = new ArrayList<>();
                if (f.returnType() instanceof Type.GenericType g && !g.arguments().isEmpty()
                        && g.arguments().get(0) instanceof Type.RelationType r) {
                    r.columns().stream().map(Type.Column::name).filter(n -> !n.equals("key") && !n.equals("date"))
                            .forEach(gives::add);
                }
                String doc = f.definition() == null ? null : f.definition().taggedValues().stream()
                        .filter(t -> t.tagName().equals("doc") && t.profileName().endsWith("doc"))
                        .map(com.legend.model.TaggedValue::value).findFirst().orElse(null);
                List<String> aliases = f.definition() == null ? List.of() : f.definition().taggedValues().stream()
                        .filter(t -> t.tagName().equals("alias") && t.profileName().equals(TsToPure.LIBRARY + "Words"))
                        .map(com.legend.model.TaggedValue::value).toList();
                // a word's facts: the profile's other tags by name, in the order written, and its stereotypes (A3)
                java.util.Map<String, List<String>> facts = new java.util.LinkedHashMap<>();
                List<String> marks = new ArrayList<>();
                if (f.definition() != null) {
                    for (com.legend.model.TaggedValue t : f.definition().taggedValues()) {
                        if (t.profileName().equals(TsToPure.LIBRARY + "Words") && !t.tagName().equals("alias")) {
                            facts.computeIfAbsent(t.tagName(), k -> new ArrayList<>()).add(t.value());
                        }
                    }
                    for (com.legend.model.StereotypeApplication s : f.definition().stereotypes()) {
                        if (s.profileName().equals(TsToPure.LIBRARY + "Words")) {
                            marks.add(s.stereotypeName());
                        }
                    }
                }
                out.add(new TsToPure.Overload(ps, gives, doc, aliases, facts, marks,
                        f.body().map(b -> delegate(model, f, b)).orElse(null)));
            }
            return List.copyOf(out);
        }

        /** A parameter's type as the words read it: a line ({@link TsToPure#LINE}), a long relation of weighted members
         *  ({@link TsToPure#MEMBERS}), else its type's path. */
        private static String typeOf(Type t) {
            if (t instanceof Type.GenericType g && !g.arguments().isEmpty()
                    && g.arguments().get(0) instanceof Type.RelationType r) {
                java.util.Set<String> columns = new java.util.TreeSet<>(r.columns().stream().map(Type.Column::name)
                        .toList());
                if (columns.equals(java.util.Set.of("key", "date", "value"))) {
                    return TsToPure.LINE;
                }
                if (columns.equals(java.util.Set.of("key", "date", "time", "value"))) {
                    return TsToPure.TIMED_LINE;
                }
                if (columns.equals(java.util.Set.of("key", "date", "member", "weight", "members", "value"))) {
                    return TsToPure.MEMBERS;
                }
                if (columns.equals(java.util.Set.of("key", "date", "time", "member", "weight", "members", "value"))) {
                    return TsToPure.TIMED_MEMBERS;
                }
            }
            return t.typeName();
        }

        @Override
        public List<String> functionsIn(String pkg) {
            return model.derived(Words.class, Words::new).functionsIn(pkg);
        }
    }
}
