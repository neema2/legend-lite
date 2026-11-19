// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.ExecutionContext;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedEval;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedGraphFetch;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedMilestonedAccess;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.compiler.spec.typed.TypedViewRelation;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.FunctionId;
import com.legend.model.MappingDefinition;
import com.legend.platform.Feature;
import com.legend.protocol.SourceInfo;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * THE PURE-MEANING DETECTOR at legend-engine's door (A3 piece 3; the final plan's section 4a,
 * docs/records/compat-final-plan-2026-11-19.md on claude/compat-program; D1, ruled by the user 2026-11-19 ~13:25
 * ("Okay agree with all"), and the default, ruled ~19:20 ("I think it has to be refuse then for now")).
 *
 * <p>Since A3, lite's {@code substring} and {@code indexOf} mean legend-pure's: 0-based, end-exclusive, -1 on a miss.
 * legend-engine gives them SQL's meaning (1-based, 0 on a miss) in the code it sends to the database, and legend-pure's
 * everywhere else (H5's F1; asked of legend-engine 4.145.0: a store-free {@code let} is computed before the query,
 * docs/records/compat-homework/H16-engine-answer-opt-in.md). So a legend-engine request whose store-bound code calls the
 * String {@code substring} or the 2-argument String {@code indexOf} would get a different answer here. This check
 * finds each such call and the route says so: it refuses the request by default, or under
 * {@code LEGEND_LITE_PURE_MEANING=warn} answers it with a header and a log line.
 *
 * <p>It is a reading of the typed tree, and nothing else:
 * <ul>
 *   <li>it never changes the query, since the route plans and runs the {@link TypedQuery} it already holds;</li>
 *   <li>it computes no value;</li>
 *   <li>it adds nothing to the core: every reader is the product's public API.</li>
 * </ul>
 *
 * <p>What it reads, as legend-engine plans the same code (each read in the pinned 4.145.0 sources):
 * <ul>
 *   <li>the query's store-bound statements (those that read a store, directly or through the functions they call),
 *       and the let-bound lambdas they name;</li>
 *   <li>the user functions they call, transitively, which includes the qualified properties they read: the typer
 *       types those as calls of their lifted bodies;</li>
 *   <li>the class mappings of the classes they read:
 *     <ul>
 *       <li>a relational binding's lifted body, which also carries a {@code ~func} relation function;</li>
 *       <li>an M2M binding's transforms only when the mapping is chained over a store and the query is not a
 *           graphFetch ({@code chain.pure:111-116} pushes them into the source query; {@code storeContract.pure:89-91}
 *           runs a graphFetch's in Java);</li>
 *     </ul></li>
 * </ul>
 *
 * <p>What it leaves out:
 * <ul>
 *   <li>a class constraint's lifted body: legend-engine checks it in Java. A constraint {@code validate} turns into a
 *       filter is inline, and found there.</li>
 *   <li>{@code ###Relational} dynaFunctions and the {@code #SQL}, {@code #Cypher} and {@code #IQ} islands: they keep
 *       SQL's meaning through lite-internal natives (A3 piece 1).</li>
 * </ul>
 *
 * <p>Quiet:
 * <ul>
 *   <li>{@code substring} under {@code CORRECT_SQL_SUBSTRING_INDEXING}, which legend-engine's processSubstring honours
 *       ({@code pureToSQLQuery.pure:4662}). {@code indexOf} has no such option ({@code :10644}), so it is always
 *       flagged.</li>
 *   <li>A literal start of 0 where it is measured equal to Pure's answer
 *       ({@code docs/records/compat-homework/a3/*-literal-zero.tsv}): on H2 both forms, on DuckDB the 2-argument form
 *       alone.</li>
 * </ul>
 *
 * <p>A call it cannot follow (a function value it does not hold, a body that does not type) is reported as
 * {@code unverified}, never as clean.
 */
public final class PureMeaningCheck {

    private PureMeaningCheck() {
    }

    /** The server setting, read once at the edge's composition root ({@code EdgeServer.UPSTREAM}). */
    static final String SETTING = "LEGEND_LITE_PURE_MEANING";

    /** The answer's header: each code and its count, never the query's text. */
    static final String HEADER = "x-legend-lite-pure-meaning";

    /** What the route does with a request the check flags. */
    public enum Mode {
        /** The default (the user, ~19:20): the request is refused, naming each call. */
        REFUSE,
        /** An operator's choice: the request is answered, with the header and a log line. */
        WARN
    }

    /** The setting's value: unset, or {@code refuse}, is {@link Mode#REFUSE}; {@code warn} is {@link Mode#WARN}; any
     *  other value stops the server's start (no fallback). A further value (the engine-answer opt-in, H16) joins this
     *  switch only by the user's ruling. */
    public static Mode mode(@com.legend.base.Nullable String value) {
        if (value == null) {
            return Mode.REFUSE;
        }
        return switch (value) {
            case "", "refuse" -> Mode.REFUSE;
            case "warn" -> Mode.WARN;
            default -> throw new IllegalStateException(SETTING + "=" + value + " is neither refuse nor warn");
        };
    }

    /** A flagged call's kind, at most four, in the header's order. */
    enum Code {
        SUBSTRING_2("substring/2"),
        SUBSTRING_3("substring/3"),
        INDEX_OF_2("indexOf/2"),
        /** a call the check could not follow, never counted clean */
        UNVERIFIED("unverified");

        final String text;

        Code(String text) {
            this.text = text;
        }
    }

    /** One flagged call: its kind, where it is (the query, a function, a mapping's class), and its position when the
     *  request or the model carried one. */
    record Site(Code code, String where, @com.legend.base.Nullable SourceInfo pos) {

        String described() {
            return code.text + (pos == null ? "" : " at " + pos.startLine() + ":" + pos.startColumn()) + " in " + where;
        }
    }

    /** The check's answer: the flagged calls, in the order the walk met them. */
    record Result(List<Site> sites) {

        Result {
            sites = List.copyOf(sites);
        }

        boolean clean() {
            return sites.isEmpty();
        }

        /** The header's value: each code present and its count, in {@link Code}'s order, e.g.
         *  {@code substring/3=2, indexOf/2=1, unverified=1}. At most four codes, so bounded by construction. */
        String header() {
            Map<Code, Integer> counts = new EnumMap<>(Code.class);
            for (Site s : sites) {
                counts.merge(s.code(), 1, Integer::sum);
            }
            List<String> parts = new ArrayList<>();
            counts.forEach((c, n) -> parts.add(c.text + "=" + n));
            return String.join(", ", parts);
        }

        /** The refusal's and the log line's sentence: each call by kind, position and place (at most 20, then a count),
         *  never the query's text. */
        String sentence(String route, Mode mode) {
            StringBuilder b = new StringBuilder("pure/v1 ").append(route).append(": ");
            int shown = Math.min(sites.size(), MAX_SITES_SHOWN);
            for (int i = 0; i < shown; i++) {
                b.append(i == 0 ? "" : ", ").append(sites.get(i).described());
            }
            if (sites.size() > shown) {
                b.append(", and ").append(sites.size() - shown).append(" more");
            }
            b.append(". Lite answers substring and indexOf with legend-pure's meaning (0-based; -1 on a miss), where"
                    + " legend-engine sends this code to the database with SQL's (1-based; 0 on a miss), so the answer"
                    + " would differ (register S130, S134); a call marked unverified could not be followed and is not"
                    + " counted clean. ");
            b.append(mode == Mode.REFUSE
                    ? "Refused; " + SETTING + "=warn answers such a query with legend-pure's meaning"
                    : "Answered with legend-pure's meaning (" + SETTING + "=warn)");
            return b.toString();
        }
    }

    private static final int MAX_SITES_SHOWN = 20;

    private static final FunctionId STRING_INDEX_OF_2 =
            FunctionId.of(com.legend.builtin.Pure.INDEX_OF__STRING_1__STRING_1);

    /** The query's flagged calls. {@code runtime} is the runtime the route runs it on (the request's or the query's
     *  own), {@code requestMapping} a mapping the request names beside the query (the test-data routes'); either may
     *  be null. */
    static Result of(TypedQuery query, ModelContext ctx, @com.legend.base.Nullable String runtime,
            @com.legend.base.Nullable String requestMapping) {
        Walk w = new Walk(query, ctx, target(ctx, runtime));
        Set<Feature> features = ExecutionContext.treeFeatures(query.body());
        if (requestMapping != null) {
            w.mappings.add(requestMapping);
        }
        if (runtime != null) {
            ctx.findRuntime(runtime).ifPresent(rt -> w.mappings.addAll(rt.mappings()));
        }
        Map<String, TypedSpec> lets = new LinkedHashMap<>();
        List<TypedSpec> hostSide = new ArrayList<>();
        for (TypedSpec stmt : query.body()) {
            if (w.readsAStore(stmt)) {
                w.node(stmt, features, "the query");
            } else {
                hostSide.add(stmt);
            }
            if (stmt instanceof TypedLet let) {
                lets.put(let.name(), let.value());
            }
        }
        // a host-side let bound to a lambda that store-bound code names is store-bound code too (to a fixpoint). Only a
        // let whose value IS a lambda: a let whose value is computed with one (['ax']->map(s|$s->indexOf('x'))) is a
        // store-free value legend-engine computes on the host before the query, Pure's meaning (asked of 4.145.0, E-M1:
        // `qty in (1, 1)` in its SQL)
        Set<String> walkedLets = new HashSet<>();
        boolean grew = true;
        while (grew) {
            grew = false;
            for (TypedSpec stmt : hostSide) {
                if (stmt instanceof TypedLet let && w.named.contains(let.name()) && walkedLets.add(let.name())
                        && let.value() instanceof TypedLambda) {
                    w.node(let.value(), features, "the query");
                    grew = true;
                }
            }
        }
        w.mappingReach(features);
        // a variable the query evaluates, bound by a let the walk did not take: the typer substitutes a literal lambda
        // let into its uses, so this let's value is a function computed some other way ([{s|...}]->toOne(), a
        // function's answer, a cast), whose body the walk never reads: unverified, never clean (the reviewer's R2-S1)
        for (String v : w.evaluated) {
            if (lets.containsKey(v) && !(walkedLets.contains(v) && lets.get(v) instanceof TypedLambda)) {
                w.sites.add(new Site(Code.UNVERIFIED, "the query (the function value of let '" + v + "')", null));
            }
        }
        return new Result(w.sites);
    }

    /** The database the runtime's connections declare, when they declare one; else null (no exemption applies). */
    private static @com.legend.base.Nullable DatabaseType target(ModelContext ctx,
            @com.legend.base.Nullable String runtime) {
        if (runtime == null) {
            return null;
        }
        Set<DatabaseType> types = new LinkedHashSet<>();
        ctx.findRuntime(runtime).ifPresent(rt -> {
            for (List<String> connections : rt.connectionBindings().values()) {
                for (String c : connections) {
                    ctx.findConnection(c).ifPresent(cd -> types.add(cd.databaseType()));
                }
            }
            for (com.legend.model.PackageableElement e : rt.inlineConnections()) {
                if (e instanceof com.legend.model.ConnectionDefinition cd) {
                    types.add(cd.databaseType());
                }
            }
        });
        return types.size() == 1 ? types.iterator().next() : null;
    }

    /** One check's walk: what it met, and what it has already followed. */
    private static final class Walk {
        final TypedQuery query;
        final ModelContext ctx;
        final @com.legend.base.Nullable DatabaseType target;
        final List<Site> sites = new ArrayList<>();
        /** the functions followed, with whether the corrected option held for them */
        final Set<String> followed = new HashSet<>();
        /** the variables store-bound code names */
        final Set<String> named = new HashSet<>();
        /** the classes store-bound code reads, and the mappings in scope */
        final Set<String> classes = new LinkedHashSet<>();
        final Set<String> mappings = new LinkedHashSet<>();
        boolean chained;
        boolean graphFetch;
        /** how deep the walk is in bodies it followed (a function's, a mapping's): 0 in the query's own tree */
        int following;
        /** the variables the query's own tree evaluates as functions ({@code $f->eval(...)}) */
        final Set<String> evaluated = new LinkedHashSet<>();
        final Map<FunctionId, Boolean> storeMemo = new HashMap<>();

        Walk(TypedQuery query, ModelContext ctx, @com.legend.base.Nullable DatabaseType target) {
            this.query = query;
            this.ctx = ctx;
            this.target = target;
        }

        /** Whether a statement reads a store, directly or through the functions it calls. */
        boolean readsAStore(TypedSpec stmt) {
            return com.legend.compiler.spec.StatementEffects.reaches(stmt, Walk::isAStoreRead, query.specs(),
                    storeMemo);
        }

        static boolean isAStoreRead(TypedSpec n) {
            return n instanceof TypedGetAll || n instanceof TypedTableReference || n instanceof TypedFrom
                    || n instanceof TypedMilestonedAccess || n instanceof TypedViewRelation;
        }

        void node(TypedSpec n, Set<Feature> features, String where) {
            Set<Feature> here = features;
            switch (n) {
                case TypedFrom f -> {
                    ExecutionContext c = f.context();
                    if (!c.features().isEmpty()) {
                        here = new HashSet<>(features);
                        here.addAll(c.features());
                    }
                    c.mapping().ifPresent(m -> mappings.add(m.fullPath()));
                    mappings.addAll(c.chainMappings());
                    chained |= !c.chainMappings().isEmpty();
                }
                case TypedNativeCall nc -> call(nc, features, where);
                case TypedUserCall uc -> follow(uc.callee(), features, uc.pos());
                case TypedGetAll g -> classes.add(g.classFqn());
                case TypedGraphFetch g -> {
                    graphFetch = true;
                    // the tree's properties are not the node's children: a qualified property it reads is computed in
                    // the database by legend-engine (asked of 4.145.0, E-M1: deskPart() answers SQL's 'Cre'), so its
                    // body is followed, and each class the tree reaches joins the mapping reach
                    if (g.source().info().type() instanceof Type.ClassType ct) {
                        tree(ct.fqn(), g.tree(), features, where);
                    }
                }
                case TypedPropertyAccess p -> {
                    if (p.info().type() instanceof Type.ClassType ct) {
                        classes.add(ct.fqn());
                    }
                }
                case TypedVariable v -> named.add(v.name());
                case TypedPackageableRef r -> {
                    // a function named as a value (map(my::f)): its body runs where the call runs
                    if (ctx.functionFqns().contains(r.fullPath())) {
                        for (TypedFunction tf : ctx.findFunction(r.fullPath())) {
                            follow(tf, features, null);
                        }
                    }
                }
                case TypedEval e -> {
                    if (e.fn() instanceof TypedVariable v && following == 0) {
                        evaluated.add(v.name());
                    } else if (!(e.fn() instanceof TypedLambda || e.fn() instanceof TypedVariable
                            || e.fn() instanceof TypedPackageableRef)) {
                        sites.add(new Site(Code.UNVERIFIED, where + " (a function value)", null));
                    }
                }
                default -> {
                }
            }
            for (TypedSpec c : n.children()) {
                node(c, here, where);
            }
        }

        /** A graph fetch tree over {@code classFqn}: each derived (qualified) property's body followed, each node's
         *  arguments walked, and each class reached added to the classes read. */
        void tree(String classFqn, List<com.legend.compiler.spec.typed.TypedGraphTree> nodes, Set<Feature> features,
                String where) {
            classes.add(classFqn);
            for (com.legend.compiler.spec.typed.TypedGraphTree t : nodes) {
                for (TypedSpec a : t.args()) {
                    node(a, features, where);
                }
                String owner = t.subTypeFqn() != null ? t.subTypeFqn() : classFqn;
                com.legend.compiler.element.Property p = ctx.findProperty(owner, t.property()).orElse(null);
                if (p == null) {
                    sites.add(new Site(Code.UNVERIFIED, where + " (graph fetch property " + owner + "."
                            + t.property() + " not found)", null));
                    continue;
                }
                if (p instanceof com.legend.compiler.element.Property.Derived d) {
                    List<TypedFunction> body = ctx.findFunction(d.bodyFunctionFqn());
                    if (body.isEmpty()) {
                        sites.add(new Site(Code.UNVERIFIED, owner + "." + t.property()
                                + " (its body is not in the model)", null));
                    }
                    for (TypedFunction tf : body) {
                        follow(tf, features, null);
                    }
                }
                if (p.type() instanceof Type.ClassType child) {
                    tree(child.fqn(), t.children(), features, where);
                }
            }
        }

        /** A native call: the String substring (unless corrected, or a measured literal-0 start), the 2-argument
         *  String indexOf (always). */
        void call(TypedNativeCall nc, Set<Feature> features, String where) {
            FunctionId id = nc.callee().id();
            if (com.legend.builtin.Pure.AT_STRING_SUBSTRING.contains(id)) {
                int arity = nc.args().size();
                if (features.contains(Feature.CORRECT_SQL_SUBSTRING_INDEXING) || zeroStartExempt(nc, arity)) {
                    return;
                }
                sites.add(new Site(arity == 2 ? Code.SUBSTRING_2 : Code.SUBSTRING_3, where, nc.pos()));
            } else if (STRING_INDEX_OF_2.equals(id)) {
                sites.add(new Site(Code.INDEX_OF_2, where, nc.pos()));
            }
        }

        /** A literal start of 0, where legend-engine's answer is measured equal to Pure's on the target: the edge's
         *  registry holds the measured table, per database and arity ({@link PureText#zeroStartAnswersAsPure}). */
        boolean zeroStartExempt(TypedNativeCall nc, int arity) {
            if (!(nc.args().get(1) instanceof TypedCInteger start) || start.value().longValue() != 0) {
                return false;
            }
            return PureText.zeroStartAnswersAsPure(target, arity);
        }

        /** A user function's body (or a lifted one: a qualified property, a class mapping), once per corrected
         *  state. A constraint's lifted body is legend-engine's to check in Java, so it is not followed. */
        void follow(TypedFunction fn, Set<Feature> features, @com.legend.base.Nullable SourceInfo pos) {
            if (fn.definition() instanceof com.legend.model.FunctionDefinition fd && fd.synthesizedFrom() != null
                    && (fd.synthesizedFrom().hat() == com.legend.model.SynthHat.CONSTRAINT
                        || fd.synthesizedFrom().hat() == com.legend.model.SynthHat.CONSTRAINT_MSG)) {
                return;
            }
            if (com.legend.builtin.Subsumed.of(fn.qualifiedName()).isPresent() || fn.body().isEmpty()) {
                return;
            }
            boolean corrected = features.contains(Feature.CORRECT_SQL_SUBSTRING_INDEXING);
            String key = (fn.id() == null ? fn.qualifiedName() : fn.id().qualified()) + (corrected ? "+c" : "");
            if (!followed.add(key)) {
                return;
            }
            List<TypedSpec> body;
            try {
                body = query.specs().compile(fn).body();
            } catch (com.legend.error.LegendCompileException e) {
                sites.add(new Site(Code.UNVERIFIED, fn.qualifiedName() + " (its body does not type)", pos));
                return;
            }
            following++;
            try {
                for (TypedSpec s : body) {
                    node(s, features, fn.qualifiedName());
                }
            } finally {
                following--;
            }
        }

        /** The class mappings of the classes store-bound code reads: a relational binding's lifted body; an M2M
         *  binding's only when chained over a store and the query is not a graphFetch; an operation's (a union's sets
         *  are followed as its calls). Each mapping's association bindings, when the code navigates to a class. */
        void mappingReach(Set<Feature> features) {
            Set<String> done = new HashSet<>();
            boolean more = true;
            while (more) {
                more = false;
                for (String m : List.copyOf(mappings)) {
                    MappingDefinition md = ctx.findMapping(m).orElse(null);
                    if (md == null) {
                        // a mapping the model does not declare: what it binds cannot be read, so never clean
                        if (done.add("mapping " + m)) {
                            sites.add(new Site(Code.UNVERIFIED, "mapping " + m + " (not in the model)", null));
                        }
                        continue;
                    }
                    for (MappingDefinition.ClassBinding b : md.classBindingsWithIncludes(ctx::findMapping)) {
                        if (!classes.contains(b.classFqn()) || !done.add(b.functionFqn())) {
                            continue;
                        }
                        if (b instanceof MappingDefinition.ClassBinding.Pure && (graphFetch || !chained)) {
                            continue;
                        }
                        int before = classes.size() + mappings.size();
                        List<TypedFunction> bound = ctx.findFunction(b.functionFqn());
                        if (bound.isEmpty()) {
                            // a binding whose lifted body the model does not hold: never clean
                            sites.add(new Site(Code.UNVERIFIED, "mapping " + m + "'s " + b.classFqn()
                                    + " (its binding's body is not in the model)", null));
                        }
                        for (TypedFunction tf : bound) {
                            follow(tf, features, null);
                        }
                        more |= classes.size() + mappings.size() != before;
                    }
                    for (MappingDefinition.AssociationBinding a : md.associationBindings()) {
                        if (done.add(a.predicateFunctionFqn())) {
                            for (TypedFunction tf : ctx.findFunction(a.predicateFunctionFqn())) {
                                follow(tf, features, null);
                            }
                        }
                    }
                }
            }
        }
    }
}
