// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.error.NotImplementedException;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * The body of {@link ExecutionContext.Reader#read}: the ONE place that knows
 * the shape of the engine's runtime classes ({@code connectionStores},
 * {@code connection}, {@code mappings}, {@code testDataSetupSqls},
 * {@code testDataSetupCsv}, {@code quoteIdentifiers}, {@code timeZone},
 * {@code type}, {@code element}, {@code url}) — over a typed value, a
 * let-bound variable (chased through {@code bind}) or a helper call's raw
 * body (through {@code fnBody}; nested helpers depth-capped). Package-
 * private: readers are minted through {@link ExecutionContext#reader()}.
 */
final class ContextReading {
    private final Function<String, Optional<List<ValueSpecification>>> fnBody;
    private final UnaryOperator<TypedSpec> bind;
    private final UnaryOperator<String> canon;
    private final Function<TypedCopyInstance, @com.legend.Nullable String> dbOfCopy;

    ContextReading(Function<String, Optional<List<ValueSpecification>>> fnBody,
            UnaryOperator<TypedSpec> bind, UnaryOperator<String> canon,
            Function<TypedCopyInstance, @com.legend.Nullable String> dbOfCopy) {
        this.fnBody = fnBody;
        this.bind = bind;
        this.canon = canon;
        this.dbOfCopy = dbOfCopy;
    }

    ExecutionContext read(Optional<TypedPackageableRef> mapping,
            @com.legend.Nullable TypedSpec runtimeArg) {
        if (runtimeArg == null) {
            return ExecutionContext.of(mapping, Optional.empty());
        }
        if (runtimeArg instanceof TypedPackageableRef ref) {
            return ExecutionContext.of(mapping, Optional.of(ref));
        }
        scope(runtimeArg, java.util.Set.of());
        scoped.add(runtimeArg);
        List<String> chain = new ArrayList<>();
        collectChain(runtimeArg, chain);
        Map<String, String> json = new LinkedHashMap<>();
        collectJson(runtimeArg, json);
        List<String> sql = new ArrayList<>();
        List<ExecutionContext.CsvSetup> csv = new ArrayList<>();
        collectSetups(runtimeArg, sql, csv, null);
        TypedNewInstance conn = connectionInstance(runtimeArg);
        return new ExecutionContext(mapping, Optional.empty(), chain, json, sql, csv,
                connectionName(runtimeArg), quoteIdentifiers(runtimeArg),
                timeZone(runtimeArg),
                conn == null ? null : databaseType(conn), conn,
                storeFqn(runtimeArg), false, List.of(), postProcessors(runtimeArg),
                java.util.Set.of());
    }

    /** The connection's SQL post-processors (sqlQueryPostProcessors /
     * sqlQueryPostProcessorsConnectionAware hooks, MapperPostProcessor
     * postProcessors) as the frame's post-processor facts. */
    ExecutionContext.PostProcessors postProcessors(TypedSpec runtimeArg) {
        Map<String, String> out = new LinkedHashMap<>();
        // [0] = CTE extraction installed, [1] = nonExecutable installed
        boolean[] cte = {false, false};
        collectConnections(runtimeArg, out, cte, this::chase);
        return new ExecutionContext.PostProcessors(out, cte[0], cte[1]);
    }

    private static void collectConnections(TypedSpec n,
            Map<String, String> out, boolean[] cte,
            java.util.function.UnaryOperator<TypedSpec> bind) {
        if (n instanceof TypedNewInstance ni) {
            TypedSpec aware = ni.properties().get(
                    "sqlQueryPostProcessorsConnectionAware");
            if (aware != null) {
                for (TypedSpec hook : ppElements(aware)) {
                    readHook(hook, out, cte, bind);
                }
            }
            // the PLAIN slot carries the same replaceTables shape (hook
            // takes (SQLQuery) instead of (SQLQuery, DatabaseConnection))
            TypedSpec plain = ni.properties().get("sqlQueryPostProcessors");
            if (plain != null) {
                for (TypedSpec hook : ppElements(plain)) {
                    // LOUD (deep-audit D2-4, slice zero 2026-08-15;
                    // user ruling): the old catch-and-skip silently
                    // dropped any hook the recognizer didn't parse — and
                    // the cteExtraction corpus tests were "passing" with
                    // the very feature they test skipped (a false
                    // green). A hook is either recognized-and-applied
                    // (the replaceTables pattern) or the query REFUSES;
                    // the 7 cteExtraction tests are adjudicated
                    // blocked-on-feature until an IR CTE-extraction pass
                    // exists.
                    readHook(hook, out, cte, bind);
                }
            }
        }
        if (n instanceof TypedNewInstance ni
                && ni.properties().get("postProcessors") instanceof TypedSpec pps) {
            // the CONNECTION-LEVEL mapper post-processor (batch 80):
            // postProcessors = ^MapperPostProcessor(mappers = ^TableNameMapper(
            // schema = ^SchemaNameMapper(from, to), from, to)) — the engine
            // renames tables/schemas in the generated SQL
            // (runtime/connection/postprocessor.pure:35-48); a table
            // rename is the same IR pass as replaceTables (tableReplace).
            // Exact-FQN dispatch; any other post-processor kind is loud —
            // no IR pass exists for it, and the query must not run
            // un-post-processed.
            for (TypedSpec pp : ppElements(pps)) {
                readMapperPostProcessor(pp, out);
            }
        }
        if (n instanceof com.legend.compiler.spec.typed
                .TypedCopyInstance cp) {
            for (String key : new String[] {
                    "sqlQueryPostProcessorsConnectionAware",
                    "sqlQueryPostProcessors"}) {
                TypedSpec hooks = cp.overrides().get(key);
                if (hooks != null) {
                    for (TypedSpec hook : ppElements(hooks)) {
                        readHook(hook, out, cte, bind);
                    }
                }
            }
        }
        for (TypedSpec c : n.children()) {
            collectConnections(c, out, cte, bind);
        }
    }

    private static List<TypedSpec> ppElements(TypedSpec v) {
        return v instanceof TypedCollection tc ? tc.elements() : List.of(v);
    }

    private static final String MAPPER_PP_FQN =
            "meta::pure::alloy::connections::MapperPostProcessor";
    private static final String TABLE_MAPPER_FQN =
            "meta::pure::alloy::connections::TableNameMapper";
    private static final String SCHEMA_MAPPER_FQN =
            "meta::pure::alloy::connections::SchemaNameMapper";

    /** One {@code postProcessors} element: a MapperPostProcessor's table
     * mappers become tableReplace renames; a schema mapper that moves a
     * table to ANOTHER schema has no IR pass yet (loud), an identity
     * schema mapper is a no-op. */
    private static void readMapperPostProcessor(TypedSpec pp,
            Map<String, String> out) {
        if (!(pp instanceof TypedNewInstance mp)
                || !MAPPER_PP_FQN.equals(mp.classFqn())) {
            throw new NotImplementedException("connection post-processor "
                    + (pp instanceof TypedNewInstance x ? "'" + x.classFqn() + "'"
                            : pp.getClass().getSimpleName())
                    + " has no IR pass (only MapperPostProcessor is compiled)");
        }
        TypedSpec mappers = mp.properties().get("mappers");
        for (TypedSpec m : mappers == null ? List.<TypedSpec>of() : ppElements(mappers)) {
            if (!(m instanceof TypedNewInstance mi)) {
                throw new NotImplementedException(
                        "MapperPostProcessor mapper is not an instance literal");
            }
            String from = mapperLiteral(mi, "from");
            String to = mapperLiteral(mi, "to");
            if (TABLE_MAPPER_FQN.equals(mi.classFqn())) {
                if (mi.properties().get("schema") instanceof TypedNewInstance sch
                        && !mapperLiteral(sch, "from").equals(mapperLiteral(sch, "to"))) {
                    throw new NotImplementedException("TableNameMapper moving '"
                            + from + "' to another schema has no IR pass yet");
                }
                out.putIfAbsent(from, to);
            } else if (SCHEMA_MAPPER_FQN.equals(mi.classFqn())) {
                if (!from.equals(to)) {
                    throw new NotImplementedException("SchemaNameMapper '" + from
                            + "' -> '" + to + "' has no IR pass yet");
                }
            } else {
                throw new NotImplementedException("MapperPostProcessor mapper '"
                        + mi.classFqn() + "' is not a table/schema mapper");
            }
        }
    }

    private static String mapperLiteral(TypedNewInstance mi, String prop) {
        if (mi.properties().get(prop) instanceof com.legend.compiler.spec.typed.TypedCString cs) {
            return cs.value();
        }
        throw new NotImplementedException("mapper '" + prop
                + "' is not a string literal on " + mi.classFqn());
    }

    /** One hook lambda: the ONLY recognized body is a terminal
     * {@code replaceTables($query, <pairs>)} call. */
    private static @com.legend.Nullable String calleeOf(TypedSpec n) {
        return switch (n) {
            case TypedNativeCall c -> c.callee().qualifiedName();
            case com.legend.compiler.spec.typed.TypedUserCall u -> u.callee().qualifiedName();
            default -> null;
        };
    }


    private static void readHook(TypedSpec hook, Map<String, String> out,
            boolean[] cte, java.util.function.UnaryOperator<TypedSpec> bind) {
        // {s | ^Result<SelectSQLQuery|1>(values = $s->extractSubqueriesAsCTEs())}
        // — the CTE-extraction processor (cteExtractionPostProcessor.pure:139)
        if (hook instanceof TypedLambda cl && !cl.body().isEmpty()
                && cl.body().get(cl.body().size() - 1) instanceof TypedNewInstance rni
                && rni.properties().get("values") instanceof TypedSpec vals
                && com.legend.builtin.NativeFn.ContextOption.of(calleeOf(vals)).orElse(null) == com.legend.builtin.NativeFn.ContextOption.EXTRACT_SUBQUERIES_AS_CTES) {
            cte[0] = true;
            return;
        }
        // {query | nonExecutable($query, extensions)} — the engine's
        // nonExecutable processor (nonExecutablePostProcessor.pure:24): a
        // platform post-processor, applied as the IR pass nonExecutable()
        if (hook instanceof TypedLambda nl && !nl.body().isEmpty()
                && com.legend.builtin.NativeFn.ContextOption.of(calleeOf(nl.body().get(nl.body().size() - 1))).orElse(null) == com.legend.builtin.NativeFn.ContextOption.NON_EXECUTABLE) {
            cte[1] = true;
            return;
        }
        // IDENTITY hook (ledger cluster 63): {query|$query->postprocess(
        // {rel|$rel})} — recognized-and-applied, and the application is
        // a no-op (the inner transform returns its argument). Any other
        // postprocess body stays at the loud wall below.
        if (hook instanceof TypedLambda idl && !idl.body().isEmpty()
                && idl.body().get(idl.body().size() - 1)
                        instanceof com.legend.compiler.spec.typed
                                .TypedUserCall pu
                && "meta::relational::postProcessor::postprocess"
                        .equals(pu.callee().qualifiedName())
                && pu.args().size() == 2
                && pu.args().get(1) instanceof TypedLambda inner
                && inner.parameters().size() == 1
                && inner.body().size() == 1
                && inner.body().get(0) instanceof com.legend.compiler.spec
                        .typed.TypedVariable iv
                && iv.name().equals(inner.parameters().get(0))) {
            return;
        }
        if (!(hook instanceof TypedLambda lam) || lam.body().isEmpty()
                || !(lam.body().get(lam.body().size() - 1)
                        instanceof TypedNativeCall call)
                || com.legend.builtin.NativeFn.ContextOption.of(call.callee().qualifiedName()).orElse(null) != com.legend.builtin.NativeFn.ContextOption.REPLACE_TABLES
                || call.args().size() != 2) {
            throw new NotImplementedException(
                    "sqlQueryPostProcessorsConnectionAware hook shape is"
                    + " not a replaceTables lambda — post-processor"
                    + " recognizer pending for: " + hook);
        }
        for (TypedSpec pair : ppElements(bind.apply(peel(call.args().get(1), bind)), bind)) {
            TypedSpec p = peel(pair, bind);
            if (!(p instanceof TypedNativeCall pc)
                    || !com.legend.compiler.element.type.PlatformTypes.PAIR_FN.equals(pc.callee().qualifiedName())
                    || pc.args().size() != 2) {
                throw new NotImplementedException("replaceTables pair"
                        + " argument is not a literal pair(): " + pair);
            }
            composeRename(out, tableName(pc.args().get(0), bind),
                    tableName(pc.args().get(1), bind));
        }
    }

    /** Hooks apply SEQUENTIALLY (engine semantics): a later
     *  {@code from -> to} first rewrites the RESULTS of earlier renames
     *  (so A->B then B->A nets to identity), then registers itself for
     *  tables the earlier hooks left untouched. */
    private static void composeRename(Map<String, String> out, String from,
            String to) {
        for (var e : out.entrySet()) {
            if (e.getValue().equals(from)) {
                e.setValue(to);
            }
        }
        out.putIfAbsent(from, to);
    }

    /** {@code db->schema('X')->toOne()->table('Y')->toOne()} spelled as
     * the lowerer spells FROM sources: {@code Y}, or {@code X.Y} for a
     * non-default schema. */
    private static String tableName(TypedSpec nav,
            java.util.function.UnaryOperator<TypedSpec> bind) {
        var r = StoreElementIdentity.tableRef(nav, x -> peel(x, bind));
        if (r != null) {
            return "default".equals(r.schema()) ? r.table() : r.schema() + "." + r.table();
        }
        throw new NotImplementedException("replaceTables pair side is not"
                + " a schema()/table() navigation: " + nav);
    }

    /** toOne()/cast wrappers peel — identity for navigation. */
    private static List<TypedSpec> ppElements(TypedSpec v,
            java.util.function.UnaryOperator<TypedSpec> bind) {
        List<TypedSpec> out = new java.util.ArrayList<>();
        for (TypedSpec e : ppElements(v)) {
            out.add(bind.apply(e));
        }
        return out;
    }

    private static TypedSpec peel(TypedSpec v) {
        return peel(v, java.util.function.UnaryOperator.identity());
    }

    private static TypedSpec peel(TypedSpec v,
            java.util.function.UnaryOperator<TypedSpec> bind) {
        TypedSpec cur = v;
        while (true) {
            if (cur instanceof com.legend.compiler.spec.typed.TypedVariable) {
                TypedSpec bound = bind.apply(cur);
                if (bound != cur) {
                    cur = bound;
                    continue;
                }
            }
            if (cur instanceof TypedNativeCall c && c.args().size() == 1
                    && (com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())
                            || c.callee().qualifiedName()
                                    .endsWith("::toOneMany"))) {
                cur = c.args().get(0);
                continue;
            }
            if (cur instanceof com.legend.compiler.spec.typed.TypedCast tc) {
                cur = tc.source();
                continue;
            }
            return cur;
        }
    }


    /** A Boolean option ({@code addDriverTablePkForProject},
     * {@code importDataFlow}) off an execute call's ExecutionContext
     * argument — a RelationalExecutionContext instance (let-bound or literal)
     * whose flag is a literal true; anything else is the default (false). */
    /** The feature flags an execute call's context argument carries: an
     *  {@code ExecutionOptionContext} (bound through the lets) whose
     *  {@code executionOptions} hold {@code FeatureFlagOption}s — every
     *  {@code flags} value is a literal member of the engine's Feature enum
     *  (a computed value, or a name the platform's mirror lacks, is loud: a
     *  flag is a compile-time fact, never guessed). Any other context kind
     *  carries none. */
    static java.util.Set<Feature> contextFeatures(@com.legend.Nullable TypedSpec contextArg,
            UnaryOperator<TypedSpec> bind) {
        if (contextArg == null) {
            return java.util.Set.of();
        }
        TypedSpec v = bind.apply(contextArg);
        if (!(v instanceof TypedNewInstance ni)
                || !PlatformTypes.EXECUTION_OPTION_CONTEXT.equals(ni.classFqn())) {
            return java.util.Set.of();
        }
        TypedSpec options = ni.properties().get("executionOptions");
        if (options == null) {
            return java.util.Set.of();
        }
        java.util.Set<Feature> out = java.util.EnumSet.noneOf(Feature.class);
        for (TypedSpec o : options instanceof TypedCollection c ? c.elements() : List.of(options)) {
            TypedSpec ob = bind.apply(o);
            if (!(ob instanceof TypedNewInstance opt)
                    || !PlatformTypes.FEATURE_FLAG_OPTION.equals(opt.classFqn())) {
                continue;
            }
            TypedSpec flags = opt.properties().get("flags");
            if (flags != null) {
                literalFlags(flags, out);
            }
        }
        return java.util.Set.copyOf(out);
    }

    /** The engine's SECOND flag carrier: {@code withFeatureFlags(query, flags)}
     *  calls inside the query itself (plan generation finds them in the
     *  function body — executionPlan_generation.pure — and adds their flags
     *  to the context; the call is the identity for row semantics). */
    static java.util.Set<Feature> treeFeatures(List<TypedSpec> body) {
        java.util.Set<Feature> out = java.util.EnumSet.noneOf(Feature.class);
        for (TypedSpec s : body) {
            collectTreeFeatures(s, out);
        }
        return out.isEmpty() ? java.util.Set.of() : java.util.Set.copyOf(out);
    }

    private static void collectTreeFeatures(TypedSpec n, java.util.Set<Feature> out) {
        if (n instanceof TypedNativeCall call && call.args().size() == 2
                && com.legend.builtin.Pure.WITH_FEATURE_FLAGS__T_MANY__ENUM_MANY.signatureKey()
                        .equals(call.callee().signatureKey())) {
            literalFlags(call.args().get(1), out);
        }
        for (TypedSpec c : n.children()) {
            collectTreeFeatures(c, out);
        }
    }

    /** {@code flags} — one literal Feature value or a collection of them — into {@code out}. */
    private static void literalFlags(TypedSpec flags, java.util.Set<Feature> out) {
        for (TypedSpec f : flags instanceof TypedCollection fc ? fc.elements() : List.of(flags)) {
            if (!(f instanceof TypedEnumValue ev) || !Feature.FQN.equals(ev.enumFqn())) {
                throw new com.legend.error.NotImplementedException(
                        "a feature flag must be a literal Feature value; got "
                                + f.getClass().getSimpleName());
            }
            try {
                out.add(Feature.valueOf(ev.value()));
            } catch (IllegalArgumentException e) {
                throw new com.legend.error.NotImplementedException(
                        "feature flag " + ev.value() + " is not in the platform's mirror of "
                                + Feature.FQN + " (regenerate the mirror at the bump)");
            }
        }
    }

    static boolean contextFlag(String option, @com.legend.Nullable TypedSpec contextArg,
            UnaryOperator<TypedSpec> bind) {
        if (contextArg == null) {
            return false;
        }
        TypedSpec v = bind.apply(contextArg);
        if (!(v instanceof TypedNewInstance ni)
                || !PlatformTypes.RELATIONAL_EXECUTION_CONTEXT.equals(ni.classFqn())) {
            return false;
        }
        TypedSpec flag = ni.properties().get(option);
        if (flag == null) {
            return false;
        }
        if (flag instanceof TypedCBoolean b) {
            return b.value();
        }
        // the option is a compile-time fact of the call; a computed value
        // is loud, never a silent default (the reader never guesses)
        throw new com.legend.error.NotImplementedException(
                option + " must be a literal; got " + flag.getClass().getSimpleName());
    }

    /** Variable OCCURRENCES bound by an enclosing lambda parameter (by
     * identity): the let chase never reaches them — a hook lambda's
     * {@code query} is its own parameter even when a statement let shares
     * the name. Every subtree the chase brings in is scoped the same way
     * when first met. */
    private final java.util.Set<TypedSpec> lambdaBound =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    private final java.util.Set<TypedSpec> scoped =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());

    private void scope(TypedSpec n, java.util.Set<String> shadows) {
        if (n instanceof TypedVariable v) {
            if (shadows.contains(v.name())) {
                lambdaBound.add(v);
            }
            return;
        }
        if (n instanceof TypedLambda l) {
            java.util.Set<String> inner = new java.util.HashSet<>(shadows);
            inner.addAll(l.parameters());
            for (TypedSpec c : l.body()) {
                scope(c, inner);
            }
            return;
        }
        for (TypedSpec c : n.children()) {
            scope(c, shadows);
        }
    }

    private TypedSpec chase(TypedSpec v) {
        if (v instanceof TypedVariable && lambdaBound.contains(v)) {
            return v;
        }
        if (scoped.add(v)) {
            scope(v, java.util.Set.of());
        }
        TypedSpec b = bind.apply(v);
        if (b != v && scoped.add(b)) {
            scope(b, java.util.Set.of());
        }
        return b == null ? v : b;
    }

    // ---- chain mappings -------------------------------------------

    private void collectChain(TypedSpec n, List<String> out) {
        if (n instanceof TypedVariable) {
            TypedSpec b = chase(n);
            if (b != n) {
                collectChain(b, out);
            }
            return;
        }
        if (n instanceof TypedUserCall uc && uc.callee().body().isPresent()) {
            // a helper-built runtime: its ModelChainConnection lives in
            // the callee's raw body (nested helpers through fnBody)
            for (ValueSpecification b : uc.callee().body().get()) {
                collectChainRaw(b, out, 0);
            }
            return;
        }
        if (n instanceof TypedNewInstance ni
                && PlatformTypes.MODEL_CHAIN_CONNECTION.equals(ni.classFqn())) {
            TypedSpec ms = ni.properties().get("mappings");
            List<TypedSpec> els = switch (ms) {
                case TypedCollection tc -> tc.elements();
                case null -> List.of();
                default -> List.of(ms);
            };
            for (TypedSpec e : els) {
                if (e instanceof TypedPackageableRef pr && !out.contains(pr.fullPath())) {
                    out.add(pr.fullPath());
                }
            }
            return;
        }
        for (TypedSpec c : n.children()) {
            collectChain(c, out);
        }
    }

    private void collectChainRaw(ValueSpecification v, List<String> out, int depth) {
        switch (v) {
            case com.legend.protocol.spec.NewInstance ni -> {
                if (PlatformTypes.isModelChainConnection(ni.className())) {
                    var ms = ni.first("mappings");
                    List<ValueSpecification> els = ms == null ? List.of()
                            : ms.value() instanceof com.legend.protocol.spec.PureCollection pc
                                    ? pc.values() : List.of(ms.value());
                    for (ValueSpecification e : els) {
                        if (e instanceof com.legend.protocol.spec.PackageableElementPtr pr
                                && !out.contains(canon.apply(pr.fullPath()))) {
                            out.add(canon.apply(pr.fullPath()));
                        }
                    }
                    return;
                }
                for (var ke : ni.properties().stream()
                        .map(com.legend.protocol.spec.NewInstance.KeyBinding::expression)
                        .toList()) {
                    collectChainRaw(ke.value(), out, depth);
                }
            }
            case com.legend.protocol.spec.AppliedFunction af -> {
                for (var p : af.parameters()) {
                    collectChainRaw(p, out, depth);
                }
                if (depth < 3 && !"letFunction".equals(af.function())) {
                    var body = fnBody.apply(af.function());
                    if (body.isPresent()) {
                        for (var b : body.get()) {
                            collectChainRaw(b, out, depth + 1);
                        }
                    }
                }
            }
            case com.legend.protocol.spec.LambdaFunction lf -> {
                for (var b : lf.body()) {
                    collectChainRaw(b, out, depth);
                }
            }
            case com.legend.protocol.spec.PureCollection pc -> {
                for (var e : pc.values()) {
                    collectChainRaw(e, out, depth);
                }
            }
            default -> { }
        }
    }

    // ---- JSON sources ---------------------------------------------

    private void collectJson(TypedSpec n, Map<String, String> out) {
        if (n instanceof TypedVariable) {
            TypedSpec b = chase(n);
            if (b != n) {
                collectJson(b, out);
            }
            return;
        }
        if (n instanceof TypedUserCall uc && uc.callee().body().isPresent()) {
            for (ValueSpecification b : uc.callee().body().get()) {
                collectJsonRaw(b, out);
            }
            return;
        }
        if (n instanceof TypedNewInstance ni
                && PlatformTypes.JSON_MODEL_CONNECTION.equals(ni.classFqn())) {
            TypedSpec cls = ni.properties().get("class");
            String url = foldLiteral(ni.properties().get("url"));
            if (cls instanceof TypedPackageableRef pr && url != null) {
                out.put(pr.fullPath(), url);
            }
            return;
        }
        for (TypedSpec c : n.children()) {
            collectJson(c, out);
        }
    }

    private void collectJsonRaw(ValueSpecification v, Map<String, String> out) {
        switch (v) {
            case com.legend.protocol.spec.NewInstance ni -> {
                if (PlatformTypes.isJsonModelConnection(ni.className())) {
                    var cls = ni.first("class");
                    var url = ni.first("url");
                    if (cls != null && cls.value()
                            instanceof com.legend.protocol.spec.PackageableElementPtr pr
                            && url != null && url.value()
                                    instanceof com.legend.protocol.spec.CString us) {
                        out.put(canon.apply(pr.fullPath()), us.value());
                    }
                    return;
                }
                for (var ke : ni.properties().stream()
                        .map(com.legend.protocol.spec.NewInstance.KeyBinding::expression)
                        .toList()) {
                    collectJsonRaw(ke.value(), out);
                }
            }
            case com.legend.protocol.spec.AppliedFunction af -> {
                for (var p2 : af.parameters()) {
                    collectJsonRaw(p2, out);
                }
            }
            case com.legend.protocol.spec.LambdaFunction lf -> {
                for (var b2 : lf.body()) {
                    collectJsonRaw(b2, out);
                }
            }
            case com.legend.protocol.spec.PureCollection pc -> {
                for (var e2 : pc.values()) {
                    collectJsonRaw(e2, out);
                }
            }
            default -> { }
        }
    }

    // ---- setup SQL / CSV -------------------------------------------

    private void collectSetups(TypedSpec n, List<String> out, List<ExecutionContext.CsvSetup> csv,
            @com.legend.Nullable String dbRef) {
        if (n instanceof TypedVariable) {
            TypedSpec b = chase(n);
            if (b != n) {
                collectSetups(b, out, csv, dbRef);
            }
            return;
        }
        if (n instanceof TypedUserCall uc && uc.callee().body().isPresent()) {
            Map<String, ValueSpecification> lets = new java.util.HashMap<>();
            for (ValueSpecification b : uc.callee().body().get()) {
                collectSetupsRaw(b, lets, out, 0, csv, dbRef);
            }
            return;
        }
        if (n instanceof TypedNewInstance ni) {
            String db = ni.properties().get("element")
                    instanceof TypedPackageableRef el ? el.fullPath() : dbRef;
            if (PlatformTypes.LOCAL_H2_DATASOURCE_SPECIFICATION.equals(ni.classFqn())) {
                String s = foldLiteral(ni.properties().get("testDataSetupSqls"));
                if (s != null) {
                    out.add(s);
                }
            }
            String csvText = foldLiteral(ni.properties().get("testDataSetupCsv"));
            if (csvText != null) {
                csv.add(new ExecutionContext.CsvSetup(csvText, db));
            }
            for (TypedSpec c : n.children()) {
                collectSetups(c, out, csv, db);
            }
            return;
        }
        if (n instanceof TypedCopyInstance cp
                && foldLiteral(cp.overrides().get("testDataSetupCsv")) instanceof String c2) {
            csv.add(new ExecutionContext.CsvSetup(c2, dbOfCopy.apply(cp)));
        }
        for (TypedSpec c : n.children()) {
            collectSetups(c, out, csv, dbRef);
        }
    }

    /** The unchecked-source mirror of {@link #collectSetups}: helper
     * bodies carry the blobs behind lets; a nested helper call expands
     * its body in a fresh let scope (depth-capped). */
    private void collectSetupsRaw(ValueSpecification v,
            Map<String, ValueSpecification> lets, List<String> out, int depth,
            List<ExecutionContext.CsvSetup> csv, @com.legend.Nullable String dbRef) {
        switch (v) {
            case com.legend.protocol.spec.AppliedFunction af -> {
                if ("letFunction".equals(af.function())
                        && af.parameters().size() == 2
                        && af.parameters().get(0)
                                instanceof com.legend.protocol.spec.CString nm) {
                    lets.put(nm.value(), af.parameters().get(1));
                }
                for (var p : af.parameters()) {
                    collectSetupsRaw(p, lets, out, depth, csv, dbRef);
                }
                if (depth < 3 && !"letFunction".equals(af.function())) {
                    var body = fnBody.apply(af.function());
                    if (body.isPresent()) {
                        Map<String, ValueSpecification> inner = new java.util.HashMap<>();
                        for (var b : body.get()) {
                            collectSetupsRaw(b, inner, out, depth + 1, csv, dbRef);
                        }
                    }
                }
            }
            case com.legend.protocol.spec.NewInstance ni -> {
                var el = ni.first("element");
                String db = el != null && el.value()
                        instanceof com.legend.protocol.spec.PackageableElementPtr ptr
                        ? ptr.fullPath() : dbRef;
                if (PlatformTypes.isLocalH2DatasourceSpecification(ni.className())) {
                    var ke = ni.first("testDataSetupSqls");
                    String s = ke == null ? null : foldRawLiteral(ke.value(), lets);
                    if (s != null) {
                        out.add(s);
                    }
                }
                var kc = ni.first("testDataSetupCsv");
                String c = kc == null ? null : foldRawLiteral(kc.value(), lets);
                if (c != null) {
                    csv.add(new ExecutionContext.CsvSetup(c, db));
                }
                for (var ke : ni.properties().stream()
                        .map(com.legend.protocol.spec.NewInstance.KeyBinding::expression)
                        .toList()) {
                    collectSetupsRaw(ke.value(), lets, out, depth, csv, db);
                }
            }
            case com.legend.protocol.spec.LambdaFunction lf -> {
                for (var b : lf.body()) {
                    collectSetupsRaw(b, lets, out, depth, csv, dbRef);
                }
            }
            case com.legend.protocol.spec.PureCollection pc -> {
                for (var e : pc.values()) {
                    collectSetupsRaw(e, lets, out, depth, csv, dbRef);
                }
            }
            default -> { }
        }
    }

    // ---- connection flags -----------------------------------------

    /** The FIRST connection instance under the value, or null. */
    private @com.legend.Nullable TypedNewInstance connectionInstance(TypedSpec runtimeArg) {
        ArrayDeque<TypedSpec> work = new ArrayDeque<>();
        work.add(runtimeArg);
        while (!work.isEmpty()) {
            TypedSpec t = work.poll();
            if (t instanceof TypedVariable) {
                TypedSpec b = chase(t);
                if (b != t) {
                    work.add(b);
                }
                continue;
            }
            if (t instanceof TypedNewInstance ni
                    && PlatformTypes.isRelationalConnectionClass(ni.classFqn())) {
                return ni;
            }
            work.addAll(t.children());
        }
        return null;
    }

    /** The first ConnectionStore's {@code element} store reference, or null. */
    private @com.legend.Nullable String storeFqn(TypedSpec runtimeArg) {
        ArrayDeque<TypedSpec> work = new ArrayDeque<>();
        work.add(runtimeArg);
        while (!work.isEmpty()) {
            TypedSpec t = work.poll();
            if (t instanceof TypedVariable) {
                TypedSpec b = chase(t);
                if (b != t) {
                    work.add(b);
                }
                continue;
            }
            if (t instanceof TypedNewInstance ni
                    && PlatformTypes.CONNECTION_STORE.equals(ni.classFqn())
                    && ni.properties().get("element") instanceof TypedPackageableRef pr) {
                return pr.fullPath();
            }
            work.addAll(t.children());
        }
        return null;
    }

    /** The connection's DatabaseType name ("H2" when unspelled). */
    static String databaseType(TypedNewInstance conn) {
        return conn.properties().get("type") instanceof TypedEnumValue ev
                ? String.valueOf(ev.value()) : "H2";
    }

    /** The connection's plan-text spelling ({@code DatabaseConnection(type =
     * "DB2")}): the instance's class simple name with its DatabaseType;
     * a helper-constructed runtime's instance lives in the callee's raw
     * body. Null when no connection instance appears. */
    private @com.legend.Nullable String connectionName(TypedSpec n) {
        if (n instanceof TypedVariable) {
            TypedSpec b = chase(n);
            return b == n ? null : connectionName(b);
        }
        if (n instanceof TypedNewInstance ni) {
            String simple = PlatformTypes.relationalConnectionSimpleName(ni.classFqn());
            if (simple != null) {
                return simple + "(type = \"" + databaseType(ni) + "\")";
            }
        }
        if (n instanceof TypedUserCall uc && uc.callee().body().isPresent()) {
            for (ValueSpecification b : uc.callee().body().get()) {
                String r = rawConnectionName(b);
                if (r != null) {
                    return r;
                }
            }
        }
        for (TypedSpec c : n.children()) {
            String r = connectionName(c);
            if (r != null) {
                return r;
            }
        }
        return null;
    }

    private @com.legend.Nullable String rawConnectionName(ValueSpecification n) {
        if (n instanceof com.legend.protocol.spec.NewInstance ni) {
            String simple = PlatformTypes.relationalConnectionSimpleName(ni.className());
            if (simple != null) {
                com.legend.protocol.spec.KeyExpression ke = ni.first("type");
                String db = ke != null && ke.value()
                        instanceof com.legend.protocol.spec.EnumValue ev
                        ? ev.value() : "H2";
                return simple + "(type = \"" + db + "\")";
            }
        }
        List<ValueSpecification> kids = switch (n) {
            case com.legend.protocol.spec.AppliedFunction af -> af.parameters();
            case com.legend.protocol.spec.NewInstance ni2 -> ni2.properties().stream()
                    .map(b -> b.expression().value()).toList();
            case com.legend.protocol.spec.PureCollection pc -> pc.values();
            case com.legend.protocol.spec.LambdaFunction lf -> lf.body();
            default -> List.of();
        };
        for (ValueSpecification c : kids) {
            String r = rawConnectionName(c);
            if (r != null) {
                return r;
            }
        }
        return null;
    }

    /** {@code quoteIdentifiers} off a connection instance; the platform-
     * native {@code testRuntime(quoteIdentifiers)} overload carries the
     * flag as its argument (the corpus contract, relationalSetUp.pure). */
    private boolean quoteIdentifiers(TypedSpec runtimeArg) {
        ArrayDeque<TypedSpec> work = new ArrayDeque<>();
        work.add(runtimeArg);
        while (!work.isEmpty()) {
            TypedSpec t = work.poll();
            if (t instanceof TypedVariable) {
                TypedSpec b = chase(t);
                if (b != t) {
                    work.add(b);
                }
                continue;
            }
            if (t instanceof TypedNewInstance ni
                    && ni.properties().get("quoteIdentifiers") instanceof TypedSpec qv) {
                Boolean b2 = staticBool(qv);
                if (b2 != null) {
                    return b2;
                }
            }
            if (t instanceof TypedNativeCall nc
                    && PlatformTypes.TEST_RUNTIME.equals(nc.callee().qualifiedName())
                    && nc.args().size() == 1
                    && nc.args().get(0) instanceof TypedCBoolean fb) {
                return fb.value();
            }
            work.addAll(t.children());
        }
        return false;
    }

    /** The connection's {@code timeZone}: the property may be a helper's
     * parameter bound through lets — chased through {@link #bind}. */
    private @com.legend.Nullable String timeZone(TypedSpec runtimeArg) {
        ArrayDeque<TypedSpec> work = new ArrayDeque<>();
        work.add(runtimeArg);
        java.util.Set<TypedSpec> seen = java.util.Collections.newSetFromMap(
                new java.util.IdentityHashMap<>());
        while (!work.isEmpty()) {
            TypedSpec t = work.poll();
            if (!seen.add(t)) {
                continue;
            }
            if (t instanceof TypedVariable) {
                TypedSpec b = chase(t);
                if (b != t) {
                    work.add(b);
                }
                continue;
            }
            if (t instanceof TypedNewInstance ni
                    && ni.properties().get("timeZone") != null) {
                TypedSpec tzv = chase(ni.properties().get("timeZone"));
                if (tzv instanceof TypedCString tzs) {
                    return tzs.value();
                }
            }
            work.addAll(t.children());
        }
        return null;
    }

    /** Bounded constant-fold of the corpus connection-builder idiom
     * ({@code if($q->isEmpty(), |false, |$q->toOne())} over an inlined
     * literal). Null = not statically known; never guesses. */
    private static @com.legend.Nullable Boolean staticBool(TypedSpec t) {
        return switch (t) {
            case TypedCBoolean b -> b.value();
            case TypedNativeCall nc
                    when com.legend.builtin.Pure.isToOneCall(nc.callee().qualifiedName())
                    && nc.args().size() >= 1 -> staticBool(nc.args().get(0));
            case TypedIf i -> {
                Boolean empt = staticIsEmpty(i.condition());
                if (empt == null) {
                    yield null;
                }
                TypedSpec branch = empt ? i.thenBranch() : i.elseBranch().orElse(null);
                if (branch instanceof TypedLambda l && !l.body().isEmpty()) {
                    branch = l.body().get(l.body().size() - 1);
                }
                yield branch == null ? null : staticBool(branch);
            }
            default -> null;
        };
    }

    private static @com.legend.Nullable Boolean staticIsEmpty(TypedSpec cond) {
        if (!(cond instanceof TypedNativeCall nc
                && PlatformTypes.IS_EMPTY.equals(nc.callee().qualifiedName())
                && nc.args().size() == 1)) {
            return null;
        }
        TypedSpec x = nc.args().get(0);
        if (x instanceof TypedCollection c) {
            return c.elements().isEmpty();
        }
        if (x instanceof TypedCBoolean || x instanceof TypedCString
                || x instanceof TypedCInteger) {
            return false;
        }
        return null;
    }

    // ---- literal folding -------------------------------------------

    /** A '+'-folded string literal, null when any part is non-literal. */
    private static @com.legend.Nullable String foldLiteral(@com.legend.Nullable TypedSpec n) {
        if (n instanceof TypedCString cs) {
            return cs.value();
        }
        if (n instanceof TypedNativeCall c
                && PlatformTypes.isPlus(c.callee().qualifiedName())) {
            StringBuilder sb = new StringBuilder();
            for (TypedSpec a : c.args()) {
                String part = foldLiteral(a);
                if (part == null) {
                    return null;
                }
                sb.append(part);
            }
            return sb.toString();
        }
        if (n instanceof TypedCollection tc) {
            StringBuilder sb = new StringBuilder();
            for (TypedSpec a : tc.elements()) {
                String part = foldLiteral(a);
                if (part == null) {
                    return null;
                }
                sb.append(part);
            }
            return sb.toString();
        }
        return null;
    }

    /** A raw-spec string literal folded through '+' chains, collections
     * and let-bound variables; null when any part is non-literal. */
    private static @com.legend.Nullable String foldRawLiteral(ValueSpecification v,
            Map<String, ValueSpecification> lets) {
        return switch (v) {
            case com.legend.protocol.spec.CString cs -> cs.value();
            case com.legend.protocol.spec.Variable vr -> {
                var bound = lets.get(vr.name());
                yield bound == null ? null : foldRawLiteral(bound, lets);
            }
            case com.legend.protocol.spec.AppliedFunction af
                    when PlatformTypes.isPlus(af.function()) -> {
                // the parser's n-ary carrier plus([a, b, …]) (upstream's
                // string::plus(String[*]), batch 5 leg 5) — its run
                List<ValueSpecification> run = af.parameters().size() == 1
                        && af.parameters().get(0) instanceof com.legend.protocol.spec.PureCollection pc
                        ? pc.values() : af.parameters();
                StringBuilder sb = new StringBuilder();
                for (var p : run) {
                    String part = foldRawLiteral(p, lets);
                    if (part == null) {
                        yield null;
                    }
                    sb.append(part);
                }
                yield sb.toString();
            }
            case com.legend.protocol.spec.PureCollection pc -> {
                StringBuilder sb = new StringBuilder();
                for (var e : pc.values()) {
                    String part = foldRawLiteral(e, lets);
                    if (part == null) {
                        yield null;
                    }
                    if (sb.length() > 0) {
                        sb.append('\n');
                    }
                    sb.append(part);
                }
                yield sb.isEmpty() ? null : sb.toString();
            }
            default -> null;
        };
    }
}
