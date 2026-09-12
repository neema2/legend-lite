// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.PlatformTypes;
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
 * The EXECUTION CONTEXT of a query as a VALUE (docs/EXECUTION_CONTEXT_DESIGN
 * _2026_09_06.md): what a {@code from(mapping, runtime)}, an {@code execute},
 * an {@code executionPlan} or an {@code executeLegendQuery} binds the
 * enclosed program to. Bound ONCE by the special form's rule through
 * {@link Reader#read}, read everywhere else as fields — no consumer walks a
 * runtime expression for a shape.
 *
 * @param mapping        the mapping the program compiles against (a reference)
 * @param runtime        a NAMED runtime (a reference), when the runtime is one
 * @param chainMappings  ModelChainConnection mappings — an M2M mapping's ~src
 *                       classes resolve THROUGH these, outermost first
 * @param jsonSources    class FQN → JsonModelConnection data url
 * @param sqlSetups      LocalH2 {@code testDataSetupSqls} blobs, in order —
 *                       the engine runs them when it establishes the
 *                       connection, before the query
 * @param csvSetups      {@code testDataSetupCsv} blocks with the store they seed
 * @param connectionName the connection's plan-text spelling, null when the
 *                       runtime carries no connection instance
 * @param quoteIdentifiers the connection's quoteIdentifiers flag
 * @param timeZone       the connection's timeZone, null when absent
 * @param databaseType   the connection's DatabaseType name, null when the
 *                       runtime carries no connection instance
 * @param connectionInstance the first relational connection INSTANCE under
 *                       the runtime value (the plan surface spells its
 *                       datasource), null when none
 * @param storeFqn       the first ConnectionStore's element store, null when none
 * @param driverTablePk  the engine's {@code addDriverTablePkForProject} execution option
 *                       (RelationalExecutionContext): projections gain their driver
 *                       table's primary-key columns
 * @param postProcessors the connection's SQL post-processors (engine
 *                       sqlQueryPostProcessors / MapperPostProcessor): the table
 *                       renames, CTE extraction, the nonExecutable pass — IR
 *                       passes applied over the frame's lowered plan
 */
public record ExecutionContext(Optional<TypedPackageableRef> mapping,
                               Optional<TypedPackageableRef> runtime,
                               List<String> chainMappings,
                               Map<String, String> jsonSources,
                               List<String> sqlSetups,
                               List<CsvSetup> csvSetups,
                               @com.legend.Nullable String connectionName,
                               boolean quoteIdentifiers,
                               @com.legend.Nullable String timeZone,
                               @com.legend.Nullable String databaseType,
                               @com.legend.Nullable TypedNewInstance connectionInstance,
                               @com.legend.Nullable String storeFqn,
                               boolean driverTablePk,
                               List<com.legend.compiler.element.type.Type.Column> importDataFlowColumns,
                               PostProcessors postProcessors,
                               java.util.Set<Feature> features) {
    /** The connection post-processor facts of one frame: {@code tableReplace}
     * renames (TableNameMapper), whether CTE extraction is installed, whether
     * the nonExecutable pass is installed. */
    public record PostProcessors(Map<String, String> tableReplace, boolean extractCtes,
                                 boolean nonExecutable) {
        public static final PostProcessors NONE = new PostProcessors(Map.of(), false, false);

        public PostProcessors {
            tableReplace = Map.copyOf(tableReplace);
        }

        public PostProcessors withNonExecutable(boolean on) {
            return on == nonExecutable ? this
                    : new PostProcessors(tableReplace, extractCtes, on);
        }
    }

    /** A {@code testDataSetupCsv} block with the DATABASE it seeds (the
     * enclosing connection store's {@code element}; null when no store is
     * in view). */
    public record CsvSetup(String csv, @com.legend.Nullable String dbFqn) {
    }

    public ExecutionContext {
        chainMappings = List.copyOf(chainMappings);
        jsonSources = Map.copyOf(jsonSources);
        sqlSetups = List.copyOf(sqlSetups);
        csvSetups = List.copyOf(csvSetups);
        importDataFlowColumns = List.copyOf(importDataFlowColumns);
    }

    /** Whether an execute call's ExecutionContext argument asks for the
     * engine's {@code importDataFlow} option (a literal flag on a
     * RelationalExecutionContext instance, let-bound or literal). */
    public static boolean importDataFlowRequested(@com.legend.Nullable TypedSpec contextArg,
            java.util.function.UnaryOperator<TypedSpec> bind) {
        return ContextReading.contextFlag("importDataFlow", contextArg, bind);
    }

    /** This context with the importDataFlow RESULT COLUMNS (the union's
     * primary-key threads the executed projection gains; empty = off). */
    public ExecutionContext withImportDataFlowColumns(
            List<com.legend.compiler.element.type.Type.Column> cols) {
        return cols.equals(importDataFlowColumns) ? this
                : new ExecutionContext(mapping, runtime, chainMappings, jsonSources, sqlSetups,
                        csvSetups, connectionName, quoteIdentifiers, timeZone, databaseType,
                        connectionInstance, storeFqn, driverTablePk, cols, postProcessors, features);
    }

    /** No binding at all (a from() the resolver scopes from its outer context). */
    public static final ExecutionContext NONE = of(Optional.empty(), Optional.empty());

    /** References only — nothing to read off a value. */
    public static ExecutionContext of(Optional<TypedPackageableRef> mapping,
            Optional<TypedPackageableRef> runtime) {
        return new ExecutionContext(mapping, runtime, List.of(), Map.of(), List.of(),
                List.of(), null, false, null, null, null, null, false, List.of(), PostProcessors.NONE, java.util.Set.of());
    }

    /** References plus a chain (a wrapper envelope inheriting a resolver context). */
    public static ExecutionContext of(Optional<TypedPackageableRef> mapping,
            Optional<TypedPackageableRef> runtime, List<String> chainMappings,
            Map<String, String> jsonSources) {
        return new ExecutionContext(mapping, runtime, chainMappings, jsonSources,
                List.of(), List.of(), null, false, null, null, null, null, false, List.of(), PostProcessors.NONE, java.util.Set.of());
    }

    /** This context with the given mapping reference. */
    public ExecutionContext withMapping(Optional<TypedPackageableRef> m) {
        return new ExecutionContext(m, runtime, chainMappings, jsonSources, sqlSetups,
                csvSetups, connectionName, quoteIdentifiers, timeZone, databaseType,
                connectionInstance, storeFqn, driverTablePk, importDataFlowColumns, postProcessors, features);
    }

    /** This context with the given runtime reference. */
    public ExecutionContext withRuntime(Optional<TypedPackageableRef> r) {
        return new ExecutionContext(mapping, r, chainMappings, jsonSources, sqlSetups,
                csvSetups, connectionName, quoteIdentifiers, timeZone, databaseType,
                connectionInstance, storeFqn, driverTablePk, importDataFlowColumns, postProcessors, features);
    }

    /** This context with more chain mappings appended (the query-side
     * {@code withChainedMappings} channel joins the runtime's). */
    public ExecutionContext plusChain(List<String> more) {
        if (more.isEmpty()) {
            return this;
        }
        List<String> merged = new ArrayList<>(chainMappings);
        more.stream().filter(m -> !merged.contains(m)).forEach(merged::add);
        return new ExecutionContext(mapping, runtime, merged, jsonSources, sqlSetups,
                csvSetups, connectionName, quoteIdentifiers, timeZone, databaseType,
                connectionInstance, storeFqn, driverTablePk, importDataFlowColumns, postProcessors, features);
    }

    /** This context with the execution OPTIONS read off an execute call's
     * ExecutionContext argument (the engine's exeCtx overload). */
    public ExecutionContext withOptions(@com.legend.Nullable TypedSpec contextArg,
            java.util.function.UnaryOperator<TypedSpec> bind) {
        boolean pk = ContextReading.contextFlag("addDriverTablePkForProject", contextArg, bind);
        ExecutionContext out = pk == driverTablePk ? this
                : new ExecutionContext(mapping, runtime, chainMappings, jsonSources, sqlSetups,
                        csvSetups, connectionName, quoteIdentifiers, timeZone, databaseType,
                        connectionInstance, storeFqn, pk, importDataFlowColumns, postProcessors, features);
        return out.withFeatures(ContextReading.contextFeatures(contextArg, bind));
    }

    /** This context with other post-processor facts (a text surface that
     * runs its query under the producer's own nonExecutable pass). */
    /** The execute-call context argument's feature flags folded in (an
     *  {@code ExecutionOptionContext}'s {@code FeatureFlagOption.flags}). */
    /** The flags {@code withFeatureFlags} calls inside a query body carry
     *  (the engine's second carrier; ContextReading.treeFeatures). */
    public static java.util.Set<Feature> treeFeatures(java.util.List<TypedSpec> body) {
        return ContextReading.treeFeatures(body);
    }

    public ExecutionContext withFeatures(java.util.Set<Feature> more) {
        if (more.isEmpty() || features.containsAll(more)) {
            return this;
        }
        java.util.Set<Feature> all = java.util.EnumSet.noneOf(Feature.class);
        all.addAll(features);
        all.addAll(more);
        return new ExecutionContext(mapping, runtime, chainMappings, jsonSources, sqlSetups,
                csvSetups, connectionName, quoteIdentifiers, timeZone, databaseType,
                connectionInstance, storeFqn, driverTablePk, importDataFlowColumns, postProcessors,
                java.util.Set.copyOf(all));
    }

    public ExecutionContext withPostProcessors(PostProcessors pp) {
        return pp.equals(postProcessors) ? this
                : new ExecutionContext(mapping, runtime, chainMappings, jsonSources, sqlSetups,
                        csvSetups, connectionName, quoteIdentifiers, timeZone, databaseType,
                        connectionInstance, storeFqn, driverTablePk, importDataFlowColumns, pp, features);
    }

    /** This context with an INHERITED chain when it declares none of its own
     * (a nested from() under a plan execution re-evaluated as a value). */
    public ExecutionContext inheritingChain(List<String> outerChain) {
        return chainMappings.isEmpty() && !outerChain.isEmpty()
                ? new ExecutionContext(mapping, runtime, outerChain, jsonSources,
                        sqlSetups, csvSetups, connectionName, quoteIdentifiers,
                        timeZone, databaseType, connectionInstance, storeFqn, driverTablePk, importDataFlowColumns, postProcessors, features)
                : this;
    }

    /** The from() envelopes under a statement, in tree order — the ONE walk
     * that finds execution contexts (the statement channel establishes each
     * context's setups before the statement runs). */
    public static List<TypedFrom> froms(TypedSpec statement) {
        List<TypedFrom> out = new ArrayList<>();
        ArrayDeque<TypedSpec> work = new ArrayDeque<>();
        work.add(statement);
        while (!work.isEmpty()) {
            TypedSpec t = work.poll();
            if (t instanceof TypedFrom fr) {
                out.add(fr);
            }
            work.addAll(t.children());
        }
        return out;
    }

    // =====================================================================
    // The reader — the only code that knows the runtime value's shape
    // =====================================================================

    /** A reader with defaults (the only minting of one outside this class:
     * typed-HIR constructors belong to the compiler layers). */
    public static Reader reader() {
        return new Reader();
    }

    /** Reads a context off a runtime ARGUMENT. The argument is a reference,
     * a constructed instance, a let-bound variable (chased through
     * {@link #bind}) or a helper call (its body read raw through
     * {@link #fnBody} — at typing nothing is inlined yet; an inlined value
     * needs neither). Field names of the engine's runtime classes are
     * spelled here and nowhere else. */
    public record Reader(
            /** RAW body lookup for helper calls met in the value (typing time). */
            Function<String, Optional<List<ValueSpecification>>> fnBody,
            /** Chases a variable met in the value to its binding (a let). */
            UnaryOperator<TypedSpec> bind,
            /** Canonicalizes a class name read from an unchecked helper body. */
            UnaryOperator<String> canon,
            /** The database a COPIED connection seeds (structural resolution). */
            Function<TypedCopyInstance, @com.legend.Nullable String> dbOfCopy) {

        public Reader() {
            this(f -> Optional.empty(), UnaryOperator.identity(), UnaryOperator.identity(),
                    cp -> null);
        }

        public Reader fnBody(Function<String, Optional<List<ValueSpecification>>> f) {
            return new Reader(f, bind, canon, dbOfCopy);
        }

        public Reader bind(UnaryOperator<TypedSpec> b) {
            return new Reader(fnBody, b, canon, dbOfCopy);
        }

        public Reader canon(UnaryOperator<String> c) {
            return new Reader(fnBody, bind, c, dbOfCopy);
        }

        public Reader dbOfCopy(Function<TypedCopyInstance, @com.legend.Nullable String> d) {
            return new Reader(fnBody, bind, canon, d);
        }

        public ExecutionContext read(Optional<TypedPackageableRef> mapping,
                @com.legend.Nullable TypedSpec runtimeArg) {
            return new ContextReading(fnBody, bind, canon, dbOfCopy).read(mapping, runtimeArg);
        }

        /** The connection's DatabaseType name ("H2" when unspelled). */
        public static String databaseType(TypedNewInstance conn) {
            return ContextReading.databaseType(conn);
        }
    }
}
