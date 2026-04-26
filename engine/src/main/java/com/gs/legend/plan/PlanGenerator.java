package com.gs.legend.plan;

import com.gs.legend.compiled.CompiledExpression;
import com.gs.legend.compiled.ResolvedExpression;
import com.gs.legend.compiler.MappingNormalizer;
import com.gs.legend.compiler.MappingResolver;
import com.gs.legend.compiler.ResolvedMappings;
import com.gs.legend.compiler.TypeChecker;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Type;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.resultformat.ResultFormatClassifier;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SQLDialect;

import java.util.IdentityHashMap;

/**
 * Entry point for lowering a typed HIR ({@link TypedSpec}) tree to a
 * {@link SingleExecutionPlan} containing dialect-specific SQL.
 *
 * <p><strong>Three-IR pipeline</strong>:
 * <pre>
 *   TypedSpec (HIR)  --Lowerer-->  SqlRelation (MIR)  --SQLDialect.render-->  SQL text
 *                 \--ResultFormatClassifier--> ResultFormat
 * </pre>
 *
 * <p>This class owns pipeline orchestration only. All structural decisions live
 * in the per-stage modules:
 * <ul>
 *   <li>{@code plan.lowering.relation.**} and {@code plan.lowering.scalar.**}
 *       — one file per operator family, sealed dispatch by {@link Lowerer}.</li>
 *   <li>{@code plan.sql} — the immutable {@link SqlRelation} MIR.</li>
 *   <li>{@link com.gs.legend.sqlgen.SQLDialect#render(SqlRelation)} —
 *       dialect-owned codegen (per AGENTS.md invariant 3a, the IR is
 *       data and the dialect is the only thing that emits SQL).</li>
 *   <li>{@code plan.resultformat} — execution-format classification.</li>
 * </ul>
 *
 * <p>Stage 1 skeleton (c0954a): every rule module throws
 * {@link PlanGenNotPortedException}. The pipeline wires up end-to-end via the
 * trivial {@link SqlRelation.SourceExprRel} case for scalar-at-root expressions.
 */
public final class PlanGenerator {

    /**
     * How a Graph-shaped result (graphFetch / bare ClassType) should be emitted.
     *
     * <ul>
     *   <li>{@link #SNAPSHOT}: DB aggregates all rows into a single JSON array via
     *       outer {@code json_group_array(json_object(...))}.</li>
     *   <li>{@link #STREAMING}: DB emits one {@code json_object(...)} per row so the
     *       engine can byte-passthrough each row to the sink as it arrives.</li>
     * </ul>
     *
     * <p>Non-Graph plans are identical in both modes.
     */
    public enum Mode { SNAPSHOT, STREAMING }

    private final ResolvedExpression resolved;
    private final SQLDialect dialect;
    private final Mode mode;

    /** Primary constructor: PlanGen consumes MappingResolver's output. */
    public PlanGenerator(ResolvedExpression resolved, SQLDialect dialect) {
        this(resolved, dialect, Mode.SNAPSHOT);
    }

    public PlanGenerator(ResolvedExpression resolved, SQLDialect dialect, Mode mode) {
        this.resolved = resolved;
        this.dialect = dialect;
        this.mode = mode;
    }

    /**
     * Test-only convenience: plan a {@link CompiledExpression} with empty mappings.
     * Intended for cases that don't exercise mapping-dependent code paths
     * (e.g., scalar-only or TDS-only queries).
     */
    public PlanGenerator(CompiledExpression unit, SQLDialect dialect) {
        this(new ResolvedExpression(unit, ResolvedMappings.ofStoreResolutions(new IdentityHashMap<>())),
                dialect, Mode.SNAPSHOT);
    }

    // ===== Static factories =====

    public static SingleExecutionPlan generate(
            com.gs.legend.model.PureModelBuilder model,
            String query, String runtimeName) {
        return generate(model, query, runtimeName, Mode.SNAPSHOT);
    }

    public static SingleExecutionPlan generate(String pureSource, String query, String runtimeName) {
        var model = new com.gs.legend.model.PureModelBuilder().addSource(pureSource);
        return generate(model, query, runtimeName);
    }

    public static SingleExecutionPlan generate(
            com.gs.legend.model.PureModelBuilder model,
            String query, String runtimeName, Mode mode) {
        SQLDialect dialect = model.resolveDialect(runtimeName);
        var mappingNames = model.resolveMappingNames(runtimeName);
        var normalizer = new MappingNormalizer(model, mappingNames);

        var vs = model.resolveQuery(query);
        var unit = new TypeChecker(normalizer.modelContext()).check(vs);

        var resolved = new MappingResolver(
                unit, normalizer.normalizedMapping(), model).resolve();
        return new PlanGenerator(resolved, dialect, mode).generate();
    }

    // ===== Orchestration =====

    /** Three-pass pipeline: lower -> classify -> print. */
    public SingleExecutionPlan generate() {
        var ctx = LoweringContext.root(resolved.mappings(), mode);
        var hir = resolved.hir();
        var rel = Lowerer.lowerRelation(hir, ctx);
        var format = ResultFormatClassifier.classify(hir);
        String sql = dialect.render(rel);

        Type.Schema schema = hir.schema();
        return new SingleExecutionPlan(
                new SQLExecutionNode(sql, schema, null),
                hir.info(),
                format);
    }

    // ===== Accessors =====

    public ResolvedExpression resolved() { return resolved; }
    public CompiledExpression unit() { return resolved.compiled(); }
    public SQLDialect dialect() { return dialect; }
    public ResolvedMappings mappings() { return resolved.mappings(); }
    public Mode mode() { return mode; }
}
