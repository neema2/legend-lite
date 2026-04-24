package com.gs.legend.plan;

import com.gs.legend.compiled.CompiledExpression;
import com.gs.legend.compiler.MappingNormalizer;
import com.gs.legend.compiler.MappingResolver;
import com.gs.legend.compiler.StoreResolution;
import com.gs.legend.compiler.TypeChecker;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Type;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.printing.SqlRelationPrinter;
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
 *   TypedSpec (HIR)  --Lowerer-->  SqlRelation (MIR)  --SqlRelationPrinter-->  SQL text
 *                 \--ResultFormatClassifier--> ResultFormat
 * </pre>
 *
 * <p>This class owns pipeline orchestration only. All structural decisions live
 * in the per-stage modules:
 * <ul>
 *   <li>{@code plan.lowering.relation.**} and {@code plan.lowering.scalar.**}
 *       — one file per operator family, sealed dispatch by {@link Lowerer}.</li>
 *   <li>{@code plan.sql} — the immutable {@link SqlRelation} MIR.</li>
 *   <li>{@code plan.printing} — dialect-specific text rendering, fusion &amp;
 *       subquery-wrap decisions.</li>
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

    private final CompiledExpression unit;
    private final SQLDialect dialect;
    private final IdentityHashMap<TypedSpec, StoreResolution> storeResolutions;
    private final Mode mode;

    public PlanGenerator(CompiledExpression unit, SQLDialect dialect) {
        this(unit, dialect, new IdentityHashMap<>(), Mode.SNAPSHOT);
    }

    public PlanGenerator(CompiledExpression unit, SQLDialect dialect,
            IdentityHashMap<TypedSpec, StoreResolution> storeResolutions) {
        this(unit, dialect, storeResolutions, Mode.SNAPSHOT);
    }

    public PlanGenerator(CompiledExpression unit, SQLDialect dialect,
            IdentityHashMap<TypedSpec, StoreResolution> storeResolutions,
            Mode mode) {
        this.unit = unit;
        this.dialect = dialect;
        this.storeResolutions = storeResolutions;
        this.mode = mode;
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

        var storeResolutions = new MappingResolver(
                unit, normalizer.normalizedMapping(), model).resolve();
        return new PlanGenerator(unit, dialect, storeResolutions, mode).generate();
    }

    // ===== Orchestration =====

    /** Three-pass pipeline: lower -> classify -> print. */
    public SingleExecutionPlan generate() {
        var ctx = LoweringContext.root(storeResolutions, mode);
        var rel = Lowerer.lowerRelation(unit.hir(), ctx);
        var format = ResultFormatClassifier.classify(unit.hir());
        // Future (Stage 3): if (format instanceof ResultFormat.Graph) rel = JsonEnvelope.wrap(rel, mode, unit.hir());
        String sql = SqlRelationPrinter.print(rel, dialect);

        Type.Schema schema = unit.hir().schema();
        return new SingleExecutionPlan(
                new SQLExecutionNode(sql, schema, null),
                unit.hir().info(),
                format);
    }

    // ===== Accessors =====

    public CompiledExpression unit() { return unit; }
    public SQLDialect dialect() { return dialect; }
    public IdentityHashMap<TypedSpec, StoreResolution> storeResolutions() { return storeResolutions; }
    public Mode mode() { return mode; }
}
