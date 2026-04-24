package com.gs.legend.plan;

import com.gs.legend.compiler.typed.TypedSpec;

/**
 * Thrown by {@link PlanGenerator} when a typed-HIR node variant has not yet
 * been ported to the new three-IR pipeline (TypedSpec -> SqlRelation -> SQL).
 *
 * <p>Used as a structured, greppable signal during the staged port work
 * tracked in {@code plangen-typed-port-c0954a.md}. The message prefix
 * {@code [plangen-c0954a]} allows quickly counting / categorising unported
 * variants from Surefire output:
 *
 * <pre>
 *   grep "PlanGenNotPortedException" engine/target/surefire-reports/*.txt | wc -l
 *   grep -oE "Not yet ported: (Typed[A-Za-z]+)" engine/target/surefire-reports/*.txt | sort | uniq -c
 * </pre>
 *
 * <p>All uses of this exception — including this class itself — are deleted
 * in Stage 6 once every TypedSpec variant has been ported.
 */
public final class PlanGenNotPortedException extends UnsupportedOperationException {

    public final String nodeKind;
    public final String stage;
    public final String hint;

    public PlanGenNotPortedException(TypedSpec node, String stage, String hint) {
        super(String.format(
                "[plangen-c0954a] Not yet ported: %s (%s). %s. See plangen-typed-port-c0954a.md",
                node == null ? "<null>" : node.getClass().getSimpleName(), stage, hint));
        this.nodeKind = node == null ? "<null>" : node.getClass().getSimpleName();
        this.stage = stage;
        this.hint = hint;
    }

    public static PlanGenNotPortedException stage0(TypedSpec node) {
        return new PlanGenNotPortedException(node, "stage-0-stub",
                "PlanGenerator is stubbed for compile-green; lowering not yet implemented");
    }

    public static PlanGenNotPortedException stage1(TypedSpec node) {
        return new PlanGenNotPortedException(node, "stage-1-skeleton",
                "Lowering rule module exists but rule body not yet implemented");
    }

    public static PlanGenNotPortedException stage2(TypedSpec node) {
        return new PlanGenNotPortedException(node, "stage-2-leaf",
                "Leaf/source node lowering not yet ported");
    }

    public static PlanGenNotPortedException stage3(TypedSpec node) {
        return new PlanGenNotPortedException(node, "stage-3-relation",
                "Relation / core-scalar operator lowering not yet ported");
    }

    /** Stage 3 variant with a case-specific hint (e.g., {@code "associationPath"}). */
    public static PlanGenNotPortedException stage3(TypedSpec node, String hint) {
        return new PlanGenNotPortedException(node, "stage-3-relation",
                "Variant sub-case not yet ported: " + hint);
    }

    public static PlanGenNotPortedException stage4(TypedSpec node) {
        return new PlanGenNotPortedException(node, "stage-4-scalar",
                "Scalar operator lowering not yet ported");
    }

    public static PlanGenNotPortedException stage5(TypedSpec node) {
        return new PlanGenNotPortedException(node, "stage-5-misc",
                "Control-flow / IO / misc lowering not yet ported");
    }
}
