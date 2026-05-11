package com.legend;

/**
 * Top-level entry point for the Legend Lite compiler pipeline.
 *
 * <p>Drives the eleven steps listed in {@code package-info.java}:
 * lex → parse-element → parse-spec → resolve-names → normalize →
 * compile-element → compile-spec → resolve-mapping → build-sql →
 * render-sql → execute.
 *
 * <p><strong>Status: skeleton.</strong> No step is implemented yet.
 * The signature is provisional — return type and parameters will firm
 * up once {@code plan.ExecutionPlan} and {@code lexer/} land.
 *
 * <p>This class exists today only to anchor the module so the
 * architecture wall test ({@code ArchitectureTest}) has something to
 * verify and so downstream parity harnesses can compile against a stable
 * symbol while steps are filled in vertical-slice by vertical-slice.
 */
public final class Compiler {

    private Compiler() {}

    /**
     * Compile a Pure model + query against a runtime to a SQL execution plan.
     *
     * @param model      Pure model source (classes, mappings, stores, runtimes, ...).
     * @param query      Pure query expression (a {@code ValueSpecification} in legacy terms).
     * @param runtime    FQN of the runtime to compile against.
     * @return SQL execution plan in the runtime's dialect.
     */
    public static String compile(String model, String query, String runtime) {
        throw new UnsupportedOperationException(
            "core/ pipeline not implemented yet. See core/README.md for the migration plan.");
    }
}
