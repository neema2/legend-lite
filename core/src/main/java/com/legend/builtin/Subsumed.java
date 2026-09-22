package com.legend.builtin;

import java.util.Map;
import java.util.Optional;

/**
 * SUBSUMED ENGINE PROGRAMS — the platform's third kind of "not lowered"
 * claim, beside a lowering and a wall (upstream boundary program, USER
 * 2026-09-10).
 *
 * <p>A member is an upstream ENGINE PROGRAM (bodied Pure, present in every
 * corpus graph because the corpus compiles its module) that belongs to a
 * subsystem this platform replaces wholesale, so the program is moot here:
 * its body is never spliced, its value is never consumed, and a call to it
 * stays a typed opaque value typed by upstream's OWN declaration (the
 * platform declares no signature of its own — a declaration nothing
 * implements does not belong in {@code Pure.java}).
 *
 * <p>The contract, each part pinned by {@code SubsumedRegistryTest}:
 * <ol>
 *   <li>NOT DECLARED HERE: no {@code Pure.java} overload, no {@code NativeFn}
 *       family member, no platform-owned name — the corpus definition is the
 *       typing source;</li>
 *   <li>DEAD VALUE: no main-tree code names the FQN outside this file — nothing
 *       reads the value, so no test can pass by leaning on it;</li>
 *   <li>CITED: the upstream file and lines of the body being subsumed, so a
 *       bump can diff them.</li>
 * </ol>
 * The count is a shrink-only ratchet: a new member is a decision with a
 * written reason, never a convenience.
 *
 * <p>This is NOT a wall. A wall ({@code Pure.WALLED_NATIVES}) is an upstream
 * NATIVE the platform cannot implement — a call reaching lowering must fail
 * loudly. A subsumed program is expected inside passing tests; what must never
 * happen is its value being needed.
 */
public enum Subsumed {

    /** The engine's Pure SQL PRINTER config: the fourth overload builds a
     *  {@code DbConfig} whose {@code dbExtension} comes from
     *  {@code loadDbExtension}, the reflective extension registry. This
     *  platform's compiler IS the printer; no platform code consumes a
     *  DbConfig value. Until batch 4b this was seven hand-typed native
     *  overloads (three of them Any-widened shapes upstream never declared)
     *  plus a kernel tie-break so the copy beat the real definition — a stub
     *  silencing a dead call in 51 corpus tests. */
    CREATE_DB_CONFIG("meta::relational::functions::sqlQueryToString::createDbConfig",
            "legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/"
                    + "legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/"
                    + "src/main/resources/core_relational/relational/sqlQueryToString/dbExtension.pure",
            241, 262,
            "the engine's Pure SQL printer config; its body reads loadDbExtension (the reflective"
                    + " extension registry) and this platform's compiler is the printer — no code"
                    + " here consumes a DbConfig value"),


    /** The engine's EXTENSION REGISTRY: {@code relationalExtension()} builds
     *  an {@code ^Extension(...)} of routing, plan-generation and SQL-printing
     *  hooks (lambdas over the engine's own metamodel). This platform is its
     *  own extensions — no arm reads an {@code Extension} value; the corpus
     *  passes the call as the {@code extensions} argument of execute /
     *  toSQLString / executionPlan in 3,072 places, and those arms never
     *  look at it. Until now a hand-typed native won the overload tie-break
     *  over the corpus's own definition ("signature-broken", batch 147 — the
     *  file parses and compiles today). */
    RELATIONAL_EXTENSIONS("meta::relational::extension::relationalExtensions",
            "legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/"
                    + "legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/"
                    + "src/main/resources/core_relational/relational/extensions/extension.pure",
            62, 65,
            "the engine's extension registry (routing, plan-generation and SQL-printing hooks);"
                    + " this platform is its own extensions and no arm reads an Extension value");

    private final String fqn;
    private final String engineFile;
    private final int fromLine;
    private final int toLine;
    private final String reason;

    Subsumed(String fqn, String engineFile, int fromLine, int toLine, String reason) {
        this.fqn = fqn;
        this.engineFile = engineFile;
        this.fromLine = fromLine;
        this.toLine = toLine;
        this.reason = reason;
    }

    /** The program's FQN. */
    public String fqn() {
        return fqn;
    }

    /** The upstream file declaring the body, relative to the legend-engine root. */
    public String engineFile() {
        return engineFile;
    }

    /** First line of the upstream declarations. */
    public int fromLine() {
        return fromLine;
    }

    /** Last line of the upstream declarations. */
    public int toLine() {
        return toLine;
    }

    /** Why the program is moot on this platform. */
    public String reason() {
        return reason;
    }

    private static final Map<String, Subsumed> BY_FQN;

    static {
        Map<String, Subsumed> m = new java.util.HashMap<>();
        for (Subsumed s : values()) {
            m.put(s.fqn, s);
        }
        BY_FQN = Map.copyOf(m);
    }

    /** The subsumed program a callee FQN names — empty for every other callee. */
    public static Optional<Subsumed> of(@com.legend.base.Nullable String calleeFqn) {
        return calleeFqn == null ? Optional.empty() : Optional.ofNullable(BY_FQN.get(calleeFqn));
    }
}
