// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * WALLED BODIES — Pure bodies the platform carries verbatim (the prelude
 * module's derived properties and constraints, the engine's SQL
 * post-processing entry points) and REFUSES to compile, each with the
 * reason it is the engine's implementation of a concern the platform
 * serves itself or does not serve (SYSTEM_PRELUDE_DESIGN §3 last row;
 * COMPILE_EVERYTHING_HOMEWORK §10.3 ruling 2, USER 2026-09-09). ONE list,
 * consulted by {@link SpecCompiler#compile} (a walled body is never typed:
 * the census counts it WALLED, not failing) and by {@link UserCallInliner}
 * (a program reaching one fails at once, naming the wall). Shrink-only: an
 * entry leaves with a witness that needs the body, and the generator's
 * call-following slice then brings its callees in.
 */
public final class WalledBodies {

    private WalledBodies() {
    }

    private static final String PRINTER =
            "the engine's SQL printer — the platform's compiler is the implementation"
            + " (the post-processor design session decides which passes the platform implements)";
    private static final String SCHEMA =
            "the engine's plan-time schema inference for TDS operations — the platform's typer infers schemas";

    static final Map<String, String> REASONS = build();

    private static Map<String, String> build() {
        Map<String, String> REASONS = new LinkedHashMap<>();
        // the SQL post-processing machinery (USER 2026-09-08; UserCallInliner's original five)
        REASONS.put("meta::relational::functions::sqlQueryToString::sqlQueryToString", PRINTER);
        REASONS.put("meta::relational::runtime::PostProcessor$prop$planPostProcessorId", PRINTER);
        REASONS.put("meta::relational::runtime::PostProcessor$prop$executionPostProcessorId", PRINTER);
        REASONS.put("meta::relational::runtime::PostProcessors$prop$_sqlQueryPostProcessorId", PRINTER);
        REASONS.put("meta::relational::runtime::PostProcessors$prop$sqlQueryPostProcessorId", PRINTER);
        // the printer's bodies on the prelude's vocabulary classes (the census's B3 rows)
        for (String p : new String[] {"dataTypeToSqlText", "dynaFuncDispatch", "joinProcessor",
                "lateralJoinProcessor", "literalProcessor",
                // 4.145.0 (batch 8): the printer grew two properties — CTE
                // extraction on the select processor, WITHIN GROUP dispatch
                "selectSQLQueryProcessor", "withinGroupProcessor"}) {
            REASONS.put("meta::relational::functions::sqlQueryToString::DbConfig$prop$" + p, PRINTER);
        }
        // 4.145.0: null-ordering rendering — the printer's, per dialect
        REASONS.put("meta::relational::functions::sqlQueryToString::NullOrderingSupport$prop$processSortItem",
                PRINTER);
        REASONS.put("meta::relational::functions::sqlQueryToString::DynaFunctionToSql$prop$toSql", PRINTER);
        // (SQLResult.toSQLString left the wall 2026-09-11, batch 5 leg 5c: it is the
        // qualified property the toSQLString ROUTINE implements — NativeFn.JavaRoutine
        // .implementedDerived — its lifted declaration types the call, never its body)
        // SchemaState: plan-time schema inference (the census's B2/B2b/B4 rows)
        for (String p : new String[] {"columnValueDifference", "extend", "groupBy", "join", "olap",
                "rename", "restrict"}) {
            REASONS.put("meta::pure::tds::schema::SchemaState$prop$" + p, SCHEMA);
        }
        REASONS.put("meta::pure::extension::Extension$prop$fetchSerializerExtension",
                "the engine's serializer-extension registry lookup — the platform has its own extensions");
        // the descriptors' constraints call checkSuperType, whose reflection
        // helper getAllClassGeneralisations lives in the engine's
        // corefunctions/metaExtension.pure — a stdlib-extension file the
        // 2026-08-28 ruling refuses as runtime; walled until that ruling
        // is revisited (COMPILE_EVERYTHING_HOMEWORK §12 group D)
        REASONS.put("meta::external::format::shared::ExternalFormatFromPureDescriptor$constraint$configurationType",
                "external-format binding validation — its reflection helper is in the refused stdlib-extension files");
        REASONS.put("meta::external::format::shared::ExternalFormatToPureDescriptor$constraint$configurationType",
                "external-format binding validation — its reflection helper is in the refused stdlib-extension files");
        // legend-pure new.pure (parser leg, batch 174): instantiation from a Class VALUE —
        // new($l1->class(), '') — is reflection; the platform's new is the ^X(...) form
        REASONS.put("meta::pure::functions::lang::tests::new::testNewGenericFunc",
                "REFLECTION: instantiation from a Class value (new(class, id)) — not modeled");
        return Map.copyOf(REASONS);   // immutable (ArchitectureTest invariant 3)
    }

    /** The wall reason for a body FQN (a lifted derived property / constraint or a function), or null. */
    public static @com.legend.Nullable String reason(String fqn) {
        return REASONS.get(fqn);
    }

    public static int count() {
        return REASONS.size();
    }
}
