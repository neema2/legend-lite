// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import com.legend.testing.Lean;
import com.legend.testing.Program;

import com.legend.database.Databases;
import com.legend.testbackend.TestBackends;
import com.legend.test.StorelessRuntime;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE PCT.function SUPPRESSION RULE (FunctionCompiler.addModelOverloads,
 * 2026-08-19 standard-suite landing): the engine's {@code <<PCT.function>>}
 * stereotype marks "the platform function under conformance" — where the
 * registry owns a NATIVE at that FQN, the native IS the definition and
 * the reference pure body never joins the overload set (tenet #2: the
 * reference body is the SPEC, not our implementation). A PCT.function
 * with NO registered native keeps its body — the model IS the
 * implementation. Witnessed by chB-std testOr (the fold body's SQL was
 * wrong); pinned here so the rule can never silently invert.
 */
class PctFunctionSuppressionTest {

    private static Connection conn;

    @BeforeAll
    static void open() throws Exception {
        conn = TestBackends.platform().open();
    }

    @AfterAll
    static void close() throws Exception {
        conn.close();
    }

    @Test
    @DisplayName("native-owned FQN: the PCT.function body is suppressed — the native wins")
    void nativeOwnedSuppresses() throws Exception {
        // a DELIBERATELY WRONG reference body: if it joined the overload
        // set and won, or([true]) would be false
        String model = "function <<meta::pure::test::pct::PCT.function>>"
                + " meta::pure::functions::collection::or(vals:Boolean[*]):Boolean[1]"
                + " { $vals->fold({i,a|false}, false) }";
        Lean.Result r = Program.last(StorelessRuntime.with(model, Databases.PLATFORM),
                "{|or([true, false])}", StorelessRuntime.RUNTIME, conn);
        assertEquals(Boolean.TRUE, r.one(),
                "the native must own the platform FQN — the reference"
                + " body is the spec, never the implementation");
    }

    @Test
    @DisplayName("no native at the FQN: the PCT.function body IS the implementation")
    void unownedKeepsBody() throws Exception {
        String model = "function <<meta::pure::test::pct::PCT.function>>"
                + " my::pkg::triple(x:Integer[1]):Integer[1] { $x * 3 }";
        Lean.Result r = Program.last(StorelessRuntime.with(model, Databases.PLATFORM),
                "{|my::pkg::triple(2)}", StorelessRuntime.RUNTIME, conn);
        assertEquals(6L, ((Number) r.one())
                .longValue());
    }
}
