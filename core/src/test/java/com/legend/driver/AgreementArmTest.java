// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.driver;

import com.legend.error.AssertFailed;
import com.legend.judge.AssertListener;
import com.legend.judge.TypedSide;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE AGREEMENT GATE's rule (the plan path program's step 6, docs/PLAN_PATH_PROGRAM_2026_10_17.md §6): every outcome
 * pair of the two arms, by stubs -- the host's verdict of record, the database's agreement checked, a disagreement a
 * failure naming both, a decline or an UNJUDGED outcome told to the listener, a failing statement a failure.
 */
class AgreementArmTest {

    private static final TypedSide HELD = AssertVerdicts.ok();

    private static Supplier<TypedSide> held() {
        return () -> HELD;
    }

    private static Supplier<TypedSide> declined() {
        return () -> null;
    }

    private static Supplier<TypedSide> failed(String message) {
        return () -> {
            throw new AssertFailed(message);
        };
    }

    private static Supplier<TypedSide> unjudged(String reason) {
        return () -> {
            throw AssertFailed.unjudged(reason, "UNJUDGED: " + reason);
        };
    }

    private static Supplier<TypedSide> errored(String message) {
        return () -> {
            throw new com.legend.error.DataError(message, null);
        };
    }

    /** A listener that records what it is told. */
    private static final class Told implements AssertListener {
        final List<String> lines = new ArrayList<>();

        @Override
        public void verdict(String assertName, boolean pass, @com.legend.base.Nullable String detail) {
            lines.add("verdict " + assertName + " " + pass);
        }

        @Override
        public void declined(String assertName, String reason) {
            lines.add("declined " + assertName + ": " + reason);
        }

        @Override
        public void unjudged(String assertName, String reason) {
            lines.add("unjudged " + assertName + ": " + reason);
        }
    }

    @Test
    void bothHold_theHostsVerdictIsTheAnswer() {
        Told told = new Told();
        assertSame(HELD, AgreementArm.agree("assertEquals", told, held(), held()));
        assertTrue(told.lines.isEmpty(), told.lines.toString());
    }

    @Test
    void bothFail_theHostsFailureIsRaised() {
        var e = assertThrows(AssertFailed.class,
                () -> AgreementArm.agree("assertEquals", null, failed("host: 1 != 2"), failed("db: 1 != 2")));
        assertEquals("host: 1 != 2", e.getMessage());
    }

    @Test
    void aDisagreementFailsNamingBothVerdicts() {
        var hostFails = assertThrows(AssertFailed.class,
                () -> AgreementArm.agree("assertEquals", null, failed("host: 1 != 2"), held()));
        assertTrue(hostFails.getMessage().contains("THE JUDGES DISAGREE"), hostFails.getMessage());
        assertTrue(hostFails.getMessage().contains("the host judge fails (host: 1 != 2), the database judge holds"),
                hostFails.getMessage());
        var dbFails = assertThrows(AssertFailed.class,
                () -> AgreementArm.agree("assertEquals", null, held(), failed("db: 1 != 2")));
        assertTrue(dbFails.getMessage().contains("the host judge holds, the database judge fails (db: 1 != 2)"),
                dbFails.getMessage());
    }

    @Test
    void anUnjudgedOrDeclinedDatabaseVerdictIsToldAndTheHostDecides() {
        Told told = new Told();
        assertSame(HELD, AgreementArm.agree("assertEquals", told, held(), unjudged("kind-gate: non-primitive")));
        assertSame(HELD, AgreementArm.agree("assertSize", told, held(), declined()));
        var e = assertThrows(AssertFailed.class,
                () -> AgreementArm.agree("assertEq", told, failed("host: 1 != 2"), unjudged("graph side")));
        assertEquals("host: 1 != 2", e.getMessage());
        assertEquals(List.of("unjudged assertEquals: kind-gate: non-primitive",
                "declined assertSize: database judge: not this shape", "unjudged assertEq: graph side"), told.lines);
    }

    @Test
    void aFailingDatabaseStatementIsAFailureNamingBoth() {
        var e = assertThrows(AssertFailed.class,
                () -> AgreementArm.agree("assertEquals", null, held(), errored("Binder Error: no column x")));
        assertTrue(e.getMessage().contains("THE JUDGES DID NOT AGREE"), e.getMessage());
        assertTrue(e.getMessage().contains("DataError: Binder Error: no column x"), e.getMessage());
        assertTrue(e.getMessage().contains("where the host judge holds"), e.getMessage());
    }

    @Test
    void aShapeTheHostLeavesToTheGenericPathIsNotPutToTheDatabase() {
        boolean[] asked = {false};
        assertNull(AgreementArm.agree("assertEquals", null, declined(), () -> {
            asked[0] = true;
            return HELD;
        }));
        assertTrue(!asked[0], "the database arm was asked for a shape the host declined");
    }

    @Test
    void aHostErrorPropagatesAsTheBodysOwn() {
        assertThrows(com.legend.error.DataError.class,
                () -> AgreementArm.agree("assertEquals", null, errored("host side failed"), held()));
    }
}
