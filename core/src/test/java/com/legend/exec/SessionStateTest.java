// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** What a session holds ESTABLISHED: a write or a rollback forgets every key's seeds (the MR inventory's latent defect
 *  4, fixed at MRe: one flag stood for all keys, and establishing one cleared it for every other). */
class SessionStateTest {

    private record Key(String name) {
    }

    @Test
    void aWriteForgetsEveryKeysSeeds() {
        SessionState s = new SessionState();
        s.establish(new Key("a"));
        s.establish(new Key("b"));
        assertTrue(s.established(new Key("a")) && s.established(new Key("b")));
        s.markWriting();
        s.establish(new Key("a"));
        assertTrue(s.established(new Key("a")));
        assertFalse(s.established(new Key("b")), "b's seeds were written over: they establish again");
    }

    @Test
    void aRollbackForgetsEveryKeysSeeds() {
        SessionState s = new SessionState();
        s.establish(new Key("a"));
        s.rolledBack();
        assertFalse(s.established(new Key("a")));
    }
}
