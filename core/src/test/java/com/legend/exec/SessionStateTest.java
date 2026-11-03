// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** What a session holds ESTABLISHED: a write or a rollback forgets every key's seeds (the MR inventory's latent defect
 *  4, fixed at MRe); and establishing one key forgets the others, since a seed drops and creates the tables it fills and
 *  the record does not know which tables another key filled (the design audit's H1, 2026-11-02: A, then B over the same
 *  table, then A again skipped the third seed and read B's rows). */
class SessionStateTest {

    private record Key(String name) {
    }

    @Test
    void aBThenAAgainSeedsAAgain() {
        SessionState s = new SessionState();
        s.establish(new Key("a"));
        assertTrue(s.established(new Key("a")));
        s.establish(new Key("b"));
        assertTrue(s.established(new Key("b")));
        assertFalse(s.established(new Key("a")), "b's seeds may have written over a's tables: a seeds again");
        s.establish(new Key("a"));
        assertTrue(s.established(new Key("a")));
        assertFalse(s.established(new Key("b")));
    }

    @Test
    void aWriteForgetsEveryKeysSeeds() {
        SessionState s = new SessionState();
        s.establish(new Key("a"));
        s.markWriting();
        assertFalse(s.established(new Key("a")), "a write may change a seeded table: its seeds establish again");
    }

    @Test
    void aRollbackForgetsEveryKeysSeeds() {
        SessionState s = new SessionState();
        s.establish(new Key("a"));
        s.rolledBack();
        assertFalse(s.established(new Key("a")));
    }
}
