// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import java.util.ArrayList;
import java.util.List;

/**
 * THE REFEREE'S RAW-SQL LEDGER (Phase 6's B4 (i), 2026-11-17, the orchestrator's GO: moved from
 * {@code sql.dialect.RawSqlBoundary}): the statements a test body's session ran, as the referee replays them -- each
 * executed statement ({@link Raw}), a file's load with its verified bytes ({@link FileRead}), and the recorder that
 * keeps them ({@link Recorder}). The compatibility edge's: the referee is the edge's appeals (G6).
 */
public final class RawLedger {

    private RawLedger() {
    }

    /** One EXECUTED raw statement: the corpus's own H2 text and whether
     * it produced a result set (a query) — the executor's fact, never a
     * reading of the text. {@code read}: where the entry is a FILE'S LOAD
     * on the referee's own database (B2b, 2026-11-11), the file's verified
     * bytes and the statements that read a copy of them, each {@code ?} the
     * copy's path, as the plan's load reads its own copy ({@code sql} then
     * their text, joined, for the ledger's reader); none for a statement. */
    public record Raw(String sql, boolean query, @com.legend.base.Nullable FileRead read) {
        public Raw {
            if (read != null && query) {
                throw new IllegalArgumentException("a file's load answers no rows: " + sql);
            }
        }

        public Raw(String sql, boolean query) {
            this(sql, query, null);
        }
    }

    /**
     * A FILE'S LOAD AS THE REFEREE REPLAYS IT (B2b, 2026-11-11, docs/DATA_DELIVERY_DESIGN_2026_11_10.md §6): the bytes of
     * the file the plan's load read, verified against its reference by the edge, and the referee's own statements that
     * read a copy of them ({@code statements}, each {@code ?} the copy's path; the referee's H2 {@code CSVREAD}), then
     * {@code after}, which undo what they set, run on every exit. The referee writes the copy and deletes it: no Java
     * reads the file's cells (until B2b the edge split them, {@code base.CsvCells}, census M15).
     */
    public record FileRead(byte[] bytes, List<String> statements, List<String> after) {
        public FileRead {
            bytes = bytes.clone();
            statements = List.copyOf(statements);
            after = List.copyOf(after);
            if (statements.isEmpty()) {
                throw new IllegalArgumentException("a file's load of no statement");
            }
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }

        /** The statements, then the undo, as one text (the ledger's reader's). */
        public String text() {
            List<String> all = new ArrayList<>(statements);
            all.addAll(after);
            return String.join(";\n", all);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof FileRead f && java.util.Arrays.equals(bytes, f.bytes)
                    && statements.equals(f.statements) && after.equals(f.after);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(java.util.Arrays.hashCode(bytes), statements, after);
        }

        @Override
        public String toString() {
            return "FileRead[" + bytes.length + " bytes, " + text() + "]";
        }
    }

    /** THE RAW-SQL LEDGER (Phase 2b, batch 137): an OBJECT the caller
     * creates per execution and hands through {@code ExecuteOptions}; the
     * executor appends every raw statement that EXECUTED (recorded after
     * the fact, with its kind — a failed statement is never recorded, so
     * the ledger mirrors executed reality by construction); the referee
     * reads it from the caller that owns it. Until batch 137 this was a
     * thread-local the harness installed and the referee read back — the
     * last of the ledger slots on the thread. (The metadata-only side
     * channel that rode beside it had no reader and is gone.) */
    public static final class Recorder {
        private final List<Raw> entries = new java.util.ArrayList<>();

        public Recorder() {
        }

        /** A ledger that starts with a PREFIX (the session's seeds so far). */
        public Recorder(List<Raw> prefix) {
            entries.addAll(prefix);
        }

        public void recordExecuted(String sql, boolean query) {
            entries.add(new Raw(sql, query));
        }

        /** A file's load ran on the plan's target: the referee replays {@code read} -- its own statements over a
         *  copy of the verified bytes -- where it replays a seed (B2b). */
        public void recordRead(FileRead read) {
            entries.add(new Raw(read.text(), false, read));
        }

        public List<Raw> entries() {
            return java.util.Collections.unmodifiableList(entries);
        }

        /** The NON-QUERY statements only — the referee's seeds: a query
         * never seeds, and this subsequence is append-only across a
         * session's tests, so the mirror's cursor indexes it stably. */
        public List<Raw> seeds() {
            return entries.stream().filter(r -> !r.query()).toList();
        }

        /** Ledger position for a transactional attempt: entries and seeds. */
        public record Mark(int entries, int seeds) {
        }

        public Mark mark() {
            return new Mark(entries.size(), seeds().size());
        }

        /** Statements recorded during a rolled-back attempt leave with it. */
        public void truncateTo(Mark m) {
            while (entries.size() > m.entries()) {
                entries.remove(entries.size() - 1);
            }
        }
    }
}
