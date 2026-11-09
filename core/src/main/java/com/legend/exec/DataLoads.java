// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.error.DataError;
import com.legend.executionplan.DataRef;
import com.legend.executionplan.DataStores;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * A FILE'S LOAD, ITS DATA (docs/DATA_IN_THE_PLAN_2026_11_09.md 2.7, 2026-11-09): the reference resolved through the
 * stores the run was given, the bytes refused unless their size and digest are the plan's, then made ready as the
 * plan's delivery says -- the runner's own copy of the verified bytes for a database that reads it, or their cells for
 * one the runner streams them to. The plan runner sends the statement ({@code PlanRunner.seed}: it is the one caller of a
 * session's statements); nothing here touches a session.
 */
final class DataLoads {

    private DataLoads() {
    }

    /** The runner's own copy of verified bytes, deleted when closed: what the database reads is what was verified, at
     *  a short path of the runner's (a store's path may be long: Windows refuses past 260 characters). */
    static final class Staged implements AutoCloseable {
        private final Path path;

        private Staged(Path path) {
            this.path = path;
        }

        /** The copy's path, bound to the statement's one {@code ?}. */
        String path() {
            return path.toString();
        }

        @Override
        public void close() {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                // the copy is the runner's scratch; a delete that fails leaves it to the temporary directory's own
                // cleaning, and the load stands
                path.toFile().deleteOnExit();
            }
        }
    }

    /** {@code bytes} in a new file of the runner's own. */
    static Staged stage(byte[] bytes) {
        try {
            Path staged = Files.createTempFile("legend-load-", ".csv");
            Files.write(staged, bytes);
            return new Staged(staged);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** {@code bytes}' data rows, each {@code columns} wide, split by the platform's CSV rule ({@code base.CsvCells}):
     *  refused by name unless the header names {@code columns} columns, as the database refuses a load whose columns
     *  differ in number. */
    static List<List<String>> cells(DataRef ref, byte[] bytes, int columns) {
        List<List<String>> rows = com.legend.base.CsvCells.rows(new String(bytes, StandardCharsets.UTF_8));
        if (rows.isEmpty() || rows.get(0).size() != columns) {
            throw new DataError("the file '" + ref.name() + "' names " + (rows.isEmpty() ? 0 : rows.get(0).size())
                    + " column(s) in its header, and its load takes " + columns,
                    new IllegalArgumentException(ref.name()));
        }
        return com.legend.base.CsvCells.body(rows, columns);
    }

    /** {@code ref}'s bytes, read through {@code stores} and refused by name unless their size and SHA-256 are the
     *  reference's (the design's P6: the plan loads what it was planned against, on every machine). */
    static byte[] verified(DataRef ref, DataStores stores) {
        byte[] bytes;
        try {
            bytes = stores.read(ref.namespace(), ref.name());
        } catch (IllegalStateException | UncheckedIOException e) {
            throw new DataError("the plan's data '" + ref.name() + "': " + e.getMessage(), e);
        }
        if (bytes.length != ref.size() || !DataStores.sha256(bytes).equals(ref.digest())) {
            throw new DataError("the plan's data '" + ref.name() + "' is not the data it was planned against: "
                    + bytes.length + " bytes of digest " + DataStores.sha256(bytes) + ", where the plan names "
                    + ref.size() + " bytes of digest " + ref.digest(), new IllegalStateException(ref.name()));
        }
        return bytes;
    }

}
