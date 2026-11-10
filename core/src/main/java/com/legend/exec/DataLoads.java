// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.error.DataError;
import com.legend.executionplan.DataRef;
import com.legend.executionplan.DataStores;

import java.io.UncheckedIOException;

/**
 * A FILE'S LOAD, ITS DATA (docs/DATA_IN_THE_PLAN_2026_11_09.md 2.7, 2026-11-09): the reference resolved through the
 * stores the run was given, the bytes refused unless their size and digest are the plan's, then written to a copy of the
 * runner's own, which the database reads with its own reader (the user's ruling, 2026-11-09: no streamer of lite's own;
 * until then the runner split the text into cells and streamed them). The plan runner sends the statements
 * ({@code PlanRunner.seed}: it is the one caller of a session's statements); nothing here touches a session.
 */
final class DataLoads {

    private DataLoads() {
    }

    /** {@code bytes}' data rows, each exactly {@code columns} wide, split by the reference's declared format
     *  ({@code DataRef.Csv}: {@code base.CsvCells}, upstream's rule, Commons CSV 1.5 {@code EXCEL}): the header dropped
     *  unread, as upstream drops it; refused by name unless every data record is {@code columns} wide and well-formed --
     *  never padded, cut or repaired (AGENTS.md invariant 4; planning refused such a file already, and the digest makes
     *  these its bytes). The streamed delivery's: the user's ruling of 2026-11-10, "Okay I guess that can be used for
     *  databases that have no csv read then". */
    static java.util.List<java.util.List<String>> cells(DataRef ref, byte[] bytes, int columns,
            int sizeLimitMegabytes) {
        try {
            // the load's size limit, again on the bytes the run read (D-b, 2026-11-09): the plan's, never exec's
            com.legend.base.CsvCells.requireSize(ref.namespace() == DataRef.Namespace.RESOURCE ? "/" + ref.name()
                    : ref.name(), bytes, sizeLimitMegabytes);
            return switch (ref.format()) {
                case DataRef.Csv csv -> com.legend.base.CsvCells.table(new String(bytes,
                        java.nio.charset.StandardCharsets.UTF_8), csv.delimiter(), csv.quote(), columns);
            };
        } catch (com.legend.base.CsvCells.TooLarge e) {
            throw new DataError(String.valueOf(e.getMessage()), e);
        } catch (com.legend.base.CsvCells.Malformed e) {
            throw new DataError("the file '" + ref.name() + "': " + e.getMessage(), e);
        }
    }

    /** The runner's own copy of verified bytes, deleted when closed: what the database reads is what was verified, at a
     *  short path of the runner's (a store's path may be long: Windows refuses past 260 characters). */
    static final class Staged implements AutoCloseable {
        private final java.nio.file.Path path;

        private Staged(java.nio.file.Path path) {
            this.path = path;
        }

        /** The copy's path, bound to each load statement's {@code ?}. */
        String path() {
            return path.toString();
        }

        @Override
        public void close() {
            try {
                java.nio.file.Files.deleteIfExists(path);
            } catch (java.io.IOException e) {
                // the copy is the runner's scratch: a delete that fails leaves it to the temporary directory's own
                // cleaning, and the load stands
                path.toFile().deleteOnExit();
            }
        }
    }

    /** {@code ref}'s verified {@code bytes}, within the load's size limit ({@code sizeLimitMegabytes}: the plan's, again
     *  on the bytes the run read, D-b), in a new file of the runner's own. */
    static Staged stage(DataRef ref, byte[] bytes, int sizeLimitMegabytes) {
        try {
            com.legend.base.CsvCells.requireSize(ref.namespace() == DataRef.Namespace.RESOURCE ? "/" + ref.name()
                    : ref.name(), bytes, sizeLimitMegabytes);
        } catch (com.legend.base.CsvCells.TooLarge e) {
            throw new DataError(String.valueOf(e.getMessage()), e);
        }
        try {
            java.nio.file.Path staged = java.nio.file.Files.createTempFile("legend-load-", ".csv");
            java.nio.file.Files.write(staged, bytes);
            return new Staged(staged);
        } catch (java.io.IOException e) {
            throw new UncheckedIOException(e);
        }
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
