// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.error.DataError;
import com.legend.executionplan.DataRef;
import com.legend.executionplan.DataStores;

import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A FILE'S LOAD, ITS DATA (docs/DATA_IN_THE_PLAN_2026_11_09.md 2.7, 2026-11-09): the reference resolved through the
 * stores the run was given, the bytes refused unless their size and digest are the plan's, then split into the cells
 * the runner streams to the database by the reference's declared format. The plan runner sends the statement
 * ({@code PlanRunner.seed}: it is the one caller of a session's statements); nothing here touches a session.
 */
final class DataLoads {

    private DataLoads() {
    }

    /** {@code bytes}' data rows, each exactly {@code columns} wide, split by the reference's declared format
     *  ({@code DataRef.Csv}: {@code base.CsvCells}, upstream's rule): the header dropped unread, as upstream drops it;
     *  refused by name unless every data record is {@code columns} wide and well-formed -- never padded, cut or repaired
     *  (AGENTS.md invariant 4; planning refused such a file already, and the digest makes these its bytes). */
    static List<List<String>> cells(DataRef ref, byte[] bytes, int columns, int sizeLimitMegabytes) {
        try {
            // the load's size limit, again on the bytes the run read (D-b, 2026-11-09): the plan's, never exec's
            com.legend.base.CsvCells.requireSize(ref.namespace() == DataRef.Namespace.RESOURCE ? "/" + ref.name()
                    : ref.name(), bytes, sizeLimitMegabytes);
            return switch (ref.format()) {
                case DataRef.Csv csv -> com.legend.base.CsvCells.table(new String(bytes, StandardCharsets.UTF_8),
                        csv.delimiter(), csv.quote(), columns);
            };
        } catch (com.legend.base.CsvCells.TooLarge e) {
            throw new DataError(String.valueOf(e.getMessage()), e);
        } catch (com.legend.base.CsvCells.Malformed e) {
            throw new DataError("the file '" + ref.name() + "': " + e.getMessage(), e);
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
