// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.error.DataError;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A live source's guard (L6, the user's ruling of 2026-11-10: a model's {@code file:} source is a pointer, as a table
 * is): any file by default (A5); a host's opt-in fence the analogue of a database's permissions, a run reading a source
 * only under a directory it admits; every scheme but {@code file:} refused by name.
 */
class SourceAdmissionTest {

    @Test
    void aFileUnderAnAdmittedDirectoryIsItsPath(@TempDir Path dir) throws Exception {
        Path file = Files.writeString(dir.resolve("people.json"), "{}");
        String path = SourceAdmission.of(dir).admit(file.toUri().toString());
        assertEquals(file.toRealPath().toString().replace(java.io.File.separatorChar, '/'), path);
    }

    /** Any file by default, as upstream reads any (the user's ruling on the data delivery design, A5: "any file by
     *  default, with an opt-in fence, default off"); a file that does not resolve is refused by name all the same. */
    @Test
    void anyFileByDefault(@TempDir Path dir) throws Exception {
        Path file = Files.writeString(dir.resolve("anywhere.json"), "{}");
        assertEquals(file.toRealPath().toString().replace(java.io.File.separatorChar, '/'),
                SourceAdmission.any().admit(file.toUri().toString()));
        assertThrows(DataError.class, () -> SourceAdmission.any().admit(dir.resolve("missing.json").toUri().toString()));
    }

    /** A host's opt-in FENCE: none admitted, or a directory, and never outside it. */
    @Test
    void aFenceAdmitsOnlyItsDirectoriesAndNeverOutsideThem(@TempDir Path dir) throws Exception {
        Path inside = Files.createDirectory(dir.resolve("inside"));
        Path outside = Files.writeString(dir.resolve("secret.json"), "{}");
        DataError none = assertThrows(DataError.class,
                () -> SourceAdmission.none().admit(outside.toUri().toString()));
        assertTrue(none.getMessage().contains("admits none"), none.getMessage());
        DataError out = assertThrows(DataError.class,
                () -> SourceAdmission.of(inside).admit(outside.toUri().toString()));
        assertTrue(out.getMessage().contains("not under a directory this run admits"), out.getMessage());
        // a path that climbs out is the file it names, refused as that file
        String climbing = inside.toUri() + "../secret.json";
        assertThrows(DataError.class, () -> SourceAdmission.of(inside).admit(climbing));
        // a link inside that leads out is refused: the real path is compared
        try {
            Path link = Files.createSymbolicLink(inside.resolve("link.json"), outside);
            assertThrows(DataError.class, () -> SourceAdmission.of(inside).admit(link.toUri().toString()));
        } catch (UnsupportedOperationException | java.io.IOException noLinks) {
            // a file system without links (Windows without the privilege): nothing to lead out
        }
    }

    @Test
    void onlyAFileSourceIsRead(@TempDir Path dir) {
        for (String location : new String[] {"http://example.com/people.json", "https://example.com/x.json",
                "s3://bucket/x.json"}) {
            DataError e = assertThrows(DataError.class, () -> SourceAdmission.of(dir).admit(location), location);
            assertTrue(e.getMessage().contains("only a file: source is read"), e.getMessage());
        }
    }
}
