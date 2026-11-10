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
 * is, and its guard the analogue of a database's permissions): a run reads a source only under a directory it admits;
 * every other location, and every scheme but {@code file:}, is refused by name.
 */
class SourceAdmissionTest {

    @Test
    void aFileUnderAnAdmittedDirectoryIsItsPath(@TempDir Path dir) throws Exception {
        Path file = Files.writeString(dir.resolve("people.json"), "{}");
        String path = SourceAdmission.of(dir).admit(file.toUri().toString());
        assertEquals(file.toRealPath().toString().replace(java.io.File.separatorChar, '/'), path);
    }

    @Test
    void aRunAdmitsNothingUnlessToldAndNeverOutsideWhatItAdmits(@TempDir Path dir) throws Exception {
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
