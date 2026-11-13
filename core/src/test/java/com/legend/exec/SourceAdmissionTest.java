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
        String one = "the source '" + outside.toUri() + "' is not a file this run admits";
        assertEquals(one, none.getMessage());
        DataError out = assertThrows(DataError.class,
                () -> SourceAdmission.of(inside).admit(outside.toUri().toString()));
        // ONE message, naming the source alone: never the fence's folders, and the same whether the path exists (the A
        // delta audit's 4, 2026-11-11)
        assertEquals(one, out.getMessage());
        DataError missing = assertThrows(DataError.class,
                () -> SourceAdmission.of(inside).admit(dir.resolve("no-such.json").toUri().toString()));
        assertEquals("the source '" + dir.resolve("no-such.json").toUri() + "' is not a file this run admits",
                missing.getMessage());
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

    /** A host's configuration builds the fence (the A checkpoint's tenets audit, 11, 2026-11-10): absent, any file (the
     *  default, unchanged); {@code none}, none; a list of directories, a fence of them; a listed path that is no
     *  directory refused by name. */
    @Test
    void aHostsConfigurationBuildsTheFence(@TempDir Path dir) throws Exception {
        assertEquals(SourceAdmission.any(), SourceAdmission.configured(null));
        assertEquals(SourceAdmission.any(), SourceAdmission.configured("  "));
        assertEquals(SourceAdmission.any(), SourceAdmission.configured("any"));
        assertEquals(SourceAdmission.none(), SourceAdmission.configured("none"));
        // a refusal in the host's own words: the server's says how it names its folders, never which they are; a
        // library's names no host (the A delta audit's 13, 2026-11-11)
        String x = Files.writeString(dir.resolve("x.json"), "[]").toUri().toString();
        var none = assertThrows(com.legend.error.DataError.class, () -> SourceAdmission.none().onTheServer().admit(x));
        assertTrue(none.getMessage().contains("none are configured: start it with --sources DIR"), none.getMessage());
        var library = assertThrows(com.legend.error.DataError.class, () -> SourceAdmission.none().admit(x));
        assertEquals("the source '" + x + "' is not a file this run admits", library.getMessage());
        Path sub = Files.createDirectory(dir.resolve("sub"));
        var named = assertThrows(com.legend.error.DataError.class,
                () -> SourceAdmission.of(sub).onTheServer().admit(x));
        assertTrue(named.getMessage().contains("--sources DIR") && !named.getMessage().contains(sub.toString()),
                named.getMessage());
        Path inside = Files.writeString(dir.resolve("rows.json"), "[]");
        SourceAdmission fenced = SourceAdmission.configured(dir.toString());
        // the admitted path as a database reads it on every system: real, forward slashes (Windows CI, PR #95,
        // 2026-11-12: the expectation spelled the machine's separator)
        assertEquals(inside.toRealPath().toString().replace(java.io.File.separatorChar, '/'),
                fenced.admit(inside.toUri().toString()));
        var refused = assertThrows(IllegalArgumentException.class,
                () -> SourceAdmission.configured(dir.resolve("rows.json").toString()));
        assertTrue(refused.getMessage().contains("is no directory"), refused.getMessage());
    }
}
