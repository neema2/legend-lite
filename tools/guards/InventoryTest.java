package com.legend.tools.guards;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.Runfile;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * G0 (Bazel workplan P6-00): the repository inventory is complete and every file in it is a declared input. The
 * inventory (@repo_inventory//:files.txt) lists every file outside .bazelignore's trees; //tools/guards:repository_files
 * collects every package's all_files (guards_package); every inventoried file must be among them, so a content guard
 * that reads repository_files sees the whole repository. (A package without guards_package() already fails analysis
 * of repository_files.)
 */
class InventoryTest {

    @Test
    void everyInventoriedFileIsInSomePackagesAllFiles() throws IOException {
        List<String> files = Files.readAllLines(Runfile.property("inventory.files")).stream()
                .filter(l -> !l.isBlank()).toList();
        assertTrue(files.size() > 1000, "the inventory lists " + files.size() + " files: it is not looking");
        List<String> missing = files.stream()
                .filter(f -> {
                    Path p = Runfile.of("_main/" + f);
                    return p == null || !Files.exists(p);
                })
                .toList();
        assertEquals(List.of(), missing, "inventoried files no package's all_files declares (guards_package's glob)");
    }
}
