// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql.hardening;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

/** DEVELOPER EXPLORER (runs only with {@code -Dgql.probe=true}): each model of /tmp/models.txt (blocks split by a
 *  line {@code ----}) compiled, its error or OK written to /tmp/models-out.md. */
@org.junit.jupiter.api.condition.EnabledIfSystemProperty(named = "gql.probe", matches = "true")
class ModelProbeTest {
    @Test
    void probe() throws Exception {
        StringBuilder out = new StringBuilder();
        for (String m : Files.readString(Path.of("/tmp/models.txt")).split("\n----\n")) {
            out.append("### ").append(m.replace('\n', ' ')).append("\n");
            try {
                com.legend.Compiler.compileModel(m);
                out.append("OK\n");
            } catch (RuntimeException e) {
                out.append(e).append("\n");
            }
        }
        Files.writeString(Path.of("/tmp/models-out.md"), out.toString());
    }
}
