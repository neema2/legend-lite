// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.compiler.spec.typed.Feature;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The platform's mirror of the engine's execution feature flags
 * ({@link Feature}) is the engine's enum member for member, in order —
 * {@code meta::pure::executionPlan::features::Feature} in
 * executionPlanFeature.pure (a corpus SHAPE file: Corpus.SHAPE_FILES, so no
 * new hardcoded upstream path). A member the engine adds is a leg the bump
 * opens (its consumer, or loud); a member it removes is a stale mirror.
 */
class FeatureFlagParityTest {

    private static final Pattern ENUM = Pattern.compile(
            "Enum\\s+meta::pure::executionPlan::features::Feature\\s*\\{([^}]*)\\}");

    @Test
    @DisplayName("Feature mirrors the engine's Feature enum, member for member")
    void featureEnumIsUpstreams() throws Exception {
        Path file = com.legend.rcorpus.Corpus.SHAPE_FILES.stream()
                .filter(p -> p.getFileName().toString().equals("executionPlanFeature.pure"))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "executionPlanFeature.pure is not among Corpus.SHAPE_FILES"));
        Matcher m = ENUM.matcher(Files.readString(file, StandardCharsets.UTF_8));
        if (!m.find()) {
            throw new AssertionError("no Feature enum in " + file);
        }
        List<String> upstream = new ArrayList<>();
        for (String v : m.group(1).split(",")) {
            String s = v.strip();
            if (!s.isEmpty()) {
                upstream.add(s);
            }
        }
        assertEquals(upstream, Arrays.stream(Feature.values()).map(Enum::name).toList(),
                "Feature (the platform's mirror) != the engine's enum at the pinned release");
    }
}
