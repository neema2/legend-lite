// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package planoracle;

import java.nio.charset.StandardCharsets;

/**
 * One answer of legend-engine's, as its HTTP route sends it: the status and the body's bytes. Test tooling (slice R of
 * the presentation program, 2026-11-18): the oracle lite's export and import are measured against, never a product
 * component (AGENTS.md, the reference-checkout tenet).
 */
public record EngineAnswer(int status, byte[] body)
{
    public static EngineAnswer of(int status, String body)
    {
        return new EngineAnswer(status, body.getBytes(StandardCharsets.UTF_8));
    }

    public String text()
    {
        return new String(this.body, StandardCharsets.UTF_8);
    }
}
