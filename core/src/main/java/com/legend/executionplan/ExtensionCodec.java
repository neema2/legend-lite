// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

import com.legend.base.Nullable;
import com.legend.json.Json;

import java.util.Map;

/**
 * The JSON of the edge's {@link Extension} records (docs/PLAN_AS_DATA_2026_11_07.md 2.7): registered by the edge through
 * the {@code ServiceLoader} seam ({@code META-INF/services}), so a plan's JSON, a product plan's or a test plan's, reads
 * its extensions back with no build dependency on the edge ({@code PlanJson#registeredCodecs}; moved down from the test
 * plan's package with {@link Extension}, S1 of the presentation program, 2026-11-19). Where the loader finds none
 * (WebAssembly's, which returns none), the edge hands its codec to the writer itself ({@code PlanJson#write(ExecutionPlan,
 * ExtensionCodec)}). Each record carries its type tag; a tag no codec knows is refused by name.
 */
public interface ExtensionCodec {

    /** {@code e} as a JSON object with its {@code _type} tag, or null where {@code e} is none of this codec's. */
    @Nullable Map<String, Object> write(Extension e);

    /** The record tagged {@code tag}, read from {@code o}, or null where the tag is none of this codec's. */
    @Nullable Extension read(String tag, Json.Obj o);
}
