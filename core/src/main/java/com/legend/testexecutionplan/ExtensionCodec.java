// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import com.legend.base.Nullable;
import com.legend.json.Json;

import java.util.Map;

/**
 * The JSON of the edge's {@link Extension} records (docs/PLAN_AS_DATA_2026_11_07.md 2.7): registered by the edge through
 * the {@code ServiceLoader} seam ({@code META-INF/services}), so a test plan's JSON reads its extensions back with no
 * build dependency on the edge. Each record carries its type tag; a tag no codec knows is refused by name.
 */
public interface ExtensionCodec {

    /** {@code e} as a JSON object with its {@code _type} tag, or null where {@code e} is none of this codec's. */
    @Nullable Map<String, Object> write(Extension e);

    /** The record tagged {@code tag}, read from {@code o}, or null where the tag is none of this codec's. */
    @Nullable Extension read(String tag, Json.Obj o);
}
