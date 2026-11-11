// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.testexecutionplan.Extension;

/**
 * A load step that makes a live source's session table -- a {@code data:} payload's, a JSON connection's frame -- planned
 * by the compatibility edge at the step (B2a, 2026-11-11, docs/DATA_DELIVERY_DESIGN_2026_11_10.md §5): the edge records
 * none of its statements, since the referee replays legend-engine's own text, which reads no table of lite's (measured,
 * docs/data-delivery-homework/H2_DATA_TABLE_MEASURED_2026_11_11.md 8). A step that ran with this record is told to the
 * edge and recorded as nothing, by name.
 */
public record LiveSourceLoad() implements Extension {
}
