// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import java.util.List;
import java.util.Map;

/**
 * The enumerations a side's rows decode, read from the model at planning (the plan-as-data design's slice 4, hole 1,
 * 2026-11-08): their names, each one's decode under the mapping (a source value to the value's name), and the root
 * class's properties of an enumeration type. Product data; the edge's appeal carries it (census C2, 2026-11-17).
 */
public record EnumFacts(List<String> enums, Map<String, Map<String, String>> decodes, Map<String, String> properties) {
}
