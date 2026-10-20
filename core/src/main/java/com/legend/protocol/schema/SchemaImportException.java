// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.schema;

import java.util.List;

/**
 * A schema that cannot be imported, with EVERY reason: each construct with no Pure form named where the schema has it,
 * so one answer says all that stands in the way (AGENTS.md invariant 4: refused, never dropped).
 */
public final class SchemaImportException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final List<String> reasons;

    public SchemaImportException(List<String> reasons) {
        super(String.join("; ", reasons));
        this.reasons = List.copyOf(reasons);
    }

    /** Each reason, naming where in the schema. */
    public List<String> reasons() {
        return reasons;
    }
}
