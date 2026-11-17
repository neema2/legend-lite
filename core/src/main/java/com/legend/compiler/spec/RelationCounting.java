// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * HOW A TEST BODY COUNTS A RELATION passed to {@code assertSize}, {@code assertEmpty} or {@code assertNotEmpty}
 * (register S137, the user's ruling of 2026-11-05; census C1, the orchestrator's ruling of 2026-11-17): a two-form kind
 * ({@code PlanStyle.twoForm}, {@code testplan.PlanStyleFacets.TWO_FORMS}) -- the product's form refuses the three over a
 * relation, naming what to say ({@link RelationCountAsserts#REFUSED}); the edge's counts the relation as ONE value, as
 * upstream does. Signatures only.
 */
public interface RelationCounting {

    /** {@code body} as this form plans it: the body itself, or rewritten; a refused body raises by name. */
    List<TypedSpec> counted(List<TypedSpec> body);
}
