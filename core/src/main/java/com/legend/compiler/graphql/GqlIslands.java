// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.graphql;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.graphql.GqlSchema.Schema;
import com.legend.compiler.graphql.GqlValidator.Problem;
import com.legend.compiler.graphql.GqlValidator.Validation;
import com.legend.protocol.spec.GqlIsland;

/**
 * THE ONE PLACE a {@code #GQL{...}#} island meets the model (docs/GRAPHQL_STUDY_2026_10_18.md §6, §8.3): the
 * island's document is validated against the schema generated for the whole model (cached per model context).
 * This landing (PR1) validates and stops: an invalid island is the compile error its problems spell; a valid one
 * is refused by name until PR2 translates it. The Typer's island arm calls here.
 */
public final class GqlIslands {

    private GqlIslands() {
    }

    /** The schema of every user class of the model, computed once per model. */
    public static Schema schemaOf(ModelContext model) {
        return model.derived(Schema.class, ctx -> SchemaGenerator.generate(ctx, Scope.all(ctx)));
    }

    /** Validates the island; the exception to throw for it (never returns normally in this landing). */
    public static RuntimeException refusal(GqlIsland island, ModelContext model) {
        Validation v = GqlValidator.validate(island.document(), schemaOf(model), GqlValidator.Limits.DEFAULT, null);
        if (!v.valid()) {
            StringBuilder b = new StringBuilder("#GQL{...}# does not validate against the model's schema:");
            for (Problem p : v.errors()) {
                b.append("\n  ").append(p);
            }
            return new com.legend.compiler.spec.TypeInferenceException(b.toString());
        }
        return new com.legend.error.NotImplementedException("#GQL{...}# validates against the model's schema, its"
                + " result shaped " + v.shape() + "; its execution lands with the GraphQL line's PR2"
                + " (docs/GRAPHQL_STUDY_2026_10_18.md §10)");
    }
}
