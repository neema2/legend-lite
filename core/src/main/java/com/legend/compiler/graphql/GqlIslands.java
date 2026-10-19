// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.graphql;

import com.legend.compiler.NameResolver;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.graphql.GqlValidator.Problem;
import com.legend.compiler.graphql.GqlValidator.Validation;
import com.legend.compiler.spec.TypeInferenceException;
import com.legend.error.NotImplementedException;
import com.legend.protocol.graphql.GqlSchema;
import com.legend.protocol.graphql.GqlSchema.Schema;
import com.legend.protocol.graphql.GqlToPure;
import com.legend.protocol.graphql.GqlTranslationException;
import com.legend.protocol.spec.GqlIsland;
import com.legend.protocol.spec.ValueSpecification;

import java.util.Map;

/**
 * THE ONE PLACE a {@code #GQL{...}#} island meets the model (docs/GRAPHQL_STUDY_2026_10_18.md §6, §8.3): the
 * island's document is validated against the schema generated for the whole model (cached per model context),
 * translated to the Pure expression its one root field means ({@link GqlToPure}), the names resolved as any query's
 * are; the Typer then types it once, like any query. An invalid island is the compile error its problems spell,
 * every problem listed; a construct the line has not built yet is refused by name. The island's variables are the
 * enclosing Pure scope's ({@code $d} in the document is the function's {@code $d}).
 */
public final class GqlIslands {

    private GqlIslands() {
    }

    /** The schema of every user class of the model, computed once per model. */
    public static Schema schemaOf(ModelContext model) {
        return model.derived(Schema.class, ctx -> SchemaGenerator.generate(ctx, Scope.all(ctx)));
    }

    /** The Pure expression {@code island} means over {@code model}, names resolved: what the Typer types. */
    public static ValueSpecification desugar(GqlIsland island, ModelContext model) {
        return NameResolver.resolveQuery(translate(island, model));
    }

    /** The Pure expression {@code island} means, as the translator spells it (names as a person writes them,
     *  before resolution): what a showcase dump prints. */
    public static ValueSpecification translate(GqlIsland island, ModelContext model) {
        Schema schema = schemaOf(model);
        Validation v = GqlValidator.validate(island.document(), schema, GqlValidator.Limits.DEFAULT, null);
        if (!v.valid()) {
            StringBuilder b = new StringBuilder("#GQL{...}# does not validate against the model's schema:");
            for (Problem p : v.errors()) {
                b.append("\n  ").append(p);
            }
            throw new TypeInferenceException(b.toString());
        }
        GqlToPure.Translation t;
        try {
            t = GqlToPure.translate(island.document(), schema, null, Map.of());
        } catch (GqlTranslationException e) {
            String message = "#GQL{...}#" + (e.path().isEmpty() ? "" : " at " + e.path()) + ": " + e.getMessage();
            throw switch (e.kind()) {
                case INVALID -> new TypeInferenceException(message);
                case UNSUPPORTED -> new NotImplementedException(message);
            };
        }
        if (t.roots().size() != 1) {
            throw new TypeInferenceException("#GQL{...}# selects " + t.roots().size() + " root fields: an island"
                    + " is one value, so it selects one (the endpoint runs several, each under its key)");
        }
        return t.roots().get(0).expression();
    }
}
