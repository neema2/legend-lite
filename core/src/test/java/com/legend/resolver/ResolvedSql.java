// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.lowering.Lowerer;
import com.legend.lowering.PlatformRegistrations;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Own;

import java.util.List;

/**
 * A query's SQL from the resolver and the Lowerer composed directly (names, G, H, I), rendered by the platform's
 * dialect: the resolver tests' one harness. Each test had its own copy (the dialect program's Phase 6, 2026-10-17).
 */
public final class ResolvedSql {

    private ResolvedSql() {
    }

    /** {@code query} over {@code model}, its runtime the query's own ({@code ->from(...)}). */
    public static String of(String model, String query) {
        return of(model, query, null);
    }

    /** {@code query} over {@code model} as a statement with its raise sites (phase D's Phase 2: a raise the statement
     *  sends is a site's token, read back through them). */
    public static com.legend.sql.dialect.RenderedStatement statement(String model, String query) {
        var ctx = Compiler.compileModel(model);
        SpecCompiler specs = new SpecCompiler(ctx);
        List<TypedSpec> body = specs.typeQueryBody(NameResolver.resolveQuery(Own.spec(query)));
        List<TypedSpec> resolved = new StoreResolver(ctx, specs).resolve(body, null);
        return TestBackends.platform().dialect().renderStatement(
                new Lowerer(PlatformRegistrations.catalogTable()).withStyled(com.legend.lowering.Styled.PRODUCT).lower(resolved));
    }

    /** {@code query} over {@code model}, resolved against {@code runtime} (the driver's runtime: a query with no
     *  {@code ->from(...)}), or the query's own when it is null. */
    public static String of(String model, String query, String runtime) {
        var ctx = Compiler.compileModel(model);
        SpecCompiler specs = new SpecCompiler(ctx);
        List<TypedSpec> body = specs.typeQueryBody(NameResolver.resolveQuery(Own.spec(query)));
        List<TypedSpec> resolved = new StoreResolver(ctx, specs).resolve(body, runtime);
        return TestBackends.platform().dialect().render(
                new Lowerer(PlatformRegistrations.catalogTable()).withStyled(com.legend.lowering.Styled.PRODUCT).lower(resolved));
    }
}
