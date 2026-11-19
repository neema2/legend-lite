// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlQuery;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

/**
 * PROBE, NEVER FOR MAIN (census B2's CP-4 measurement, 2026-11-19, branch claude/census-b2-sqltext): legend-engine's SQL
 * text replaced by lite's own. Every engine-text and plan-text printer ({@link PureText#engineText},
 * {@link PureText#enginePlanText}) answers what the product's H2 dialect writes for the same MIR, so a corpus test that
 * passes only because lite reproduces legend-engine's text moves to fail. The judging is left as it is: a text verdict
 * that fails appeals to rows, as on main. The engine-only parts of the printer interface answer lite's plainest form:
 * no template parameters, an alias as given, the select list cut from lite's text, no FreeMarker collection splice
 * (refused, marked {@link #MARK}).
 */
final class LiteTextProbe {

    static final String MARK = "SQLTEXT-PROBE";

    private LiteTextProbe() {
    }

    static EngineTextDialect of(String where) {
        com.legend.sql.dialect.H2 h2 = new com.legend.sql.dialect.H2();
        return (EngineTextDialect) Proxy.newProxyInstance(EngineTextDialect.class.getClassLoader(),
                new Class<?>[] {EngineTextDialect.class}, (proxy, m, args) -> {
                    if (m.getDeclaringClass() == EngineTextDialect.class) {
                        switch (m.getName()) {
                            case "params":
                                return null;
                            case "collectionSplice":
                                throw new UnsupportedOperationException(MARK + ": lite has no FreeMarker collection"
                                        + " splice (" + where + ")");
                            case "renderedAlias":
                                return args[0];
                            case "selectList":
                                return selectList(h2.render((SqlQuery) args[0]));
                            case "renderAsAliased":
                                return h2.render((SqlQuery) args[0]);
                            case "inTempTableSelect":
                                return "select * from " + args[0];
                            default:
                                throw new IllegalStateException(MARK + ": an engine-text method the probe does not"
                                        + " answer: " + m);
                        }
                    }
                    try {
                        return m.invoke(h2, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    /** The select list of lite's text: between its first "select " and the first " from " after it; null if none. */
    private static @com.legend.base.Nullable String selectList(String sql) {
        String low = sql.toLowerCase(java.util.Locale.ROOT);
        int s = low.indexOf("select ");
        if (s < 0) {
            return null;
        }
        int f = low.indexOf(" from ", s);
        return f < 0 ? sql.substring(s + 7) : sql.substring(s + 7, f);
    }
}
