// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.model;

import com.legend.protocol.Protocol;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code PRelOp} &rarr; {@link RelationalOperation}: the operation half of
 * finishing the protocol-first migration (PARSER_COMPLETENESS_PLAN.md §1).
 *
 * <p>The two trees are shaped for different jobs. The PROTOCOL tree is the
 * engine's wire shape and is deliberately flat — every operator is a
 * {@code dynaFunc} carrying a name, because that is what serialises. The MODEL
 * tree is what the compiler reasons over, so comparisons, boolean connectives
 * and null tests are distinct typed variants. Transforming one into the other
 * therefore means RE-DERIVING semantics from a closed vocabulary of function
 * names, which is exactly where a quiet SQL-semantics bug could hide.
 *
 * <p>That vocabulary is small and fixed — the twelve names below are every
 * operator {@code DatabaseProtocolParser} and {@code OperatorParts} can mint —
 * and anything outside it stays a {@link RelationalOperation.FunctionCall},
 * which is what the legacy parser did with an unrecognised function too.
 *
 * <p>Correctness is not argued, it is DIFFERENTIALLY TESTED: {@code
 * MigrationEquivalenceTest} parses every corpus database both ways and requires
 * the resulting models to be equal. The legacy parser is not deleted until that
 * is green over the whole corpus.
 */
public final class RelOpFromProtocol {

    private RelOpFromProtocol() {
    }

    public static RelationalOperation op(Protocol.PRelOp p) {
        return op(p, null);
    }

    /**
     * @param enclosingDb the database whose body this operation appears in.
     *     The wire ALWAYS resolves a table's database because the engine's
     *     JSON must; the model records it as WRITTEN, so a reference to the
     *     enclosing database is null there. Passing it here reproduces that
     *     exactly instead of leaving every column ref self-qualified.
     */
    public static RelationalOperation op(Protocol.PRelOp p,
            @com.legend.Nullable String enclosingDb) {
        return switch (p) {
            case Protocol.PColumnRef c -> columnRef(c, enclosingDb);
            case Protocol.PRelLiteral l -> new RelationalOperation.Literal(l.value());
            case Protocol.PRelLiteralList l -> new RelationalOperation.ArrayLiteral(
                    l.values().stream().map(v -> op(v, enclosingDb)).toList());
            case Protocol.PElemtWithJoins j -> joinNavigation(j, enclosingDb);
            case Protocol.PDynaFunc f -> dynaFunc(f, enclosingDb);
            // 4.145.0 relational lambdas (filter/map/fold over an array
            // take): carried structurally; the translator refuses them at
            // LOWERING until the leg lands
            case Protocol.PRelLambda l -> new RelationalOperation.Lambda(l.parameterNames(),
                    op(l.body(), enclosingDb));
            case Protocol.PLambdaParam lp -> new RelationalOperation.LambdaParam(lp.name());
        };
    }

    /** {@code {target}.COL} arrives with the sentinel table alias the engine
     *  uses for a self-join's far side; everything else is a plain column. */
    private static RelationalOperation columnRef(Protocol.PColumnRef c,
            @com.legend.Nullable String enclosingDb) {
        Protocol.PTablePtr t = c.table();
        // a self-join's far side is spelled {target} in the TABLE position
        if ("{target}".equals(t.table()) || "target".equals(c.tableAlias())) {
            return new RelationalOperation.TargetColumnRef(c.column());
        }
        String db = t.database() != null && t.database().equals(enclosingDb)
                ? null : t.database();
        // the wire splits schema from table; the model carries the name as
        // written, which for a NAMED schema is "schema.table". A
        // double-quoted relational identifier ("test table") reaches the
        // wire WITH its quotes and the model stores it bare.
        String table = t.schema() == null || "default".equals(t.schema())
                ? unquote(t.table())
                : unquote(t.schema()) + "." + unquote(t.table());
        return new RelationalOperation.ColumnRef(db, table, c.column());
    }

    private static String unquote(String s) {
        return s.length() >= 2 && s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"'
                ? s.substring(1, s.length() - 1) : s;
    }

    private static RelationalOperation joinNavigation(Protocol.PElemtWithJoins j,
            @com.legend.Nullable String enclosingDb) {
        List<JoinChainElement> chain = new ArrayList<>();
        String db = null;
        for (Protocol.PJoinPtr ptr : j.joins()) {
            if (db == null) {
                db = ptr.db();
            }
            // the joinType rides the NEXT pointer on the wire (relational leg)
            // — the model hangs it on the element it qualifies
            chain.add(new JoinChainElement(ptr.name(),
                    ptr.joinType() == null ? null
                            : JoinType.fromIdentifier(ptr.joinType()),
                    ptr.db(), false));
        }
        // The TERMINAL is written relative to the NAV's own root database,
        // not to whatever encloses the nav: `[db::DB] @J | T_FIRM.NAME`
        // spells the terminal bare because the join already named db::DB.
        // Inside a property mapping the two coincide (the PM record carries
        // the same db), but a `~groupBy` / `~primaryKey` key has no PM record
        // to lift the qualifier onto, so it arrives with no enclosing db at
        // all — and passing that down left the terminal carrying `db::DB`
        // where the legacy parser recorded null, which is enough to stop
        // GroupBySynthesis#groupByOpsMatch recognising the key.
        String navDb = db != null ? db : enclosingDb;
        // SAME as-written rule columnRef applies: a nav rooted in the
        // ENCLOSING database is written bare, so the model's db is null.
        // The CHAIN elements keep their resolved db either way — that is
        // what the legacy parser records too.
        if (db != null && db.equals(enclosingDb)) {
            db = null;
        }
        return new RelationalOperation.JoinNavigation(db, chain,
                j.relationalElement() == null ? null
                        : op(j.relationalElement(), navDb));
    }

    /**
     * The closed operator vocabulary. Anything not named here is a genuine
     * function call — the same fallback the legacy parser applied.
     */
    private static RelationalOperation dynaFunc(Protocol.PDynaFunc f,
            @com.legend.Nullable String enclosingDb) {
        List<RelationalOperation> args = f.parameters().stream()
                .map(a -> op(a, enclosingDb)).toList();
        ComparisonOp cmp = comparison(f.funcName());
        if (cmp != null && args.size() == 2) {
            return new RelationalOperation.Comparison(args.get(0), cmp, args.get(1));
        }
        switch (f.funcName()) {
            case "and", "or" -> {
                if (args.size() >= 2) {
                    LogicalOp lop = "and".equals(f.funcName())
                            ? LogicalOp.AND : LogicalOp.OR;
                    // the wire flattens a same-operator run into ONE n-ary
                    // dynaFunc. The model is binary and RIGHT-associative —
                    // a AND (b AND c), matching how the legacy parser nests
                    // them — so fold from the right, not the left.
                    RelationalOperation acc = args.get(args.size() - 1);
                    for (int i = args.size() - 2; i >= 0; i--) {
                        acc = new RelationalOperation.BooleanOp(args.get(i), lop, acc);
                    }
                    return acc;
                }
            }
            case "isNull" -> {
                if (args.size() == 1) {
                    return new RelationalOperation.IsNull(args.get(0));
                }
            }
            case "isNotNull" -> {
                if (args.size() == 1) {
                    return new RelationalOperation.IsNotNull(args.get(0));
                }
            }
            case "group" -> {
                if (args.size() == 1) {
                    return new RelationalOperation.Group(args.get(0));
                }
            }
            default -> {
                // fall through to FunctionCall
            }
        }
        return new RelationalOperation.FunctionCall(f.funcName(), args);
    }

    private static @com.legend.Nullable ComparisonOp comparison(String name) {
        return switch (name) {
            case "equal" -> ComparisonOp.EQ;
            // the engine spells inequality two ways: '!=' mints notEqual and
            // '<>' mints notEqualAnsi; both are NEQ to the compiler
            case "notEqual", "notEqualAnsi" -> ComparisonOp.NEQ;
            case "lessThan" -> ComparisonOp.LT;
            case "lessThanEqual" -> ComparisonOp.LTE;
            case "greaterThan" -> ComparisonOp.GT;
            case "greaterThanEqual" -> ComparisonOp.GTE;
            default -> null;
        };
    }
}
