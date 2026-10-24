// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql;

import com.legend.protocol.sql.SqlLexer.Kind;
import com.legend.protocol.sql.SqlLexer.Token;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Postgres SQL text to {@link SqlAst}: a hand-written recursive-descent parser over {@link SqlLexer}'s tokens, with
 * Postgres's operator precedence (the manual's §4.1.6, lowest first): {@code OR}; {@code AND}; {@code NOT};
 * {@code IS}, {@code ISNULL}, {@code NOTNULL}; comparison; {@code BETWEEN}, {@code IN}, {@code LIKE}, {@code ILIKE};
 * other operators ({@code ||}); {@code + -}; {@code * / %}; unary {@code + -}; {@code ::}.
 *
 * <p>It accepts ONE query statement: a single {@code SELECT} over at most one FROM item, with {@code WHERE},
 * {@code GROUP BY}, {@code HAVING}, {@code ORDER BY}, {@code LIMIT}, {@code OFFSET} and {@code FETCH FIRST}. Every
 * Postgres form outside that is refused here with its name and position ("joins are not supported yet"), never
 * parsed as something else: the set grows construct by construct.
 */
public final class SqlParser {

    /** Postgres's reserved key words (Appendix C, "reserved"): never a column name, never a bare alias. */
    private static final Set<String> RESERVED = Set.of("all", "analyse", "analyze", "and", "any", "array", "as", "asc",
            "asymmetric", "both", "case", "cast", "check", "collate", "column", "constraint", "create",
            "current_catalog", "current_date", "current_role", "current_time", "current_timestamp", "current_user",
            "default", "deferrable", "desc", "distinct", "do", "else", "end", "except", "false", "fetch", "for",
            "foreign", "from", "grant", "group", "having", "in", "initially", "intersect", "into", "lateral", "leading",
            "limit", "localtime", "localtimestamp", "not", "null", "offset", "on", "only", "or", "order", "placing",
            "primary", "references", "returning", "select", "session_user", "some", "symmetric", "system_user", "table",
            "then", "to", "trailing", "true", "union", "unique", "user", "using", "variadic", "when", "where", "window",
            "with");

    /** Postgres's "reserved (can be function or type name)" key words: a function name, never a column name. */
    private static final Set<String> TYPE_FUNC_NAME = Set.of("authorization", "binary", "collation", "concurrently",
            "cross", "current_schema", "freeze", "full", "ilike", "inner", "is", "isnull", "join", "left", "like",
            "natural", "notnull", "outer", "overlaps", "right", "similar", "tablesample", "verbose");

    /** The key words that may name a select-list column only after {@code AS} (Postgres 16's kwlist.h, the
     *  {@code AS_LABEL} ones): every other word may stand bare, {@code select 1 select} included. */
    private static final Set<String> AS_LABEL = Set.of("array", "as", "char", "character", "create", "day", "except",
            "fetch", "filter", "for", "from", "grant", "group", "having", "hour", "intersect", "into", "isnull", "limit",
            "minute", "month", "notnull", "offset", "on", "order", "over", "overlaps", "precision", "returning",
            "second", "to", "union", "varying", "where", "window", "with", "within", "without", "year");

    /** The operators the levels below {@code Op} read themselves; Postgres reads every other one at {@code Op}. */
    private static final Set<String> OWN_LEVEL_OPERATORS = Set.of(":", "+", "-", "*", "/", "%", "^", "<", ">", "=", "<=",
            ">=", "<>", "!=", "::", "=>", ":=");

    /** The SQL value functions written without parentheses (Postgres §9.9.5): read as calls with no arguments. */
    private static final Set<String> VALUE_FUNCTIONS = Set.of("current_date", "current_time", "current_timestamp",
            "localtime", "localtimestamp");

    /** The SQL value functions of the session's identity (Postgres §9.27, "Session Information Functions"). */
    private static final Set<String> SESSION_VALUES = Set.of("current_user", "current_role", "session_user", "user",
            "system_user", "current_catalog", "current_schema");

    private final List<Token> tokens;
    private int at;

    private SqlParser(List<Token> tokens) {
        this.tokens = tokens;
    }

    /** The one statement {@code sql} holds (an optional trailing {@code ;}). */
    public static SqlAst.Statement parse(String sql) {
        SqlParser p = new SqlParser(SqlLexer.tokenize(sql));
        SqlAst.Statement statement = p.statement();
        if (p.peek().kind() == Kind.SEMICOLON) {
            p.at++;
        }
        if (p.peek().kind() != Kind.EOF) {
            if (p.tokens.get(p.at - 1).kind() == Kind.SEMICOLON) {
                throw p.error("only one statement is supported");
            }
            throw p.unexpected();
        }
        return statement;
    }

    // ------------------------------------------------------------------ statements

    /** A query (Postgres's {@code SelectStmt}): {@code [WITH ...] term [UNION | EXCEPT term ...] [ORDER BY ...]
     *  [LIMIT | OFFSET | FETCH ...]}, INTERSECT binding tighter than UNION and EXCEPT, the sort and the slice the
     *  whole result's. */
    private SqlAst.Statement statement() {
        Token t = peek();
        if (t.is("with")) {
            return with();
        }
        if (t.is("insert")) {
            return insert();
        }
        if (t.is("update")) {
            return update();
        }
        if (t.is("delete")) {
            return delete();
        }
        if (!t.is("select") && !t.is("values") && !t.is("table") && t.kind() != Kind.LPAREN) {
            if (t.kind() == Kind.EOF) {
                throw error("syntax error at end of input");
            }
            throw error("only queries and INSERT, UPDATE and DELETE are supported: a statement must begin with"
                    + " SELECT, not \"" + t.text() + "\"");
        }
        SqlAst.Statement query = setExpression();
        return tail(query);
    }

    /** A query as a statement's own part (a subquery's, an INSERT's source): never one that writes, which Postgres
     *  reads nowhere but as a WITH query ({@link #cteQuery}). */
    private SqlAst.Statement query() {
        if (writes(peek())) {
            throw error("syntax error at or near \"" + peek().text() + "\"");
        }
        return statement();
    }

    /** A WITH query, or the statement a WITH clause heads: one that writes (a data-modifying WITH) refused by name. */
    private SqlAst.Statement cteQuery() {
        if (writes(peek())) {
            throw unsupported("a statement that writes a table in a WITH clause is");
        }
        return statement();
    }

    private static boolean writes(Token t) {
        return t.is("insert") || t.is("update") || t.is("delete");
    }

    // ------------------------------------------------------------------ INSERT, UPDATE, DELETE (writes leg 4)

    /** {@code INSERT INTO target [AS alias] [(columns)] query [ON CONFLICT ...] [RETURNING ...]}. */
    private SqlAst.Statement insert() {
        SqlAst.Pos pos = expectWord("insert").pos();
        expectWord("into");
        SqlAst.FromItem target = dmlTarget(true);
        List<SqlAst.Ident> columns = new ArrayList<>();
        if (peek().kind() == Kind.LPAREN && !queryAhead(1)) {
            at++;
            do {
                columns.add(ident(expectName()));
            } while (accept(Kind.COMMA));
            expect(Kind.RPAREN);
        }
        if (peek().is("default")) {
            throw unsupported("INSERT ... DEFAULT VALUES is");
        }
        if (peek().is("overriding")) {
            throw unsupported("INSERT ... OVERRIDING is");
        }
        // (a VALUES list's literals are typed by the columns they are written into: none read as text here)
        boolean values = peek().is("values");
        SqlAst.Statement source = values ? tail(values(false)) : query();
        SqlAst.OnConflict onConflict = null;
        if (peek().is("on") && peekAt(1).is("conflict")) {
            Token on = next();
            at++;
            List<SqlAst.Ident> keys = new ArrayList<>();
            if (peek().is("on") && peekAt(1).is("constraint")) {
                throw unsupported("ON CONFLICT ON CONSTRAINT is");
            }
            if (accept(Kind.LPAREN)) {
                do {
                    keys.add(ident(expectName()));
                } while (accept(Kind.COMMA));
                expect(Kind.RPAREN);
            }
            if (peek().is("where")) {
                throw unsupported("an ON CONFLICT target's WHERE is");
            }
            expectWord("do");
            if (acceptWord("nothing")) {
                onConflict = new SqlAst.OnConflict(keys, null, null, on.pos());
            } else {
                expectWord("update");
                expectWord("set");
                List<SqlAst.Assignment> sets = assignments();
                SqlAst.Expr where = acceptWord("where") ? expr() : null;
                onConflict = new SqlAst.OnConflict(keys, sets, where, on.pos());
            }
        }
        return new SqlAst.Insert(target, columns, source, values, onConflict, returning(), pos);
    }

    /** {@code UPDATE [ONLY] target [[AS] alias] SET assignments [FROM ...] [WHERE] [RETURNING ...]}. */
    private SqlAst.Statement update() {
        SqlAst.Pos pos = expectWord("update").pos();
        if (peek().is("only")) {
            throw unsupported("UPDATE ONLY is");
        }
        SqlAst.FromItem target = dmlTarget(false);
        expectWord("set");
        List<SqlAst.Assignment> sets = assignments();
        List<SqlAst.FromItem> from = new ArrayList<>();
        if (acceptWord("from")) {
            do {
                from.add(fromItem());
            } while (accept(Kind.COMMA));
        }
        SqlAst.Expr where = null;
        if (acceptWord("where")) {
            if (peek().is("current") && peekAt(1).is("of")) {
                throw unsupported("WHERE CURRENT OF is");
            }
            where = expr();
        }
        return new SqlAst.Update(target, sets, from, where, returning(), pos);
    }

    /** {@code DELETE FROM [ONLY] target [[AS] alias] [USING ...] [WHERE] [RETURNING ...]}. */
    private SqlAst.Statement delete() {
        SqlAst.Pos pos = expectWord("delete").pos();
        expectWord("from");
        if (peek().is("only")) {
            throw unsupported("ONLY before a DELETE's table is");
        }
        SqlAst.FromItem target = dmlTarget(false);
        List<SqlAst.FromItem> using = new ArrayList<>();
        if (acceptWord("using")) {
            do {
                using.add(fromItem());
            } while (accept(Kind.COMMA));
        }
        SqlAst.Expr where = null;
        if (acceptWord("where")) {
            if (peek().is("current") && peekAt(1).is("of")) {
                throw unsupported("WHERE CURRENT OF is");
            }
            where = expr();
        }
        return new SqlAst.Delete(target, using, where, returning(), pos);
    }

    /** A statement's table: a name, or the source function {@code tb('pkg::Database.schema.table')} (its string
     *  the only argument, as FROM's); an INSERT's alias only after AS (its column list follows a bare name). */
    private SqlAst.FromItem dmlTarget(boolean insert) {
        Token t = peek();
        List<SqlAst.Ident> name = qualifiedName();
        List<SqlAst.Arg> args = null;
        if (peek().kind() == Kind.LPAREN && peekAt(1).kind() == Kind.STRING) {
            at++;
            args = new ArrayList<>();
            do {
                args.add(arg());
            } while (accept(Kind.COMMA));
            expect(Kind.RPAREN);
        }
        SqlAst.Ident alias = null;
        if (acceptWord("as")) {
            alias = ident(expectName());
        } else if (!insert && isName(peek()) && !isKeyword(peek()) && !peek().is("set") && !peek().is("where")
                && !peek().is("using") && !peek().is("returning")) {
            // (a bare alias, never the clause that follows it: SET is no reserved word)
            alias = ident(next());
        }
        return args != null ? new SqlAst.TableFunction(name, args, alias, List.of(), t.pos())
                : new SqlAst.TableName(name, alias, List.of(), t.pos());
    }

    /** {@code column = value, ...} of a SET; a row of columns or DEFAULT refused by name. */
    private List<SqlAst.Assignment> assignments() {
        List<SqlAst.Assignment> sets = new ArrayList<>();
        do {
            if (peek().kind() == Kind.LPAREN) {
                throw unsupported("a SET of a row of columns is");
            }
            Token column = expectName();
            if (peek().kind() == Kind.DOT || peek().kind() == Kind.LBRACKET) {
                throw unsupported("a SET of a column's field or element is");
            }
            Token eq = next();
            if (!eq.isOp("=")) {
                throw new SqlSyntaxException("syntax error at or near \"" + eq.text() + "\"", eq.pos());
            }
            if (peek().is("default")) {
                throw unsupported("SET column = DEFAULT is");
            }
            sets.add(new SqlAst.Assignment(ident(column), expr(), column.pos()));
        } while (accept(Kind.COMMA));
        return sets;
    }

    /** {@code RETURNING * | items}; none when absent. */
    private List<SqlAst.SelectItem> returning() {
        List<SqlAst.SelectItem> items = new ArrayList<>();
        if (acceptWord("returning")) {
            do {
                items.add(selectItem());
            } while (accept(Kind.COMMA));
        }
        return items;
    }

    private SqlAst.Statement with() {
        SqlAst.Pos pos = expectWord("with").pos();
        boolean recursive = acceptWord("recursive");
        List<SqlAst.Cte> ctes = new ArrayList<>();
        do {
            Token name = expectName();
            List<SqlAst.Ident> columns = new ArrayList<>();
            if (accept(Kind.LPAREN)) {
                do {
                    columns.add(ident(expectName()));
                } while (accept(Kind.COMMA));
                expect(Kind.RPAREN);
            }
            expectWord("as");
            // [NOT] MATERIALIZED: how Postgres evaluates the CTE, never what it answers
            if (acceptWord("not")) {
                expectWord("materialized");
            } else {
                acceptWord("materialized");
            }
            expect(Kind.LPAREN);
            SqlAst.Statement query = cteQuery();
            expect(Kind.RPAREN);
            ctes.add(new SqlAst.Cte(ident(name), columns, query, name.pos()));
        } while (accept(Kind.COMMA));
        SqlAst.Statement body = cteQuery();
        return new SqlAst.With(recursive, ctes, body, pos);
    }

    /** {@code term ((UNION | EXCEPT) [ALL | DISTINCT] term)*}, left-associative. */
    private SqlAst.Statement setExpression() {
        SqlAst.Statement left = setTerm();
        while (peek().is("union") || peek().is("except")) {
            Token op = next();
            boolean all = acceptWord("all");
            if (!all) {
                acceptWord("distinct");
            }
            SqlAst.Statement right = setTerm();
            left = new SqlAst.SetOperation(op.is("union") ? SqlAst.SetOp.UNION : SqlAst.SetOp.EXCEPT, all, left, right,
                    List.of(), null, null, op.pos());
        }
        return left;
    }

    private SqlAst.Statement setTerm() {
        SqlAst.Statement left = setPrimary();
        while (peek().is("intersect")) {
            Token op = next();
            boolean all = acceptWord("all");
            if (!all) {
                acceptWord("distinct");
            }
            left = new SqlAst.SetOperation(SqlAst.SetOp.INTERSECT, all, left, setPrimary(), List.of(), null, null,
                    op.pos());
        }
        return left;
    }

    /** A SELECT, or a parenthesized query (which may have its own sort and slice). */
    private SqlAst.Statement setPrimary() {
        Token t = peek();
        if (t.kind() == Kind.LPAREN) {
            at++;
            SqlAst.Statement inner = query();
            expect(Kind.RPAREN);
            return inner;
        }
        if (t.is("values")) {
            return values();
        }
        if (t.is("table")) {
            return table();
        }
        return selectCore();
    }

    /** {@code TABLE name}: Postgres's {@code SELECT * FROM name} (2026-10-17, the regression suite's 25). The name
     *  may be the island's source function, {@code TABLE tb('pkg::Database.table')}, as a FROM list's may. */
    private SqlAst.Statement table() {
        Token t = expectWord("table");
        if (peek().is("only")) {
            throw unsupported("TABLE ONLY is");
        }
        Token first = peek();
        List<SqlAst.Ident> name = qualifiedName();
        SqlAst.FromItem item;
        if (peek().kind() == Kind.LPAREN) {
            Token open = next();
            List<SqlAst.Arg> args = new ArrayList<>();
            if (!accept(Kind.RPAREN)) {
                do {
                    args.add(arg());
                } while (accept(Kind.COMMA));
                expect(Kind.RPAREN);
            }
            SqlAst.TableFunction source = new SqlAst.TableFunction(name, args, null, List.of(), first.pos());
            if (!SqlToPure.isSourceFunction(source)) {
                throw new SqlSyntaxException("syntax error at or near \"(\"", open.pos());
            }
            item = source;
        } else {
            descendants();
            item = new SqlAst.TableName(name, null, List.of(), first.pos());
        }
        return new SqlAst.Select(false, List.of(new SqlAst.AllColumns(null, t.pos())), List.of(item), null,
                List.of(), null, List.of(), List.of(), null, null, t.pos());
    }

    /** A table's {@code TABLESAMPLE}, after its alias (Postgres's {@code relation_expr opt_alias_clause
     *  tablesample_clause}; 2026-10-17, the regression suite's 17); refused by name. */
    private void tablesample() {
        if (peek().is("tablesample")) {
            throw unsupported("TABLESAMPLE is");
        }
    }

    /** A table name's {@code *}: its descendants too, which a table read includes anyway (Postgres's
     *  {@code relation_expr}; 2026-10-17, the regression suite's {@code FROM a_star*}). */
    private void descendants() {
        if (peek().isOp("*")) {
            at++;
        }
    }

    /** {@code VALUES (e, ...), (e, ...)}, as Postgres defines it: {@code SELECT e AS column1, ... UNION ALL SELECT
     *  ...}, its columns named column1 to columnN and typed by UNION's rules (2026-10-17, the regression suite's 364
     *  VALUES lists). The rows are combined as a balanced tree, of depth log n for the translator and the planner
     *  (UNION ALL is associative, and the rows keep their order). */
    private SqlAst.Statement values() {
        return values(true);
    }

    /** {@code VALUES (...), ...}; {@code typed} false for an INSERT's, whose column of NULLs alone is the column's
     *  type, not text. */
    private SqlAst.Statement values(boolean typed) {
        expectWord("values");
        List<List<SqlAst.Expr>> cells = new ArrayList<>();
        List<SqlAst.Pos> opens = new ArrayList<>();
        do {
            Token open = expect(Kind.LPAREN);
            List<SqlAst.Expr> row = new ArrayList<>();
            do {
                if (peek().is("default")) {
                    // (an INSERT's VALUES may write a column's default, which no write here reads yet)
                    throw typed ? error("DEFAULT is not allowed in this context")
                            : unsupported("DEFAULT in an INSERT's VALUES is");
                }
                SqlAst.Expr cell = expr();
                // a VALUES row is no select list: a set-returning function would not expand the rows, and an
                // aggregate or a window function has no rows to read (the review of 2026-10-17: VALUES (count(*))
                // answered 1)
                String not = SqlToPure.notInValues(cell);
                if (not != null) {
                    throw new SqlSyntaxException(not, cell.pos());
                }
                row.add(cell);
            } while (accept(Kind.COMMA));
            expect(Kind.RPAREN);
            if (!cells.isEmpty() && row.size() != cells.get(0).size()) {
                throw new SqlSyntaxException("VALUES lists must all be the same length", open.pos());
            }
            cells.add(row);
            opens.add(open.pos());
        } while (accept(Kind.COMMA));
        // a column of NULLs alone is text, as Postgres types one (VALUES (NULL) UNION ALL SELECT 1 is its error);
        // a NULL beside a value takes the column's type, the rows typed together (SqlAst.SetOperation#values)
        int width = typed ? cells.get(0).size() : 0;
        for (int k = 0; k < width; k++) {
            int column = k;
            if (cells.stream().allMatch(row -> row.get(column) instanceof SqlAst.NullLit)) {
                for (List<SqlAst.Expr> row : cells) {
                    SqlAst.Expr cell = row.get(column);
                    row.set(column, new SqlAst.Cast(cell, new SqlAst.TypeName("text", List.of(), cell.pos()),
                            cell.pos()));
                }
            }
        }
        List<SqlAst.Statement> rows = new ArrayList<>();
        for (int r = 0; r < cells.size(); r++) {
            List<SqlAst.SelectItem> items = new ArrayList<>();
            for (SqlAst.Expr cell : cells.get(r)) {
                items.add(new SqlAst.SelectExpr(cell, new SqlAst.Ident("column" + (items.size() + 1), false,
                        cell.pos()), cell.pos()));
            }
            rows.add(new SqlAst.Select(false, items, List.of(), null, List.of(), null, List.of(), List.of(), null,
                    null, opens.get(r)));
        }
        return unionAll(rows, 0, rows.size());
    }

    private static SqlAst.Statement unionAll(List<SqlAst.Statement> rows, int from, int to) {
        if (to - from == 1) {
            return rows.get(from);
        }
        int mid = from + (to - from) / 2;
        return new SqlAst.SetOperation(SqlAst.SetOp.UNION, true, unionAll(rows, from, mid), unionAll(rows, mid, to),
                List.of(), null, null, rows.get(mid).pos(), true);
    }

    /** The sort and the slice after a query, attached to it (Postgres refuses a second ORDER BY or LIMIT). */
    private SqlAst.Statement tail(SqlAst.Statement query) {
        List<SqlAst.SortItem> orderBy = new ArrayList<>();
        if (acceptWord("order")) {
            expectWord("by");
            do {
                orderBy.add(sortItem());
            } while (accept(Kind.COMMA));
        }
        SqlAst.Expr limit = null;
        SqlAst.Expr offset = null;
        boolean sawLimit = false;
        boolean sawOffset = false;
        while (true) {
            if (!sawLimit && peek().is("limit")) {
                at++;
                sawLimit = true;
                if (!acceptWord("all")) {
                    limit = expr();
                }
            } else if (!sawOffset && peek().is("offset")) {
                at++;
                sawOffset = true;
                offset = expr();
                if (!acceptWord("row")) {
                    acceptWord("rows");
                }
            } else if (!sawLimit && peek().is("fetch")) {
                at++;
                sawLimit = true;
                if (!acceptWord("first")) {
                    expectWord("next");
                }
                limit = peek().is("row") || peek().is("rows") ? new SqlAst.IntegerLit(BigInteger.ONE, peek().pos())
                        : expr();
                if (!acceptWord("row")) {
                    expectWord("rows");
                }
                if (peek().is("with")) {
                    throw unsupported("FETCH ... WITH TIES is");
                }
                expectWord("only");
            } else {
                break;
            }
        }
        if (peek().is("for")) {
            throw unsupported("locking clauses (FOR UPDATE, FOR SHARE) are");
        }
        if (orderBy.isEmpty() && !sawLimit && !sawOffset) {
            return query;
        }
        return switch (query) {
            case SqlAst.Select q -> {
                if (!q.orderBy().isEmpty() && !orderBy.isEmpty()) {
                    throw error("multiple ORDER BY clauses not allowed");
                }
                if ((q.limit() != null || q.offset() != null) && (sawLimit || sawOffset)) {
                    throw error("multiple LIMIT clauses not allowed");
                }
                yield new SqlAst.Select(q.distinct(), q.items(), q.from(), q.where(), q.groupBy(), q.having(),
                        q.windows(), orderBy.isEmpty() ? q.orderBy() : orderBy, sawLimit ? limit : q.limit(),
                        sawOffset ? offset : q.offset(), q.pos());
            }
            case SqlAst.SetOperation q -> {
                if (!q.orderBy().isEmpty() && !orderBy.isEmpty()) {
                    throw error("multiple ORDER BY clauses not allowed");
                }
                if ((q.limit() != null || q.offset() != null) && (sawLimit || sawOffset)) {
                    throw error("multiple LIMIT clauses not allowed");
                }
                yield new SqlAst.SetOperation(q.op(), q.all(), q.left(), q.right(),
                        orderBy.isEmpty() ? q.orderBy() : orderBy, sawLimit ? limit : q.limit(),
                        sawOffset ? offset : q.offset(), q.pos(), q.values());
            }
            case SqlAst.With w -> throw unsupported("ORDER BY or LIMIT after a parenthesized WITH query is");
            case SqlAst.Write w -> throw new IllegalStateException("a statement that writes is never a query's operand");
        };
    }

    /** {@code SELECT [DISTINCT | ALL] items [FROM ...] [WHERE] [GROUP BY] [HAVING] [WINDOW]}: no sort, no slice. */
    private SqlAst.Select selectCore() {
        SqlAst.Pos pos = expectWord("select").pos();
        boolean distinct = false;
        if (acceptWord("distinct")) {
            if (peek().is("on")) {
                throw unsupported("DISTINCT ON is");
            }
            distinct = true;
        } else {
            acceptWord("all");
        }
        List<SqlAst.SelectItem> items = new ArrayList<>();
        // Postgres allows an empty select list (SELECT FROM t), though not after DISTINCT
        if (distinct || !endsTargetList(peek())) {
            do {
                items.add(selectItem());
            } while (accept(Kind.COMMA));
        }
        if (peek().is("into")) {
            throw unsupported("an INTO clause is");
        }
        List<SqlAst.FromItem> from = new ArrayList<>();
        if (acceptWord("from")) {
            do {
                from.add(fromItem());
            } while (accept(Kind.COMMA));
        }
        SqlAst.Expr where = acceptWord("where") ? expr() : null;
        List<SqlAst.Expr> groupBy = new ArrayList<>();
        if (acceptWord("group")) {
            expectWord("by");
            if (peek().is("all") || peek().is("distinct")) {
                throw unsupported("GROUP BY ALL / DISTINCT is");
            }
            do {
                if ((peek().is("rollup") || peek().is("cube")) && peekAt(1).kind() == Kind.LPAREN
                        || peek().is("grouping") && peekAt(1).is("sets")) {
                    throw unsupported("ROLLUP, CUBE and GROUPING SETS are");
                }
                groupBy.add(expr());
            } while (accept(Kind.COMMA));
        }
        SqlAst.Expr having = acceptWord("having") ? expr() : null;
        List<SqlAst.NamedWindow> windows = new ArrayList<>();
        if (acceptWord("window")) {
            do {
                SqlAst.Ident name = ident(expectName());
                expectWord("as");
                expect(Kind.LPAREN);
                windows.add(new SqlAst.NamedWindow(name, windowSpec(name.pos())));
            } while (accept(Kind.COMMA));
        }
        return new SqlAst.Select(distinct, items, from, where, groupBy, having, windows, List.of(), null, null, pos);
    }

    /** True iff {@code t} cannot begin a select list, so the list before it is empty. */
    private static boolean endsTargetList(Token t) {
        return t.kind() == Kind.EOF || t.kind() == Kind.SEMICOLON || t.kind() == Kind.RPAREN || t.is("from")
                || t.is("where") || t.is("group") || t.is("having") || t.is("window") || t.is("union")
                || t.is("intersect") || t.is("except") || t.is("order") || t.is("limit") || t.is("offset")
                || t.is("fetch") || t.is("for") || t.is("into");
    }

    private SqlAst.SelectItem selectItem() {
        Token t = peek();
        if (t.isOp("*")) {
            at++;
            return new SqlAst.AllColumns(null, t.pos());
        }
        if (isName(t) && peekAt(1).kind() == Kind.DOT && peekAt(2).isOp("*")) {
            at += 3;
            return new SqlAst.AllColumns(ident(t), t.pos());
        }
        SqlAst.Expr e = expr();
        SqlAst.Ident alias = null;
        if (acceptWord("as")) {
            alias = label();
        } else if (isBareLabel(peek())) {
            alias = ident(next());
        }
        return new SqlAst.SelectExpr(e, alias, t.pos());
    }

    private SqlAst.SortItem sortItem() {
        SqlAst.Expr e = expr();
        boolean descending = false;
        if (acceptWord("desc")) {
            descending = true;
        } else if (!acceptWord("asc") && peek().is("using")) {
            throw unsupported("ORDER BY ... USING is");
        }
        Boolean nullsFirst = null;
        if (acceptWord("nulls")) {
            if (acceptWord("first")) {
                nullsFirst = Boolean.TRUE;
            } else {
                expectWord("last");
                nullsFirst = Boolean.FALSE;
            }
        }
        return new SqlAst.SortItem(e, descending, nullsFirst, e.pos());
    }

    // ------------------------------------------------------------------ FROM

    /** A FROM item and the joins that follow it, left-associative: {@code a JOIN b ON ... LEFT JOIN c USING (x)}. */
    private SqlAst.FromItem fromItem() {
        SqlAst.FromItem left = fromPrimary();
        while (true) {
            Token t = peek();
            if (t.is("cross")) {
                at++;
                expectWord("join");
                left = new SqlAst.Join(SqlAst.JoinType.CROSS, false, left, fromPrimary(), null, List.of(), t.pos());
                continue;
            }
            boolean natural = acceptWord("natural");
            SqlAst.JoinType type = null;
            if (acceptWord("inner")) {
                type = SqlAst.JoinType.INNER;
            } else if (acceptWord("left")) {
                acceptWord("outer");
                type = SqlAst.JoinType.LEFT;
            } else if (acceptWord("right")) {
                acceptWord("outer");
                type = SqlAst.JoinType.RIGHT;
            } else if (acceptWord("full")) {
                acceptWord("outer");
                type = SqlAst.JoinType.FULL;
            }
            if (!peek().is("join")) {
                if (type != null || natural) {
                    throw unexpected();
                }
                return left;
            }
            at++;
            type = type == null ? SqlAst.JoinType.INNER : type;
            SqlAst.FromItem right = fromPrimary();
            if (natural) {
                left = new SqlAst.Join(type, true, left, right, null, List.of(), t.pos());
            } else if (acceptWord("on")) {
                left = new SqlAst.Join(type, false, left, right, expr(), List.of(), t.pos());
            } else if (acceptWord("using")) {
                expect(Kind.LPAREN);
                List<SqlAst.Ident> using = new ArrayList<>();
                do {
                    using.add(ident(expectName()));
                } while (accept(Kind.COMMA));
                expect(Kind.RPAREN);
                if (peek().is("as")) {
                    throw unsupported("an alias for a USING join's columns is");
                }
                left = new SqlAst.Join(type, false, left, right, null, using, t.pos());
            } else {
                throw unexpected();
            }
        }
    }

    /** A table, a function, a subquery, or a parenthesized join. */
    private SqlAst.FromItem fromPrimary() {
        Token t = peek();
        if (t.is("lateral")) {
            // LATERAL (subquery): it sees the FROM items before it; a function in FROM always does
            at++;
            if (peek().kind() == Kind.LPAREN && queryAhead(1)) {
                Token open = next();
                SqlAst.Statement query = query();
                expect(Kind.RPAREN);
                Alias alias = alias();
                return new SqlAst.SubqueryFrom(query, alias.name(), alias.columns(), true, open.pos());
            }
            SqlAst.FromItem item = fromPrimary();
            if (!(item instanceof SqlAst.TableFunction)) {
                throw new SqlSyntaxException("syntax error at or near \"" + t.text() + "\"", t.pos());
            }
            return item;
        }
        if (t.is("only")) {
            throw unsupported("FROM ONLY is");
        }
        if (t.kind() == Kind.LPAREN) {
            if (queryAhead(1)) {
                at++;
                SqlAst.Statement query = query();
                expect(Kind.RPAREN);
                Alias alias = alias();
                return new SqlAst.SubqueryFrom(query, alias.name(), alias.columns(), false, t.pos());
            }
            at++;
            SqlAst.FromItem inner = fromItem();
            expect(Kind.RPAREN);
            if (peek().is("as") || isName(peek()) && !isKeyword(peek())) {
                throw unsupported("an alias for a parenthesized join is");
            }
            return inner;
        }
        List<SqlAst.Ident> name = qualifiedName();
        if (accept(Kind.LPAREN)) {
            List<SqlAst.Arg> args = new ArrayList<>();
            if (!accept(Kind.RPAREN)) {
                do {
                    args.add(arg());
                } while (accept(Kind.COMMA));
                expect(Kind.RPAREN);
            }
            if (peek().is("with") && peekAt(1).is("ordinality")) {
                throw unsupported("WITH ORDINALITY is");
            }
            Alias alias = alias();
            SqlAst.TableFunction f = new SqlAst.TableFunction(name, args, alias.name(), alias.columns(), t.pos());
            if (SqlToPure.isSourceFunction(f)) {
                tablesample();
            }
            return f;
        }
        descendants();
        tablesample();
        Alias alias = alias();
        tablesample();
        return new SqlAst.TableName(name, alias.name(), alias.columns(), t.pos());
    }

    /** True iff the tokens from {@code ahead} on, past any opening parentheses, begin a query. */
    private boolean queryAhead(int ahead) {
        int i = ahead;
        while (peekAt(i).kind() == Kind.LPAREN) {
            i++;
        }
        Token t = peekAt(i);
        return t.is("select") || t.is("with") || t.is("values") || t.is("table");
    }

    /** Whether a named argument begins here: {@code name => value}, or {@code name := value}, its older spelling. */
    private boolean named() {
        return isName(peek()) && (peekAt(1).isOp("=>") || peekAt(1).isOp(":="));
    }

    private SqlAst.Arg arg() {
        if (named()) {
            SqlAst.Ident name = ident(next());
            at++;
            return new SqlAst.Arg(name, expr());
        }
        return new SqlAst.Arg(null, expr());
    }

    /** An alias and its column names: {@code [AS] name [(column, ...)]}. */
    private record Alias(SqlAst.@com.legend.base.Nullable Ident name, List<SqlAst.Ident> columns) {
    }

    private Alias alias() {
        SqlAst.Ident name = null;
        if (acceptWord("as")) {
            name = ident(expectName());
        } else if (isName(peek()) && !isKeyword(peek())) {
            name = ident(next());
        }
        List<SqlAst.Ident> columns = new ArrayList<>();
        if (name != null && accept(Kind.LPAREN)) {
            do {
                columns.add(ident(expectName()));
            } while (accept(Kind.COMMA));
            expect(Kind.RPAREN);
        }
        return new Alias(name, columns);
    }

    // ------------------------------------------------------------------ windows

    /** The inside of {@code OVER ( ... )} or of a WINDOW clause's {@code AS ( ... )}, through its closing
     *  parenthesis: {@code [base] [PARTITION BY ...] [ORDER BY ...] [frame]}. */
    private SqlAst.WindowSpec windowSpec(SqlAst.Pos pos) {
        SqlAst.Ident base = null;
        if (isName(peek()) && !peek().is("partition") && !peek().is("order") && !peek().is("rows")
                && !peek().is("range") && !peek().is("groups")) {
            base = ident(next());
        }
        List<SqlAst.Expr> partitionBy = new ArrayList<>();
        if (acceptWord("partition")) {
            expectWord("by");
            do {
                partitionBy.add(expr());
            } while (accept(Kind.COMMA));
        }
        List<SqlAst.SortItem> orderBy = new ArrayList<>();
        if (acceptWord("order")) {
            expectWord("by");
            do {
                orderBy.add(sortItem());
            } while (accept(Kind.COMMA));
        }
        SqlAst.Frame frame = null;
        if (peek().is("rows") || peek().is("range") || peek().is("groups")) {
            Token unit = next();
            SqlAst.FrameUnit u = unit.is("rows") ? SqlAst.FrameUnit.ROWS : unit.is("range") ? SqlAst.FrameUnit.RANGE
                    : SqlAst.FrameUnit.GROUPS;
            SqlAst.FrameBound start;
            SqlAst.FrameBound end;
            if (acceptWord("between")) {
                start = frameBound();
                expectWord("and");
                end = frameBound();
            } else {
                start = frameBound();
                end = new SqlAst.FrameBound(SqlAst.BoundKind.CURRENT_ROW, null);
            }
            String exclude = null;
            if (acceptWord("exclude")) {
                if (acceptWord("current")) {
                    expectWord("row");
                    exclude = "CURRENT ROW";
                } else if (acceptWord("group")) {
                    exclude = "GROUP";
                } else if (acceptWord("ties")) {
                    exclude = "TIES";
                } else {
                    expectWord("no");
                    expectWord("others");
                }
            }
            frame = new SqlAst.Frame(u, start, end, exclude, unit.pos());
        }
        expect(Kind.RPAREN);
        return new SqlAst.WindowSpec(base, partitionBy, orderBy, frame, pos);
    }

    private SqlAst.FrameBound frameBound() {
        if (acceptWord("unbounded")) {
            if (acceptWord("preceding")) {
                return new SqlAst.FrameBound(SqlAst.BoundKind.UNBOUNDED_PRECEDING, null);
            }
            expectWord("following");
            return new SqlAst.FrameBound(SqlAst.BoundKind.UNBOUNDED_FOLLOWING, null);
        }
        if (acceptWord("current")) {
            expectWord("row");
            return new SqlAst.FrameBound(SqlAst.BoundKind.CURRENT_ROW, null);
        }
        SqlAst.Expr offset = expr();
        if (acceptWord("preceding")) {
            return new SqlAst.FrameBound(SqlAst.BoundKind.PRECEDING, offset);
        }
        expectWord("following");
        return new SqlAst.FrameBound(SqlAst.BoundKind.FOLLOWING, offset);
    }

    // ------------------------------------------------------------------ expressions

    /** A value expression (Postgres's {@code a_expr}). */
    private SqlAst.Expr expr() {
        return or();
    }

    private SqlAst.Expr or() {
        SqlAst.Expr left = and();
        while (peek().is("or")) {
            Token op = next();
            left = new SqlAst.Binary(SqlAst.BinaryOp.OR, left, and(), op.pos());
        }
        return left;
    }

    private SqlAst.Expr and() {
        SqlAst.Expr left = not();
        while (peek().is("and")) {
            Token op = next();
            left = new SqlAst.Binary(SqlAst.BinaryOp.AND, left, not(), op.pos());
        }
        return left;
    }

    private SqlAst.Expr not() {
        if (peek().is("not")) {
            Token op = next();
            return new SqlAst.Unary(SqlAst.UnaryOp.NOT, not(), op.pos());
        }
        return is();
    }

    private SqlAst.Expr is() {
        SqlAst.Expr left = comparison();
        while (true) {
            Token t = peek();
            if (t.is("isnull") || t.is("notnull")) {
                at++;
                left = new SqlAst.IsNull(left, t.is("notnull"), t.pos());
            } else if (t.is("is")) {
                at++;
                boolean negated = acceptWord("not");
                if (acceptWord("null")) {
                    left = new SqlAst.IsNull(left, negated, t.pos());
                } else if (acceptWord("true")) {
                    left = new SqlAst.IsTruth(left, SqlAst.Truth.TRUE, negated, t.pos());
                } else if (acceptWord("false")) {
                    left = new SqlAst.IsTruth(left, SqlAst.Truth.FALSE, negated, t.pos());
                } else if (acceptWord("unknown")) {
                    left = new SqlAst.IsTruth(left, SqlAst.Truth.UNKNOWN, negated, t.pos());
                } else if (acceptWord("distinct")) {
                    expectWord("from");
                    left = new SqlAst.IsDistinct(left, comparison(), negated, t.pos());
                } else {
                    throw unexpected();
                }
            } else {
                return left;
            }
        }
    }

    private SqlAst.Expr comparison() {
        SqlAst.Expr left = predicate();
        SqlAst.BinaryOp op = comparisonOp(peek());
        if (op == null) {
            return left;
        }
        Token t = next();
        if (peek().is("any") || peek().is("some") || peek().is("all")) {
            Token quantifier = next();
            if (peek().kind() != Kind.LPAREN || !queryAhead(1)) {
                throw unsupported("ANY, SOME and ALL over an array are");
            }
            at++;
            SqlAst.Statement query = query();
            expect(Kind.RPAREN);
            SqlAst.Expr quantified = new SqlAst.Quantified(op, quantifier.is("all"), left, query, t.pos());
            if (comparisonOp(peek()) != null) {
                throw unexpected();
            }
            return quantified;
        }
        SqlAst.Expr result = new SqlAst.Binary(op, left, predicate(), t.pos());
        if (comparisonOp(peek()) != null) {
            // Postgres's comparison operators do not associate: a < b < c is a syntax error there too
            throw unexpected();
        }
        return result;
    }

    private static SqlAst.@com.legend.base.Nullable BinaryOp comparisonOp(Token t) {
        if (t.kind() != Kind.OP) {
            return null;
        }
        return switch (t.text()) {
            case "=" -> SqlAst.BinaryOp.EQ;
            case "<>", "!=" -> SqlAst.BinaryOp.NE;
            case "<" -> SqlAst.BinaryOp.LT;
            case "<=" -> SqlAst.BinaryOp.LE;
            case ">" -> SqlAst.BinaryOp.GT;
            case ">=" -> SqlAst.BinaryOp.GE;
            default -> null;
        };
    }

    /** {@code [NOT] BETWEEN | IN | LIKE | ILIKE | SIMILAR TO} over the "other operators" level. */
    private SqlAst.Expr predicate() {
        SqlAst.Expr left = other();
        Token t = peek();
        boolean negated = false;
        if (t.is("not") && (peekAt(1).is("between") || peekAt(1).is("in") || peekAt(1).is("like")
                || peekAt(1).is("ilike") || peekAt(1).is("similar"))) {
            negated = true;
            at++;
            t = peek();
        }
        if (t.is("between")) {
            at++;
            if (peek().is("symmetric") || peek().is("asymmetric")) {
                throw unsupported("BETWEEN SYMMETRIC is");
            }
            SqlAst.Expr low = other();
            expectWord("and");
            return new SqlAst.Between(left, low, other(), negated, t.pos());
        }
        if (t.is("in")) {
            at++;
            if (peek().kind() == Kind.LPAREN && queryAhead(1)) {
                at++;
                SqlAst.Statement query = query();
                expect(Kind.RPAREN);
                return new SqlAst.InSubquery(left, query, negated, t.pos());
            }
            expect(Kind.LPAREN);
            List<SqlAst.Expr> values = new ArrayList<>();
            do {
                values.add(expr());
            } while (accept(Kind.COMMA));
            expect(Kind.RPAREN);
            return new SqlAst.InList(left, values, negated, t.pos());
        }
        if (t.is("like") || t.is("ilike")) {
            at++;
            SqlAst.Expr pattern = other();
            SqlAst.Expr escape = acceptWord("escape") ? other() : null;
            return new SqlAst.Like(left, pattern, escape, negated, t.is("ilike"), t.pos());
        }
        if (t.is("similar")) {
            throw unsupported("SIMILAR TO is");
        }
        return left;
    }

    /** Postgres's {@code Op} level: every operator but the arithmetic and comparison ones, left-associative,
     *  binding looser than {@code + -} and tighter than {@code LIKE}, {@code IN} and the comparisons. {@code ||} is
     *  concatenation; the rest are kept by spelling for the translator. */
    private SqlAst.Expr other() {
        SqlAst.Expr left = additive();
        while (isOtherOperator(peek())) {
            Token op = next();
            SqlAst.Expr right = additive();
            left = op.text().equals("||") ? new SqlAst.Binary(SqlAst.BinaryOp.CONCAT, left, right, op.pos())
                    : new SqlAst.Operator(op.text(), left, right, op.pos());
        }
        return left;
    }

    private static boolean isOtherOperator(Token t) {
        return t.kind() == Kind.OP && !OWN_LEVEL_OPERATORS.contains(t.text());
    }

    private SqlAst.Expr additive() {
        SqlAst.Expr left = multiplicative();
        while (peek().isOp("+") || peek().isOp("-")) {
            Token op = next();
            left = new SqlAst.Binary(op.text().equals("+") ? SqlAst.BinaryOp.PLUS : SqlAst.BinaryOp.MINUS, left,
                    multiplicative(), op.pos());
        }
        return left;
    }

    private SqlAst.Expr multiplicative() {
        SqlAst.Expr left = power();
        while (peek().isOp("*") || peek().isOp("/") || peek().isOp("%")) {
            Token op = next();
            SqlAst.BinaryOp kind = switch (op.text()) {
                case "*" -> SqlAst.BinaryOp.TIMES;
                case "/" -> SqlAst.BinaryOp.DIVIDE;
                default -> SqlAst.BinaryOp.MODULO;
            };
            left = new SqlAst.Binary(kind, left, power(), op.pos());
        }
        return left;
    }

    /** {@code ^}: left-associative in Postgres ({@code 2 ^ 3 ^ 2} is 64), and looser than a sign
     *  ({@code - 2 ^ 2} is 4). */
    private SqlAst.Expr power() {
        SqlAst.Expr left = unary();
        while (peek().isOp("^")) {
            Token op = next();
            left = new SqlAst.Binary(SqlAst.BinaryOp.POWER, left, unary(), op.pos());
        }
        return left;
    }

    private SqlAst.Expr unary() {
        if (peek().isOp("-") || peek().isOp("+")) {
            Token op = next();
            return new SqlAst.Unary(op.text().equals("-") ? SqlAst.UnaryOp.MINUS : SqlAst.UnaryOp.PLUS, unary(),
                    op.pos());
        }
        if (isOtherOperator(peek())) {
            // a prefix operator (@ x, |/ x) takes all that binds tighter than its level: @ 3 * 2 + 1 is @ (3 * 2 + 1)
            Token op = next();
            return new SqlAst.Operator(op.text(), null, additive(), op.pos());
        }
        return postfix();
    }

    private SqlAst.Expr postfix() {
        SqlAst.Expr e = primary();
        while (true) {
            if (peek().isOp("::")) {
                Token op = next();
                e = new SqlAst.Cast(e, typeName(), op.pos());
            } else if (peek().kind() == Kind.LBRACKET) {
                Token open = next();
                SqlAst.Expr index = expr();
                if (peek().isOp(":")) {
                    throw unsupported("array slices are");
                }
                expect(Kind.RBRACKET);
                e = new SqlAst.Subscript(e, index, open.pos());
            } else if (peek().is("at") && (peekAt(1).is("time") || peekAt(1).is("local"))) {
                throw unsupported("AT TIME ZONE is");
            } else if (peek().is("collate")) {
                throw unsupported("COLLATE is");
            } else {
                return e;
            }
        }
    }

    private SqlAst.Expr primary() {
        Token t = peek();
        switch (t.kind()) {
            case INTEGER -> {
                at++;
                return new SqlAst.IntegerLit(new BigInteger(t.text()), t.pos());
            }
            case NUMERIC -> {
                at++;
                return new SqlAst.NumericLit(new BigDecimal(t.text()), t.pos());
            }
            case STRING -> {
                at++;
                return new SqlAst.StringLit(t.text(), t.pos());
            }
            case PARAM -> {
                at++;
                return new SqlAst.Param(Integer.parseInt(t.text()), t.pos());
            }
            case LPAREN -> {
                if (queryAhead(1)) {
                    at++;
                    SqlAst.Statement query = query();
                    expect(Kind.RPAREN);
                    return new SqlAst.ScalarSubquery(query, t.pos());
                }
                at++;
                SqlAst.Expr inner = expr();
                if (peek().kind() == Kind.COMMA) {
                    throw unsupported("row constructors are");
                }
                expect(Kind.RPAREN);
                return inner;
            }
            case QUOTED_IDENT -> {
                return nameOrCall();
            }
            case IDENT -> {
                return keywordOrName(t);
            }
            default -> throw unexpected();
        }
    }

    private SqlAst.Expr keywordOrName(Token t) {
        String word = t.text().toLowerCase(Locale.ROOT);
        switch (word) {
            case "true", "false" -> {
                at++;
                return new SqlAst.BooleanLit(word.equals("true"), t.pos());
            }
            case "null" -> {
                at++;
                return new SqlAst.NullLit(t.pos());
            }
            case "case" -> {
                return caseExpr();
            }
            case "cast" -> {
                at++;
                expect(Kind.LPAREN);
                SqlAst.Expr operand = expr();
                expectWord("as");
                SqlAst.TypeName type = typeName();
                expect(Kind.RPAREN);
                return new SqlAst.Cast(operand, type, t.pos());
            }
            case "exists" -> {
                if (peekAt(1).kind() != Kind.LPAREN || !queryAhead(2)) {
                    return nameOrCall();
                }
                at += 2;
                SqlAst.Statement query = query();
                expect(Kind.RPAREN);
                return new SqlAst.Exists(query, t.pos());
            }
            case "array" -> {
                if (peekAt(1).kind() != Kind.LBRACKET) {
                    throw unsupported("ARRAY (subquery) is");
                }
                at += 2;
                List<SqlAst.Expr> elements = new ArrayList<>();
                if (peek().kind() != Kind.RBRACKET) {
                    do {
                        if (peek().kind() == Kind.LBRACKET) {
                            throw unsupported("multidimensional arrays are");
                        }
                        elements.add(expr());
                    } while (accept(Kind.COMMA));
                }
                expect(Kind.RBRACKET);
                return new SqlAst.ArrayConstructor(elements, t.pos());
            }
            case "interval" -> {
                // INTERVAL 'text' [unit]: a typed constant; the SQL standard's trailing unit (INTERVAL '3' DAY) is
                // the text's unit
                if (peekAt(1).kind() != Kind.STRING) {
                    throw unsupported("INTERVAL other than a constant is");
                }
                at++;
                String value = next().text();
                if (peek().kind() == Kind.IDENT && INTERVAL_UNITS.contains(peek().text().toLowerCase(Locale.ROOT))) {
                    // Postgres keeps only the field's part of the text (INTERVAL '1 day 1' MONTH is a month): a
                    // single whole number reads the same either way
                    String whole = value.strip();
                    int digit = whole.startsWith("+") || whole.startsWith("-") ? 1 : 0;
                    boolean number = digit < whole.length();
                    for (; digit < whole.length(); digit++) {
                        number &= whole.charAt(digit) >= '0' && whole.charAt(digit) <= '9';
                    }
                    if (!number) {
                        throw unsupported("INTERVAL '...' FIELD other than of one whole number is");
                    }
                    value = value + " " + next().text().toLowerCase(Locale.ROOT);
                    if (peek().is("to")) {
                        throw unsupported("INTERVAL ... TO ... is");
                    }
                }
                return new SqlAst.TypedLit(new SqlAst.TypeName("interval", List.of(), t.pos()), value, t.pos());
            }
            case "row" -> throw unsupported("ROW constructors are");
            default -> {
            }
        }
        if (SESSION_VALUES.contains(word) && !(word.equals("current_schema") && peekAt(1).kind() == Kind.LPAREN)) {
            // the session's role, database or schema: the server's own state (2026-10-17, the regression suite's 22)
            throw unsupported(word.toUpperCase(Locale.ROOT) + " is");
        }
        if (VALUE_FUNCTIONS.contains(word)) {
            at++;
            if (peek().kind() == Kind.LPAREN) {
                throw unsupported(word.toUpperCase(Locale.ROOT) + " with a precision is");
            }
            return new SqlAst.FunctionCall(List.of(new SqlAst.Ident(word, false, t.pos())), List.of(), false, false,
                    t.pos());
        }
        // a typed constant: type 'string' (DATE '2024-01-31', TIMESTAMP '2024-01-31 10:00:00')
        if (peekAt(1).kind() == Kind.STRING && !RESERVED.contains(word)) {
            at++;
            String value = next().text();
            return new SqlAst.TypedLit(new SqlAst.TypeName(word, List.of(), t.pos()), value, t.pos());
        }
        if ((word.equals("timestamp") || word.equals("time")) && peekAt(1).is("with") || peekAt(1).is("without")
                && (word.equals("timestamp") || word.equals("time"))) {
            SqlAst.TypeName type = typeName();
            if (peek().kind() != Kind.STRING) {
                throw unexpected();
            }
            return new SqlAst.TypedLit(type, next().text(), t.pos());
        }
        if (RESERVED.contains(word)) {
            throw unexpected();
        }
        if (TYPE_FUNC_NAME.contains(word) && peekAt(1).kind() != Kind.LPAREN) {
            throw unexpected();
        }
        if (peekAt(1).kind() == Kind.LPAREN) {
            SqlAst.Expr special = switch (word) {
                case "extract" -> extract();
                case "substring" -> substring();
                case "position" -> position();
                case "overlay" -> overlay();
                case "trim" -> trim();
                default -> null;
            };
            if (special != null) {
                return special;
            }
        }
        return nameOrCall();
    }

    // ------------------------------------------------------------------ SQL's special call syntax

    /** {@code EXTRACT(field FROM x)}: Postgres's {@code extract('field', x)}. Null when the parentheses hold an
     *  ordinary argument list. */
    private SqlAst.@com.legend.base.Nullable Expr extract() {
        Token name = peek();
        if (!(peekAt(2).kind() == Kind.IDENT || peekAt(2).kind() == Kind.STRING) || !peekAt(3).is("from")) {
            return null;
        }
        at += 2;
        Token field = next();
        at++;
        SqlAst.Expr source = expr();
        expect(Kind.RPAREN);
        String f = field.kind() == Kind.STRING ? field.text() : field.text().toLowerCase(Locale.ROOT);
        return call(name, "extract", List.of(new SqlAst.StringLit(f, field.pos()), source));
    }

    /** {@code SUBSTRING(x FROM a [FOR b])}, {@code SUBSTRING(x FOR b [FROM a])}: Postgres's
     *  {@code substring(x, a[, b])}, {@code substring(x, 1, b)}. */
    private SqlAst.@com.legend.base.Nullable Expr substring() {
        int mark = at;
        Token name = next();
        at++;
        SqlAst.Expr string = expr();
        SqlAst.Expr from = null;
        SqlAst.Expr length = null;
        if (peek().is("similar")) {
            throw unsupported("SUBSTRING ... SIMILAR is");
        }
        if (!peek().is("from") && !peek().is("for")) {
            at = mark;
            return null;
        }
        while (peek().is("from") || peek().is("for")) {
            boolean isFrom = next().is("from");
            if (isFrom) {
                from = expr();
            } else {
                length = expr();
            }
        }
        expect(Kind.RPAREN);
        List<SqlAst.Expr> args = new ArrayList<>(List.of(string));
        if (from == null) {
            args.add(new SqlAst.IntegerLit(BigInteger.ONE, name.pos()));
        } else {
            args.add(from);
        }
        if (length != null) {
            args.add(length);
        }
        return call(name, "substring", args);
    }

    /** {@code POSITION(a IN b)}: Postgres's {@code position(b, a)}, its operands below IN's level ({@code b_expr}). */
    private SqlAst.@com.legend.base.Nullable Expr position() {
        int mark = at;
        Token name = next();
        at++;
        SqlAst.Expr needle = other();
        if (!acceptWord("in")) {
            at = mark;
            return null;
        }
        SqlAst.Expr haystack = other();
        expect(Kind.RPAREN);
        return call(name, "position", List.of(haystack, needle));
    }

    /** {@code OVERLAY(x PLACING y FROM a [FOR b])}: Postgres's {@code overlay(x, y, a[, b])}. */
    private SqlAst.@com.legend.base.Nullable Expr overlay() {
        int mark = at;
        Token name = next();
        at++;
        SqlAst.Expr string = expr();
        if (!acceptWord("placing")) {
            at = mark;
            return null;
        }
        List<SqlAst.Expr> args = new ArrayList<>(List.of(string, expr()));
        expectWord("from");
        args.add(expr());
        if (acceptWord("for")) {
            args.add(expr());
        }
        expect(Kind.RPAREN);
        return call(name, "overlay", args);
    }

    /** {@code TRIM([BOTH | LEADING | TRAILING] [chars] FROM x)} and {@code TRIM(x[, chars])}: Postgres's
     *  {@code btrim}, {@code ltrim} or {@code rtrim}, of {@code x} then the characters. */
    private SqlAst.Expr trim() {
        Token name = next();
        at++;
        String fn = "btrim";
        if (acceptWord("both")) {
            fn = "btrim";
        } else if (acceptWord("leading")) {
            fn = "ltrim";
        } else if (acceptWord("trailing")) {
            fn = "rtrim";
        }
        List<SqlAst.Expr> args = new ArrayList<>();
        if (acceptWord("from")) {
            do {
                args.add(expr());
            } while (accept(Kind.COMMA));
        } else {
            SqlAst.Expr first = expr();
            if (acceptWord("from")) {
                do {
                    args.add(expr());
                } while (accept(Kind.COMMA));
                args.add(first);
            } else {
                args.add(first);
                while (accept(Kind.COMMA)) {
                    args.add(expr());
                }
            }
        }
        expect(Kind.RPAREN);
        return call(name, fn, args);
    }

    private static SqlAst.FunctionCall call(Token name, String fn, List<SqlAst.Expr> args) {
        return new SqlAst.FunctionCall(List.of(new SqlAst.Ident(fn, false, name.pos())), args, false, false,
                name.pos());
    }

    /** A column reference ({@code a}, {@code t.a}) or a function call ({@code f(...)}, {@code s.f(...)}). */
    private SqlAst.Expr nameOrCall() {
        Token first = peek();
        List<SqlAst.Ident> name = new ArrayList<>();
        name.add(ident(next()));
        while (peek().kind() == Kind.DOT) {
            at++;
            if (peek().isOp("*")) {
                throw unsupported("\"" + first.text() + ".*\" outside a select list is");
            }
            name.add(ident(expectName()));
        }
        if (peek().kind() != Kind.LPAREN) {
            return new SqlAst.ColumnRef(name, first.pos());
        }
        at++;
        boolean distinct = false;
        boolean star = false;
        List<SqlAst.Expr> args = new ArrayList<>();
        if (peek().isOp("*") && peekAt(1).kind() == Kind.RPAREN) {
            at++;
            star = true;
        } else if (peek().kind() != Kind.RPAREN) {
            if (acceptWord("distinct")) {
                distinct = true;
            } else {
                acceptWord("all");
            }
            do {
                // VARIADIC marks the last argument (2026-10-17: the regression suite's concat(1, VARIADIC ...))
                if (peek().is("variadic")) {
                    throw unsupported("VARIADIC arguments are");
                }
                if (named()) {
                    throw unsupported("named arguments to scalar functions are");
                }
                args.add(expr());
            } while (accept(Kind.COMMA));
        }
        List<SqlAst.SortItem> orderBy = new ArrayList<>();
        if (!star && acceptWord("order")) {
            expectWord("by");
            do {
                orderBy.add(sortItem());
            } while (accept(Kind.COMMA));
        }
        expect(Kind.RPAREN);
        List<SqlAst.SortItem> withinGroup = new ArrayList<>();
        if (acceptWord("within")) {
            expectWord("group");
            expect(Kind.LPAREN);
            expectWord("order");
            expectWord("by");
            do {
                withinGroup.add(sortItem());
            } while (accept(Kind.COMMA));
            expect(Kind.RPAREN);
        }
        SqlAst.Expr filter = null;
        if (acceptWord("filter")) {
            expect(Kind.LPAREN);
            expectWord("where");
            filter = expr();
            expect(Kind.RPAREN);
        }
        SqlAst.FunctionCall call = new SqlAst.FunctionCall(name, args, distinct, star, orderBy, withinGroup, filter,
                first.pos());
        if (acceptWord("over")) {
            Token over = tokens.get(at - 1);
            if (accept(Kind.LPAREN)) {
                return new SqlAst.Window(call, windowSpec(over.pos()), first.pos());
            }
            SqlAst.Ident named = ident(expectName());
            return new SqlAst.Window(call, new SqlAst.WindowSpec(named, List.of(), List.of(), null, named.pos()),
                    first.pos());
        }
        return call;
    }

    private SqlAst.Expr caseExpr() {
        SqlAst.Pos pos = expectWord("case").pos();
        SqlAst.Expr operand = peek().is("when") ? null : expr();
        List<SqlAst.When> whens = new ArrayList<>();
        while (acceptWord("when")) {
            SqlAst.Expr condition = expr();
            expectWord("then");
            whens.add(new SqlAst.When(condition, expr()));
        }
        if (whens.isEmpty()) {
            throw unexpected();
        }
        SqlAst.Expr otherwise = acceptWord("else") ? expr() : null;
        expectWord("end");
        return new SqlAst.Case(operand, whens, otherwise, pos);
    }

    /** A type name: its words ({@code double precision}, {@code timestamp with time zone}) and its modifiers
     *  ({@code numeric(10,2)}); an array type is refused. */
    private SqlAst.TypeName typeName() {
        Token first = peek();
        StringBuilder name = new StringBuilder();
        if (first.kind() == Kind.QUOTED_IDENT) {
            name.append(next().text());
        } else {
            String word = expectWord().toLowerCase(Locale.ROOT);
            name.append(word);
            switch (word) {
                case "double" -> name.append(' ').append(expectWord("precision").text().toLowerCase(Locale.ROOT));
                case "character", "char" -> {
                    if (acceptWord("varying")) {
                        name.append(" varying");
                    }
                }
                default -> {
                }
            }
        }
        List<Integer> modifiers = new ArrayList<>();
        if (accept(Kind.LPAREN)) {
            do {
                Token n = expect(Kind.INTEGER);
                modifiers.add(Integer.parseInt(n.text()));
            } while (accept(Kind.COMMA));
            expect(Kind.RPAREN);
        }
        String base = name.toString();
        if ((base.equals("timestamp") || base.equals("time")) && (peek().is("with") || peek().is("without"))) {
            name.append(' ').append(next().text().toLowerCase(Locale.ROOT));
            expectWord("time");
            expectWord("zone");
            name.append(" time zone");
        }
        if (peek().kind() == Kind.LBRACKET || peek().is("array")) {
            throw unsupported("array types are");
        }
        return new SqlAst.TypeName(name.toString(), modifiers, first.pos());
    }

    // ------------------------------------------------------------------ names

    private List<SqlAst.Ident> qualifiedName() {
        List<SqlAst.Ident> name = new ArrayList<>();
        name.add(ident(expectName()));
        while (accept(Kind.DOT)) {
            name.add(ident(expectName()));
        }
        return name;
    }

    /** An alias after {@code AS}: any identifier, key words included (Postgres's {@code ColLabel}). */
    private SqlAst.Ident label() {
        Token t = peek();
        if (t.kind() == Kind.IDENT || t.kind() == Kind.QUOTED_IDENT) {
            return ident(next());
        }
        throw unexpected();
    }

    /** An alias without {@code AS}: any identifier or key word but the {@code AS_LABEL} ones (Postgres's
     *  {@code BareColLabel}). */
    private static boolean isBareLabel(Token t) {
        return t.kind() == Kind.QUOTED_IDENT
                || t.kind() == Kind.IDENT && !AS_LABEL.contains(t.text().toLowerCase(Locale.ROOT));
    }

    private static boolean isKeyword(Token t) {
        if (t.kind() != Kind.IDENT) {
            return false;
        }
        String word = t.text().toLowerCase(Locale.ROOT);
        return RESERVED.contains(word) || TYPE_FUNC_NAME.contains(word);
    }


    /** An identifier usable as a name: quoted, or unquoted and not reserved. */
    private static boolean isName(Token t) {
        return t.kind() == Kind.QUOTED_IDENT
                || t.kind() == Kind.IDENT && !RESERVED.contains(t.text().toLowerCase(Locale.ROOT));
    }

    private static SqlAst.Ident ident(Token t) {
        return new SqlAst.Ident(t.text(), t.kind() == Kind.QUOTED_IDENT, t.pos());
    }

    private Token expectName() {
        Token t = peek();
        if (isName(t)) {
            at++;
            return t;
        }
        throw unexpected();
    }

    // ------------------------------------------------------------------ tokens

    private Token peek() {
        return tokens.get(at);
    }

    private Token peekAt(int ahead) {
        return tokens.get(Math.min(at + ahead, tokens.size() - 1));
    }

    private Token next() {
        Token t = tokens.get(at);
        if (t.kind() != Kind.EOF) {
            at++;
        }
        return t;
    }

    private boolean accept(Kind kind) {
        if (peek().kind() == kind) {
            at++;
            return true;
        }
        return false;
    }

    private Token expect(Kind kind) {
        if (peek().kind() == kind) {
            return next();
        }
        throw unexpected();
    }

    private boolean acceptWord(String word) {
        if (peek().is(word)) {
            at++;
            return true;
        }
        return false;
    }

    private Token expectWord(String word) {
        if (peek().is(word)) {
            return next();
        }
        throw unexpected();
    }

    private String expectWord() {
        if (peek().kind() == Kind.IDENT) {
            return next().text();
        }
        throw unexpected();
    }

    private SqlSyntaxException unexpected() {
        Token t = peek();
        if (t.kind() == Kind.EOF) {
            return new SqlSyntaxException("syntax error at end of input", t.pos());
        }
        return new SqlSyntaxException("syntax error at or near \"" + t.text() + "\"", t.pos());
    }

    /** The units an INTERVAL constant may name after its text (INTERVAL '3' DAY). */
    private static final java.util.Set<String> INTERVAL_UNITS = java.util.Set.of("year", "month", "day", "hour",
            "minute", "second");

    private SqlSyntaxException unsupported(String what) {
        return SqlSyntaxException.unsupported(what + " not supported yet", peek().pos());
    }

    private SqlSyntaxException error(String message) {
        return new SqlSyntaxException(message, peek().pos());
    }
}
