// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.cypher;

import com.legend.protocol.sql.SqlAst.Pos;

import java.util.List;
import java.util.Map;

/**
 * The syntax of a Cypher query as {@link CypherParser} reads it (docs/CYPHER_STUDY_2026_10_15.md §5.1): records with
 * positions, no semantics. The spec is Neo4j's Cypher manual for the pinned release, Cypher 25 by default and
 * Cypher 5 when the text says {@code CYPHER 5}. Every clause has a record so that a clause the translator does not
 * read yet is refused by name after parsing, never as a syntax error.
 */
public final class CypherAst {

    private CypherAst() {
    }

    /** A query: the language version its {@code CYPHER n} prefix names (null for the default), then its clauses. */
    public record Statement(@com.legend.base.Nullable Integer version, List<Clause> clauses, Pos pos) {
    }

    // ------------------------------------------------------------------ clauses

    public sealed interface Clause permits Match, With, Return, Unwind, Use, ProcedureCall, OtherClause {
        Pos pos();
    }

    /** {@code CALL name[(args)] [YIELD * | item, ...]} (the study's §12.60): a procedure of the model, called standalone
     *  or over the rows in hand. {@code args} is null when the call is written without parentheses (its arguments
     *  implicit: the query's parameters); {@code yields} null when nothing is yielded, {@code yieldAll} for
     *  {@code YIELD *}. */
    public record ProcedureCall(String name, @com.legend.base.Nullable List<Expr> args,
            @com.legend.base.Nullable List<YieldItem> yields, boolean yieldAll, Pos pos) implements Clause {
    }

    /** A yielded output of a procedure, under its alias when written. */
    public record YieldItem(String name, @com.legend.base.Nullable String alias, Pos pos) {
    }

    /** {@code [OPTIONAL] MATCH pattern, ... [WHERE expr]}. */
    public record Match(boolean optional, List<PathPattern> patterns, @com.legend.base.Nullable Expr where, Pos pos)
            implements Clause {
    }

    /** {@code WITH [DISTINCT] items [ORDER BY ...] [SKIP n] [LIMIT n] [WHERE expr]}. */
    public record With(boolean distinct, List<Item> items, List<SortItem> orderBy, @com.legend.base.Nullable Expr skip,
            @com.legend.base.Nullable Expr limit, @com.legend.base.Nullable Expr where, Pos pos) implements Clause {
    }

    /** {@code RETURN [DISTINCT] items [ORDER BY ...] [SKIP n] [LIMIT n]}. */
    public record Return(boolean distinct, List<Item> items, List<SortItem> orderBy,
            @com.legend.base.Nullable Expr skip, @com.legend.base.Nullable Expr limit, Pos pos) implements Clause {
    }

    /** {@code USE graph}: the graph (a data space's path) the query runs over. */
    /** {@code UNWIND list AS variable}. */
    public record Unwind(Expr list, String variable, Pos pos) implements Clause {
    }

    public record Use(String graph, Pos pos) implements Clause {
    }

    /** A clause the reader recognises by its keyword and does not read yet ({@code UNWIND}, {@code CREATE},
     *  {@code CALL}, ...): its keyword, as written, and its text up to the next clause. The translator refuses it by
     *  name. */
    public record OtherClause(String keyword, String text, Pos pos) implements Clause {
    }

    /** A projected item: its expression, its alias when written, and its text as written (Neo4j names an unaliased
     *  item by its text). A {@code *} item has a null expression. */
    public record Item(@com.legend.base.Nullable Expr expr, @com.legend.base.Nullable String alias, String text,
            Pos pos) {
    }

    public record SortItem(Expr expr, boolean descending, Pos pos) {
    }

    // ------------------------------------------------------------------ patterns

    /** A path pattern, optionally named: nodes and relationships alternating, a node first and last. */
    public record PathPattern(@com.legend.base.Nullable String variable, List<PatternElement> elements, Pos pos) {
    }

    public sealed interface PatternElement permits NodePattern, RelPattern {
        Pos pos();
    }

    /** {@code (v:Label1:Label2 {key: expr})}; labels as written, including a backticked one's text. */
    public record NodePattern(@com.legend.base.Nullable String variable, List<String> labels,
            Map<String, Expr> properties, Pos pos) implements PatternElement {
    }

    public enum Direction { OUT, IN, BOTH }

    /** {@code -[v:T1|T2 {key: expr}]->}, {@code <-[...]-} or {@code -[...]-}; {@code lengthRange} is non-null for a
     *  variable-length relationship ({@code *}, {@code *2}, {@code *1..3}: the bounds, -1 for an unwritten one). */
    public record RelPattern(@com.legend.base.Nullable String variable, List<String> types, Direction direction,
            Map<String, Expr> properties, @com.legend.base.Nullable LengthRange lengthRange, Pos pos)
            implements PatternElement {
    }

    public record LengthRange(int lower, int upper) {
    }

    // ------------------------------------------------------------------ expressions

    public sealed interface Expr permits IntLit, FloatLit, StringLit, BoolLit, NullLit, Var, Prop, Param, ListLit,
            MapLit, Unary, Binary, Call, CountStar, Case, Paren, Quantifier, ListComprehension, PatternComprehension,
            LabelPredicate, Index, Slice, PatternPredicate, ExistsSubquery {
        Pos pos();
    }

    /** A pattern as a predicate ({@code WHERE (n)-[:T]->()}, {@code RETURN (n)-->() AS has}): true where the
     *  pattern matches from its bound node (docs/CYPHER_STUDY_2026_10_15.md §12.40). */
    public record PatternPredicate(PathPattern pattern, Pos pos) implements Expr {
    }

    /** {@code EXISTS { MATCH pattern WHERE where RETURN ... }}, or {@code EXISTS { pattern }}: true where the pattern
     *  has a match (the items returned do not matter; the subquery's one pattern, one optional WHERE); with a
     *  {@code counted} part, {@code EXISTS { MATCH pattern WITH keys, count(*) AS alias WHERE having RETURN ... }}
     *  (docs/CYPHER_STUDY_2026_10_15.md §12.77). */
    public record ExistsSubquery(PathPattern pattern, @com.legend.base.Nullable Expr where, List<Expr> items,
            @com.legend.base.Nullable Counted counted, Pos pos) implements Expr {
        public ExistsSubquery(PathPattern pattern, @com.legend.base.Nullable Expr where, List<Expr> items, Pos pos) {
            this(pattern, where, items, null, pos);
        }
    }

    /** An EXISTS subquery's {@code WITH keys, count(*) AS alias [WHERE having]}: the matches grouped by the keys
     *  (variables), counted, the groups kept where {@code having} holds. */
    public record Counted(List<String> keys, String alias, @com.legend.base.Nullable Expr having, Pos pos) {
    }

    public record IntLit(long value, String text, Pos pos) implements Expr {
    }

    public record FloatLit(double value, String text, Pos pos) implements Expr {
    }

    public record StringLit(String value, Pos pos) implements Expr {
    }

    public record BoolLit(boolean value, Pos pos) implements Expr {
    }

    public record NullLit(Pos pos) implements Expr {
    }

    public record Var(String name, Pos pos) implements Expr {
    }

    /** {@code expr.name}. */
    public record Prop(Expr target, String name, Pos pos) implements Expr {
    }

    /** {@code $name}. */
    public record Param(String name, Pos pos) implements Expr {
    }

    public record ListLit(List<Expr> elements, Pos pos) implements Expr {
    }

    public record MapLit(Map<String, Expr> entries, Pos pos) implements Expr {
    }

    public enum UnaryOp { NEG, POS, NOT, IS_NULL, IS_NOT_NULL }

    public record Unary(UnaryOp op, Expr operand, Pos pos) implements Expr {
    }

    public enum BinaryOp {
        OR, XOR, AND, EQ, NE, LT, LE, GT, GE, PLUS, MINUS, TIMES, DIVIDE, MODULO, POWER,
        STARTS_WITH, ENDS_WITH, CONTAINS, IN, REGEX
    }

    public record Binary(BinaryOp op, Expr left, Expr right, Pos pos) implements Expr {
    }

    /** {@code name(args)}, {@code name(DISTINCT args)}; the name as written (Cypher's function names are
     *  case-insensitive), with its namespace parts ({@code duration.between}). */
    public record Call(String name, boolean distinct, List<Expr> args, Pos pos) implements Expr {
    }

    /** {@code count(*)}. */
    public record CountStar(Pos pos) implements Expr {
    }

    /** {@code CASE [operand] WHEN c THEN r ... [ELSE e] END}. */
    public record Case(@com.legend.base.Nullable Expr operand, List<When> whens,
            @com.legend.base.Nullable Expr elseResult, Pos pos) implements Expr {
    }

    public record When(Expr condition, Expr result) {
    }

    /** {@code (expr)}: kept so that an item's text and a pattern's reading see the parentheses. */
    public record Paren(Expr inner, Pos pos) implements Expr {
    }

    /** {@code all(x IN list WHERE predicate)}, {@code any}, {@code none}, {@code single}: the name as written. */
    public record Quantifier(String name, String variable, Expr list, Expr where, Pos pos) implements Expr {
    }

    /** {@code [x IN list WHERE predicate | expression]}: the WHERE and the projection each optional. */
    public record ListComprehension(String variable, Expr list, @com.legend.base.Nullable Expr where,
            @com.legend.base.Nullable Expr map, Pos pos) implements Expr {
    }

    /** {@code [p = (a)-[:T]->(b) WHERE predicate | expression]}: a pattern comprehension, the list of
     *  {@code expression} over every match of the pattern from the bound nodes it names. */
    public record PatternComprehension(PathPattern pattern, @com.legend.base.Nullable Expr where, Expr map, Pos pos)
            implements Expr {
    }

    /** {@code n:Label:Other}: a label predicate, true when the node carries every label. */
    public record LabelPredicate(Expr target, List<String> labels, Pos pos) implements Expr {
    }

    /** {@code target[index]}: a list's element or a map's entry. */
    public record Index(Expr target, Expr index, Pos pos) implements Expr {
    }

    /** {@code target[from..to]}: a list's slice, either bound optional. */
    public record Slice(Expr target, @com.legend.base.Nullable Expr from, @com.legend.base.Nullable Expr to, Pos pos)
            implements Expr {
    }
}
