// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CDate;
import com.legend.protocol.spec.CDecimal;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.InputDialect;
import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlSyntaxException;
import com.legend.protocol.sql.SqlToPure;
import com.legend.protocol.sql.SqlToPure.Kind;
import com.legend.protocol.sql.SqlToPure.Tx;
import com.legend.protocol.sql.SqlTranslationException;
import com.legend.values.PureDateLiteral;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SAP IQ 16.2 as {@code SqlToPure} reads it: the {@code #IQ{...}#} island's dialect (docs/IQ_SQL_STUDY_2026_10_13.md
 * §4-§5), under a vector of IQ's settings ({@link IqSettings}) and one of IQ's two {@link Engine}s. Its reader is
 * {@link IqParser}, its types {@link IqType} and {@link IqTypes}; its rules, so far (M0):
 * <ul>
 *   <li><b>Names</b> match without regard to ASCII case, quoted or not (IQ's identifiers are case-insensitive, and fold
 *       A-Z only); a name two candidates match is an ambiguity. An unquoted alias keeps its case. A column shows as
 *       its reference is written ({@code SELECT ID} is a column {@code ID}); any other unaliased item as IQ names it,
 *       its own rendering of the item ({@code 1.50} shows as {@code 1.5}, {@code 'a'} as {@code 'a'}), or the item is
 *       refused where that rendering is not known. A select-list alias is visible to WHERE, to the select list and
 *       inside an ORDER BY expression in IQ, before a column of its name: a reference that would read one is refused,
 *       not yet read (a bare ORDER BY name reads the select list first, as IQ does).</li>
 *   <li><b>NULL</b> is the lowest value: first ascending, last descending. A bare NULL is a SMALLINT NULL. Under
 *       {@code ansinull} Off (a TDS login), {@code x = NULL} is {@code x IS NULL} and {@code x != NULL} is
 *       {@code x IS NOT NULL} in every search condition, but SQL Anywhere's front end leaves the conditions of a CASE
 *       or an IF UNKNOWN (measured) where the IQ engine rewrites them too (the research's D-L1); any other comparison
 *       with NULL is UNKNOWN.</li>
 *   <li><b>Strings</b> compare case-sensitively in an IQ profile's database, ignoring trailing blanks (BLANK PADDING
 *       ON): {@code =} holds when one value starts with the other and the rest is spaces; {@code <} and the other
 *       orderings compare the two without their trailing spaces (IQ ignores the blanks before comparing, which no
 *       padding reproduces for characters below the space: {@code 'a' < 'a\t'}). A {@code sqla17} profile's
 *       database compares them by their ASCII letters folded to lower case ({@code '_' < 'a'}), trailing blanks
 *       significant ({@link #collated}; IQ island, 2026-10-14: both composed of existing natives, the study's O1).
 *       ORDER BY sorts strings by the same collation key, COUNT(DISTINCT) counts them by it, and a GROUP BY key the
 *       query does not show groups by it; DISTINCT, a GROUP BY key shown, UNION, INTERSECT, EXCEPT, MIN, MAX and LIST
 *       read strings only where each value is its own key ({@link #key}, {@link IqKeys}; IQ island, 2026-10-14, the
 *       merge of M1c and the natives slice). Under the IQ engine a CHAR(n) column's value is padded with
 *       blanks to n (the research's D-S1), which the source read computes once, so every reference sees it.
 *       {@code ||} joins two values as text, a single NULL among them read as '' and all NULL giving NULL (the
 *       research's D-S3); {@code +} of two strings does the same, while a string and a number (or an untyped NULL)
 *       is IQ's NUMERIC addition, refused until the conversions land.</li>
 *   <li><b>BIT</b> is an integer, 0 or 1, in IQ, not a truth value: a BIT column is read as one, so it compares with
 *       integers, sorts, and computes as IQ's BIT does.</li>
 *   <li><b>Integers</b> compute as IQ's do ({@link IqTypes}): each constant and result typed SMALLINT, INTEGER or
 *       BIGINT, a result that leaves its type's range IQ's error -158 (checked with Pure's own natives: an empty value
 *       read as one raises, as {@code SqlToPure}'s range checks do), {@code /} truncating toward zero, {@code %}
 *       keeping the dividend's sign, a zero divisor IQ's error -628 (DIVIDE_BY_ZERO_ERROR's default). The database
 *       computes each operation in 64 bits ({@code floor}, Pure's integer of a number, before each operand
 *       that might be narrower), so only IQ's own ranges bound a result. Arithmetic on a string or a date is
 *       refused by name, and so are the integer results IQ types NUMERIC or unsigned. A constant result out of its
 *       type is IQ's error at translation only when every operand is a constant IQ folds; a BIGINT result is the
 *       database's own overflow; an expression whose checks would write out more Pure than {@link #LARGEST} nodes is
 *       refused.</li>
 *   <li><b>CASE, IF, ISNULL, COALESCE, IFNULL, NULLIF</b>: one type for their values by IQ's unification
 *       ({@link #unify}), where it is one the island can prove; {@code IFNULL(a, b)} is NULL when {@code a} is not
 *       (IQ's, not COALESCE's); {@code NULLIF(a, b)} reads {@code b} as {@code a}'s type.</li>
 *   <li><b>Row limits</b>: {@code TOP n} and {@code START AT m}, each a constant IQ types and checks (a literal TOP 0,
 *       a negative count or a START AT below 1 is IQ's error -674, the research's D-Q1), which the planner folds. A
 *       row limit over a FROM clause needs an ORDER BY, and its select list and ORDER BY no operation that can raise
 *       (IQ computes them only for the rows it keeps): both refused by name.</li>
 *   <li><b>SQL Anywhere's own relations</b> (M1a): {@code DUMMY} and {@code sa_rowgenerator(...)} ({@link #source}), a
 *       statement over them alone running in SQL Anywhere's front end ({@link #engineOf}).</li>
 *   <li><b>Comparisons</b> (M1a) by IQ's conversions ({@link #compared}): a string with a number as NUMERIC (in SQL
 *       Anywhere's front end, for a constant string; the IQ engine's NUMERIC is D-N1), with a date as a timestamp (a
 *       constant in ISO spelling), an exact number with a DOUBLE as a DOUBLE. {@code BETWEEN}, {@code IN} and
 *       {@code IS [NOT] DISTINCT FROM} compare as {@code =}, {@code <=} and {@code >=} do, in three-valued logic; a
 *       NULL in an IN list is UNKNOWN whatever ansinull says. {@code LIKE} ({@link #like}) without {@code [set]}
 *       classes, case-sensitively, trailing blanks significant.</li>
 *   <li><b>NUMERIC arithmetic</b> (M1a) in SQL Anywhere's front end only, {@code + - *} at the research's TYPING §4
 *       precision and scale ({@link #decimalArithmetic}), and (IQ island, 2026-10-14) {@code /} and {@code %} exactly
 *       as SQL Anywhere computes them, a quotient where it is shown ({@link #quotient}); <b>DOUBLE arithmetic</b> in
 *       both engines, IEEE as every database computes it, past the largest DOUBLE or by zero IQ's error
 *       ({@link #doubleArithmetic}).</li>
 *   <li><b>An unaliased item's name</b> (M1b): SQL Anywhere's rendering of it ({@link IqNames}, measured), a column
 *       qualified by its FROM item's name; refused where the statement shows a name that is not known.</li>
 *   <li><b>Relations</b> (M1b): joins by comma, [INNER], LEFT, RIGHT, FULL and CROSS JOIN with IQ's comparisons in ON;
 *       NATURAL JOIN keeps both sides' columns ({@link #mergesNaturalJoin}); KEY JOIN refused. Derived tables name each
 *       column; an unaliased table function {@code tb('...t')} is known by its table's name. Subqueries -- scalar,
 *       EXISTS, IN, ANY/SOME/ALL ({@link #quantifiedTest}, IQ's comparisons) -- where IQ's engine takes them
 *       ({@link #subqueryPlaces}). UNION, EXCEPT and INTERSECT with IQ's unification of each column
 *       ({@link #setColumn}) and IQ's ORDER BY rules ({@link #setOrder}). The FROM clause's and the query's structural
 *       errors IQ raises before running ({@link #fromClause}, {@link #bound}) are raised before translating.</li>
 *   <li><b>Names bound outside the SQL</b> (IQ island, 2026-10-14, M1e): an owner-qualified table to the model's
 *       tables ({@link #table}), a host variable to the enclosing Pure query's variable ({@link #parameter}), both
 *       {@link IqBindings}'; a constant date string read by DATE_ORDER and a date written as text by the profile's
 *       formats, {@link IqDateTime}'s.</li>
 * </ul>
 * Refusals that IQ makes too carry IQ's SQLCODE and SQLSTATE beside the message, which is this dialect's own wording.
 */
public final class IqDialect implements InputDialect {

    /** Which rules a statement follows (the research's SEMANTICS §2): IQ's engine for a query over IQ tables, SQL
     *  Anywhere's front end (IQ's own) for a query without FROM. Only the typing and function layer reads it. */
    public enum Engine { IQ, SA }

    /** The store types a column is read as stored in: every other refuses its table by name (CHAR is padded and BIT
     *  read as an integer, below). */
    private static final Set<String> READ_AS_STORED = Set.of("SMALLINT", "INTEGER", "BIGINT", "TINYINT", "DECIMAL",
            "NUMERIC", "DOUBLE", "VARCHAR", "DATE", "TIMESTAMP");

    private final IqSettings settings;
    private final @com.legend.base.Nullable Engine engine;
    /** The comparisons that are (part of) a CASE's or an IF's condition, in the statement being read: SQL Anywhere's
     *  front end does not rewrite {@code = NULL} there under ansinull Off. */
    private final Set<SqlAst.Expr> caseConditions;
    /** The names (ASCII lower case) the statement being read refers to a column by: a source column read otherwise
     *  ({@link #read}) is read so only where it is referred to, or where its source is in the FROM clause of a query
     *  with a star ({@link #starred}); null when every column is read so. */
    private final @com.legend.base.Nullable Set<String> referenced;
    /** The places of the FROM items (tables, functions) of the queries with a star in their select list: each of
     *  their columns is read as the dialect reads it ({@link #read}), referred to or not. */
    private final Set<SqlAst.Pos> starred;
    /** The select items whose names the statement shows (its first branch's, and each derived table's): one IQ's
     *  name for is not known is refused there; anywhere else (a subquery's column) no one sees the name. */
    private final Set<SqlAst.SelectExpr> shown;
    /** The correlation names (ASCII lower case) of the statement's derived tables: SQL Anywhere names an expression
     *  over their columns by the columns' definitions, which is not rendered. */
    private final Set<String> derived;
    /** IQ island, 2026-10-14 (M1c): those of them over a set operation or a grouped query, whose column an aggregate
     *  names as {@code d.x} (measured; SQL Anywhere merges a plain SELECT into its query and names the column by its
     *  definition, {@code COUNT(null as a)}). */
    private final Set<String> setDerived;
    /** The names given so far to unaliased items no one sees (null where IQ's is not known): a parenthesized query
     *  shown as a select item is named by its one item's. */
    private final Map<SqlAst.SelectExpr, String> given = new IdentityHashMap<>();
    /** IQ island, 2026-10-14 (M1c): the names of the statement's grouped queries' items
     *  ({@link IqAggregates.Naming}). */
    private final IqAggregates.Naming naming;
    /** The texts the trees {@link #parse} read do not keep (M1c: their GROUP BY items' and aggregates', by identity),
     *  for {@link #reading}. */
    private final Map<SqlAst.Expr, String> spans = new IdentityHashMap<>();
    /** IQ island, 2026-10-14 (the third landing): the names inside a subquery that an enclosing SELECT's alias may be
     *  ({@link IqAliases}), by identity: read as their own query's columns only ({@link #columnNotHere}). */
    private final Set<SqlAst.ColumnRef> outerReads = Collections.newSetFromMap(new IdentityHashMap<>());
    /** IQ island, 2026-10-17: the outer joins IQ's engine reads as inner ones ({@link #nullRejected}), by identity,
     *  for {@link #joinType}. */
    private final Set<SqlAst.Join> innerJoins = Collections.newSetFromMap(new IdentityHashMap<>());
    /** IQ island, 2026-10-17: the calls of no function of IQ's in columns read nowhere, read as a NULL string
     *  ({@link #unreadUnknownFunctions}). */
    private final Set<SqlAst.FunctionCall> placeheld = Collections.newSetFromMap(new IdentityHashMap<>());
    /** The error held while the statement is translated ({@link #hold}), raised once it is ({@link #translated}). */
    private final SqlTranslationException[] held = {null};
    /** A subquery's placement error, named before translating and held while it is ({@link #failed}). */
    private final SqlTranslationException[] placement = {null};
    /** IQ island, 2026-10-17: each statement's WITH RECURSIVE error ({@link #recursion}), by identity, for
     *  {@link #reading}; the statement's, raised by {@link #failed} and {@link #translated}. */
    private final Map<SqlAst.Statement, Recursive> recursives = new IdentityHashMap<>();
    /** IQ island, 2026-10-17: each statement {@link #parse} read whose ROLLUP or CUBE reads a DISTINCT aggregate under
     *  IQ's engine, by identity, with that error ({@link #rollupDistinct}), raised late ({@link #executed}). */
    private final Map<SqlAst.Statement, SqlTranslationException> rolledDistinct = new IdentityHashMap<>();
    private final Recursive[] recursive = {null};
    /** The comparisons simple CASEs' WHENs read as (IqParser.Parsed#simpleWhens), by identity ({@link #simpleWhen}). */
    final Set<SqlAst.Expr> simpleWhens = Collections.newSetFromMap(new IdentityHashMap<>());
    /** IQ island, 2026-10-17: an error IQ raises as it runs the statement, after every other one
     *  ({@link #windowKeyComparisons}), raised once the statement is translated. */
    private final SqlTranslationException[] executed = {null};
    /** The statement's calls of no function of IQ's, in order, but those read as a NULL string ({@link #failed}). */
    private final List<SqlAst.FunctionCall> unknownCalls = new ArrayList<>();
    /** IQ island, 2026-10-17: the text each concatenation of string literals alone spells, by its value's identity
     *  ({@link #dateOf}). */
    private final Map<ValueSpecification, String> literalJoins = new IdentityHashMap<>();
    /** IQ island, 2026-10-14 (the third landing): the aliases (lower case) each unaliased select item reads
     *  ({@link IqAliases}), by identity: its name shows them as written. */
    private final Map<SqlAst.SelectExpr, Set<String>> aliasesRead = new IdentityHashMap<>();
    /** IQ island, 2026-10-14 (review 7's B2): the unaliased items read from a query without FROM, named by that
     *  query's own alias; and those that name no derived table's column ({@link IqAliases}), by identity. */
    private final Map<SqlAst.SelectExpr, SqlAst.Ident> innerNames = new IdentityHashMap<>();
    private final Set<SqlAst.SelectExpr> nameless = Collections.newSetFromMap(new IdentityHashMap<>());
    /** IQ island, 2026-10-14 (the third landing): the SELECTs of the statement no one reads the output names of -- an
     *  EXISTS subquery's, a set operation's branches but its first, a derived table's whose column list names each of
     *  its columns (measured: SQL Anywhere answers them with two columns of one name) -- by identity
     *  ({@link #namesUnread}). */
    private final Set<SqlAst.Select> unreadNames = Collections.newSetFromMap(new IdentityHashMap<>());
    /** IQ island, 2026-10-14 (the third landing): each row limit's count (TOP's, START AT's) by identity: whether its
     *  query keeps rows of its plan's choosing (a FROM, no ORDER BY, not an EXISTS subquery's) and whether it has a
     *  TOP count ({@link #rowCount}). */
    private final Map<SqlAst.Expr, boolean[]> limits = new IdentityHashMap<>();
    /** IQ island, 2026-10-14 (review 7's M1): the column references the statement's queries read in their select lists,
     *  GROUP BY and ORDER BY outside an aggregate, by identity ({@link #enclosingColumn}). */
    private final Set<SqlAst.ColumnRef> computedReads = Collections.newSetFromMap(new IdentityHashMap<>());
    /** IQ island, 2026-10-14 (review 7's m1): the statement's queries that have an ORDER BY, by identity. */
    private final Set<SqlAst.Statement> ordered = Collections.newSetFromMap(new IdentityHashMap<>());
    /** IQ island, 2026-10-14 (the third landing): the column references of the statement's ORDER BY clauses, by
     *  identity (a grouped query's column no key reads is IQ's -854 there, -149 elsewhere). */
    private final Set<Object> orderRefs = Collections.newSetFromMap(new IdentityHashMap<>());
    /** IQ island, 2026-10-17: the columns a grouped query's subqueries read of it inside one of its GROUP BY
     *  expressions ({@code month(p1.CreationDate)} beside {@code GROUP BY month(CreationDate)}), by identity: IQ's
     *  -1005028, which matches no expression across a subquery's edge. */
    private final Set<Object> outerGroupRefs = Collections.newSetFromMap(new IdentityHashMap<>());
    /** IQ island, 2026-10-14 (review 7's m2): the select-list and HAVING references of a grouped SELECT whose ORDER BY
     *  reads a column not grouped ({@link IqAggregates#ordersUngrouped}), which IQ reports first. */
    private final Set<Object> orderFirst = Collections.newSetFromMap(new IdentityHashMap<>());
    /** IQ island, 2026-10-14 (natives): the expressions the statement shows as they are -- its select items, when it
     *  is one SELECT without DISTINCT (not {@link #shown}, the items whose names it shows: a merge of M1b's and the
     *  natives slice's fields of one name). SQL Anywhere rounds a NUMERIC quotient to its scale only where it shows it:
     *  an operand, a CAST or a comparison reads more digits ({@link #quotient}). */
    private final Set<SqlAst.Expr> shownValues;
    /** IQ island, 2026-10-14 (the merge of M1b and the natives slice): the names (ASCII lower case) of derived tables'
     *  columns SQL Anywhere may type by a folded constant ({@link #folded}); a quotient over one is refused. */
    private final Set<String> folded;
    /** IQ island, 2026-10-14 (the merge of M1c and the natives slice): which of the statement's strings are their own
     *  collation keys ({@link IqKeys}). */
    private final IqKeys keys;
    /** IQ island, 2026-10-14 (review 3's B1): the shapes ({@link IqAggregates#shape}) of the aggregate calls the
     *  statement reads conditionally ({@link #lazyAggregates}). */
    private final Set<String> lazy;
    /** IQ island, 2026-10-14 (review 3's M1): what SQL Anywhere does with the parentheses of each select item's
     *  constant chain, by its query's FROM ({@link IqNames.Chains}). */
    private final Map<SqlAst.SelectExpr, IqNames.Chains> chains;
    /** IQ island, 2026-10-14 (M1e): the host variables read so far as the enclosing Pure query's variables (not as a
     *  column of their name), by identity: an unaliased item over one is not named (IQ's name of it, measured with
     *  the value written in, is the value's). */
    private final Set<SqlAst.Param> variables = Collections.newSetFromMap(new IdentityHashMap<>());
    /** IQ island, 2026-10-14 (review 5's B2; review 6's B1): under ansinull Off, how each comparison of the statement
     *  with a host variable operand reads ({@link #twoValuedPlaces}), by identity. */
    private final Map<SqlAst.Expr, Reading> twoValued;
    /** IQ island, 2026-10-14 (review 6's B1): each SELECT of the statement as {@link IqAggregates#grouping} reads it
     *  (its GROUP BY and HAVING with their aliases' expressions in place), made once, so that the comparisons
     *  {@link #twoValuedPlaces} placed in a HAVING are the ones translated. */
    private final Map<SqlAst.Select, SqlAst.Select> grouped;
    /** IQ island, 2026-10-15: the statement's set operations that are a branch of another, by identity (an all-NULL
     *  column stays untyped there, {@link #setColumn}). */
    private final Set<SqlAst.SetOperation> innerSets;
    /** IQ island, 2026-10-15: arithmetic whose value an enclosing + - * (or a / as its dividend) reads, by identity: a
     *  DOUBLE there leaves its overflow to the chain's end ({@link #doubleArithmetic}). */
    private final Set<SqlAst.Expr> chained;
    /** IQ island, 2026-10-15: the IN and LIKE predicates a search condition (WHERE, HAVING, ON) reaches through AND, OR
     *  and NOT alone, by identity: where IQ's engine reads a NULL in them two-valued under ansinull Off. */
    private final Set<SqlAst.Expr> filters;
    /** IQ island, 2026-10-14 (the options framework): each statement {@link #parse} read after SET statements, by
     *  identity, with the dialect of the settings they made, which translates it ({@link #reading}). */
    private final Map<SqlAst.Statement, IqDialect> prefixed = new IdentityHashMap<>();

    private IqDialect(IqSettings settings, @com.legend.base.Nullable Engine engine, Set<SqlAst.Expr> caseConditions,
            @com.legend.base.Nullable Set<String> referenced, Set<SqlAst.SelectExpr> shown,
            Set<String> derived, Set<SqlAst.Pos> starred,
            IqAggregates.Naming naming, Set<String> setDerived, Set<SqlAst.Expr> shownValues, Set<String> folded,
            @com.legend.base.Nullable IqKeys keys, Set<String> lazy, Map<SqlAst.SelectExpr, IqNames.Chains> chains,
            Map<SqlAst.Expr, Reading> twoValued, Map<SqlAst.Select, SqlAst.Select> grouped,
            Set<SqlAst.SetOperation> innerSets, Set<SqlAst.Expr> chained, Set<SqlAst.Expr> filters) {
        this.settings = settings;
        this.filters = filters;
        this.innerSets = innerSets;
        this.chained = chained;
        this.twoValued = twoValued;
        this.grouped = grouped;
        this.naming = naming;
        this.setDerived = setDerived;
        this.starred = starred;
        this.engine = engine;
        this.caseConditions = caseConditions;
        this.referenced = referenced;
        this.shown = shown;
        this.derived = derived;
        this.shownValues = shownValues;
        this.folded = folded;
        this.keys = keys != null ? keys : IqKeys.none(settings);
        this.lazy = lazy;
        this.chains = chains;
    }

    /** The dialect of an island written for {@code settings}: each statement's engine chosen by its FROM clause
     *  ({@link #engineOf}, the study's I3: {@code engine=auto}). */
    public static IqDialect of(IqSettings settings) {
        return new IqDialect(settings, null, Set.of(), null, Set.of(), Set.of(), Set.of(),
                IqAggregates.Naming.none(), Set.of(), Set.of(), Set.of(), null, Set.of(), Map.of(), Map.of(),
                Map.of(), Set.of(), Set.of(), Set.of());
    }

    /** The dialect under {@code settings} with {@code engine}'s rules whatever the statement. */
    public static IqDialect of(IqSettings settings, Engine engine) {
        return new IqDialect(settings, engine, Set.of(), null, Set.of(), Set.of(), Set.of(),
                IqAggregates.Naming.none(), Set.of(), Set.of(), Set.of(), null, Set.of(), Map.of(), Map.of(),
                Map.of(), Set.of(), Set.of(), Set.of());
    }

    public IqSettings settings() {
        return settings;
    }

    /** The engine this dialect reads by; null while it is chosen per statement. */
    public @com.legend.base.Nullable Engine engine() {
        return engine;
    }

    /** The engine IQ runs {@code statement} in under {@code settings}: SQL Anywhere's front end when no query of it
     *  (a set operation's branch, a derived table, a subquery) reads a table, IQ's otherwise (a table of the island's
     *  is an IQ table; the research's D-E1 gives a mix to IQ). A {@code sqla17-*} profile is a SQL Anywhere database:
     *  no IQ engine there, whatever the FROM. IQ island, 2026-10-17 (the differential fuzzer's fifth round; measured
     *  on IQ 17.1): a statement over IQ tables that reads GREATER or LESSER anywhere runs in the front end too
     *  ({@link #readsFrontEndFunction}: CEILING(nm) beside one is a DOUBLE, it + 1 past an INTEGER its -158). */
    public static Engine engineOf(SqlAst.Statement statement, IqSettings settings) {
        Engine store = storeOf(statement, settings);
        return store == Engine.IQ && readsFrontEndFunction(statement) ? Engine.SA : store;
    }

    /** The engine of the store {@code statement}'s tables are in ({@link #engineOf} but for its functions). */
    static Engine storeOf(SqlAst.Statement statement, IqSettings settings) {
        if (!settings.iqDatabase()) {
            return Engine.SA;
        }
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.FromItem f : s.from()) {
                    for (SqlAst.FromItem leaf : leaves(f)) {
                        if (!(leaf instanceof SqlAst.SubqueryFrom) && !frontEndSource(leaf)) {
                            return Engine.IQ;
                        }
                    }
                }
            }
        }
        return Engine.SA;
    }

    /** Whether {@code statement} reads GREATER or LESSER: in any expression of any query of it but a derived table's
     *  column its enclosing query does not read (IQ drops that first: measured on IQ 17.1, 2026-10-17). */
    private static boolean readsFrontEndFunction(SqlAst.Statement statement) {
        Set<SqlAst.Expr> unread = Collections.newSetFromMap(new IdentityHashMap<>());
        for (SqlAst.Statement q : queries(statement)) {
            if (!(q instanceof SqlAst.Select outer)) {
                continue;
            }
            for (SqlAst.FromItem f : outer.from()) {
                for (SqlAst.FromItem leaf : leaves(f)) {
                    if (!(leaf instanceof SqlAst.SubqueryFrom d) || !(d.query() instanceof SqlAst.Select ds)) {
                        continue;
                    }
                    String alias = d.alias() == null ? null
                            : IqLexer.lower(java.util.Objects.requireNonNull(d.alias()).text());
                    if (outer.items().stream().anyMatch(i -> i instanceof SqlAst.AllColumns a && (a.qualifier() == null
                            || IqLexer.lower(java.util.Objects.requireNonNull(a.qualifier()).text()).equals(alias)))) {
                        continue;
                    }
                    Set<String> read = new java.util.HashSet<>();
                    for (SqlAst.Expr e : readExpressions(outer)) {
                        namesRead(e, alias, read);
                        List<SqlAst.Statement> subs = new ArrayList<>();
                        subqueries(e, subs);
                        for (SqlAst.Statement sub : subs) {
                            for (SqlAst.Statement inner : queries(sub)) {
                                if (inner instanceof SqlAst.Select is) {
                                    readExpressions(is).forEach(x -> namesRead(x, alias, read));
                                }
                            }
                        }
                    }
                    for (int i = 0; i < ds.items().size(); i++) {
                        String name = itemName(ds, i, d.columnAliases());
                        if (name != null && !read.contains(name) && ds.items().get(i) instanceof SqlAst.SelectExpr x) {
                            unread.add(x.expr());
                        }
                    }
                }
            }
        }
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.Expr e : readExpressions(s)) {
                    if (!unread.contains(e) && callsComparisonFunction(e)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /** Whether {@code e} calls GREATER or LESSER outside its subqueries. */
    private static boolean callsComparisonFunction(SqlAst.Expr e) {
        return e instanceof SqlAst.FunctionCall f && Set.of("greater", "lesser").contains(IqLexer.lower(
                f.name().get(f.name().size() - 1).text())) || children(e).stream().anyMatch(
                IqDialect::callsComparisonFunction);
    }

    /** Whether {@code item} is one of SQL Anywhere's own relations, which IQ's front end reads (the research's
     *  SEMANTICS §2.1: a statement that reads no IQ table runs there): {@code DUMMY} ({@code SYS.DUMMY}), or the
     *  system procedure {@code sa_rowgenerator} ({@code dbo.sa_rowgenerator}). */
    public static boolean frontEndSource(SqlAst.FromItem item) {
        return switch (item) {
            case SqlAst.TableName t -> dummy(t.name());
            case SqlAst.TableFunction f -> rowGenerator(f.name());
            default -> false;
        };
    }

    /** {@code DUMMY} or {@code SYS.DUMMY}, as IQ matches names (IQ island, 2026-10-14, M1e: a database's and a server's
     *  qualifiers ignored, {@code db..DUMMY} unqualified). */
    private static boolean dummy(List<SqlAst.Ident> name) {
        String last = IqLexer.lower(name.get(name.size() - 1).text());
        SqlAst.Ident owner = IqBindings.owner(name);
        return last.equals("dummy") && (owner == null || IqLexer.lower(owner.text()).equals("sys"));
    }

    /** {@code sa_rowgenerator} or {@code dbo.sa_rowgenerator}, as IQ matches names. */
    private static boolean rowGenerator(List<SqlAst.Ident> name) {
        String last = IqLexer.lower(name.get(name.size() - 1).text());
        return last.equals("sa_rowgenerator") && (name.size() == 1 || name.size() == 2
                && IqLexer.lower(name.get(0).text()).equals("dbo"));
    }

    /** IQ's code for a syntax error, as this dialect's messages carry it. */
    static String syntaxCode() {
        return code(-131, "42W04");
    }

    static String code(int sqlcode, String sqlstate) {
        return " (IQ: SQLCODE " + sqlcode + ", SQLSTATE " + sqlstate + ")";
    }

    /** IQ's code for a value outside its type's range. */
    private static String overflowCode() {
        return code(-158, "22003");
    }

    /** IQ's query: {@link IqParser}'s, with what the island refuses before translating it -- a FROM function other
     *  than the table source ({@code tb}), a function the catalog lacks (so no other dialect's reading of its name
     *  applies), two select-list aliases IQ cannot tell apart (its error -830), an alias read anywhere but as a bare
     *  ORDER BY key -- and, in each query of it (M1b), the FROM clause's errors IQ raises ({@link #fromClause}), a
     *  derived table's unnamed columns ({@link #derivedNames}), a set operation's ORDER BY ({@link #setOrder}), a row
     *  limit anywhere but on the statement's own SELECT. */
    @Override
    public SqlAst.Statement parse(String sql) {
        // IQ island, 2026-10-14 (the options framework): IQ's SET statements before the SELECT, each read under the
        // settings the ones before it made; the SELECT is read, and translated, under the last ones (by the dialect of
        // those settings, which reading() hands the statement to)
        IqSetPrefix.Prefix prefix = IqSetPrefix.read(sql, settings);
        if (prefix != null) {
            IqDialect under = engine == null ? of(prefix.settings()) : of(prefix.settings(), engine);
            SqlAst.Statement statement = under.parse(prefix.select());
            prefixed.put(statement, under);
            return statement;
        }
        // what the settings would need that is not built is refused
        settings.check();
        IqParser.Parsed read = IqParser.parsed(sql, settings);
        Engine statementEngine = engine != null ? engine : engineOf(read.statement(), settings);
        Recursive claim = recursion(read.recursion(), read.statement(), statementEngine);
        SqlTranslationException distinct = statementEngine == Engine.IQ ? rollupDistinct(read.statement()) : null;
        SqlAst.Statement statement;
        try {
            statement = checked(read, statementEngine);
        } catch (SqlTranslationException e) {
            // IQ island, 2026-10-17: what is found before translating that IQ raises after -706 or -922
            int code = codeOf(java.util.Objects.requireNonNullElse(e.getMessage(), ""));
            throw claim == null || claim.last() || code == 0 || claim.before().contains(code) ? e : claim.error();
        }
        if (claim != null) {
            recursives.put(statement, claim);
        }
        if (distinct != null) {
            rolledDistinct.put(statement, distinct);
        }
        return statement;
    }

    /** {@link #parse}'s checks of the statement {@code read} holds, and its rewrites: the statement translated. */
    private SqlAst.Statement checked(IqParser.Parsed read, Engine statementEngine) {
        SqlAst.Statement statement = read.statement();
        if (!settings.tsqlOuterJoins()) {
            // IQ island, 2026-10-15 (checklist 0.6, measured on IQ 17.1): *= and =* under tsql_outer_joins Off
            for (SqlAst.Statement q : queries(statement)) {
                if (q instanceof SqlAst.Select s) {
                    for (SqlAst.Expr e : expressions(s)) {
                        SqlAst.Operator o = tsqlOuterJoin(e);
                        if (o != null) {
                            throw SqlTranslationException.unsupported("the T-SQL outer join " + o.operator() + " under"
                                    + " tsql_outer_joins Off is IQ's error -1099 (Transact-SQL outer joins are"
                                    + " disabled), refused before translating", o.pos());
                        }
                    }
                }
            }
        }
        if (statementEngine != Engine.IQ) {
            frontEndStyles(statement);
        }
        simpleWhens.addAll(read.simpleWhens());
        for (IqParser.EngineCast cast : read.engineCasts()) {
            // IQ island, 2026-10-15 (checklist 0.5): a constant one engine raises for and the other converts
            // (IqFunctions.constantCast), refused under the engine that raises
            String refusal = statementEngine == Engine.IQ ? cast.refusals().iqEngine() : cast.refusals().frontEnd();
            if (refusal != null) {
                throw com.legend.protocol.sql.SqlSyntaxException.unsupported(refusal, cast.pos());
            }
        }
        for (SqlAst.Statement later : read.later()) {
            laterStatement(later);
        }
        if (!read.laterals().isEmpty()) {
            // IQ island, 2026-10-17 (the corpus lane's 4; measured on IQ 17.1): a lateral derived table is its engine's
            // -1001030; SQL Anywhere's front end reads one, which the island does not yet
            throw SqlTranslationException.unsupported(statementEngine == Engine.IQ ? "LATERAL in FROM is IQ's error"
                    + " -1001030 under its engine (a lateral derived table is not supported), refused before translating"
                    : "LATERAL in FROM is not supported yet under SQL Anywhere's front end", read.laterals().get(0));
        }
        if (!read.droppedOrderKeys().isEmpty() && statementEngine == Engine.IQ) {
            // IQ island, 2026-10-15 (measured on IQ 17.1): under IQ's engine ORDER BY (1) is the first column's
            // position, where SQL Anywhere's front end orders by nothing
            throw SqlTranslationException.unsupported("ORDER BY a parenthesized integer is a position under IQ's engine"
                    + " and a constant on SQL Anywhere's front end: write the position without parentheses", read
                    .droppedOrderKeys().get(0));
        }
        // IQ island, 2026-10-14 (the third landing): two aliases of one name are IQ's error, and an alias spelled like
        // a host variable is refused, as written; then every alias is read where IQ reads it (IqAliases)
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                uniqueAliases(s);
                variableAliases(s);
            }
        }
        spans.putAll(read.spans());
        IqAliases.Read aliased = IqAliases.resolve(statement, spans, settings,
                (engine != null ? engine : engineOf(statement, settings)) == Engine.SA);
        statement = aliased.statement();
        // IQ island, 2026-10-15 (checklist 2.16b, measured on IQ 17.1): ROLLUP and CUBE as a UNION of their groupings
        // the rewrites' items keep their originals' facts (2026-10-16, the landing's review: by identity, the alias
        // reads of a rolled-up item were lost)
        Map<SqlAst.SelectExpr, SqlAst.SelectExpr> remade = new IdentityHashMap<>();
        statement = rolledUp(statement, engine != null ? engine : engineOf(statement, settings), remade);
        // IQ island, 2026-10-15 (checklist 1.10, measured on IQ 17.1): NUMBER(*) as ROW_NUMBER over the ORDER BY
        Set<SqlAst.Window> numbering = Collections.newSetFromMap(new IdentityHashMap<>());
        statement = numbered(statement, numbering, remade);
        outerReads.addAll(aliased.outerReads());
        aliasesRead.putAll(aliased.aliasesRead());
        innerNames.putAll(aliased.innerNames());
        nameless.addAll(aliased.nameless());
        remade.forEach((now, was) -> {
            Set<String> aliases = aliased.aliasesRead().get(was);
            if (aliases != null) {
                aliasesRead.put(now, aliases);
            }
            SqlAst.Ident inner = aliased.innerNames().get(was);
            if (inner != null) {
                innerNames.put(now, inner);
            }
            if (aliased.nameless().contains(was)) {
                nameless.add(now);
            }
        });
        Set<SqlAst.Select> setBranches = Collections.newSetFromMap(new IdentityHashMap<>());
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.SetOperation o) {
                List<SqlAst.Select> all = new ArrayList<>();
                branches(o, all);
                setBranches.addAll(all);
            }
        }
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.SetOperation o) {
                setWidths(o);
                setOrder(o);
                continue;
            }
            SqlAst.Select s = (SqlAst.Select) q;
            for (SqlAst.FromItem from : s.from()) {
                for (SqlAst.FromItem item : leaves(from)) {
                    if (item instanceof SqlAst.TableFunction f && IqFunctions.NO_SUCH_FUNCTION.contains(
                            IqLexer.lower(f.name().get(f.name().size() - 1).text()))) {
                        // IQ island, 2026-10-16 (measured on IQ 17.1): STRING_SPLIT
                        throw IqFunctions.noSuchFunction(f.name().get(f.name().size() - 1).text(), f.pos());
                    }
                    if (item instanceof SqlAst.TableFunction f && !(f.name().size() == 1
                            && IqLexer.lower(f.name().get(0).text()).equals("tb")) && !rowGenerator(f.name())) {
                        String fn = f.name().get(f.name().size() - 1).text();
                        String lower = IqLexer.lower(fn);
                        if (f.name().size() == 1 && !lower.startsWith("sa_") && !lower.startsWith("sp_")
                                && !lower.startsWith("xp_")) {
                            // IQ island, 2026-10-17 (the corpus lane's FROM u (NOLOCK); measured on IQ 17.1): a
                            // name of no system procedure called in FROM is IQ's -265 (a table's name too)
                            throw SqlTranslationException.unsupported("the FROM call " + fn + "(...) is IQ's error -265"
                                    + " (procedure '" + fn + "' not found: no procedure of IQ's, unless a user procedure"
                                    + " holds the name), refused before translating", f.pos());
                        }
                        throw SqlTranslationException.unsupported("the FROM function " + fn + "(...) is not supported"
                                + " yet: an #IQ{...}# island reads tables, tb('...'), and sa_rowgenerator(...)",
                                f.pos());
                    }
                    if (item instanceof SqlAst.SubqueryFrom d) {
                        derivedNames(d);
                    }
                    // IQ island, 2026-10-14 (M1c): names that are no table of the island's
                    if (item instanceof SqlAst.TableName t) {
                        tableName(t);
                    }
                }
            }
            for (SqlAst.Expr e : expressions(s)) {
                knownFunctions(e);
                likePatterns(e);
                subqueryWidths(e);
            }
            // IQ island, 2026-10-15 (windows): the errors IQ raises for a query's window functions
            IqWindows.parsed(s, engine != null ? engine : engineOf(statement, settings), settings, numbering);
            // IQ island, 2026-10-14 (M1c): the errors IQ raises for a query's aggregates and grouping
            IqAggregates.parsed(s);
            nameLengths(s);
            fromClause(s);
            orderPositions(s);
            if (s != statement && (s.limit() != null || s.offset() != null) && setBranches.contains(s)) {
                // IQ island, 2026-10-14 (the third landing): a subquery's and a derived table's are read
                // ({@link #reading}); a set operation's branch's stays refused
                SqlAst.Expr at = s.limit() != null ? s.limit() : s.offset();
                throw SqlTranslationException.unsupported("TOP, FIRST or START AT in a subquery, a derived table or a"
                        + " set operation's branch is not supported yet (IQ's engine restricts it too)",
                        java.util.Objects.requireNonNull(at).pos());
            }
        }
        bound(statement, Map.of());
        // IQ island, 2026-10-14 (M1c): the grouping errors a SELECT's own reading cannot see
        IqAggregates.statement(statement);
        innerNamesInOrderBy(statement);
        existsOrder(statement);
        return statement;
    }

    /**
     * IQ island, 2026-10-17 (the differential fuzzer's fifth round; measured on IQ 17.1): under IQ's engine a window's
     * PARTITION BY or ORDER BY that holds GREATER or LESSER -- itself, or through a derived table's column (a CTE's)
     * that IQ merges into the query, one with no DISTINCT, grouping, aggregate, row limit or window of its own -- is its
     * -1001025 ("Function not currently supported"), raised as it runs the statement, after every other error
     * ({@link #executed}); ORDER BY, GROUP BY and a window's argument read them, and so does SQL Anywhere's front end.
     */
    private static @com.legend.base.Nullable SqlTranslationException windowKeyComparisons(
            SqlAst.Statement statement) {
        for (SqlAst.Statement q : queries(statement)) {
            if (!(q instanceof SqlAst.Select s)) {
                continue;
            }
            List<SqlAst.WindowSpec> specs = new ArrayList<>();
            readExpressions(s).forEach(e -> windowSpecs(e, specs));
            for (SqlAst.WindowSpec spec : specs) {
                List<SqlAst.Expr> keys = new ArrayList<>(spec.partitionBy());
                spec.orderBy().forEach(o -> keys.add(o.expr()));
                for (SqlAst.Expr k : keys) {
                    SqlAst.FunctionCall f = comparisonCall(k, s);
                    if (f != null) {
                        String name = f.name().get(f.name().size() - 1).text().toUpperCase(java.util.Locale.ROOT);
                        return SqlTranslationException.invalid("Function not currently supported: " + name + " in a"
                                + " window's PARTITION BY or ORDER BY under IQ's engine" + code(-1001025, "QFA25"),
                                f.pos());
                    }
                }
            }
        }
        return null;
    }

    /** {@code claim}, when there is one, as the error raised after every other ({@link #executed}). */
    private void lateClaimed(@com.legend.base.Nullable SqlTranslationException claim) {
        if (claim != null) {
            executed[0] = claim;
        }
    }

    /**
     * IQ island, 2026-10-17 (the corpus lane's canonical TPC-DS: round(sat_sales1/sat_sales2, 2) over a CTE; measured
     * on IQ 17.1): inside an unaliased item's name IQ renders a plain derived table's column (not grouped, no set
     * operation) by its definition, in an aggregate's arguments too -- the definition's own rendering and, under an
     * alias, {@code as} the alias: {@code x} of {@code (SELECT it AS x FROM t)} is {@code t.it as x}, of {@code (SELECT
     * it + 1 AS x ...)} {@code t.it+1 as x}, of a derived table over another {@code t.it as x as z}, an unaliased column
     * {@code t.it}, {@code SUM(t.it as x)}. Keyed by the derived table's correlation name and the column's (lower case),
     * a key two derived tables of the statement share dropped; a definition not rendered here (a CAST, a subquery, a
     * column two FROM items may hold) left out.
     */
    private Map<String, String> derivedRenderings(SqlAst.Statement statement) {
        Map<String, String> out = new java.util.HashMap<>();
        Set<String> shared = new java.util.HashSet<>();
        for (SqlAst.Statement q : queries(statement)) {
            if (!(q instanceof SqlAst.Select s)) {
                continue;
            }
            for (SqlAst.FromItem f : s.from()) {
                for (SqlAst.FromItem leaf : leaves(f)) {
                    if (!(leaf instanceof SqlAst.SubqueryFrom d) || d.alias() == null
                            || !(d.query() instanceof SqlAst.Select ds) || !d.columnAliases().isEmpty()
                            || IqAggregates.grouped(ds)) {
                        continue;
                    }
                    String alias = IqLexer.lower(java.util.Objects.requireNonNull(d.alias()).text());
                    for (int i = 0; i < ds.items().size(); i++) {
                        String name = itemName(ds, i, List.of());
                        String rendering = name == null || !(ds.items().get(i) instanceof SqlAst.SelectExpr x) ? null
                                : definition(x, ds, 0);
                        if (rendering == null) {
                            continue;
                        }
                        String before = out.putIfAbsent(alias + "." + name, rendering);
                        if (before != null && !before.equals(rendering)) {
                            shared.add(alias + "." + name);
                        }
                    }
                }
            }
        }
        shared.forEach(out::remove);
        return out;
    }

    /** How IQ renders the column item {@code x} of plain derived select {@code ds} defines ({@link #derivedRenderings}),
     *  or null; {@code depth} bounds the derived tables followed inside it. */
    private @com.legend.base.Nullable String definition(SqlAst.SelectExpr x, SqlAst.Select ds, int depth) {
        if (depth > 8 || x.expr() instanceof SqlAst.Cast) {
            return null;
        }
        List<SqlAst.FromItem> sources = new ArrayList<>();
        ds.from().forEach(f -> sources.addAll(leaves(f)));
        IqNames.Columns inner = (ref, inAggregate) -> innerColumn(ref, sources, depth);
        String body = x.expr() instanceof SqlAst.ColumnRef r ? inner.rendering(r, false)
                : IqNames.name(x.text(), settings, inner);
        if (body == null) {
            return null;
        }
        return x.alias() == null ? body : body + " as " + java.util.Objects.requireNonNull(x.alias()).text();
    }

    /** How IQ renders {@code ref}, a column of a derived select whose FROM items are {@code sources}, inside its
     *  definition ({@link #definition}): qualified by the item it reads, a plain derived table's column by its own
     *  definition, a grouped one's by its correlation name; null where not known. */
    private @com.legend.base.Nullable String innerColumn(SqlAst.ColumnRef ref, List<SqlAst.FromItem> sources,
            int depth) {
        if (ref.parts().size() > 2) {
            return null;
        }
        String column = ref.parts().get(ref.parts().size() - 1).text();
        String shown = IqLexer.reserved(column) ? "\"" + column + "\"" : column;
        SqlAst.FromItem source = null;
        if (ref.parts().size() == 2) {
            String qualifier = IqLexer.lower(ref.parts().get(0).text());
            for (SqlAst.FromItem l : sources) {
                SqlAst.Ident c = correlation(l);
                if (c != null && IqLexer.lower(c.text()).equals(qualifier)) {
                    source = l;
                }
            }
        } else if (sources.size() == 1) {
            source = sources.get(0);
        }
        if (source == null) {
            return null;
        }
        if (!(source instanceof SqlAst.SubqueryFrom sd)) {
            SqlAst.Ident c = correlation(source);
            return c == null ? null : (ref.parts().size() == 2 ? ref.parts().get(0).text() : c.text()) + "." + shown;
        }
        if (sd.alias() == null || !(sd.query() instanceof SqlAst.Select inner) || !sd.columnAliases().isEmpty()) {
            return null;
        }
        if (IqAggregates.grouped(inner)) {
            return java.util.Objects.requireNonNull(sd.alias()).text() + "." + shown;
        }
        for (int i = 0; i < inner.items().size(); i++) {
            if (IqLexer.lower(column).equals(itemName(inner, i, List.of()))
                    && inner.items().get(i) instanceof SqlAst.SelectExpr x) {
                return definition(x, inner, depth + 1);
            }
        }
        return null;
    }

    /** The error IQ raises for {@code statement} after every other ({@link #executed}): a ROLLUP's or a CUBE's DISTINCT
     *  aggregate ({@link #rollupDistinct}), else a GREATER or LESSER in a window's keys ({@link #windowKeyComparisons});
     *  null for none. */
    private @com.legend.base.Nullable SqlTranslationException lateClaim(SqlAst.Statement statement, Engine chosen) {
        SqlTranslationException distinct = rolledDistinct.get(statement);
        return distinct != null ? distinct : (engine != null ? engine : storeOf(statement, settings)) == Engine.IQ
                ? windowKeyComparisons(statement) : null;
    }

    /** IQ island, 2026-10-17 (the differential fuzzer's sixth round; measured on IQ 17.1): under IQ's engine a ROLLUP or
     *  a CUBE beside a DISTINCT aggregate is its -1001030 (CUBE or ROLLUP with DISTINCT Aggregate function), after the
     *  names' errors (-143, -265) and a constant's conversion (-1009145). */
    private static @com.legend.base.Nullable SqlTranslationException rollupDistinct(SqlAst.Statement statement) {
        if (!(statement instanceof SqlAst.Select s) || rollup(s) == null) {
            return null;
        }
        List<SqlAst.Expr> read = new ArrayList<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x) {
                read.add(x.expr());
            }
        }
        if (s.having() != null) {
            read.add(java.util.Objects.requireNonNull(s.having()));
        }
        for (SqlAst.Expr e : read) {
            for (SqlAst.FunctionCall f : IqAggregates.aggregateCalls(e)) {
                if (f.distinct()) {
                    return SqlTranslationException.invalid("Feature 'CUBE or ROLLUP with DISTINCT Aggregate function'"
                            + " is not supported under IQ's engine" + code(-1001030, "QFA2A"), f.pos());
                }
            }
        }
        return null;
    }

    /** Into {@code out}, the window specifications {@code e} holds outside its subqueries. */
    private static void windowSpecs(SqlAst.Expr e, List<SqlAst.WindowSpec> out) {
        if (e instanceof SqlAst.Window w) {
            out.add(w.spec());
        }
        children(e).forEach(c -> windowSpecs(c, out));
    }

    /** A GREATER or LESSER call {@code e} holds in select {@code s}, itself or through a merged derived table's column
     *  ({@link #windowKeyComparisons}); null for none. */
    private static SqlAst.@com.legend.base.Nullable FunctionCall comparisonCall(SqlAst.Expr e, SqlAst.Select s) {
        if (e instanceof SqlAst.FunctionCall f && Set.of("greater", "lesser").contains(IqLexer.lower(
                f.name().get(f.name().size() - 1).text()))) {
            return f;
        }
        for (SqlAst.Expr c : children(e)) {
            SqlAst.FunctionCall found = comparisonCall(c, s);
            if (found != null) {
                return found;
            }
        }
        if (!(e instanceof SqlAst.ColumnRef r)) {
            return null;
        }
        String name = IqLexer.lower(r.parts().get(r.parts().size() - 1).text());
        String qualifier = r.parts().size() > 1 ? IqLexer.lower(r.parts().get(r.parts().size() - 2).text()) : null;
        for (SqlAst.FromItem f : s.from()) {
            for (SqlAst.FromItem leaf : leaves(f)) {
                if (!(leaf instanceof SqlAst.SubqueryFrom d) || !(d.query() instanceof SqlAst.Select ds)
                        || qualifier != null && (d.alias() == null || !qualifier.equals(IqLexer.lower(
                        java.util.Objects.requireNonNull(d.alias()).text()))) || ds.distinct()
                        || !ds.groupBy().isEmpty() || ds.having() != null || ds.limit() != null || ds.offset() != null
                        || expressions(ds).stream().anyMatch(x -> hasAggregate(x) || hasWindowCall(x))) {
                    continue;
                }
                for (int i = 0; i < ds.items().size(); i++) {
                    if (ds.items().get(i) instanceof SqlAst.SelectExpr x && name.equals(itemName(ds, i,
                            d.columnAliases()))) {
                        SqlAst.FunctionCall found = comparisonCall(x.expr(), ds);
                        if (found != null) {
                            return found;
                        }
                    }
                }
            }
        }
        return null;
    }

    /** The claims a reading holds from before translating: a subquery's placement error ({@link #placements}), the
     *  statement's WITH RECURSIVE error ({@link #recursion}). */
    private void claims(@com.legend.base.Nullable SqlTranslationException placed,
            @com.legend.base.Nullable Recursive recursion) {
        if (placed != null) {
            placement[0] = placed;
        }
        if (recursion != null) {
            recursive[0] = recursion;
        }
    }

    /** IQ's error for a statement's WITH RECURSIVE ({@link #recursion}), the codes IQ raises before it, and whether it
     *  is raised after every other ({@code last}). */
    private record Recursive(SqlTranslationException error, Set<Integer> before, boolean last) {
    }

    /**
     * IQ island, 2026-10-17 (the TPC-H corpus lane's 1,805 WITH RECURSIVE, 1,180 of them recursing; measured on IQ
     * 17.1): a WITH RECURSIVE is IQ's error as IQ runs it. A CTE named inside its own reading ({@link
     * IqParser.Recursion}) is a recursion SQL Anywhere runs: with no column list -922, after -154 only; read beside an
     * IQ table (IQ's engine) -706, after the names (-141 to -144), -154, -163 and -812 and before grouping, -265 and
     * every other, since IQ's store cannot answer it. A WITH RECURSIVE whose CTEs recurse nowhere read, under IQ's
     * engine, is -1001030 (recursive queries in IQ store) after every other error where IQ's engine runs the statement
     * whole -- no DUMMY, no query without FROM anywhere in it -- and plain CTEs where SQL Anywhere runs part of it (one
     * of those). A recursion IQ answers (over no IQ table) is refused by name: lite's recursion native, the Cypher
     * island's {@code meta::legend::lite::recurse}, is not on main yet.
     */
    private static @com.legend.base.Nullable Recursive recursion(IqParser.Recursion r, SqlAst.Statement statement,
            Engine chosen) {
        if (!r.with()) {
            return null;
        }
        if (!r.columnless().isEmpty()) {
            return new Recursive(SqlTranslationException.invalid("a recursive common table expression needs a column"
                    + " list" + code(-922, "42W9A"), r.columnless().get(0)), Set.of(-131, -154), false);
        }
        if (!r.reads().isEmpty()) {
            if (chosen != Engine.IQ) {
                throw SqlTranslationException.unsupported("a recursive common table expression over no IQ table is"
                        + " not supported yet (lite's recursion native is not on main)", r.reads().get(0));
            }
            return new Recursive(SqlTranslationException.invalid("a recursive common table expression read beside an"
                    + " IQ table is IQ's error -706 under its engine (SQL Anywhere runs the recursion, which IQ's store"
                    + " cannot answer)" + code(-706, "WO012"), r.reads().get(0)),
                    Set.of(-131, -141, -142, -143, -144, -154, -163, -812), false);
        }
        if (chosen != Engine.IQ) {
            return null;
        }
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                if (s.from().isEmpty()) {
                    return null;
                }
                for (SqlAst.FromItem f : s.from()) {
                    for (SqlAst.FromItem leaf : leaves(f)) {
                        if (frontEndSource(leaf)) {
                            return null;
                        }
                    }
                }
            }
        }
        return new Recursive(SqlTranslationException.invalid("WITH RECURSIVE over IQ's tables alone is IQ's error"
                + " -1001030 under its engine (recursive queries in IQ store are not supported)"
                + code(-1001030, "QFA2A"), statement.pos()), Set.of(), true);
    }

    /** IQ island, 2026-10-14 (found beside review 7's B1, measured on SQL Anywhere 17): an EXISTS subquery with its
     *  own ORDER BY (with or without TOP, under NOT, of a set operation) is SQL Anywhere's syntax error -131, where
     *  an IN, a quantified or a scalar subquery and a derived table take one. */
    private static void existsOrder(SqlAst.Statement statement) {
        for (SqlAst.Statement q : queries(statement)) {
            if (!(q instanceof SqlAst.Select s)) {
                continue;
            }
            for (SqlAst.Expr e : expressions(s)) {
                existsOrder(e);
            }
        }
    }

    private static void existsOrder(SqlAst.Expr e) {
        if (e instanceof SqlAst.Exists x) {
            List<SqlAst.SortItem> order = x.query() instanceof SqlAst.Select s ? s.orderBy()
                    : x.query() instanceof SqlAst.SetOperation o ? o.orderBy() : List.of();
            if (!order.isEmpty()) {
                SqlAst.Pos at = order.get(0).pos();
                throw new SqlTranslationException(SqlTranslationException.Kind.SYNTAX, "ORDER BY in an EXISTS"
                        + " subquery is IQ's syntax error" + syntaxCode(), at.line(), at.column());
            }
        }
        for (SqlAst.Expr child : children(e)) {
            existsOrder(child);
        }
    }

    /** IQ island, 2026-10-14 (review 7's B2, measured on SQL Anywhere 17): the name a query without FROM's own alias
     *  gives its item is no name an ORDER BY reads ({@code SELECT (SELECT id + 1 AS z) FROM t ORDER BY z} is IQ's
     *  -143 where no column is named z): an ORDER BY of such a name is refused by name. */
    private void innerNamesInOrderBy(SqlAst.Statement statement) {
        for (SqlAst.Statement q : queries(statement)) {
            List<SqlAst.SortItem> order = q instanceof SqlAst.Select s ? s.orderBy()
                    : q instanceof SqlAst.SetOperation o ? o.orderBy() : List.of();
            if (order.isEmpty()) {
                continue;
            }
            Set<String> inner = new java.util.HashSet<>();
            for (SqlAst.SelectItem item : first(q).items()) {
                SqlAst.Ident name = item instanceof SqlAst.SelectExpr x ? innerNames.get(x) : null;
                if (name != null) {
                    inner.add(IqLexer.lower(name.text()));
                }
            }
            for (SqlAst.SortItem o : order) {
                if (o.expr() instanceof SqlAst.ColumnRef r && r.parts().size() == 1
                        && inner.contains(IqLexer.lower(r.parts().get(0).text()))) {
                    throw SqlTranslationException.unsupported("ORDER BY '" + r.parts().get(0).text() + "', the name"
                            + " a subquery's own alias gives its select item, is not supported: IQ shows the item"
                            + " under that name but reads no such name in ORDER BY (its error -143 where no column has"
                            + " it)", r.pos());
                }
            }
        }
    }

    /**
     * A table named in FROM that no island table can be (IQ island, 2026-10-14, M1c), refused by name:
     * {@code sa_rowgenerator} without its parentheses (the procedure, called so, is no table: SQL Anywhere's -141), and
     * IQ's owner names {@code dbo} and {@code SYS} written as a table. IQ island, 2026-10-14 (M1e): an owner-qualified
     * name is bound to the model's tables when translated ({@link #table}, {@link IqBindings}, the study's O2); one of
     * IQ's own users' (its catalog) is refused here.
     */
    private static void tableName(SqlAst.TableName t) {
        List<SqlAst.Ident> name = t.name();
        if (dummy(name)) {
            return;
        }
        String last = IqLexer.lower(name.get(name.size() - 1).text());
        IqBindings.catalogTable(t);
        if (name.size() == 1 && (last.equals("sa_rowgenerator") || last.equals("dbo") || last.equals("sys"))) {
            throw SqlTranslationException.unsupported("the table '" + name.get(0).text() + "' is not supported: "
                    + (last.equals("sa_rowgenerator") ? "sa_rowgenerator is a procedure, called with parentheses"
                    : "it is IQ's name of an owner"), t.pos());
        }
    }

    // ------------------------------------------------------------------ the statement's queries (M1b)

    /** Every query of {@code statement}, depth first: itself, a set operation's branches, each SELECT's derived
     *  tables' and subqueries' (in its expressions and its joins' ON conditions). */
    public static List<SqlAst.Statement> queries(SqlAst.Statement statement) {
        List<SqlAst.Statement> out = new ArrayList<>();
        queries(statement, out);
        return out;
    }

    private static void queries(SqlAst.Statement statement, List<SqlAst.Statement> out) {
        out.add(statement);
        switch (statement) {
            case SqlAst.Select s -> {
                for (SqlAst.FromItem f : s.from()) {
                    for (SqlAst.FromItem leaf : leaves(f)) {
                        if (leaf instanceof SqlAst.SubqueryFrom d) {
                            queries(d.query(), out);
                        }
                        if (leaf instanceof SqlAst.TableFunction fn) {
                            fn.args().forEach(a -> subqueries(a.value(), out));
                        }
                    }
                }
                for (SqlAst.Expr e : expressions(s)) {
                    subqueries(e, out);
                }
            }
            case SqlAst.SetOperation o -> {
                queries(o.left(), out);
                queries(o.right(), out);
            }
            case SqlAst.With w -> throw new IllegalStateException("IQ's reader makes no WITH");
        }
    }

    /** The queries of the subqueries under {@code e}. */
    private static void subqueries(SqlAst.Expr e, List<SqlAst.Statement> out) {
        SqlAst.Statement q = subqueryOf(e);
        if (q != null) {
            queries(q, out);
        }
        for (SqlAst.Expr child : children(e)) {
            subqueries(child, out);
        }
    }

    /** The query of a subquery expression, or null for any other. */
    static SqlAst.@com.legend.base.Nullable Statement subqueryOf(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.ScalarSubquery q -> q.query();
            case SqlAst.Exists q -> q.query();
            case SqlAst.InSubquery q -> q.query();
            case SqlAst.Quantified q -> q.query();
            default -> null;
        };
    }

    /** A FROM item's tables, functions and derived tables, through its joins (not into the derived tables). */
    static List<SqlAst.FromItem> leaves(SqlAst.FromItem item) {
        List<SqlAst.FromItem> out = new ArrayList<>();
        leaves(item, out);
        return out;
    }

    private static void leaves(SqlAst.FromItem item, List<SqlAst.FromItem> out) {
        if (item instanceof SqlAst.Join j) {
            leaves(j.left(), out);
            leaves(j.right(), out);
        } else {
            out.add(item);
        }
    }

    /** The joins of a FROM item, outermost first. */
    private static void joins(SqlAst.FromItem item, List<SqlAst.Join> out) {
        if (item instanceof SqlAst.Join j) {
            out.add(j);
            joins(j.left(), out);
            joins(j.right(), out);
        }
    }

    /** The first SELECT of a query: itself, or its set operation's first branch's. */
    private static SqlAst.Select first(SqlAst.Statement q) {
        return switch (q) {
            case SqlAst.Select s -> s;
            case SqlAst.SetOperation o -> first(o.left());
            case SqlAst.With w -> throw new IllegalStateException("IQ's reader makes no WITH");
        };
    }

    /** The name a FROM item is known by: its alias, else a table's name, a function's ({@code tb('...t')}'s table
     *  {@code t}); null for none. */
    static SqlAst.@com.legend.base.Nullable Ident correlation(SqlAst.FromItem item) {
        return switch (item) {
            case SqlAst.TableName t -> t.alias() != null ? t.alias() : t.name().get(t.name().size() - 1);
            case SqlAst.TableFunction f -> f.alias() != null ? f.alias() : tableNamed(f);
            case SqlAst.SubqueryFrom d -> d.alias();
            case SqlAst.Join j -> null;
        };
    }

    /** The name an unaliased FROM function is known by: {@code tb('pkg::Database.schema.table')}'s table (as IQ knows
     *  a table by its name), any other by its own. */
    private static SqlAst.Ident tableNamed(SqlAst.TableFunction f) {
        if (f.name().size() == 1 && IqLexer.lower(f.name().get(0).text()).equals("tb") && f.args().size() == 1
                && f.args().get(0).value() instanceof SqlAst.StringLit path) {
            String p = path.value();
            int dot = p.lastIndexOf('.');
            if (dot > 0 && dot < p.length() - 1) {
                return new SqlAst.Ident(p.substring(dot + 1), false, f.pos());
            }
        }
        return f.name().get(f.name().size() - 1);
    }

    /** IQ knows an unaliased table by its name: {@code tb('...t')} is {@code t}. */
    @Override
    public SqlAst.Ident functionName(SqlAst.TableFunction f) {
        return tableNamed(f);
    }

    /**
     * The errors IQ raises for {@code s}'s FROM clause before reading a row (measured on SQL Anywhere 17), and what is
     * refused there by name:
     * <ul>
     *   <li>two FROM items known by one name (ASCII case aside): IQ's error -139; two unaliased references to one
     *       table are refused (SQL Anywhere reads them as one table, not a cross join);</li>
     *   <li>a join with neither ON nor NATURAL is IQ's KEY JOIN, made of the tables' foreign keys: refused (M2);</li>
     *   <li>an ON naming an earlier item of the FROM list (a comma before its join): IQ's error -824.</li>
     * </ul>
     */
    private static void fromClause(SqlAst.Select s) {
        Map<String, SqlAst.FromItem> named = new HashMap<>();
        for (SqlAst.FromItem from : s.from()) {
            for (SqlAst.FromItem leaf : leaves(from)) {
                SqlAst.Ident name = correlation(leaf);
                if (name == null) {
                    continue;
                }
                SqlAst.FromItem before = named.putIfAbsent(IqLexer.lower(name.text()), leaf);
                if (before == null) {
                    continue;
                }
                if (unaliased(before) && unaliased(leaf)) {
                    throw SqlTranslationException.unsupported("the table '" + name.text() + "' twice without a"
                            + " correlation name is not supported: SQL Anywhere reads the two as one table; alias"
                            + " them", leaf.pos());
                }
                String duplicate = "two of the query's tables are named '" + name.text() + "'" + code(-139, "52012");
                // IQ island, 2026-10-14 (M1c): two names alike but for case are split in the research's syntax cases
                // (z2_redteam-866, -867 call the form accept; SQL Anywhere raises -139 for both)
                SqlAst.Ident first = java.util.Objects.requireNonNull(correlation(before));
                throw first.text().equals(name.text()) ? SqlTranslationException.invalid(duplicate, name.pos())
                        : split(duplicate, name.pos());
            }
        }
        List<String> earlier = new ArrayList<>();
        for (SqlAst.FromItem from : s.from()) {
            List<SqlAst.Join> joins = new ArrayList<>();
            joins(from, joins);
            for (SqlAst.Join j : joins) {
                if (j.on() == null && !j.natural() && j.type() != SqlAst.JoinType.CROSS) {
                    throw SqlTranslationException.unsupported("a join without ON (IQ's KEY JOIN, made of the tables'"
                            + " foreign keys) is not supported yet: write its ON condition", j.pos());
                }
                if (j.on() != null) {
                    List<SqlAst.ColumnRef> refs = new ArrayList<>();
                    columnRefs(java.util.Objects.requireNonNull(j.on()), refs);
                    for (SqlAst.ColumnRef r : refs) {
                        if (r.parts().size() == 2 && earlier.contains(IqLexer.lower(r.parts().get(0).text()))) {
                            // IQ island, 2026-10-14 (M1c): the research's syntax cases are split on this form
                            // (z2_redteam-836 calls it accept, a6_sa_query-431 and s2_query-147 reject; SQL
                            // Anywhere raises -824 for each)
                            throw split("the ON condition names '" + r.parts().get(0).text() + "', a table before"
                                    + " the comma its join follows" + code(-824, "42U01"), r.parts().get(0).pos());
                        }
                    }
                }
            }
            for (SqlAst.FromItem leaf : leaves(from)) {
                SqlAst.Ident name = correlation(leaf);
                if (name != null) {
                    earlier.add(IqLexer.lower(name.text()));
                }
            }
        }
    }

    /** The longest name of a table, a correlation or an owner IQ takes, in bytes of the database's charset (the
     *  research's GRAMMAR L13: a select-list alias is never checked). */
    private static final int LONGEST_NAME = 128;

    /** A correlation name, or a qualifier, longer than {@link #LONGEST_NAME} bytes is IQ's error -250 (measured),
     *  refused by name ({@link #split}: the research's syntax cases are split on it). */
    private void nameLengths(SqlAst.Select s) {
        List<SqlAst.Ident> names = new ArrayList<>();
        for (SqlAst.FromItem f : s.from()) {
            for (SqlAst.FromItem leaf : leaves(f)) {
                SqlAst.Ident c = correlation(leaf);
                if (c != null) {
                    names.add(c);
                }
            }
        }
        List<SqlAst.ColumnRef> refs = new ArrayList<>();
        for (SqlAst.Expr e : expressions(s)) {
            columnRefs(e, refs);
        }
        refs.forEach(r -> names.addAll(r.parts().subList(0, r.parts().size() - 1)));
        for (SqlAst.Ident name : names) {
            // IQ island, 2026-10-14 (the options framework): a name's bytes are its charset's (CharSet), not the base's
            int bytes = settings.latin1() ? name.text().length() : utf8Length(name.text());
            if (bytes > LONGEST_NAME) {
                throw split("the name '" + name.text().substring(0, 20) + "...' is longer than " + LONGEST_NAME
                        + " bytes" + code(-250, "54003"), name.pos());
            }
        }
    }

    /** The bytes of {@code text} in UTF-8. */
    private static int utf8Length(String text) {
        int n = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            n += c < 0x80 ? 1 : c < 0x800 ? 2 : Character.isHighSurrogate(c) ? 4 : Character.isLowSurrogate(c) ? 0 : 3;
        }
        return n;
    }

    /** A table or a function written without an alias. */
    private static boolean unaliased(SqlAst.FromItem item) {
        return item instanceof SqlAst.TableName t && t.alias() == null
                || item instanceof SqlAst.TableFunction f && f.alias() == null;
    }

    /**
     * A derived table's columns each need a name (measured): with a column list, as many names as its first SELECT
     * has items (fewer is IQ's error -163, more -812; over a {@code *} refused); without one, each item an alias or a
     * column written bare (an expression, a parenthesized column included, is IQ's error -163).
     */
    private void derivedNames(SqlAst.SubqueryFrom d) {
        SqlAst.Select first = first(d.query());
        String table = d.alias() == null ? "" : java.util.Objects.requireNonNull(d.alias()).text();
        boolean star = first.items().stream().anyMatch(i -> i instanceof SqlAst.AllColumns);
        int n = first.items().size();
        if (!d.columnAliases().isEmpty()) {
            if (star) {
                throw SqlTranslationException.unsupported("a derived table's column list over * is not supported yet",
                        d.pos());
            }
            if (d.columnAliases().size() < n) {
                throw split("the derived table '" + table + "' has no name for column " + (d.columnAliases().size()
                        + 1) + code(-163, "52004"), d.pos());
            }
            if (d.columnAliases().size() > n) {
                throw SqlTranslationException.invalid("the derived table '" + table + "' has no column for the name '"
                        + d.columnAliases().get(n).text() + "'" + code(-812, "42W54"), d.pos());
            }
            return;
        }
        for (int i = 0; i < n; i++) {
            // IQ island, 2026-10-14 (review 7's B2): an item read from a query without FROM is no column written bare
            if (first.items().get(i) instanceof SqlAst.SelectExpr x && x.alias() == null && (!bareColumn(x)
                    || nameless.contains(x))) {
                throw SqlTranslationException.invalid("the derived table '" + table + "' has no name for column "
                        + (i + 1) + code(-163, "52004"), x.pos());
            }
        }
    }

    /** Whether an unaliased item is a column written bare (signs aside, no parentheses), which names itself. */
    private boolean bareColumn(SqlAst.SelectExpr x) {
        SqlAst.Expr e = x.expr();
        while (e instanceof SqlAst.Unary u && u.op() == SqlAst.UnaryOp.PLUS) {
            e = u.operand();
        }
        if (!(e instanceof SqlAst.ColumnRef) || x.text() == null) {
            return false;
        }
        try {
            IqParser.Written w = IqParser.written(java.util.Objects.requireNonNull(x.text()), settings);
            for (SqlAst.Expr p = w.expr(); ; p = ((SqlAst.Unary) p).operand()) {
                if (w.parensOf(p) > 0) {
                    return false;
                }
                if (!(p instanceof SqlAst.Unary)) {
                    return true;
                }
            }
        } catch (SqlSyntaxException e2) {
            return false;
        }
    }

    /**
     * A set operation's ORDER BY (measured): a position of the first branch's items, or one of its output names
     * (ASCII case aside). A position beyond them, a qualified name or any other expression is IQ's error -152, a name
     * the first branch does not show its -854; a name among items whose names are not read here (a {@code *}, an
     * unaliased expression) is refused.
     */
    private static void setOrder(SqlAst.SetOperation o) {
        SqlAst.Select first = first(o);
        boolean star = first.items().stream().anyMatch(i -> i instanceof SqlAst.AllColumns);
        List<String> names = new ArrayList<>();
        boolean unknown = star;
        for (SqlAst.SelectItem item : first.items()) {
            if (item instanceof SqlAst.SelectExpr x) {
                SqlAst.Expr e = x.expr();
                while (e instanceof SqlAst.Unary u && u.op() == SqlAst.UnaryOp.PLUS) {
                    e = u.operand();
                }
                if (x.alias() != null) {
                    names.add(IqLexer.lower(java.util.Objects.requireNonNull(x.alias()).text()));
                } else if (e instanceof SqlAst.ColumnRef ref) {
                    names.add(IqLexer.lower(ref.parts().get(ref.parts().size() - 1).text()));
                } else {
                    unknown = true;
                }
            }
        }
        for (SqlAst.SortItem item : o.orderBy()) {
            SqlAst.Expr e = item.expr();
            if (e instanceof SqlAst.IntegerLit n) {
                if (!star && (n.value().signum() <= 0
                        || n.value().compareTo(BigInteger.valueOf(first.items().size())) > 0)) {
                    throw orderRefused(e);
                }
            } else if (e instanceof SqlAst.ColumnRef ref && ref.parts().size() == 1) {
                if (names.contains(IqLexer.lower(ref.parts().get(0).text()))) {
                    continue;
                }
                if (unknown) {
                    throw SqlTranslationException.unsupported("ORDER BY '" + ref.parts().get(0).text() + "' after a set"
                            + " operation whose first branch has items (a *, an unaliased expression) whose names are"
                            + " not read here is not supported yet: order by position", e.pos());
                }
                throw split("ORDER BY '" + ref.parts().get(0).text() + "' after a set operation names no column of"
                        + " its first branch" + code(-854, "42W90"), e.pos());
            } else {
                throw orderRefused(e);
            }
        }
    }

    /** How many items a query's first SELECT has; -1 when one is a star (its width is the translation's to know). */
    private static int width(SqlAst.Statement q) {
        SqlAst.Select first = first(q);
        return first.items().stream().anyMatch(i -> i instanceof SqlAst.AllColumns) ? -1 : first.items().size();
    }

    /** A set operation's branches select as many columns (else IQ's error -153, refused by name: the research's
     *  syntax cases are split on whether the form reads, so the reader gives no verdict on it before translating). */
    private static void setWidths(SqlAst.SetOperation o) {
        int l = width(o.left());
        int r = width(o.right());
        if (l >= 0 && r >= 0 && l != r) {
            // IQ island, 2026-10-14 (review 7's m2): SQL Anywhere's SQLSTATE, as the translation's own -153 has it
            throw split(setWidths(o.op().name()) + " (" + l + " and " + r + ")" + code(-153, "53026"), o.pos());
        }
    }

    /** IQ's -153, in one wording for both places it is found: before translating (the branches' widths known) and
     *  in translation (a {@code *} counted). */
    private static String setWidths(String op) {
        return "the " + op + "'s queries select different numbers of columns";
    }

    /** IQ's error that IQ raises before running a query, where the research's syntax cases call the form accepted in
     *  some cases and rejected in others (each measured as an error on SQL Anywhere 17): refused by name before
     *  translating, the message naming IQ's error, so that no verdict of the reader's is given on a form the cases are
     *  split on. */
    private static SqlTranslationException split(String error, SqlAst.Pos pos) {
        return splitCases(error, pos);
    }

    /** {@link #split}, for the grouping's errors ({@link IqAggregates}). */
    static SqlTranslationException splitCases(String error, SqlAst.Pos pos) {
        return SqlTranslationException.unsupported(error.replace(" (IQ: SQLCODE", ", IQ's error, refused before"
                + " translating (IQ: SQLCODE"), pos);
    }

    /** A subquery read as a value or compared ({@code (query)}, IN, ANY, SOME, ALL) selects one column (else IQ's
     *  error -151); EXISTS selects any. */
    private static void subqueryWidths(SqlAst.Expr e) {
        SqlAst.Statement q = e instanceof SqlAst.Exists ? null : subqueryOf(e);
        if (q != null && width(q) > 1) {
            throw SqlTranslationException.invalid("this subquery selects " + width(q) + " columns, where one is read"
                    + code(-151, "21W01"), e.pos());
        }
        for (SqlAst.Expr child : children(e)) {
            subqueryWidths(child);
        }
    }

    /** An ORDER BY position of a SELECT names one of its items (else IQ's error -152). */
    private static void orderPositions(SqlAst.Select s) {
        int n = width(s);
        for (SqlAst.SortItem item : s.orderBy()) {
            if (n >= 0 && item.expr() instanceof SqlAst.IntegerLit p && !pastAnInt(p) && (p.value().signum() <= 0
                    || p.value().compareTo(BigInteger.valueOf(n)) > 0)) {
                throw orderRefused(item.expr());
            }
        }
    }

    /**
     * The names a query reads where its FROM items' columns are all known before translating -- derived tables
     * (their first SELECT's names, or their column lists), DUMMY and {@code sa_rowgenerator} -- in the statement's own
     * queries and its derived tables (never an expression subquery's, whose names may be an enclosing query's): a
     * qualifier no FROM item has is IQ's error -142 (-824 when it names one of the enclosing FROM clause's other
     * items), a column none has -143, one two have -144 (measured). A query reading a table, or a FROM item whose
     * columns are not known here (a star), is left to the translation; so is an unqualified name without FROM (IQ's
     * special values read so). Each is refused by name ({@link #split}): the research's syntax cases are split on
     * such names.
     */
    private static void bound(SqlAst.Statement q, Map<String, SqlAst.FromItem> siblings) {
        switch (q) {
            case SqlAst.SetOperation o -> {
                bound(o.left(), siblings);
                bound(o.right(), siblings);
            }
            case SqlAst.Select s -> {
                Map<String, SqlAst.FromItem> mine = new HashMap<>();
                Map<String, Set<String>> columns = new HashMap<>();
                boolean known = true;
                for (SqlAst.FromItem f : s.from()) {
                    for (SqlAst.FromItem leaf : leaves(f)) {
                        SqlAst.Ident name = correlation(leaf);
                        if (name == null) {
                            known = false;
                            continue;
                        }
                        mine.put(IqLexer.lower(name.text()), leaf);
                        Set<String> cols = columnsOf(leaf);
                        if (cols == null) {
                            known = false;
                        } else {
                            columns.put(IqLexer.lower(name.text()), cols);
                        }
                        if (leaf instanceof SqlAst.SubqueryFrom d && !d.lateral()) {
                            // (IQ island, 2026-10-16: an APPLY's lateral query reads the items before it)
                            bound(d.query(), mine);
                        }
                    }
                }
                if (!known) {
                    return;
                }
                List<SqlAst.ColumnRef> refs = new ArrayList<>();
                for (SqlAst.SelectItem item : s.items()) {
                    if (item instanceof SqlAst.SelectExpr x) {
                        columnRefs(x.expr(), refs);
                    }
                }
                if (s.where() != null) {
                    columnRefs(java.util.Objects.requireNonNull(s.where()), refs);
                }
                for (SqlAst.FromItem f : s.from()) {
                    List<SqlAst.Join> joins = new ArrayList<>();
                    joins(f, joins);
                    for (SqlAst.Join j : joins) {
                        if (j.on() != null) {
                            columnRefs(java.util.Objects.requireNonNull(j.on()), refs);
                        }
                    }
                }
                for (SqlAst.ColumnRef ref : refs) {
                    resolvedHere(ref, columns, siblings, s.from().isEmpty());
                }
            }
            case SqlAst.With w -> throw new IllegalStateException("IQ's reader makes no WITH");
        }
    }

    /** The lower-case names of a FROM item's columns where they are known before translating, else null. */
    static @com.legend.base.Nullable Set<String> knownColumns(SqlAst.FromItem leaf) {
        return columnsOf(leaf);
    }

    /** The name a FROM item is known by ({@link #correlation}). */
    static SqlAst.@com.legend.base.Nullable Ident correlationOf(SqlAst.FromItem item) {
        return correlation(item);
    }

    private static @com.legend.base.Nullable Set<String> columnsOf(SqlAst.FromItem leaf) {
        if (leaf instanceof SqlAst.TableName t && dummy(t.name())) {
            return Set.of("dummy_col");
        }
        if (leaf instanceof SqlAst.TableFunction f && rowGenerator(f.name())) {
            return Set.of("row_num");
        }
        if (!(leaf instanceof SqlAst.SubqueryFrom d)) {
            return null;
        }
        Set<String> names = new java.util.HashSet<>();
        if (!d.columnAliases().isEmpty()) {
            d.columnAliases().forEach(c -> names.add(IqLexer.lower(c.text())));
            return names;
        }
        for (SqlAst.SelectItem item : first(d.query()).items()) {
            if (!(item instanceof SqlAst.SelectExpr x)) {
                return null;
            }
            SqlAst.Expr e = x.expr();
            while (e instanceof SqlAst.Unary u && u.op() == SqlAst.UnaryOp.PLUS) {
                e = u.operand();
            }
            if (x.alias() != null) {
                names.add(IqLexer.lower(java.util.Objects.requireNonNull(x.alias()).text()));
            } else if (e instanceof SqlAst.ColumnRef r) {
                names.add(IqLexer.lower(r.parts().get(r.parts().size() - 1).text()));
            } else {
                return null;
            }
        }
        return names;
    }

    /** {@code ref} resolved among a query's FROM items whose columns are known ({@code columns}). */
    private static void resolvedHere(SqlAst.ColumnRef ref, Map<String, Set<String>> columns,
            Map<String, SqlAst.FromItem> siblings, boolean noFrom) {
        List<SqlAst.Ident> parts = ref.parts();
        if (parts.size() > 2 || parts.get(0).text().startsWith("@")) {
            return;
        }
        SqlAst.Ident name = parts.get(parts.size() - 1);
        if (parts.size() == 2) {
            String q = IqLexer.lower(parts.get(0).text());
            Set<String> cols = columns.get(q);
            if (cols == null) {
                if (siblings.containsKey(q)) {
                    throw split("'" + parts.get(0).text() + "' names another item of the FROM clause, which a"
                            + " derived table cannot read" + code(-824, "42U01"), parts.get(0).pos());
                }
                throw split("no table or correlation name '" + parts.get(0).text() + "' in the query"
                        + code(-142, "52W02"), parts.get(0).pos());
            }
            if (!cols.contains(IqLexer.lower(name.text()))) {
                throw split("no column '" + name.text() + "' in '" + parts.get(0).text() + "'" + code(-143, "52003"),
                        name.pos());
            }
            return;
        }
        if (noFrom) {
            return;
        }
        int found = 0;
        for (Set<String> cols : columns.values()) {
            found += cols.contains(IqLexer.lower(name.text())) ? 1 : 0;
        }
        if (found == 0) {
            throw split("no column '" + name.text() + "' in the query's tables" + code(-143, "52003"), name.pos());
        }
        if (found > 1) {
            throw split("the column '" + name.text() + "' is in more than one of the query's tables: qualify it"
                    + code(-144, "52002"), name.pos());
        }
    }

    private static SqlTranslationException orderRefused(SqlAst.Expr e) {
        return SqlTranslationException.invalid("a set operation's ORDER BY takes a position or a name of its first"
                + " branch's columns" + code(-152, "53005"), e.pos());
    }

    /** IQ island, 2026-10-17 (measured on IQ 17.1): a later statement of a batch ({@link IqParser}'s laterStatements)
     *  whose call has a wrong number of arguments fails the batch to parse, IQ's -131 (alone, its -154); one of a
     *  function the island does not know refuses the batch by name. */
    private void laterStatement(SqlAst.Statement later) {
        for (SqlAst.Statement q : queries(later)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.Expr e : expressions(s)) {
                    try {
                        knownFunctions(e);
                    } catch (SqlTranslationException x) {
                        if (x.kind() == SqlTranslationException.Kind.UNSUPPORTED) {
                            throw x;
                        }
                        throw new com.legend.protocol.sql.SqlSyntaxException("a later statement of this batch does"
                                + " not parse, which fails the batch" + syntaxCode(), s.pos());
                    }
                }
            }
        }
    }

    /** Every call under {@code e} names a function of the catalog ({@link #function}), with as many arguments as
     *  it takes (else IQ's error -154). */
    private void knownFunctions(SqlAst.Expr e) {
        if (e instanceof SqlAst.FunctionCall f && !IqAggregates.named(f)) {
            String name = f.name().get(f.name().size() - 1).text();
            if (function(name) == null) {
                throw SqlTranslationException.unsupported("function " + name + "(...) is not supported yet", f.pos());
            }
            if (IqFunctions.NO_SUCH_FUNCTION.contains(IqLexer.lower(name))) {
                // IQ island, 2026-10-17: no function of IQ's, its -265 raised as it is translated (function)
                for (SqlAst.Expr child : children(e)) {
                    knownFunctions(child);
                }
                return;
            }
            int n = f.args().size();
            Catalog fn = Catalog.named(name);
            // IQ island, 2026-10-14 (M1d): the rest of the catalog is IqFunctions'
            int[] arity = fn != null ? new int[] {fn.min, fn.max} : IqFunctions.arity(f, settings.ansiSubstring(),
                    settings.iq17());
            if (n < arity[0] || n > arity[1]) {
                throw SqlTranslationException.invalid("wrong number of arguments to " + name.toUpperCase(
                        java.util.Locale.ROOT) + code(-154, "37505"), f.pos());
            }
            constantArguments(name, f);
        }
        for (SqlAst.Expr child : children(e)) {
            knownFunctions(child);
        }
    }

    /** IQ island, 2026-10-15 (measured on SQL Anywhere 17 and IQ 17.1): the errors a date function's constant arguments
     *  raise wherever it is computed, refused by name before translating: DATETIME of a number, YMD of constants
     *  outside the years 1 to 9999. */
    private static void constantArguments(String name, SqlAst.FunctionCall f) {
        String n = IqLexer.lower(name);
        if (n.equals("datetime") && (f.args().get(0) instanceof SqlAst.IntegerLit
                || f.args().get(0) instanceof SqlAst.NumericLit)) {
            throw SqlTranslationException.unsupported("DATETIME of a number is not supported: IQ reads it by its own"
                    + " rules (and raises for most)", f.pos());
        }
        if (n.equals("ymd") && f.args().stream().allMatch(a -> a instanceof SqlAst.IntegerLit)) {
            long[] v = new long[3];
            for (int i = 0; i < 3; i++) {
                java.math.BigInteger b = ((SqlAst.IntegerLit) f.args().get(i)).value();
                if (b.bitLength() > 31) {
                    throw ymdRange(f);
                }
                v[i] = b.longValue();
            }
            if (v[0] < 1 || v[0] > 9999) {
                throw ymdRange(f);
            }
            java.time.LocalDate d = java.time.LocalDate.of((int) v[0], 1, 1).plusMonths(v[1] - 1).plusDays(v[2] - 1);
            if (d.getYear() < 1 || d.getYear() > 9999) {
                throw ymdRange(f);
            }
        }
    }

    private static SqlTranslationException ymdRange(SqlAst.FunctionCall f) {
        return SqlTranslationException.unsupported("YMD of constants outside the years 1 to 9999 is IQ's error,"
                + " refused before translating", f.pos());
    }

    /** Every LIKE under {@code e} whose pattern is a constant has one IQ matches without an error
     *  ({@link #likeParts}): an escape of one character, before {@code %}, {@code _}, {@code [} or itself only (else
     *  IQ's errors -629 and -630, which it raises whatever the rows), a pattern of at most {@link #LONGEST_PATTERN}
     *  characters (else SQL Anywhere's error -606, for some values): each refused by name before translation. (A
     *  {@code [set]} class reads, and is refused when translated.) */
    private static void likePatterns(SqlAst.Expr e) {
        if (e instanceof SqlAst.Like l && l.pattern() instanceof SqlAst.StringLit pattern
                && !(l.escape() instanceof SqlAst.NullLit)) {
            likeParts(pattern, l.escape(), false, false);
        }
        for (SqlAst.Expr child : children(e)) {
            likePatterns(child);
        }
    }

    /** Two select-list aliases equal without regard to ASCII case are IQ's error, read or not (measured): -830; or
     *  (IQ island, 2026-10-14, review 7's m2, measured on SQL Anywhere 17) -831 where each of the two reads the name
     *  bare, so that each reads the other ({@code id AS ID, id + 1 AS Id}: an alias's own name is a column in its own
     *  definition, the other alias of that name is read there). */
    private static void uniqueAliases(SqlAst.Select s) {
        Map<String, SqlAst.SelectExpr> seen = new HashMap<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x && x.alias() != null) {
                SqlAst.Ident alias = java.util.Objects.requireNonNull(x.alias());
                String name = IqLexer.lower(alias.text());
                SqlAst.SelectExpr other = seen.putIfAbsent(name, x);
                if (other != null) {
                    if (readsBare(other.expr(), name) && readsBare(x.expr(), name)) {
                        throw SqlTranslationException.invalid("the alias '" + alias.text() + "' reads the other alias"
                                + " of its name, which reads it" + code(-831, "42W61"), alias.pos());
                    }
                    throw SqlTranslationException.invalid("the alias '" + alias.text() + "' is not unique"
                            + code(-830, "42W60"), alias.pos());
                }
            }
        }
    }

    /** Whether {@code e} reads {@code name} (lower case) unqualified, outside its subqueries. */
    private static boolean readsBare(SqlAst.Expr e, String name) {
        if (e instanceof SqlAst.ColumnRef r && r.parts().size() == 1) {
            return IqLexer.lower(r.parts().get(0).text()).equals(name);
        }
        for (SqlAst.Expr child : children(e)) {
            if (readsBare(child, name)) {
                return true;
            }
        }
        return false;
    }

    /** The dialect {@code statement} is translated in: its engine, and what the rules read of the statement as a
     *  whole -- the CASE and IF conditions, the columns it refers to, its row limit's reach, the select items whose
     *  names it shows, its derived tables. Under the IQ engine, a subquery where IQ's engine restricts it is refused
     *  here ({@link #subqueryPlaces}). */
    @Override
    public InputDialect reading(SqlAst.Statement statement) {
        IqDialect under = prefixed.get(statement);
        if (under != null) {
            return under.reading(statement);
        }
        Set<SqlAst.Expr> inCase = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<SqlAst.SelectExpr> shownHere = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<String> derivedHere = new java.util.HashSet<>();
        Set<String> setDerivedHere = new java.util.HashSet<>();
        Set<String> names = new java.util.HashSet<>();
        Set<SqlAst.Pos> starredHere = new java.util.HashSet<>();
        for (SqlAst.Statement q : queries(statement)) {
            if (!(q instanceof SqlAst.Select s)) {
                continue;
            }
            for (SqlAst.Expr e : expressions(s)) {
                caseConditions(e, inCase);
                List<SqlAst.ColumnRef> refs = new ArrayList<>();
                columnRefs(e, refs);
                refs.forEach(r -> names.add(IqLexer.lower(r.parts().get(r.parts().size() - 1).text())));
            }
            boolean star = s.items().stream().anyMatch(i -> i instanceof SqlAst.AllColumns);
            for (SqlAst.FromItem f : s.from()) {
                for (SqlAst.FromItem leaf : leaves(f)) {
                    if (star && !(leaf instanceof SqlAst.SubqueryFrom)) {
                        starredHere.add(leaf.pos());
                    }
                    if (leaf instanceof SqlAst.SubqueryFrom d && d.alias() != null) {
                        derivedHere.add(IqLexer.lower(java.util.Objects.requireNonNull(d.alias()).text()));
                        if (d.query() instanceof SqlAst.SetOperation
                                || d.query() instanceof SqlAst.Select ds && IqAggregates.grouped(ds)) {
                            setDerivedHere.add(IqLexer.lower(java.util.Objects.requireNonNull(d.alias()).text()));
                        }
                        shown(first(d.query()), shownHere);
                    }
                }
            }
        }
        shown(first(statement), shownHere);
        Engine chosen = engine != null ? engine : engineOf(statement, settings);
        Set<SqlAst.Join> innerHere = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<SqlAst.FunctionCall> placeheldHere = Collections.newSetFromMap(new IdentityHashMap<>());
        SqlTranslationException placement = placements(statement, chosen, innerHere, placeheldHere);
        // IQ island, 2026-10-14 (the third landing; measured on SQL Anywhere 17): a row limit in a subquery or a
        // derived table keeps its rows as the statement's own does; IQ's engine restricts it (the research's D-R1, IQ
        // may ignore it there), so it is read in SQL Anywhere's front end only
        Map<SqlAst.Expr, boolean[]> limits = new IdentityHashMap<>();
        Set<SqlAst.Select> inExists = Collections.newSetFromMap(new IdentityHashMap<>());
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.Expr e : expressions(s)) {
                    existsQueries(e, inExists);
                }
            }
        }
        if (statement instanceof SqlAst.SetOperation o && (o.limit() != null || o.offset() != null)) {
            // IQ island, 2026-10-16 (checklist 2.16b): a ROLLUP's or a CUBE's union of groupings holds the statement's
            // TOP ({@link #rolledUp}); IQ's own syntax puts no row limit on a set operation
            boolean[] flags = {o.orderBy().isEmpty(), o.limit() != null};
            if (o.limit() != null) {
                limits.put(java.util.Objects.requireNonNull(o.limit()), flags);
            }
            if (o.offset() != null) {
                limits.put(java.util.Objects.requireNonNull(o.offset()), flags);
            }
        }
        for (SqlAst.Statement q : queries(statement)) {
            if (!(q instanceof SqlAst.Select s) || s.limit() == null && s.offset() == null) {
                continue;
            }
            // an EXISTS subquery's rows are counted, never shown: which ones a limit keeps is no one's concern
            boolean[] flags = {s.orderBy().isEmpty() && !s.from().isEmpty() && !inExists.contains(s),
                s.limit() != null};
            if (s.limit() != null) {
                limits.put(java.util.Objects.requireNonNull(s.limit()), flags);
            }
            if (s.offset() != null) {
                limits.put(java.util.Objects.requireNonNull(s.offset()), flags);
            }
        }
        if (chosen != Engine.IQ) {
            // IQ island, 2026-10-15 (the corpus lane: 159 queries; measured on IQ 17.1): under IQ's engine a statement
            // reads one clock -- GETDATE(), NOW() and CURRENT TIMESTAMP equal, and two GETDATE()s, over 117,649 rows
            // -- so D-T13 holds in SQL Anywhere's front end alone (there GETDATE() is 3 microseconds from NOW())
            clocks(statement);
        }
        IqAggregates.Naming groupedNames = IqAggregates.read(statement, chosen, spans);
        Set<SqlAst.Expr> valuesHere = statement instanceof SqlAst.Select s ? shownOnly(s)
                : Collections.newSetFromMap(new IdentityHashMap<>());
        Map<SqlAst.Select, SqlAst.Select> groupedHere = new IdentityHashMap<>();
        Set<SqlAst.SetOperation> innerSetsHere = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<SqlAst.Expr> chainedHere = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<SqlAst.Expr> filtersHere = Collections.newSetFromMap(new IdentityHashMap<>());
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                groupedHere.put(s, IqAggregates.grouping(s));
                expressions(s).forEach(e -> chained(e, chainedHere));
                List<SqlAst.Expr> conditions = new ArrayList<>();
                if (s.where() != null) {
                    conditions.add(java.util.Objects.requireNonNull(s.where()));
                }
                if (s.having() != null) {
                    conditions.add(java.util.Objects.requireNonNull(s.having()));
                }
                for (SqlAst.FromItem f : s.from()) {
                    List<SqlAst.Join> joins = new ArrayList<>();
                    joins(f, joins);
                    for (SqlAst.Join j : joins) {
                        if (j.on() != null) {
                            conditions.add(java.util.Objects.requireNonNull(j.on()));
                        }
                    }
                }
                conditions.forEach(c -> filters(c, filtersHere));
            }
            if (q instanceof SqlAst.SetOperation o) {
                for (SqlAst.Statement branch : List.of(o.left(), o.right())) {
                    if (branch instanceof SqlAst.SetOperation inner) {
                        innerSetsHere.add(inner);
                    }
                }
            }
        }
        IqDialect read = new IqDialect(settings, chosen, inCase, names, shownHere, derivedHere,
                starredHere, groupedNames, setDerivedHere, valuesHere, folded(statement), IqKeys.of(statement, settings),
                lazyAggregates(statement), chains(statement), twoValuedPlaces(statement, groupedHere), groupedHere,
                innerSetsHere, chainedHere, filtersHere);
        read.outerLazy.addAll(outerAggregates(statement));
        read.simpleWhens.addAll(simpleWhens);
        read.innerJoins.addAll(innerHere);
        read.placeheld.addAll(placeheldHere);
        read.claims(placement, recursives.get(statement));
        read.lateClaimed(lateClaim(statement, chosen));
        read.derivedRenderings.putAll(read.derivedRenderings(statement));
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.Expr e : expressions(s)) {
                    unknownCalls(e, read.unknownCalls);
                }
            }
        }
        read.unknownCalls.removeIf(read.placeheld::contains);
        read.outerReads.addAll(outerReads);
        read.aliasesRead.putAll(aliasesRead);
        read.innerNames.putAll(innerNames);
        read.limits.putAll(limits);
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s ? !s.orderBy().isEmpty()
                    : q instanceof SqlAst.SetOperation o && !o.orderBy().isEmpty()) {
                read.ordered.add(q);
            }
        }
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.SelectItem item : s.items()) {
                    if (item instanceof SqlAst.SelectExpr x) {
                        computedReads(x.expr(), read.computedReads);
                    }
                }
                s.groupBy().forEach(g -> computedReads(g, read.computedReads));
                s.orderBy().forEach(o -> computedReads(o.expr(), read.computedReads));
            }
        }
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s && !s.groupBy().isEmpty()) {
                // IQ island, 2026-10-17 (the corpus lane's sede-6559; measured on IQ 17.1)
                Set<String> inKeys = new java.util.HashSet<>();
                for (SqlAst.Expr k : s.groupBy()) {
                    if (!(k instanceof SqlAst.ColumnRef)) {
                        List<SqlAst.ColumnRef> refs = new ArrayList<>();
                        columnRefs(k, refs);
                        // a qualified column by its qualifier too (the landing's review, 2026-10-17)
                        refs.forEach(r -> inKeys.add(r.parts().size() == 1 ? IqLexer.lower(r.parts().get(0).text())
                                : IqLexer.lower(r.parts().get(r.parts().size() - 2).text()) + "." + IqLexer.lower(
                                r.parts().get(r.parts().size() - 1).text())));
                    }
                }
                if (!inKeys.isEmpty()) {
                    Set<SqlAst.Statement> inner = Collections.newSetFromMap(new IdentityHashMap<>());
                    for (SqlAst.Expr e : expressions(s)) {
                        subqueryQueries(e, inner);
                    }
                    for (SqlAst.Statement sub : inner) {
                        if (sub instanceof SqlAst.Select ss) {
                            for (SqlAst.Expr e : expressions(ss)) {
                                List<SqlAst.ColumnRef> refs = new ArrayList<>();
                                columnRefs(e, refs);
                                refs.stream().filter(r -> {
                                    String name = IqLexer.lower(r.parts().get(r.parts().size() - 1).text());
                                    return inKeys.contains(name) || r.parts().size() >= 2 && inKeys.contains(
                                            IqLexer.lower(r.parts().get(r.parts().size() - 2).text()) + "." + name);
                                }).forEach(read.outerGroupRefs::add);
                            }
                        }
                    }
                }
            }
        }
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.SortItem o : s.orderBy()) {
                    List<SqlAst.ColumnRef> refs = new ArrayList<>();
                    columnRefs(o.expr(), refs);
                    read.orderRefs.addAll(refs);
                }
                if (IqAggregates.ordersUngrouped(s)) {
                    // IQ island, 2026-10-14 (review 7's m2): the ORDER BY's -854 comes first
                    List<SqlAst.ColumnRef> refs = new ArrayList<>();
                    for (SqlAst.SelectItem item : s.items()) {
                        if (item instanceof SqlAst.SelectExpr x) {
                            columnRefs(x.expr(), refs);
                        }
                    }
                    if (s.having() != null) {
                        columnRefs(java.util.Objects.requireNonNull(s.having()), refs);
                    }
                    read.orderFirst.addAll(refs);
                }
            }
        }
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.SetOperation o) {
                List<SqlAst.Select> later = new ArrayList<>();
                branches(o, later);
                later.remove(0);
                read.unreadNames.addAll(later);
            } else if (q instanceof SqlAst.Select s) {
                for (SqlAst.Expr e : expressions(s)) {
                    existsQueries(e, read.unreadNames);
                }
                // a derived table whose column list names every column: its query's own names are read by no one
                for (SqlAst.FromItem f : s.from()) {
                    for (SqlAst.FromItem leaf : leaves(f)) {
                        if (leaf instanceof SqlAst.SubqueryFrom d && !d.columnAliases().isEmpty()) {
                            List<SqlAst.Select> all = new ArrayList<>();
                            branches(d.query(), all);
                            SqlAst.Select named = all.get(0);
                            if (named.items().stream().allMatch(i -> i instanceof SqlAst.SelectExpr)
                                    && named.items().size() == d.columnAliases().size()) {
                                read.unreadNames.add(named);
                            }
                        }
                    }
                }
            }
        }
        return read;
    }

    /** The SELECTs of the EXISTS subqueries under {@code e} (their own queries' too, through {@link #queries}). */
    private static void existsQueries(SqlAst.Expr e, Set<SqlAst.Select> out) {
        if (e instanceof SqlAst.Exists x) {
            List<SqlAst.Select> all = new ArrayList<>();
            branches(x.query(), all);
            out.addAll(all);
        }
        for (SqlAst.Expr child : children(e)) {
            existsQueries(child, out);
        }
    }

    /** IQ island, 2026-10-14 (the third landing): an EXISTS subquery's, or a set operation's later branch's, output
     *  names are read by no one ({@link #unreadNames}). */
    @Override
    public boolean namesUnread(SqlAst.Select s) {
        return unreadNames.contains(s);
    }

    /** IQ island, 2026-10-14 (the third landing; measured on SQL Anywhere 17): a name inside a subquery that is no
     *  column of its own query's FROM items, and that an enclosing SELECT's alias may be ({@link IqAliases}): IQ reads
     *  the alias there, before the enclosing query's columns (a column of the subquery's own comes first), which is
     *  not read yet. */
    @Override
    public void columnNotHere(SqlAst.ColumnRef ref) {
        if (outerReads.contains(ref)) {
            throw SqlTranslationException.unsupported("'" + ref.parts().get(0).text() + "' names an alias of an"
                    + " enclosing query's select list, which IQ reads before its columns inside a subquery: reading it"
                    + " there is not supported yet; write the alias's expression", ref.pos());
        }
    }

    /**
     * IQ island, 2026-10-14 (review 7's M1): an enclosing query's column read in a subquery's select list, GROUP BY or
     * ORDER BY outside an aggregate ({@link #computedReads}) is refused by name. The planner computes such a value
     * as a column of the subquery's rows and reads the enclosing row's column there by its name among the subquery's
     * own columns: the subquery's column of that name where it has one (a wrong answer: {@code t.id IN (SELECT t.id
     * FROM u ...)} read {@code u.id}, wrong since the second landing), an error where it has none. The planner's fix
     * (a computed column resolving the enclosing row as a predicate does) is pending on the compiler line; WHERE, ON
     * and HAVING read such a column right and are not refused.
     */
    @Override
    public void enclosingColumn(SqlAst.ColumnRef ref) {
        if (computedReads.contains(ref)) {
            throw SqlTranslationException.unsupported("'" + String.join(".", ref.parts().stream()
                    .map(SqlAst.Ident::text).toList()) + "', an enclosing query's column read in a subquery's select"
                    + " list, GROUP BY or ORDER BY, is not supported yet: the planner reads it there as the subquery's"
                    + " own column of that name (or fails where there is none), a planner defect whose fix is pending",
                    ref.pos());
        }
    }

    /** The column references each query reads in its select list, GROUP BY and ORDER BY outside an aggregate's
     *  arguments ({@link #enclosingColumn}), into {@code out}. */
    private static void computedReads(SqlAst.Expr e, Set<SqlAst.ColumnRef> out) {
        if (e instanceof SqlAst.ColumnRef ref) {
            out.add(ref);
            return;
        }
        if (e instanceof SqlAst.FunctionCall f && IqAggregates.isAggregate(f.name().get(f.name().size() - 1).text())) {
            return;
        }
        for (SqlAst.Expr child : children(e)) {
            computedReads(child, out);
        }
    }

    /**
     * IQ island, 2026-10-14 (review 5's B2, measured on SQL Anywhere 17 with a real variable and with a value a client
     * binds): under ansinull Off (a TDS profile) SQL Anywhere reads {@code x = @v} and {@code x <> @v} (also
     * {@code !=}, either side, the variable itself an operand) two-valued in a search condition -- WHERE, HAVING, ON, a
     * subquery's WHERE -- for some operands {@code x} only: a NULL x makes it FALSE, not UNKNOWN, so
     * {@code NOT (x = @v)} keeps the rows where x is NULL. Three-valued, as lite reads them: the other comparisons
     * ({@code <}, {@code <=}, {@code >}, {@code >=}), IN, BETWEEN, LIKE, a variable inside an expression
     * ({@code x = @v + 0}), a constant, two columns, and every comparison outside a search condition (the select
     * list's IF and CASE, ORDER BY). Where the search condition reaches the comparison through AND, OR and NOT alone
     * it is read two-valued here ({@link #twoValued}) when its other operand is one SQL Anywhere reads so
     * ({@link #twoValuedOperand}, review 6's B1), and refused by name under a NOT when that operand may be NULL and is
     * any other; anywhere else inside one (an IF's or a CASE's condition, a function's argument, a truth test) SQL
     * Anywhere's answer follows its own rewriting of the condition (measured both ways:
     * {@code IF x = @v THEN 1 ELSE 0 ENDIF = 0} two-valued, inside a COALESCE three-valued), and such a comparison is
     * refused when it may be UNKNOWN. An IN or a quantified comparison with a subquery over a variable is refused
     * anywhere but through AND and OR, and a simple CASE comparing one anywhere in a search condition. Every
     * comparison of the statement with a host variable operand is placed here, a HAVING's as {@link #grouping} reads
     * it; one the translation meets unplaced is refused ({@link #twoValued}), never read three-valued by default.
     */
    private Map<SqlAst.Expr, Reading> twoValuedPlaces(SqlAst.Statement statement,
            Map<SqlAst.Select, SqlAst.Select> grouped) {
        Map<SqlAst.Expr, Reading> out = new IdentityHashMap<>();
        if (settings.ansinull()) {
            return out;
        }
        twoValuedIn(statement, List.of(), grouped, out);
        return out;
    }

    /** IQ island, 2026-10-14 (review 6's B1): how a comparison with a host variable operand reads under ansinull Off
     *  ({@link #twoValuedPlaces}). */
    private enum Reading {
        /** Three-valued: outside a search condition (measured: the select list's IF and CASE, ORDER BY's IF), or
         *  reached through AND and OR alone with an operand not read two-valued (FALSE and UNKNOWN filter alike). */
        THREE_VALUED,
        /** Reached through AND, OR and NOT in a search condition, its other operand one SQL Anywhere reads two-valued
         *  there ({@link #twoValuedOperand}). */
        TWO_VALUED,
        /** Inside an IF's or a CASE's condition, a function's argument or a truth test in a search condition: refused
         *  where it may be UNKNOWN. */
        REFUSED_PLACE,
        /** Reached through a NOT in a search condition, its other operand one SQL Anywhere reads three-valued there,
         *  or one not measured: refused where it may be NULL (through AND and OR alone, FALSE and UNKNOWN filter
         *  alike, and it is read three-valued). */
        REFUSED_OPERAND;

        /** One comparison placed twice (an alias's expression read again): the same reading, else a refusal. */
        Reading and(Reading other) {
            if (this == other) {
                return this;
            }
            if (this == REFUSED_PLACE || this == REFUSED_OPERAND) {
                return this;
            }
            return other == REFUSED_OPERAND ? other : REFUSED_PLACE;
        }
    }

    /** Places the comparisons of {@code q} and of its subqueries and derived tables, {@code outer} the SELECTs whose
     *  names {@code q} may read (the innermost first). */
    private void twoValuedIn(SqlAst.Statement q, List<SqlAst.Select> outer, Map<SqlAst.Select, SqlAst.Select> grouped,
            Map<SqlAst.Expr, Reading> out) {
        switch (q) {
            case SqlAst.SetOperation o -> {
                twoValuedIn(o.left(), outer, grouped, out);
                twoValuedIn(o.right(), outer, grouped, out);
            }
            case SqlAst.Select s0 -> {
                SqlAst.Select s = grouped.getOrDefault(s0, s0);
                List<SqlAst.Select> scopes = new ArrayList<>();
                scopes.add(s);
                scopes.addAll(outer);
                List<SqlAst.Expr> conditions = new ArrayList<>();
                if (s.where() != null) {
                    conditions.add(java.util.Objects.requireNonNull(s.where()));
                }
                if (s.having() != null) {
                    conditions.add(java.util.Objects.requireNonNull(s.having()));
                }
                for (SqlAst.FromItem f : s.from()) {
                    List<SqlAst.Join> joins = new ArrayList<>();
                    joins(f, joins);
                    for (SqlAst.Join j : joins) {
                        if (j.on() != null) {
                            conditions.add(java.util.Objects.requireNonNull(j.on()));
                        }
                    }
                    for (SqlAst.FromItem leaf : leaves(f)) {
                        if (leaf instanceof SqlAst.SubqueryFrom d) {
                            twoValuedIn(d.query(), outer, grouped, out);
                        }
                        if (leaf instanceof SqlAst.TableFunction fn) {
                            for (SqlAst.Arg a : fn.args()) {
                                List<SqlAst.Statement> inside = new ArrayList<>();
                                directSubqueries(a.value(), inside);
                                inside.forEach(sub -> twoValuedIn(sub, outer, grouped, out));
                            }
                        }
                    }
                }
                conditions.forEach(c -> searchCondition(c, true, true, scopes, out));
                List<SqlAst.Expr> values = new ArrayList<>();
                for (SqlAst.SelectItem item : s.items()) {
                    if (item instanceof SqlAst.SelectExpr x) {
                        values.add(x.expr());
                    }
                }
                values.addAll(s.groupBy());
                s.orderBy().forEach(o -> values.add(o.expr()));
                values.forEach(e -> outsideSearchConditions(e, out));
                for (SqlAst.Expr e : expressions(s)) {
                    List<SqlAst.Statement> inside = new ArrayList<>();
                    directSubqueries(e, inside);
                    inside.forEach(sub -> twoValuedIn(sub, scopes, grouped, out));
                }
            }
            case SqlAst.With w -> throw new IllegalStateException("IQ's reader makes no WITH");
        }
    }

    /** The queries of the subqueries directly under {@code e} (not those inside them). */
    private static void directSubqueries(SqlAst.Expr e, List<SqlAst.Statement> out) {
        SqlAst.Statement q = subqueryOf(e);
        if (q != null) {
            out.add(q);
        }
        for (SqlAst.Expr child : children(e)) {
            directSubqueries(child, out);
        }
    }

    /** Places the comparisons under {@code e}, a value outside the search conditions (a select item, a GROUP BY or an
     *  ORDER BY item): three-valued; one a search condition reads too (an alias's expression) is refused. */
    private static void outsideSearchConditions(SqlAst.Expr e, Map<SqlAst.Expr, Reading> out) {
        if (comparesVariable(e)) {
            place(e, Reading.THREE_VALUED, out);
        }
        for (SqlAst.Expr child : children(e)) {
            outsideSearchConditions(child, out);
        }
    }

    /** Whether {@code e} is {@code =}, {@code <>} or {@code !=} with a host variable operand. */
    private static boolean comparesVariable(SqlAst.Expr e) {
        return e instanceof SqlAst.Binary b && (b.op() == SqlAst.BinaryOp.EQ || b.op() == SqlAst.BinaryOp.NE)
                && (b.left() instanceof SqlAst.Param || b.right() instanceof SqlAst.Param);
    }

    private static void place(SqlAst.Expr b, Reading reading, Map<SqlAst.Expr, Reading> out) {
        Reading before = out.get(b);
        out.put(b, before == null ? reading : before.and(reading));
    }

    /** Places {@code e}'s comparisons ({@link #twoValuedPlaces}): {@code positive} while reached through AND and OR
     *  alone, {@code logical} through AND, OR and NOT; {@code scopes} the SELECTs whose names it reads. */
    private void searchCondition(SqlAst.Expr e, boolean positive, boolean logical, List<SqlAst.Select> scopes,
            Map<SqlAst.Expr, Reading> out) {
        if (e instanceof SqlAst.Binary b && (b.op() == SqlAst.BinaryOp.AND || b.op() == SqlAst.BinaryOp.OR)) {
            searchCondition(b.left(), positive, logical, scopes, out);
            searchCondition(b.right(), positive, logical, scopes, out);
            return;
        }
        if (e instanceof SqlAst.Unary u && u.op() == SqlAst.UnaryOp.NOT) {
            searchCondition(u.operand(), false, logical, scopes, out);
            return;
        }
        if (comparesVariable(e)) {
            SqlAst.Binary b = (SqlAst.Binary) e;
            SqlAst.Expr other = b.left() instanceof SqlAst.Param ? b.right() : b.left();
            // reached through AND and OR alone, FALSE and UNKNOWN filter alike: either reading is SQL Anywhere's
            place(b, !logical ? Reading.REFUSED_PLACE
                    : other instanceof SqlAst.Param || twoValuedOperand(other, scopes) ? Reading.TWO_VALUED
                    : positive ? Reading.THREE_VALUED : Reading.REFUSED_OPERAND, out);
        }
        if (e instanceof SqlAst.Case c && c.operand() != null) {
            // a simple CASE compares its operand with each WHEN's value: over a host variable, not measured there
            List<SqlAst.Param> compared = new ArrayList<>();
            if (c.operand() instanceof SqlAst.Param p) {
                compared.add(p);
            }
            for (SqlAst.When w : c.whens()) {
                if (w.condition() instanceof SqlAst.Param p) {
                    compared.add(p);
                }
            }
            if (!compared.isEmpty()) {
                throw twoValuedRefused(compared.get(0));
            }
        }
        SqlAst.Expr subqueryOperand = e instanceof SqlAst.InSubquery in && (!positive || in.negated()) ? in.operand()
                : e instanceof SqlAst.Quantified qc && !positive ? qc.left() : null;
        if (subqueryOperand != null) {
            List<SqlAst.Param> left = new ArrayList<>();
            params(subqueryOperand, left);
            if (!left.isEmpty()) {
                throw twoValuedRefused(left.get(0));
            }
        }
        for (SqlAst.Expr child : children(e)) {
            searchCondition(child, false, false, scopes, out);
        }
    }

    /**
     * IQ island, 2026-10-14 (review 6's B1, measured on SQL Anywhere 17 with real variables under iq16-tds and
     * sqla17-tds): whether SQL Anywhere reads {@code x = @v} two-valued in a search condition for this {@code x} --
     * a column (of a table, either side of a join, an outer join's NULL-supplying side, an enclosing query's, a GROUP
     * BY key in HAVING, a select-list alias of one there), a constant, {@code + - * / % ||} and a sign over such
     * operands (a column with a constant or with another column), a CAST or CONVERT of one, and a derived table's
     * column (or a set operation's, every branch's) defined by one. It reads three-valued a function's value
     * ({@code NULLIF}, {@code COALESCE}, {@code ISNULL}, {@code ABS}, {@code UPPER}, {@code TRIM}, {@code LENGTH}), an
     * aggregate's in HAVING, a scalar subquery's, and a derived table's column defined by a function (it merges the
     * derived table into the query); and two-valued a derived table's column it does not merge (a grouped one's
     * aggregate, a set operation's NULLIF branch), a CASE's and an IF's. Those, and every other operand, are not
     * {@code x} here: refused where they may be NULL. {@code scopes}: the SELECTs whose names {@code e} reads, the
     * innermost first.
     */
    private static boolean twoValuedOperand(SqlAst.Expr e, List<SqlAst.Select> scopes) {
        return switch (e) {
            case SqlAst.ColumnRef r -> twoValuedColumn(r, scopes);
            case SqlAst.IntegerLit n -> true;
            case SqlAst.NumericLit n -> true;
            case SqlAst.StringLit n -> true;
            case SqlAst.NullLit n -> true;
            case SqlAst.Unary u -> (u.op() == SqlAst.UnaryOp.MINUS || u.op() == SqlAst.UnaryOp.PLUS)
                    && twoValuedOperand(u.operand(), scopes);
            case SqlAst.Binary b -> switch (b.op()) {
                case PLUS, MINUS, TIMES, DIVIDE, MODULO, CONCAT -> twoValuedOperand(b.left(), scopes)
                        && twoValuedOperand(b.right(), scopes);
                case AND, OR, EQ, NE, LT, LE, GT, GE, POWER -> false;
            };
            case SqlAst.Cast c -> twoValuedOperand(c.operand(), scopes);
            default -> false;
        };
    }

    /** What a FROM item holds under a column's name, for {@link #twoValuedOperand}. */
    private enum Held {
        /** No column of that name. */
        NONE,
        /** A column SQL Anywhere compares with a variable two-valued. */
        TWO_VALUED,
        /** A column of another definition, or one whose definition is not read here. */
        OTHER,
        /** A table's or a table function's column, if it has one of that name (its columns are the catalog's). */
        TABLE
    }

    /** {@link #twoValuedOperand} of a column reference: of a table, or of a derived table whose definition of it is
     *  such an operand. An unqualified name an inner FROM item may hold and an outer one defines otherwise is not. */
    private static boolean twoValuedColumn(SqlAst.ColumnRef r, List<SqlAst.Select> scopes) {
        List<SqlAst.Ident> parts = r.parts();
        if (parts.size() > 2) {
            return false;
        }
        String name = IqLexer.lower(parts.get(parts.size() - 1).text());
        if (parts.size() == 2) {
            String qualifier = IqLexer.lower(parts.get(0).text());
            for (int i = 0; i < scopes.size(); i++) {
                for (SqlAst.FromItem f : scopes.get(i).from()) {
                    for (SqlAst.FromItem leaf : leaves(f)) {
                        SqlAst.Ident known = correlation(leaf);
                        if (known != null && IqLexer.lower(known.text()).equals(qualifier)) {
                            Held held = held(leaf, name, scopes.subList(i + 1, scopes.size()));
                            return held == Held.TWO_VALUED || held == Held.TABLE;
                        }
                    }
                }
            }
            return false;
        }
        boolean table = false;
        for (int i = 0; i < scopes.size(); i++) {
            boolean twoValued = false;
            for (SqlAst.FromItem f : scopes.get(i).from()) {
                for (SqlAst.FromItem leaf : leaves(f)) {
                    switch (held(leaf, name, scopes.subList(i + 1, scopes.size()))) {
                        case OTHER -> {
                            return false;
                        }
                        case TWO_VALUED -> twoValued = true;
                        case TABLE -> table = true;
                        case NONE -> {
                        }
                    }
                }
            }
            if (twoValued) {
                return true;
            }
        }
        return table;
    }

    /** What FROM item {@code leaf} holds under {@code name} (lower case); {@code outer} the SELECTs a derived table's
     *  query may read names of. */
    private static Held held(SqlAst.FromItem leaf, String name, List<SqlAst.Select> outer) {
        return switch (leaf) {
            case SqlAst.TableName t -> dummy(t.name()) ? (name.equals("dummy_col") ? Held.TWO_VALUED : Held.NONE)
                    : Held.TABLE;
            case SqlAst.TableFunction f -> rowGenerator(f.name()) ? (name.equals("row_num") ? Held.TWO_VALUED
                    : Held.NONE) : Held.TABLE;
            case SqlAst.SubqueryFrom d -> derivedColumn(d, name, outer);
            case SqlAst.Join j -> throw new IllegalStateException("a join is no FROM leaf");
        };
    }

    /** What derived table {@code d} holds under {@code name}: its definition in each branch (by its position), each
     *  read as {@link #twoValuedOperand} reads it in its own query. A column a {@code *} brings is read in that
     *  query's FROM; one of a set operation with a {@code *} is not read here. */
    private static Held derivedColumn(SqlAst.SubqueryFrom d, String name, List<SqlAst.Select> outer) {
        List<SqlAst.Select> branches = new ArrayList<>();
        branches(d.query(), branches);
        boolean star = false;
        for (SqlAst.Select b : branches) {
            star |= b.items().stream().anyMatch(i -> i instanceof SqlAst.AllColumns);
        }
        SqlAst.Select first = branches.get(0);
        int index = -1;
        if (!d.columnAliases().isEmpty()) {
            for (int i = 0; i < d.columnAliases().size() && index < 0; i++) {
                if (IqLexer.lower(d.columnAliases().get(i).text()).equals(name)) {
                    index = i;
                }
            }
            if (index < 0) {
                return Held.NONE;
            }
            if (star) {
                return Held.OTHER;
            }
        } else {
            for (int i = 0; i < first.items().size() && index < 0; i++) {
                if (first.items().get(i) instanceof SqlAst.SelectExpr x && name.equals(itemName(x))) {
                    index = i;
                }
            }
            if (index < 0) {
                if (!star) {
                    return Held.NONE;
                }
                if (branches.size() > 1) {
                    return Held.OTHER;
                }
                // a column the derived query's * brings: its own FROM's
                return twoValuedColumn(new SqlAst.ColumnRef(List.of(new SqlAst.Ident(name, true, d.pos())), d.pos()),
                        List.of(first)) ? Held.TWO_VALUED : Held.OTHER;
            }
            if (star && branches.size() > 1) {
                return Held.OTHER;
            }
        }
        for (SqlAst.Select b : branches) {
            if (index >= b.items().size() || !(b.items().get(index) instanceof SqlAst.SelectExpr x)) {
                return Held.OTHER;
            }
            List<SqlAst.Select> scopes = new ArrayList<>();
            scopes.add(b);
            scopes.addAll(outer);
            if (!twoValuedOperand(x.expr(), scopes)) {
                return Held.OTHER;
            }
        }
        return Held.TWO_VALUED;
    }

    /** The name (lower case) a select item shows: its alias, or a bare column's name; null for another. */
    private static @com.legend.base.Nullable String itemName(SqlAst.SelectExpr x) {
        if (x.alias() != null) {
            return IqLexer.lower(java.util.Objects.requireNonNull(x.alias()).text());
        }
        SqlAst.Expr e = x.expr();
        while (e instanceof SqlAst.Unary u && u.op() == SqlAst.UnaryOp.PLUS) {
            e = u.operand();
        }
        return e instanceof SqlAst.ColumnRef r ? IqLexer.lower(r.parts().get(r.parts().size() - 1).text()) : null;
    }

    /** A query's SELECTs in order: itself, or its set operation's branches from the left. */
    private static void branches(SqlAst.Statement q, List<SqlAst.Select> out) {
        switch (q) {
            case SqlAst.Select s -> out.add(s);
            case SqlAst.SetOperation o -> {
                branches(o.left(), out);
                branches(o.right(), out);
            }
            case SqlAst.With w -> throw new IllegalStateException("IQ's reader makes no WITH");
        }
    }

    /** {@code test}, the translation of comparison {@code b}: under ansinull Off, two-valued where SQL Anywhere reads it
     *  so (a host variable read as the enclosing query's variable, never NULL, an operand of it: FALSE where the other
     *  side is NULL), refused where its reading is SQL Anywhere's rewriting's or its operand's is not read here
     *  ({@link #twoValuedPlaces}). */
    private Tx twoValued(SqlAst.Binary b, Tx test) {
        if (twoValued.isEmpty() || !test.nullable() || !comparesVariable(b)
                || !(variable(b.left()) || variable(b.right()))) {
            return test;
        }
        SqlAst.Param p = (SqlAst.Param) (variable(b.left()) ? b.left() : b.right());
        Reading read = twoValued.get(b);
        if (read == null) {
            // IQ island, 2026-10-14 (review 6's B1): no comparison is read three-valued by default
            throw SqlTranslationException.unsupported("under ansinull Off (the " + settings.spelling() + " profile)"
                    + " this comparison with " + p.name() + " was not placed before translating (a search condition's"
                    + " reads it two-valued, another three-valued): not supported", p.pos());
        }
        return switch (read) {
            case THREE_VALUED -> test;
            case TWO_VALUED -> Tx.condition(test.vs(), SqlToPure.call("not", test.vs()), false);
            case REFUSED_PLACE -> throw twoValuedRefused(p);
            case REFUSED_OPERAND -> throw SqlTranslationException.unsupported("under ansinull Off (the "
                    + settings.spelling() + " profile) SQL Anywhere compares a host variable with a column, a constant,"
                    + " or arithmetic or a CAST over them two-valued in a search condition (NOT (x = " + p.name() + ")"
                    + " keeps the rows where x is NULL), but with a function's, an aggregate's or a subquery's value"
                    + " three-valued, and with a CASE's, an IF's or a derived table's computed column as its plan has"
                    + " it: comparing " + p.name() + " with this operand, which may be NULL, is not supported yet",
                    p.pos());
        };
    }

    /** Whether {@code e} is a host variable read as the enclosing query's variable. */
    private boolean variable(SqlAst.Expr e) {
        return e instanceof SqlAst.Param p && variables.contains(p);
    }

    private SqlTranslationException twoValuedRefused(SqlAst.Param p) {
        return SqlTranslationException.unsupported("under ansinull Off (the " + settings.spelling() + " profile) SQL"
                + " Anywhere compares a host variable two-valued in a search condition (NOT (x = " + p.name() + ")"
                + " keeps the rows where x is NULL), and inside an IF's or a CASE's condition, a function's argument,"
                + " a truth test or a subquery's IN there, as its own rewriting of the condition has it: not supported"
                + " yet", p.pos());
    }

    /**
     * The select items of top SELECT {@code s} (without DISTINCT) that nothing else in the statement reads -- the
     * values shown as they are, where a NUMERIC quotient or AVG is rounded ({@link #shownValue}). IQ island, 2026-10-14
     * (review 4's B2, measured on SQL Anywhere 17): SQL Anywhere rounds a quotient only where it shows it; read again
     * -- its alias, or its position, in another item, WHERE, GROUP BY, HAVING or ORDER BY ({@code GROUP BY q} over
     * {@code v / w AS q} found three groups where the shown values make two), or the same expression or aggregate
     * written again ({@code HAVING AVG(v) = 1.3333333333333} kept no group whose shown AVG is that) -- it is read
     * unrounded, where lite's reading would be the rounded column: such an item is no shown value.
     */
    private static Set<SqlAst.Expr> shownOnly(SqlAst.Select s) {
        Set<SqlAst.Expr> out = Collections.newSetFromMap(new IdentityHashMap<>());
        if (s.distinct()) {
            return out;
        }
        List<SqlAst.Expr> clauses = new ArrayList<>();
        if (s.where() != null) {
            clauses.add(java.util.Objects.requireNonNull(s.where()));
        }
        clauses.addAll(s.groupBy());
        if (s.having() != null) {
            clauses.add(java.util.Objects.requireNonNull(s.having()));
        }
        s.orderBy().forEach(o -> clauses.add(o.expr()));
        boolean star = false;
        java.util.Set<BigInteger> positions = new java.util.HashSet<>();
        for (SqlAst.SelectItem item : s.items()) {
            star |= !(item instanceof SqlAst.SelectExpr);
        }
        List<SqlAst.Expr> keys = new ArrayList<>(s.groupBy());
        s.orderBy().forEach(o -> keys.add(o.expr()));
        for (SqlAst.Expr k : keys) {
            if (k instanceof SqlAst.IntegerLit n) {
                positions.add(n.value());
            }
        }
        List<SqlAst.Expr> all = new ArrayList<>(clauses);
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x) {
                all.add(x.expr());
            }
        }
        ShapeIds ids = new ShapeIds();
        Map<String, Integer> shapes = new java.util.HashMap<>();
        List<SqlAst.FunctionCall> aggregates = new ArrayList<>();
        for (SqlAst.Expr e : all) {
            countShapes(e, shapes, ids);
            aggregates.addAll(IqAggregates.aggregateCalls(e));
        }
        // the names read anywhere in the statement, counted once (IQ island, 2026-10-14, the scale suite's cliff 5): an
        // item's alias is read by another part when the statement reads that name more often than the item itself does
        Map<String, Integer> reads = new java.util.HashMap<>();
        for (SqlAst.Expr e : all) {
            countNames(e, reads);
        }
        for (int i = 0; i < s.items().size(); i++) {
            if (!(s.items().get(i) instanceof SqlAst.SelectExpr x)) {
                continue;
            }
            boolean read = positions.contains(BigInteger.valueOf(i + 1)) || star && !positions.isEmpty()
                    || shapes.getOrDefault(ids.id(x.expr()), 0) > 1;
            SqlAst.Ident alias = x.alias();
            if (alias != null) {
                String name = IqLexer.lower(alias.text());
                Map<String, Integer> own = new java.util.HashMap<>();
                countNames(x.expr(), own);
                read |= reads.getOrDefault(name, 0) > own.getOrDefault(name, 0);
            }
            if (x.expr() instanceof SqlAst.FunctionCall f && IqAggregates.named(f)) {
                read |= aggregates.stream().anyMatch(c -> c != f && IqAggregates.alike(c, f));
            }
            if (!read) {
                out.add(x.expr());
            }
        }
        return out;
    }

    /** How many times each {@link IqAggregates#shape} occurs among {@code e} and its parts, into {@code out}, keyed by
     *  {@code ids}' names for the shapes. */
    private static void countShapes(SqlAst.Expr e, Map<String, Integer> out, ShapeIds ids) {
        out.merge(ids.id(e), 1, Integer::sum);
        for (SqlAst.Expr child : children(e)) {
            countShapes(child, out, ids);
        }
    }

    /** How many times each column name (its last part, ASCII lower case) is read in {@code e}, into {@code out}. */
    private static void countNames(SqlAst.Expr e, Map<String, Integer> out) {
        List<SqlAst.ColumnRef> refs = new ArrayList<>();
        columnRefs(e, refs);
        for (SqlAst.ColumnRef r : refs) {
            out.merge(IqLexer.lower(r.parts().get(r.parts().size() - 1).text()), 1, Integer::sum);
        }
    }

    /** Names for {@link IqAggregates#shape}s, equal exactly when the shapes are: hash-consed bottom-up (each node's
     *  form written with its operands' names), so naming the shapes of n nodes is O(n). */
    private static final class ShapeIds {
        private final Map<String, Integer> byForm = new java.util.HashMap<>();
        private final Map<SqlAst.Expr, String> named = new IdentityHashMap<>();

        String id(SqlAst.Expr e) {
            String known = named.get(e);
            if (known != null) {
                return known;
            }
            String form = IqAggregates.shapeOf(e, this::id);
            Integer n = byForm.get(form);
            if (n == null) {
                n = byForm.size();
                byForm.put(form, n);
            }
            String id = "\u0000" + n + "\u0000";
            named.put(e, id);
            return id;
        }
    }

    /** The functions that read the clock (IQ's names, lower case; the reader's CURRENT DATE and CURRENT TIMESTAMP). */
    private static final Set<String> CLOCKS = Set.of("getdate", "now", "today", "current date", "current timestamp");

    /**
     * IQ island, 2026-10-14 (review 3's M3; the study's D-T13): SQL Anywhere reads GETDATE() apart from a statement's
     * other readings of the clock (measured: {@code GETDATE() = CURRENT TIMESTAMP} is mostly false, {@code GETDATE() <
     * NOW()} mostly true), where lite's databases read one clock per statement: a statement that reads GETDATE() beside
     * another reading of the clock is refused by name.
     */
    private static void clocks(SqlAst.Statement statement) {
        // by identity: an IF's condition is read twice in the reader's CASE, as one call
        Set<SqlAst.FunctionCall> readings = Collections.newSetFromMap(new IdentityHashMap<>());
        SqlAst.FunctionCall getdate = null;
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.Expr e : expressions(s)) {
                    List<SqlAst.FunctionCall> calls = new ArrayList<>();
                    clockCalls(e, calls);
                    for (SqlAst.FunctionCall f : calls) {
                        readings.add(f);
                        if (IqLexer.lower(f.name().get(0).text()).equals("getdate")) {
                            getdate = f;
                        }
                    }
                }
            }
        }
        if (getdate != null && readings.size() > 1) {
            throw SqlTranslationException.unsupported("GETDATE() beside another reading of the clock is not supported:"
                    + " SQL Anywhere reads GETDATE() apart from the statement's other clock readings, lite's databases"
                    + " read one clock (the study's D-T13)", getdate.pos());
        }
    }

    private static void clockCalls(SqlAst.Expr e, List<SqlAst.FunctionCall> out) {
        if (e instanceof SqlAst.FunctionCall f && f.name().size() == 1
                && CLOCKS.contains(IqLexer.lower(f.name().get(0).text()))) {
            out.add(f);
        }
        for (SqlAst.Expr child : children(e)) {
            clockCalls(child, out);
        }
    }

    /**
     * Each select item's {@link IqNames.Chains} (review 3's M1, measured on SQL Anywhere 17): SQL Anywhere keeps a
     * constant chain's parentheses where it runs the statement without optimizing it -- one SELECT, without DISTINCT,
     * grouping or a subquery, over nothing, DUMMY or one table, with no WHERE or one measured test ({@link #simple};
     * review 4's M1: any other WHERE is not known) -- and drops them where it optimizes it: a join, a
     * derived table, {@code sa_rowgenerator}, a set operation (its branches, even without FROM), GROUP BY or an
     * aggregate, DISTINCT, a subquery. Any other query's items (a subquery's, a derived table's) are not known.
     */
    private static Map<SqlAst.SelectExpr, IqNames.Chains> chains(SqlAst.Statement statement) {
        Map<SqlAst.SelectExpr, IqNames.Chains> out = new IdentityHashMap<>();
        Set<SqlAst.Select> top = Collections.newSetFromMap(new IdentityHashMap<>());
        topBranches(statement, top);
        for (SqlAst.Statement q : queries(statement)) {
            if (!(q instanceof SqlAst.Select s)) {
                continue;
            }
            IqNames.Chains mode = IqNames.Chains.UNKNOWN;
            if (top.contains(s)) {
                mode = !(statement instanceof SqlAst.Select) || optimized(s) ? IqNames.Chains.DROPPED
                        : simple(s) ? IqNames.Chains.KEPT : IqNames.Chains.UNKNOWN;
            }
            for (SqlAst.SelectItem item : s.items()) {
                if (item instanceof SqlAst.SelectExpr x) {
                    out.put(x, mode);
                }
            }
        }
        return out;
    }

    /** Whether SQL Anywhere certainly optimizes {@code s} (measured): a join, a derived table, sa_rowgenerator,
     *  grouping, DISTINCT, a subquery. */
    private static boolean optimized(SqlAst.Select s) {
        if (s.distinct() || IqAggregates.grouped(s) || s.from().size() > 1) {
            return true;
        }
        for (SqlAst.FromItem f : s.from()) {
            if (f instanceof SqlAst.Join) {
                return true;
            }
            for (SqlAst.FromItem leaf : leaves(f)) {
                if (leaf instanceof SqlAst.SubqueryFrom || leaf instanceof SqlAst.TableFunction fn
                        && !IqLexer.lower(fn.name().get(fn.name().size() - 1).text()).equals("tb")) {
                    return true;
                }
            }
        }
        for (SqlAst.Expr e : expressions(s)) {
            List<SqlAst.Statement> inside = new ArrayList<>();
            subqueries(e, inside);
            if (!inside.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether {@code s} is a statement SQL Anywhere runs unoptimized (measured): over nothing, DUMMY or one table, with
     * an ORDER BY or a row limit at most and a WHERE of one test at most. IQ island, 2026-10-14 (review 4's M1,
     * measured over the parity tables t, g, u, o and DUMMY, 115 statement shapes, every profile alike): the WHERE
     * keeps the parentheses only when it is one comparison ({@code = <> != < <= > >=}) of a column, written first,
     * with a constant -- a number that is not negative, a string or NULL (a sign {@code +} is no sign) --, a column
     * BETWEEN two such constants, or a column IS NULL; IN, OR, AND, NOT, LIKE, IS NOT NULL, NOT BETWEEN, IS DISTINCT
     * FROM, a truth test, a constant written first, a negative number, a CAST, a function or an expression on either
     * side, and a test of constants alone drop them. Any other statement's chain is not named ({@link #chains}).
     */
    private static boolean simple(SqlAst.Select s) {
        boolean source = s.from().isEmpty() || s.from().size() == 1 && (s.from().get(0) instanceof SqlAst.TableName
                || s.from().get(0) instanceof SqlAst.TableFunction fn
                && IqLexer.lower(fn.name().get(fn.name().size() - 1).text()).equals("tb"));
        return source && (s.where() == null || keptTest(java.util.Objects.requireNonNull(s.where())));
    }

    /** A WHERE under which SQL Anywhere keeps a constant chain's parentheses ({@link #simple}). */
    private static boolean keptTest(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.Binary b when b.op() == SqlAst.BinaryOp.EQ || b.op() == SqlAst.BinaryOp.NE
                    || b.op() == SqlAst.BinaryOp.LT || b.op() == SqlAst.BinaryOp.LE || b.op() == SqlAst.BinaryOp.GT
                    || b.op() == SqlAst.BinaryOp.GE -> b.left() instanceof SqlAst.ColumnRef && keptConstant(b.right());
            case SqlAst.Between b -> !b.negated() && b.operand() instanceof SqlAst.ColumnRef && keptConstant(b.low())
                    && keptConstant(b.high());
            case SqlAst.IsNull n -> !n.negated() && n.operand() instanceof SqlAst.ColumnRef;
            default -> false;
        };
    }

    private static boolean keptConstant(SqlAst.Expr e) {
        SqlAst.Expr x = e;
        while (x instanceof SqlAst.Unary u && u.op() == SqlAst.UnaryOp.PLUS) {
            x = u.operand();
        }
        return x instanceof SqlAst.IntegerLit n && n.value().signum() >= 0
                || x instanceof SqlAst.NumericLit d && d.value().signum() >= 0 || x instanceof SqlAst.StringLit
                || x instanceof SqlAst.NullLit;
    }

    /**
     * IQ island, 2026-10-14 (review 3's B1): the aggregate calls (by {@link IqAggregates#shape}) {@code statement} reads
     * only for some rows or groups -- inside an IF or a CASE, an argument of COALESCE, ISNULL, IFNULL, NULLIF or ARGN,
     * under AND, OR, NOT, a truth test, BETWEEN or IN in HAVING, anywhere in the select list or ORDER BY of a statement
     * with a row limit, anywhere in a subquery; and (review 4's B3) anywhere in a query other than the statement's own
     * SELECT -- a derived table's, a set operation's branch's -- whose column the query around it may read for some
     * rows only (IF, CASE, AND in its WHERE, TOP: measured, SQL Anywhere raises -158 where lite's databases
     * answered). A SUM's or an AVG's overflow check (D-E3) is computed where the value is read, so lite's databases
     * raise IQ's -158 only for the groups whose value they read, where SQL Anywhere raises in the aggregation itself,
     * for every group (measured): such a checked aggregate is refused there.
     */
    private static Set<String> lazyAggregates(SqlAst.Statement statement) {
        Set<SqlAst.Statement> inSubqueries = Collections.newSetFromMap(new IdentityHashMap<>());
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.Expr e : expressions(s)) {
                    subqueryQueries(e, inSubqueries);
                }
            }
        }
        Set<String> out = new java.util.HashSet<>();
        for (SqlAst.Statement q : queries(statement)) {
            if (!(q instanceof SqlAst.Select s0)) {
                continue;
            }
            SqlAst.Select s = IqAggregates.grouping(s0);
            // review 4's B3: a derived table's, a set operation's branch's or a subquery's aggregate is read where the
            // query around it reads its column, for some rows only as that query pleases -- only the statement's own
            // SELECT shows a checked one
            boolean all = s0 != statement || inSubqueries.contains(s0) || s.limit() != null || s.offset() != null;
            Set<String> lazyHere = new java.util.HashSet<>();
            Set<String> eagerHere = new java.util.HashSet<>();
            for (SqlAst.SelectItem item : s.items()) {
                if (item instanceof SqlAst.SelectExpr x) {
                    lazyCalls(x.expr(), all, lazyHere, eagerHere);
                }
            }
            s.orderBy().forEach(o -> lazyCalls(o.expr(), all, lazyHere, eagerHere));
            if (s.having() != null) {
                lazyCalls(java.util.Objects.requireNonNull(s.having()), all, lazyHere, eagerHere);
            }
            // IQ island, 2026-10-14 (the third landing): an aggregate the query also reads for every group is
            // computed, and checked, for every group
            lazyHere.removeAll(eagerHere);
            out.addAll(lazyHere);
        }
        return out;
    }

    /** The queries of the subqueries under {@code e}, and all theirs, into {@code out}. */
    private static void subqueryQueries(SqlAst.Expr e, Set<SqlAst.Statement> out) {
        SqlAst.Statement q = subqueryOf(e);
        if (q != null) {
            out.addAll(queries(q));
        }
        for (SqlAst.Expr child : children(e)) {
            subqueryQueries(child, out);
        }
    }

    /** The NULL-reading functions, whose later arguments are read for some rows only. */
    private static final Set<String> NULL_READERS = Set.of("coalesce", "isnull", "ifnull", "nullif", "argn");

    /**
     * The shapes of the aggregate calls under {@code e} into {@code lazyOut} where they are read for some rows or groups
     * only, into {@code eagerOut} where for every one. IQ island, 2026-10-14 (the third landing; measured on SQL
     * Anywhere 17): a CASE's (an IF's) first test, a NULL-reading function's first argument (NULLIF's both), a NOT's
     * and a truth test's operand, BETWEEN's and IN's tested value are read for every row their expression is read
     * for; a CASE's later tests and its values, the other arguments, the operands of AND and OR (a database may
     * evaluate either first) and BETWEEN's bounds and IN's list for some only.
     */
    private static void lazyCalls(SqlAst.Expr e, boolean lazy, Set<String> lazyOut, Set<String> eagerOut) {
        if (e instanceof SqlAst.FunctionCall f && IqAggregates.named(f)) {
            (lazy ? lazyOut : eagerOut).add(IqAggregates.shape(f));
            return;
        }
        List<SqlAst.Expr> parts = children(e);
        for (int i = 0; i < parts.size(); i++) {
            boolean here = lazy;
            if (e instanceof SqlAst.Case) {
                // its parts in order: [operand,] test, result, test, result, ..., [else]
                SqlAst.Case c = (SqlAst.Case) e;
                here |= i != (c.operand() != null ? 1 : 0) && !(c.operand() != null && i == 0);
            } else if (e instanceof SqlAst.FunctionCall f && f.name().size() == 1
                    && NULL_READERS.contains(IqLexer.lower(f.name().get(0).text()))) {
                here |= i > (IqLexer.lower(f.name().get(0).text()).equals("nullif") ? 1 : 0);
            } else if (e instanceof SqlAst.Binary b && (b.op() == SqlAst.BinaryOp.AND || b.op() == SqlAst.BinaryOp.OR)
                    || e instanceof SqlAst.Between || e instanceof SqlAst.InList) {
                here |= e instanceof SqlAst.Binary || i > 0;
            }
            lazyCalls(parts.get(i), here, lazyOut, eagerOut);
        }
    }

    /** Whether the statement reads aggregate call {@code f} only for some rows or groups ({@link #lazyAggregates}). */
    boolean lazilyRead(SqlAst.FunctionCall f) {
        return lazy.contains(IqAggregates.shape(f));
    }

    /** The aggregate calls (by {@link IqAggregates#shape}) of a query other than the statement's own SELECT -- a
     *  derived table's, a set operation's branch's, a subquery's -- whose groups the query around it may read for some
     *  only, before their aggregation (a WHERE on a group key reaching below it). IQ island, 2026-10-17. */
    final Set<String> outerLazy = new java.util.HashSet<>();
    /** IQ island, 2026-10-17: how IQ renders a derived table's column inside an unaliased item's name, by the derived
     *  table's correlation name and the column's, both in lower case ({@link #derivedRenderings}). */
    final Map<String, String> derivedRenderings = new java.util.HashMap<>();

    /** Whether the statement reads aggregate call {@code f} from a query around it ({@link #outerLazy}). */
    boolean lazilyReadAround(SqlAst.FunctionCall f) {
        return outerLazy.contains(IqAggregates.shape(f));
    }

    private static Set<String> outerAggregates(SqlAst.Statement statement) {
        Set<SqlAst.Statement> inSubqueries = Collections.newSetFromMap(new IdentityHashMap<>());
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.Expr e : expressions(s)) {
                    subqueryQueries(e, inSubqueries);
                }
            }
        }
        Set<String> out = new java.util.HashSet<>();
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s0 && (s0 != statement || inSubqueries.contains(s0))) {
                SqlAst.Select s = IqAggregates.grouping(s0);
                Set<String> eager = new java.util.HashSet<>();
                for (SqlAst.SelectItem item : s.items()) {
                    if (item instanceof SqlAst.SelectExpr x) {
                        lazyCalls(x.expr(), true, out, eager);
                    }
                }
                s.orderBy().forEach(o -> lazyCalls(o.expr(), true, out, eager));
                if (s.having() != null) {
                    lazyCalls(java.util.Objects.requireNonNull(s.having()), true, out, eager);
                }
            }
        }
        return out;
    }

    /**
     * IQ island, 2026-10-14 (the merge of M1b and the natives slice; measured on SQL Anywhere 17): the names (ASCII lower
     * case) of the columns of {@code statement}'s derived tables that SQL Anywhere may type by a folded constant's
     * digits. It merges a derived SELECT into its query, so a column defined by a constant integer expression enters a
     * NUMERIC quotient as that constant ({@code 1.5/x} over {@code (SELECT 2+3 AS x) d} divides a NUMERIC(1,0) and is
     * a NUMERIC(4,3); so do {@code 5} and {@code CAST(5 AS INT)}), where the column's own type is an integer's
     * NUMERIC(10,0); a set operation's column is typed by the set operation (measured: NUMERIC(10,0)). By name, over
     * the whole statement: a column defined without an aggregate, a subquery or a column other than such a one (to a
     * fixpoint, through nested derived tables).
     */
    static Set<String> folded(SqlAst.Statement statement) {
        List<SqlAst.SubqueryFrom> derivedTables = new ArrayList<>();
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.FromItem f : s.from()) {
                    for (SqlAst.FromItem leaf : leaves(f)) {
                        if (leaf instanceof SqlAst.SubqueryFrom d && d.query() instanceof SqlAst.Select) {
                            derivedTables.add(d);
                        }
                    }
                }
            }
        }
        Set<String> out = new java.util.HashSet<>();
        boolean grown = true;
        while (grown) {
            grown = false;
            for (SqlAst.SubqueryFrom d : derivedTables) {
                List<SqlAst.SelectItem> items = ((SqlAst.Select) d.query()).items();
                boolean starred = false;
                for (int i = 0; i < items.size(); i++) {
                    starred |= !(items.get(i) instanceof SqlAst.SelectExpr);
                    if (!(items.get(i) instanceof SqlAst.SelectExpr x) || !foldable(x.expr(), out)) {
                        continue;
                    }
                    if (starred && !d.columnAliases().isEmpty()) {
                        // IQ island, 2026-10-14 (review 4's B1): after a * the column list's name i is some earlier
                        // column's; this item's is one of the names from i on, so each of them may be folded
                        for (int j = i; j < d.columnAliases().size(); j++) {
                            grown |= out.add(IqLexer.lower(d.columnAliases().get(j).text()));
                        }
                        continue;
                    }
                    SqlAst.Ident alias = i < d.columnAliases().size() ? d.columnAliases().get(i) : x.alias();
                    String name = alias != null ? IqLexer.lower(alias.text())
                            : x.expr() instanceof SqlAst.ColumnRef r ? IqLexer.lower(r.parts().get(r.parts().size() - 1)
                            .text()) : null;
                    if (name != null) {
                        grown |= out.add(name);
                    }
                }
            }
        }
        return out;
    }

    /** Whether {@code e} reads no aggregate, no subquery and no column but one of {@code folded}. */
    private static boolean foldable(SqlAst.Expr e, Set<String> folded) {
        if (subqueryOf(e) != null || e instanceof SqlAst.FunctionCall f && IqAggregates.named(f)) {
            return false;
        }
        if (e instanceof SqlAst.ColumnRef r) {
            return folded.contains(IqLexer.lower(r.parts().get(r.parts().size() - 1).text()));
        }
        for (SqlAst.Expr child : children(e)) {
            if (!foldable(child, folded)) {
                return false;
            }
        }
        return true;
    }

    private static void shown(SqlAst.Select s, Set<SqlAst.SelectExpr> out) {
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x) {
                out.add(x);
            }
        }
    }

    /**
     * Where IQ's engine takes a subquery (the research's GRAMMAR E15 and appendix B1, the flag list; IQ island,
     * 2026-10-15: measured on IQ 17.1, which takes more): a parenthesized query anywhere in an item of the statement's
     * own select list (a set operation's branches included) but inside a conditional expression or an aggregate
     * ({@link #shownSubqueries}), or as a comparison's or a BETWEEN's operand in a WHERE, a HAVING or a join's ON;
     * EXISTS, IN and ANY, SOME, ALL (query) as their predicates. Refused (IQ's -1001030 or -1005004): a subquery in
     * ORDER BY or GROUP BY, inside arithmetic or an IN list in a condition, anywhere in a derived table's select list;
     * a set operation inside a subquery (IQ takes an uncorrelated one, not a correlated one: not told apart here).
     */
    /**
     * IQ island, 2026-10-15 (measured on IQ 17.1; the research's D-R1): what else IQ's engine refuses, refused before
     * translating with its code -- an ORDER BY item that is a negative integer constant ({@code ORDER BY -1}, its
     * -1005018 "Illegal ORDER BY item"), and an IN list holding a value that reads a column ({@code 1 IN (sm, it)},
     * its -1001030 "non-constant expression in IN list").
     */
    private static void iqEngineRestrictions(SqlAst.Statement statement) {
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.SetOperation o) {
                for (SqlAst.SortItem item : o.orderBy()) {
                    if (!(item.expr() instanceof SqlAst.IntegerLit)) {
                        // a set operation's ORDER BY reads positions only under IQ's engine (measured on IQ 17.1)
                        throw SqlTranslationException.unsupported("a set operation's ORDER BY by anything but a"
                                + " position is IQ's error -1005012 under its engine (the field cannot be found),"
                                + " refused before translating", item.pos());
                    }
                }
            }
            if (!(q instanceof SqlAst.Select s)) {
                continue;
            }
            for (SqlAst.SortItem o : s.orderBy()) {
                if (o.expr() instanceof SqlAst.Unary u && u.op() == SqlAst.UnaryOp.MINUS
                        && u.operand() instanceof SqlAst.IntegerLit) {
                    throw SqlTranslationException.unsupported("ORDER BY a negative constant is IQ's error -1005018"
                            + " under its engine (an illegal ORDER BY item), refused before translating", o.pos());
                }
            }
            for (SqlAst.Expr e : expressions(s)) {
                java.util.ArrayDeque<SqlAst.Expr> todo = new java.util.ArrayDeque<>();
                todo.push(e);
                while (!todo.isEmpty()) {
                    SqlAst.Expr x = todo.pop();
                    if (x instanceof SqlAst.InList in) {
                        for (SqlAst.Expr v : in.values()) {
                            List<SqlAst.ColumnRef> refs = new ArrayList<>();
                            columnRefs(v, refs);
                            if (!refs.isEmpty()) {
                                throw SqlTranslationException.unsupported("an IN list holding a column is IQ's error"
                                        + " -1001030 under its engine (a non-constant expression in an IN list),"
                                        + " refused before translating", v.pos());
                            }
                        }
                    }
                    children(x).forEach(todo::push);
                }
            }
        }
    }

    /**
     * IQ island, 2026-10-16 (checklist 2.16c, measured on IQ 17.1): under IQ's engine a CROSS or OUTER APPLY of a query
     * with FROM that reads the FROM items before it ({@link IqParser} marks it lateral) is IQ's error: -1006000 (its
     * internal error) where the query aggregates, is DISTINCT or computes a window function; -1001030 where it is a
     * UNION ("correlated UNION subqueries") or reads rows ("lateral derived table"). IQ answers an APPLY of a query that
     * reads nothing before it (a join), of one row computed from them (CROSS APPLY (SELECT t.id * 2 AS d) x), and of a
     * FROM function.
     */
    private static void appliedQueries(SqlAst.Statement statement) {
        for (SqlAst.Statement q : queries(statement)) {
            if (!(q instanceof SqlAst.Select s)) {
                continue;
            }
            for (SqlAst.FromItem f : s.from()) {
                for (SqlAst.FromItem leaf : leaves(f)) {
                    if (!(leaf instanceof SqlAst.SubqueryFrom d) || !d.lateral()
                            || d.query() instanceof SqlAst.Select one && one.from().isEmpty()) {
                        continue;
                    }
                    String applied = "a CROSS or OUTER APPLY of a query with FROM that reads the FROM items before it";
                    if (d.query() instanceof SqlAst.Select a && (a.distinct() || !a.groupBy().isEmpty()
                            || expressions(a).stream().anyMatch(e -> hasAggregate(e) || hasWindowCall(e)))) {
                        throw SqlTranslationException.unsupported(applied + " and aggregates, is DISTINCT or computes a"
                                + " window function is IQ's internal error -1006000 under its engine, refused before"
                                + " translating", d.pos());
                    }
                    throw SqlTranslationException.unsupported(applied + " is IQ's error -1001030 under its engine ("
                            + (d.query() instanceof SqlAst.SetOperation ? "correlated UNION subqueries"
                            : "a lateral derived table") + "), refused before translating", d.pos());
                }
            }
        }
    }

    /**
     * IQ island, 2026-10-16 (the corpus lane: 182 queries; measured on IQ 17.1 over 40 shapes), reworked 2026-10-17
     * (the lane's 154 refusals sent to IQ 17.1, which answered 5 and raised these for the rest, and 20 shapes more
     * measured): whether IQ's engine reads the scalar subqueries of {@code ds}, a derived table's (or a common table
     * expression's) SELECT, as its outer query {@code outer} reads them. A column of {@code ds} holds a subquery when its
     * item has one, or reads, unqualified, such a column of a derived table of its own ({@link #heldColumns}). IQ drops a
     * column no one reads before it plans it: one the outer query does not read raises nothing, its item's subqueries
     * left unchecked ({@code unread}). A column read is IQ's -1001030 where {@code ds} groups, aggregates or is DISTINCT
     * ({@code *} reads every column), or where it is read in WHERE, GROUP BY, HAVING or a join's ON, or inside a
     * conditional expression or an aggregate; its -1005004 where the outer query reads a held column qualified
     * ({@code d.n}; another column qualified, {@code *} and {@code d.*} are read), or orders by a held column no
     * select item reads (each refused here, naming it). Read in
     * the select list (in arithmetic and functions too, beside a join, beside a subquery of the outer query's own,
     * through nested derived tables), IQ answers. Refused by name (false): a held column read inside a subquery of the
     * outer query, or in a window; an item holding a subquery without a name.
     */
    private static boolean derivedSubqueriesRead(SqlAst.Select ds, SqlAst.Select outer, SqlAst.SubqueryFrom d,
            Map<SqlAst.Select, Set<String>> heldOf, Set<SqlAst.SelectExpr> unread, Set<SqlAst.Select> shownFrom,
            List<SqlTranslationException> later) {
        Set<String> held = heldColumns(ds, d.columnAliases(), heldOf);
        if (held == null) {
            return false;
        }
        if (held.isEmpty()) {
            return true;
        }
        // the landing's review (2026-10-17): measured where the outer query is the statement's own SELECT or a derived
        // table's under it, through FROM alone; a subquery's outer query, and a NATURAL or USING join's columns (read
        // by no expression), are refused by name
        if (!shownFrom.contains(outer)) {
            return false;
        }
        for (SqlAst.FromItem f : outer.from()) {
            List<SqlAst.Join> joins = new ArrayList<>();
            joins(f, joins);
            if (joins.stream().anyMatch(j -> j.natural() || !j.using().isEmpty())) {
                return false;
            }
        }
        if (ds.distinct()) {
            // IQ island, 2026-10-17 (measured on IQ 17.1: a DISTINCT derived table's FORMAT column no one reads is
            // its -265): every column makes its rows, read or not
            throw SqlTranslationException.unsupported("a column of a DISTINCT derived table that holds a subquery is"
                    + " IQ's error -1001030 under its engine (a scalar value subquery outside of a top level SELECT"
                    + " list), read or not, refused before translating", d.pos());
        }
        if (expressions(ds).stream().anyMatch(IqDialect::hasWindowCall)) {
            // IQ island, 2026-10-17 (the lane's 9 such queries; measured on IQ 17.1): a derived table computing a
            // window function is planned whole, so a column holding a subquery is its -1001030, read or not
            throw SqlTranslationException.unsupported("a derived table computing a window function beside a column"
                    + " that holds a subquery is IQ's error -1001030 under its engine (a scalar value subquery outside"
                    + " of a top level SELECT list), read or not, refused before translating", d.pos());
        }
        String alias = d.alias() == null ? null : IqLexer.lower(java.util.Objects.requireNonNull(d.alias()).text());
        Set<String> read = new java.util.HashSet<>();
        for (SqlAst.SelectItem i : outer.items()) {
            if (i instanceof SqlAst.AllColumns a && (a.qualifier() == null || alias != null
                    && IqLexer.lower(java.util.Objects.requireNonNull(a.qualifier()).text()).equals(alias))) {
                read.addAll(held);
            }
        }
        // what the outer query reads: its expressions, but its own items no one reads (IQ drops them too)
        List<SqlAst.Expr> live = new ArrayList<>();
        for (SqlAst.SelectItem i : outer.items()) {
            if (i instanceof SqlAst.SelectExpr x && !unread.contains(x)) {
                live.add(x.expr());
            }
        }
        for (SqlAst.Expr e : readExpressions(outer)) {
            if (outer.items().stream().noneMatch(i -> i instanceof SqlAst.SelectExpr x && x.expr() == e)) {
                live.add(e);
            }
        }
        SqlAst.ColumnRef qualified = null;
        for (SqlAst.Expr e : live) {
            List<SqlAst.ColumnRef> refs = new ArrayList<>();
            columnRefs(e, refs);
            for (SqlAst.ColumnRef r : refs) {
                String name = IqLexer.lower(r.parts().get(r.parts().size() - 1).text());
                boolean ofD = r.parts().size() >= 2 && IqLexer.lower(r.parts().get(r.parts().size() - 2).text())
                        .equals(alias);
                if (ofD && qualified == null && held.contains(name)) {
                    qualified = r;
                }
                if ((ofD || r.parts().size() == 1) && held.contains(name)) {
                    read.add(name);
                }
            }
        }
        boolean inSubquery = live.stream().anyMatch(e -> heldInSubquery(e, held, alias));
        if (read.isEmpty() && !inSubquery) {
            for (int i = 0; i < ds.items().size(); i++) {
                if (ds.items().get(i) instanceof SqlAst.SelectExpr x && held.contains(itemName(ds, i, d
                        .columnAliases()))) {
                    unread.add(x);
                }
            }
            return true;
        }
        if (!ds.groupBy().isEmpty() || expressions(ds).stream().anyMatch(IqDialect::hasAggregate)) {
            throw SqlTranslationException.unsupported("a column of a grouped or aggregating derived table"
                    + " that holds a subquery, read by its outer query, is IQ's error -1001030 under its engine (a"
                    + " scalar value subquery outside of a top level SELECT list), refused before translating",
                    d.pos());
        }
        if (qualified != null) {
            // IQ island, 2026-10-17 (the lane's IQ_ERROR verification, 4 queries): a held item's own subquery inside a
            // conditional expression or an aggregate is reported first, its -1001030
            for (int i = 0; i < ds.items().size(); i++) {
                if (ds.items().get(i) instanceof SqlAst.SelectExpr x && held.contains(itemName(ds, i,
                        d.columnAliases()))) {
                    shownSubqueries(x.expr());
                }
            }
            // IQ island, 2026-10-17 (the claim verification's SQLStorm, 5 queries): any -1001030 of the statement's
            // other derived tables is IQ's first, so this is raised once they are checked (subqueryPlaces), the
            // derived table read meanwhile as its outer query shows it
            later.add(SqlTranslationException.unsupported("a column of a derived table that holds a subquery, read"
                    + " qualified, is IQ's error -1005004 under its engine (subqueries are"
                    + " allowed only as arguments to comparisons, IN and EXISTS), refused before translating",
                    qualified.pos()));
            return true;
        }
        if (inSubquery) {
            return false;
        }
        List<SqlAst.Expr> conditions = new ArrayList<>(outer.groupBy());
        if (outer.where() != null) {
            conditions.add(java.util.Objects.requireNonNull(outer.where()));
        }
        if (outer.having() != null) {
            conditions.add(java.util.Objects.requireNonNull(outer.having()));
        }
        for (SqlAst.FromItem f : outer.from()) {
            List<SqlAst.Join> joins = new ArrayList<>();
            joins(f, joins);
            for (SqlAst.Join j : joins) {
                if (j.on() != null) {
                    conditions.add(java.util.Objects.requireNonNull(j.on()));
                }
            }
        }
        for (SqlAst.Expr e : conditions) {
            List<SqlAst.ColumnRef> refs = new ArrayList<>();
            columnRefs(e, refs);
            for (SqlAst.ColumnRef r : refs) {
                if (r.parts().size() == 1 && held.contains(IqLexer.lower(r.parts().get(0).text()))) {
                    throw SqlTranslationException.unsupported("a column of a derived table that holds a subquery, read"
                            + " in a WHERE, a GROUP BY, a HAVING or a join's ON, is IQ's error -1001030 under its"
                            + " engine (a reference containing a scalar value subquery outside of a top level SELECT"
                            + " list or ORDER BY), refused before translating", r.pos());
                }
            }
        }
        Set<String> listed = new java.util.HashSet<>();
        List<SqlAst.Expr> shown = new ArrayList<>();
        outer.items().forEach(i -> {
            if (i instanceof SqlAst.SelectExpr x && !unread.contains(x)) {
                shown.add(x.expr());
                List<SqlAst.ColumnRef> refs = new ArrayList<>();
                columnRefs(x.expr(), refs);
                refs.forEach(r -> listed.add(IqLexer.lower(r.parts().get(r.parts().size() - 1).text())));
            } else if (i instanceof SqlAst.AllColumns) {
                listed.addAll(held);
            }
        });
        for (SqlAst.SortItem o : outer.orderBy()) {
            shown.add(o.expr());
            List<SqlAst.ColumnRef> refs = new ArrayList<>();
            columnRefs(o.expr(), refs);
            for (SqlAst.ColumnRef r : refs) {
                String name = IqLexer.lower(r.parts().get(0).text());
                if (r.parts().size() == 1 && held.contains(name) && !listed.contains(name)
                        && outer.items().stream().noneMatch(i -> i instanceof SqlAst.SelectExpr x && x.alias() != null
                        && IqLexer.lower(java.util.Objects.requireNonNull(x.alias()).text()).equals(name))) {
                    throw SqlTranslationException.unsupported("a column of a derived table that holds a subquery,"
                            + " ordered by and read by no select item, is IQ's error -1005004 under its engine"
                            + " (subqueries are allowed only as arguments to comparisons, IN and EXISTS), refused"
                            + " before translating", r.pos());
                }
            }
        }
        for (SqlAst.Expr e : shown) {
            String where = heldInside(e, held, false);
            if (where != null) {
                if (where.equals("window")) {
                    return false;
                }
                throw SqlTranslationException.unsupported("a column of a derived table that holds a subquery, read"
                        + " inside " + where + ", is IQ's error -1001030 under its engine (a scalar value subquery "
                        + (where.equals("an aggregate") ? "outside of a top level SELECT list"
                        : "inside a conditional expression") + "), refused before translating", e.pos());
            }
        }
        return true;
    }

    /** The name a derived table shows its {@code i}-th item as: its column list's, its alias, its column's; null for
     *  none. */
    private static @com.legend.base.Nullable String itemName(SqlAst.Select ds, int i, List<SqlAst.Ident> columns) {
        if (!(ds.items().get(i) instanceof SqlAst.SelectExpr x)
                || !columns.isEmpty() && ds.items().stream().anyMatch(it -> it instanceof SqlAst.AllColumns)) {
            // the landing's review (2026-10-17): beside a *, a column list's names stand at no item's position
            return null;
        }
        SqlAst.Ident name = i < columns.size() ? columns.get(i) : x.alias() != null ? x.alias()
                : x.expr() instanceof SqlAst.ColumnRef r ? r.parts().get(r.parts().size() - 1) : null;
        return name == null ? null : IqLexer.lower(name.text());
    }

    /** The columns of {@code ds} (named by {@code columns} where given) that hold a subquery: an item with one, or
     *  one reading such a column of a derived table of its own FROM, unqualified or by its correlation name ({@link
     *  #derivedSubqueriesRead}); null where such an item has no name. */
    private static @com.legend.base.Nullable Set<String> heldColumns(SqlAst.Select ds, List<SqlAst.Ident> columns,
            Map<SqlAst.Select, Set<String>> heldOf) {
        // the held columns of the derived tables of its own FROM, all and by their correlation names
        Set<String> inner = new java.util.HashSet<>();
        Map<String, Set<String>> innerOf = new java.util.HashMap<>();
        for (SqlAst.FromItem f : ds.from()) {
            for (SqlAst.FromItem leaf : leaves(f)) {
                if (leaf instanceof SqlAst.SubqueryFrom d && d.query() instanceof SqlAst.Select below) {
                    Set<String> h = heldOf.containsKey(below) ? heldOf.get(below)
                            : heldColumns(below, d.columnAliases(), heldOf);
                    if (h != null) {
                        inner.addAll(h);
                        if (d.alias() != null) {
                            innerOf.put(IqLexer.lower(java.util.Objects.requireNonNull(d.alias()).text()), h);
                        }
                    }
                }
            }
        }
        Set<String> held = new java.util.HashSet<>();
        for (int i = 0; i < ds.items().size(); i++) {
            if (ds.items().get(i) instanceof SqlAst.AllColumns a) {
                // a * shows the held columns it reads under their own names (sqlstorm-5188's pd.*)
                held.addAll(a.qualifier() == null ? inner : innerOf.getOrDefault(IqLexer.lower(
                        java.util.Objects.requireNonNull(a.qualifier()).text()), Set.of()));
                continue;
            }
            if (!(ds.items().get(i) instanceof SqlAst.SelectExpr x)) {
                continue;
            }
            boolean holds = holdsSubquery(x.expr());
            if (!holds && !inner.isEmpty()) {
                List<SqlAst.ColumnRef> refs = new ArrayList<>();
                columnRefs(x.expr(), refs);
                holds = refs.stream().anyMatch(r -> {
                    String name = IqLexer.lower(r.parts().get(r.parts().size() - 1).text());
                    return r.parts().size() == 1 ? inner.contains(name) : innerOf.getOrDefault(IqLexer.lower(
                            r.parts().get(r.parts().size() - 2).text()), Set.of()).contains(name);
                });
            }
            if (holds) {
                String name = itemName(ds, i, columns);
                if (name == null) {
                    heldOf.put(ds, null);
                    return null;
                }
                held.add(name);
            }
        }
        heldOf.put(ds, held);
        return held;
    }

    /** Whether a subquery inside {@code e} reads {@code held}'s columns: unqualified, or qualified by {@code alias}. */
    private static boolean heldInSubquery(SqlAst.Expr e, Set<String> held, @com.legend.base.Nullable String alias) {
        SqlAst.Statement q = subqueryOf(e);
        if (q != null) {
            for (SqlAst.Statement inner : queries(q)) {
                if (inner instanceof SqlAst.Select s) {
                    for (SqlAst.Expr x : expressions(s)) {
                        List<SqlAst.ColumnRef> refs = new ArrayList<>();
                        columnRefs(x, refs);
                        for (SqlAst.ColumnRef r : refs) {
                            String name = IqLexer.lower(r.parts().get(r.parts().size() - 1).text());
                            boolean ofD = r.parts().size() >= 2 && IqLexer.lower(r.parts().get(r.parts().size() - 2)
                                    .text()).equals(alias);
                            if (held.contains(name) && (ofD || r.parts().size() == 1)) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        for (SqlAst.Expr child : children(e)) {
            if (heldInSubquery(child, held, alias)) {
                return true;
            }
        }
        return false;
    }

    /** Where in {@code e} a column of {@code held} is read inside a conditional expression, an aggregate or a window:
     *  "a conditional expression", "an aggregate", "window", or null for nowhere. */
    private static @com.legend.base.Nullable String heldInside(SqlAst.Expr e, Set<String> held, boolean within) {
        if (within && e instanceof SqlAst.ColumnRef r && r.parts().size() == 1
                && held.contains(IqLexer.lower(r.parts().get(0).text()))) {
            return "";
        }
        String place = null;
        if (e instanceof SqlAst.Case) {
            place = "a conditional expression";
        } else if (e instanceof SqlAst.Window) {
            place = "window";
        } else if (e instanceof SqlAst.FunctionCall f) {
            String fn = IqLexer.lower(f.name().get(f.name().size() - 1).text());
            place = CONDITIONALS.contains(fn) ? "a conditional expression" : IqAggregates.isAggregate(fn)
                    ? "an aggregate" : null;
        }
        for (SqlAst.Expr child : children(e)) {
            String found = heldInside(child, held, within || place != null);
            if (found != null) {
                // a column found inside: the innermost place holding it names it
                return found.isEmpty() && place != null ? place : found;
            }
        }
        return null;
    }

    /** IQ island, 2026-10-15 (the corpus lane: 243 of SQLStorm's and SEDE's queries; measured on IQ 17.1, where the
     *  research's D-R1 held that IQ may ignore it): a row limit in a derived table, a common table expression, or a
     *  subquery in WHERE, IN, a join's ON or the select list is IQ's error under its engine. 2026-10-17 (the lane's
     *  IQ_ERROR verification: 7 of 7): reported before the subqueries' places ({@link #subqueryPlaces}), as IQ does. */
    private static void limitedSubqueries(SqlAst.Statement statement) {
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s && s != statement && (s.limit() != null || s.offset() != null)) {
                SqlAst.Expr at = s.limit() != null ? s.limit() : s.offset();
                throw SqlTranslationException.unsupported("TOP, FIRST or START AT in a subquery or a derived table"
                        + " is IQ's error -1001030 under its engine (TOP/FIRST/LIMIT in a subquery or a view is not"
                        + " supported), refused before translating", java.util.Objects.requireNonNull(at).pos());
            }
        }
    }

    /**
     * IQ island, 2026-10-17 (found by verifying the corpus lane's IQ_ERROR claims: IQ 17.1 answered 4 SEDE queries whose
     * derived table computes FORMAT(...) in a column the outer query never reads; 23 SQLStorm and SEDE queries more):
     * IQ's engine drops a derived table's (or a common table expression's) column no one reads before it resolves the
     * column's functions or computes its values -- its names still bind (-143) and a grouped table's columns are still
     * checked (-149), but a function of no name of IQ's there raises nothing (its -265 where read; a DISTINCT or
     * windowed derived table is planned whole, so its -265 there). Measured: {@code FORMAT(nosuch, 'x')} -143,
     * {@code STRING_AGG(vc, ',')} over an ungrouped column -149, {@code LEFT(FORMAT(dt, 'yy'), 1)}, {@code 1 / 0} and
     * {@code CAST('x' AS INT)} answered. Where the item holds such calls -- the outermost of them of a string's result
     * (STRING_AGG, FORMAT, CONCAT, ...), alone or inside an expression -- and its column is read nowhere -- not by the
     * outer query, at any depth, nor by the derived table's own clauses, with no NATURAL or USING join there -- each
     * call is read as a NULL string ({@code placeheld}; its arguments translated, so their names bind and their
     * grouping is checked). Any other such call in a column the
     * outer query's own clauses do not read is refused by name rather than claim the -265.
     */
    private static void unreadUnknownFunctions(SqlAst.Statement statement, Set<SqlAst.FunctionCall> placeheld) {
        for (SqlAst.Statement q : queries(statement)) {
            if (!(q instanceof SqlAst.Select outer)) {
                continue;
            }
            boolean matched = false;
            for (SqlAst.FromItem f : outer.from()) {
                List<SqlAst.Join> joins = new ArrayList<>();
                joins(f, joins);
                matched |= joins.stream().anyMatch(j -> j.natural() || !j.using().isEmpty());
            }
            for (SqlAst.FromItem f : outer.from()) {
                for (SqlAst.FromItem leaf : leaves(f)) {
                    if (!(leaf instanceof SqlAst.SubqueryFrom d) || !(d.query() instanceof SqlAst.Select ds)
                            || ds.distinct() || expressions(ds).stream().anyMatch(IqDialect::hasWindowCall)) {
                        continue;
                    }
                    String alias = d.alias() == null ? null
                            : IqLexer.lower(java.util.Objects.requireNonNull(d.alias()).text());
                    if (outer.items().stream().anyMatch(i -> i instanceof SqlAst.AllColumns a && (a.qualifier() == null
                            || IqLexer.lower(java.util.Objects.requireNonNull(a.qualifier()).text()).equals(alias)))) {
                        continue;
                    }
                    Set<String> read = new java.util.HashSet<>();
                    Set<String> readDeep = new java.util.HashSet<>();
                    for (SqlAst.Expr e : readExpressions(outer)) {
                        namesRead(e, alias, read);
                        List<SqlAst.Statement> subs = new ArrayList<>();
                        subqueries(e, subs);
                        for (SqlAst.Statement sub : subs) {
                            for (SqlAst.Statement inner : queries(sub)) {
                                if (inner instanceof SqlAst.Select is) {
                                    readExpressions(is).forEach(x -> namesRead(x, alias, readDeep));
                                }
                            }
                        }
                    }
                    readDeep.addAll(read);
                    // the derived table's own clauses, which may name an item by its alias
                    for (SqlAst.Expr e : readExpressions(ds)) {
                        if (ds.items().stream().noneMatch(i -> i instanceof SqlAst.SelectExpr x && x.expr() == e)) {
                            namesRead(e, null, readDeep);
                        }
                    }
                    for (int i = 0; i < ds.items().size(); i++) {
                        String name = itemName(ds, i, d.columnAliases());
                        if (name != null && !read.contains(name) && ds.items().get(i) instanceof SqlAst.SelectExpr x) {
                            List<SqlAst.FunctionCall> arrays = new ArrayList<>();
                            arrayAggregates(x.expr(), arrays);
                            if (!arrays.isEmpty()) {
                                // IQ island, 2026-10-17 (measured on IQ 17.1): an ARRAY_AGG in a column no one reads
                                // is dropped with it, the derived table grouping still
                                if (x.expr() == arrays.get(0) && !matched && !readDeep.contains(name)
                                        && !ds.groupBy().isEmpty()) {
                                    placeheld.add(arrays.get(0));
                                    continue;
                                }
                                throw SqlTranslationException.unsupported("ARRAY_AGG in derived table column '" + name
                                        + "', read nowhere, is not supported here: IQ 17.1 drops a column no one reads"
                                        + " (it answers), and the island reads that only for a whole item of a grouped"
                                        + " derived table", arrays.get(0).pos());
                            }
                            SqlAst.FunctionCall unknown = unknownCall(x.expr());
                            if (unknown == null) {
                                continue;
                            }
                            List<SqlAst.FunctionCall> all = new ArrayList<>();
                            unknownCalls(x.expr(), all);
                            List<SqlAst.FunctionCall> outermost = new ArrayList<>();
                            outermostUnknownCalls(x.expr(), outermost);
                            // IQ island, 2026-10-17 (the corpus lane's JOB and TPC-DS, ~30 queries: COALESCE(STRING_AGG(
                            // DISTINCT ...), 'none'), LENGTH(CONCAT(...))): such calls held in an expression too, each
                            // outermost one a string's result, every one of them read as a NULL string
                            if (!matched && !readDeep.contains(name) && outermost.stream().allMatch(c ->
                                    STRING_RESULTS.contains(IqLexer.lower(c.name().get(c.name().size() - 1).text())))) {
                                placeheld.addAll(all);
                                continue;
                            }
                            throw SqlTranslationException.unsupported(unknown.name().get(unknown.name().size() - 1)
                                    .text() + "(...), no function of IQ's, in derived table column '" + name + "' is"
                                    + " not supported here: IQ 17.1 drops a column no one reads before it resolves its"
                                    + " functions (it answers) and raises its -265 for one read, and this one is read"
                                    + " only inside a subquery or past a NATURAL or USING join, or holds the call in an"
                                    + " expression or one that is no string's, which the island does not read yet",
                                    unknown.pos());
                        }
                    }
                }
            }
        }
    }

    /** The names of no function of IQ's whose call is a string (other dialects'), read as a NULL string where the
     *  column holding the call is read nowhere ({@link #unreadUnknownFunctions}). */
    private static final Set<String> STRING_RESULTS = Set.of("string_agg", "concat", "concat_ws", "format",
            "split_part", "json_value", "group_concat", "strftime", "translate", "initcap", "quotename",
            "substring_index", "splitpart", "parsename", "regexp_replace", "md5", "chr", "array_to_string");

    /** A select's expressions ({@link #expressions}) and its named windows' keys (the landing's review, 2026-10-17: a
     *  WINDOW clause's PARTITION BY reads a derived table's column as an OVER's does). */
    private static List<SqlAst.Expr> readExpressions(SqlAst.Select s) {
        List<SqlAst.Expr> all = new ArrayList<>(expressions(s));
        for (SqlAst.NamedWindow w : s.windows()) {
            all.addAll(w.spec().partitionBy());
            w.spec().orderBy().forEach(o -> all.add(o.expr()));
        }
        return all;
    }

    /** Into {@code out}, the names (lower case) of the columns {@code e} reads, outside its subqueries, qualified by
     *  {@code alias} or not qualified. */
    private static void namesRead(SqlAst.Expr e, @com.legend.base.Nullable String alias, Set<String> out) {
        List<SqlAst.ColumnRef> refs = new ArrayList<>();
        columnRefs(e, refs);
        for (SqlAst.ColumnRef r : refs) {
            if (r.parts().size() == 1 || alias != null && IqLexer.lower(r.parts().get(r.parts().size() - 2).text())
                    .equals(alias)) {
                out.add(IqLexer.lower(r.parts().get(r.parts().size() - 1).text()));
            }
        }
    }

    /** Into {@code out}, every call in {@code e} (outside its subqueries) of a name no function of IQ's has. */
    private static void unknownCalls(SqlAst.Expr e, List<SqlAst.FunctionCall> out) {
        if (e instanceof SqlAst.FunctionCall f && IqFunctions.NO_SUCH_FUNCTION.contains(IqLexer.lower(
                f.name().get(f.name().size() - 1).text()))) {
            out.add(f);
        }
        for (SqlAst.Expr child : children(e)) {
            unknownCalls(child, out);
        }
    }

    /** Into {@code out}, the calls in {@code e} (outside its subqueries) of a name no function of IQ's has that no
     *  other such call holds. */
    private static void outermostUnknownCalls(SqlAst.Expr e, List<SqlAst.FunctionCall> out) {
        if (e instanceof SqlAst.FunctionCall f && IqFunctions.NO_SUCH_FUNCTION.contains(IqLexer.lower(
                f.name().get(f.name().size() - 1).text()))) {
            out.add(f);
            return;
        }
        for (SqlAst.Expr child : children(e)) {
            outermostUnknownCalls(child, out);
        }
    }

    /** A call in {@code e} (outside its subqueries) of a name no function of IQ's has ({@link
     *  IqFunctions#NO_SUCH_FUNCTION}); null for none. */
    private static SqlAst.@com.legend.base.Nullable FunctionCall unknownCall(SqlAst.Expr e) {
        if (e instanceof SqlAst.FunctionCall f && IqFunctions.NO_SUCH_FUNCTION.contains(IqLexer.lower(
                f.name().get(f.name().size() - 1).text()))) {
            return f;
        }
        for (SqlAst.Expr child : children(e)) {
            SqlAst.FunctionCall found = unknownCall(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /** Whether every subquery in {@code e} is a scalar one outside a conditional expression (the landing's review,
     *  2026-10-17: ORDER BY CASE WHEN EXISTS (...) is IQ's -1001030, a scalar subquery's -1005004). */
    private static boolean scalarOutsideConditionals(SqlAst.Expr e) {
        if (e instanceof SqlAst.Exists || e instanceof SqlAst.InSubquery || e instanceof SqlAst.Quantified) {
            return false;
        }
        if ((e instanceof SqlAst.Case || e instanceof SqlAst.FunctionCall f && CONDITIONALS.contains(IqLexer.lower(
                f.name().get(f.name().size() - 1).text()))) && subqueryIn(e)) {
            return false;
        }
        for (SqlAst.Expr child : children(e)) {
            if (!scalarOutsideConditionals(child)) {
                return false;
            }
        }
        return true;
    }

    /** Whether a subquery stands in {@code e} (outside its subqueries' own expressions). */
    private static boolean subqueryIn(SqlAst.Expr e) {
        return subqueryOf(e) != null || children(e).stream().anyMatch(IqDialect::subqueryIn);
    }

    /** IQ island, 2026-10-17: a CONVERT style of IQ's engine's only (36-40, 136-140, 365) or of neither's, under SQL
     *  Anywhere's front end (a query over no IQ table), refused by name as the reader did before its engine's styles
     *  were read (the front end's styles differ; it raises its own error for some: style 365, the syntax research's
     *  a3_sa_fn1-416). */
    private static void frontEndStyles(SqlAst.Statement statement) {
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.Expr e : expressions(s)) {
                    frontEndStyle(e);
                }
            }
        }
    }

    private static void frontEndStyle(SqlAst.Expr e) {
        if (e instanceof SqlAst.FunctionCall f && f.name().size() == 1 && !f.name().get(0).quoted()
                && f.name().get(0).text().equals(IqParser.CONVERT_STYLE) && f.args().size() == 3
                && f.args().get(1) instanceof SqlAst.IntegerLit n
                && !FRONT_END_STYLES.contains(n.value().intValue())) {
            throw com.legend.protocol.sql.SqlSyntaxException.unsupported("CONVERT style " + n.value() + " is not"
                    + " supported under SQL Anywhere's front end (a query over no IQ table): the island reads its"
                    + " styles 0-14, 20, 21, 100-114, 120 and 121", n.pos());
        }
        for (SqlAst.Expr child : children(e)) {
            frontEndStyle(child);
        }
    }

    /** The CONVERT styles the reader passes under SQL Anywhere's front end ({@link #frontEndStyles}). */
    private static final Set<Integer> FRONT_END_STYLES = Set.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 20,
            21, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 120, 121);

    /** IQ island, 2026-10-17 (the corpus lane's SQLStorm, GROUP BY (SELECT COUNT(*) FROM u), ...; measured on IQ
     *  17.1, correlated or not, with or without a FROM): a subquery in a GROUP BY item is IQ's -1001030 under its engine
     *  (a scalar value subquery outside of a top level SELECT list); the front end's reading is not built. */
    private static void groupBySubqueries(SqlAst.Statement statement, Engine engine) {
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.Expr g : s.groupBy()) {
                    refuseSubquery(g, engine == Engine.IQ ? "a subquery in GROUP BY is IQ's error -1001030 under its"
                            + " engine (a scalar value subquery outside of a top level SELECT list), refused before"
                            + " translating" : "a subquery in GROUP BY is not supported yet under SQL Anywhere's front"
                            + " end (a query over no IQ table)");
                }
            }
        }
    }

    private static void subqueryPlaces(SqlAst.Statement statement, Set<SqlAst.Join> innerJoins) {
        Map<SqlAst.Select, List<SqlAst.Expr>> held = heldNonNull(statement);
        Set<SqlAst.Select> top = Collections.newSetFromMap(new IdentityHashMap<>());
        topBranches(statement, top);
        Set<SqlAst.Select> inDerived = Collections.newSetFromMap(new IdentityHashMap<>());
        // a derived table's own SELECT, read as its outer query reads it ({@link #derivedSubqueriesRead})
        Set<SqlAst.Select> readable = Collections.newSetFromMap(new IdentityHashMap<>());
        Map<SqlAst.Select, Set<String>> heldOf = new IdentityHashMap<>();
        // a derived table's items whose columns no one reads: IQ drops them before it plans them
        Set<SqlAst.SelectExpr> unread = Collections.newSetFromMap(new IdentityHashMap<>());
        // the statement's own SELECTs and the derived tables' under them, through FROM alone
        Set<SqlAst.Select> shownFrom = Collections.newSetFromMap(new IdentityHashMap<>());
        // IQ's errors raised once the others are checked (derivedSubqueriesRead)
        List<SqlTranslationException> later = new ArrayList<>();
        java.util.ArrayDeque<SqlAst.Select> pending = new java.util.ArrayDeque<>(top);
        while (!pending.isEmpty()) {
            SqlAst.Select s = pending.pop();
            if (shownFrom.add(s)) {
                for (SqlAst.FromItem f : s.from()) {
                    for (SqlAst.FromItem leaf : leaves(f)) {
                        if (leaf instanceof SqlAst.SubqueryFrom d) {
                            List<SqlAst.Select> own = new ArrayList<>();
                            branches(d.query(), own);
                            own.forEach(pending::push);
                        }
                    }
                }
            }
        }
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.FromItem f : s.from()) {
                    for (SqlAst.FromItem leaf : leaves(f)) {
                        if (leaf instanceof SqlAst.SubqueryFrom d) {
                            queries(d.query()).forEach(x -> {
                                if (x instanceof SqlAst.Select ds) {
                                    inDerived.add(ds);
                                }
                            });
                            if (d.query() instanceof SqlAst.Select ds && derivedSubqueriesRead(ds, s, d, heldOf,
                                    unread, shownFrom, later)) {
                                readable.add(ds);
                            }
                        }
                    }
                }
            }
        }
        for (SqlAst.Statement q : queries(statement)) {
            if (!(q instanceof SqlAst.Select s)) {
                continue;
            }
            for (SqlAst.SelectItem item : s.items()) {
                if (item instanceof SqlAst.SelectExpr x) {
                    if (unread.contains(x)) {
                        // IQ island, 2026-10-17 (measured on IQ 17.1): a column no one reads, which IQ never plans
                        continue;
                    }
                    if (readable.contains(s)) {
                        // IQ island, 2026-10-16 (measured on IQ 17.1): as the outer query's own select list's
                        shownSubqueries(x.expr());
                    } else if (inDerived.contains(s)) {
                        // IQ island, 2026-10-15 (the corpus lane; measured on IQ 17.1): IQ's engine reads a scalar
                        // subquery there only where the outer query shows the column as it is -- read in its WHERE or
                        // GROUP BY, beside a join, or in a grouped derived query it is IQ's -1001030 or -1005004
                        refuseSubquery(x.expr(), "a subquery in the select list of a derived table or a common"
                                + " table expression is not supported yet: IQ's engine reads one there only where the"
                                + " outer query shows its column as it is (read in a WHERE or a GROUP BY, beside a join,"
                                + " or in a grouped query, it is IQ's -1001030 or -1005004)");
                    } else if (top.contains(s)) {
                        shownSubqueries(x.expr());
                    } else {
                        noSubquery(x.expr(), "the statement's own select list");
                    }
                }
            }
            if (s.where() != null) {
                predicates(java.util.Objects.requireNonNull(s.where()));
            }
            // IQ island, 2026-10-14 (M1c): HAVING as WHERE; none in GROUP BY
            if (s.having() != null) {
                predicates(java.util.Objects.requireNonNull(s.having()));
            }
            s.groupBy().forEach(k -> noSubquery(k, "GROUP BY"));
            for (SqlAst.SortItem o : s.orderBy()) {
                if (top.contains(s) && scalarOutsideConditionals(o.expr())) {
                    // IQ island, 2026-10-17 (the corpus lane's 4; measured on IQ 17.1, correlated or not, shown as an
                    // item or not, in arithmetic too): IQ's -1005004
                    refuseSubquery(o.expr(), "a subquery in ORDER BY is IQ's error -1005004 under its engine (subqueries"
                            + " are allowed only as arguments to comparisons, IN and EXISTS), refused before"
                            + " translating");
                } else {
                    noSubquery(o.expr(), "ORDER BY");
                }
            }
            for (SqlAst.FromItem f : s.from()) {
                List<SqlAst.Join> joins = new ArrayList<>();
                joins(f, joins);
                for (SqlAst.Join j : joins) {
                    if (j.on() != null && j.type() != SqlAst.JoinType.INNER) {
                        SqlAst.Expr on = java.util.Objects.requireNonNull(j.on());
                        // the landing's review (2026-10-17, measured on IQ 17.1): IQ reads a rejected outer join as
                        // inner, a window over it -- here or in a derived table between -- numbering the inner rows;
                        // where no window reads its rows, the inner join's rows are the outer one's
                        if (nullRejected(rejecting(s, j), held.getOrDefault(s, List.of()), j)) {
                            innerJoins.add(j);
                        } else {
                            outerJoinSubqueries(j, on);
                        }
                    }
                    if (j.on() != null) {
                        // IQ island, 2026-10-15 (measured on IQ 17.1): an ON takes them as a WHERE does
                        predicates(java.util.Objects.requireNonNull(j.on()));
                    }
                }
            }
        }
        if (!later.isEmpty()) {
            throw later.get(0);
        }
    }

    /** The SELECTs of the statement's own select lists: itself, or its set operation's branches. */
    private static void topBranches(SqlAst.Statement q, Set<SqlAst.Select> out) {
        if (q instanceof SqlAst.Select s) {
            out.add(s);
        } else if (q instanceof SqlAst.SetOperation o) {
            topBranches(o.left(), out);
            topBranches(o.right(), out);
        }
    }

    /** A WHERE's condition: EXISTS, IN, ANY/ALL subqueries as predicates, a parenthesized query as a comparison's
     *  operand; every other subquery refused. IQ island, 2026-10-16 (the corpus lane; measured on IQ 17.1): a
     *  comparison of two subqueries, and a subquery as a BETWEEN's operand, are IQ's error -1001030; so is a subquery
     *  anywhere else in a condition ({@link #conditionSubquery}). */
    private static void predicates(SqlAst.Expr e) {
        switch (e) {
            case SqlAst.Binary b when b.op() == SqlAst.BinaryOp.AND || b.op() == SqlAst.BinaryOp.OR -> {
                predicates(b.left());
                predicates(b.right());
            }
            case SqlAst.Unary u when u.op() == SqlAst.UnaryOp.NOT -> predicates(u.operand());
            case SqlAst.IsTruth t -> predicates(t.operand());
            case SqlAst.Binary b when b.op() == SqlAst.BinaryOp.EQ || b.op() == SqlAst.BinaryOp.NE
                    || b.op() == SqlAst.BinaryOp.LT || b.op() == SqlAst.BinaryOp.LE || b.op() == SqlAst.BinaryOp.GT
                    || b.op() == SqlAst.BinaryOp.GE -> {
                if (b.left() instanceof SqlAst.ScalarSubquery && b.right() instanceof SqlAst.ScalarSubquery) {
                    throw SqlTranslationException.unsupported("a comparison of two subqueries is IQ's error -1001030"
                            + " under its engine (a subquery on both sides of an expression), refused before"
                            + " translating", b.pos());
                }
                for (SqlAst.Expr side : List.of(b.left(), b.right())) {
                    if (side instanceof SqlAst.ScalarSubquery q) {
                        noSetOperation(q.query(), q.pos());
                    } else {
                        conditionSubquery(side);
                    }
                }
            }
            case SqlAst.Between b -> {
                // IQ island, 2026-10-15 (measured on IQ 17.1): a BETWEEN's bounds, as a comparison's operand
                if (b.operand() instanceof SqlAst.ScalarSubquery q) {
                    throw SqlTranslationException.unsupported("a subquery as a BETWEEN's operand is IQ's error"
                            + " -1001030 under its engine (a subquery on the left side of BETWEEN), refused before"
                            + " translating", q.pos());
                }
                conditionSubquery(b.operand());
                for (SqlAst.Expr side : List.of(b.low(), b.high())) {
                    if (side instanceof SqlAst.ScalarSubquery q) {
                        noSetOperation(q.query(), q.pos());
                    } else {
                        conditionSubquery(side);
                    }
                }
            }
            case SqlAst.Exists x -> noSetOperation(x.query(), x.pos());
            case SqlAst.InSubquery in -> {
                conditionSubquery(in.operand());
                unionOnly(in.query(), in.pos());
            }
            case SqlAst.Quantified q -> {
                conditionSubquery(q.left());
                noSetOperation(q.query(), q.pos());
            }
            default -> conditionSubquery(e);
        }
    }

    /** IQ island, 2026-10-16 (the corpus lane; measured on IQ 17.1 in WHERE, HAVING and ON): a subquery inside an
     *  expression of a condition -- arithmetic, a function's argument, a CAST, a concatenation, a negation, LIKE, IS
     *  NULL, an IN list, IN's or a quantified comparison's left side -- is IQ's error -1001030 (a scalar value subquery
     *  outside of a top level SELECT list; inside CASE or COALESCE, inside a conditional expression). */
    private static void conditionSubquery(SqlAst.Expr e) {
        refuseSubquery(e, "a subquery inside an expression of a WHERE, a HAVING or an ON (but as a comparison's"
                + " operand or a BETWEEN's bound, or as EXISTS, IN or ANY's query) is IQ's error -1001030 under its"
                + " engine (a scalar value subquery outside of a top level SELECT list), refused before translating");
    }

    /** IQ's conditional expressions besides CASE and IF (which the reader reads as a CASE): a subquery inside one is
     *  IQ's -1001030 under its engine (measured on IQ 17.1). */
    private static final Set<String> CONDITIONALS = Set.of("coalesce", "isnull", "ifnull", "nullif", "argn");

    /**
     * A subquery in an item of the statement's own select list (IQ island, 2026-10-15; measured on IQ 17.1, which the
     * research's flag list held to a whole item): a scalar subquery anywhere -- in arithmetic, a function's arguments,
     * a concatenation, a CAST -- but inside a conditional expression (CASE, IF, COALESCE, ISNULL, IFNULL, NULLIF, ARGN:
     * IQ's -1001030) or an aggregate (-1001030, "outside of a top level SELECT list"); no predicate (EXISTS, IN,
     * ANY, ALL), which IQ reads only inside a conditional there.
     */
    private static void shownSubqueries(SqlAst.Expr e) {
        String outside = "a subquery inside a conditional expression (CASE, IF, COALESCE, ISNULL, IFNULL, NULLIF, ARGN)"
                + " or an aggregate is IQ's error -1001030 under its engine (measured on IQ 17.1), refused before"
                + " translating";
        switch (e) {
            case SqlAst.ScalarSubquery q -> noSetOperation(q.query(), q.pos());
            case SqlAst.Case c -> refuseSubquery(c, outside);
            case SqlAst.Exists x -> refuseSubquery(x, outside);
            case SqlAst.InSubquery in -> refuseSubquery(in, outside);
            case SqlAst.Quantified q -> refuseSubquery(q, outside);
            case SqlAst.FunctionCall f when CONDITIONALS.contains(IqLexer.lower(f.name().get(f.name().size() - 1)
                    .text())) || IqAggregates.isAggregate(IqLexer.lower(f.name().get(f.name().size() - 1).text())) ->
                    refuseSubquery(f, outside);
            case SqlAst.Window w -> refuseSubquery(w, outside);
            default -> {
                for (SqlAst.Expr child : children(e)) {
                    shownSubqueries(child);
                }
            }
        }
    }

    private static void noSubquery(SqlAst.Expr e, String place) {
        refuseSubquery(e, "a subquery here is not supported under IQ's engine, which restricts it (the research's flag"
                + " list): IQ takes one in " + place);
    }

    /**
     * IQ island, 2026-10-15 (the corpus lane: 124 of SQLStorm's queries, which DuckDB cannot plan either; measured on IQ
     * 17.1 over 21 shapes): an outer join's ON takes a subquery only in a conjunct (of its top AND) that reads nothing
     * but the join's null-supplying side -- a LEFT JOIN's right, a RIGHT JOIN's left -- by its own columns and, through
     * correlation, by its subqueries' ({@code u.id > (SELECT MIN(id) FROM u)}, {@code EXISTS (SELECT 1 FROM g WHERE
     * g.id = u.id)}, an uncorrelated EXISTS); any other, and any in a FULL JOIN's ON, is IQ's error -1001047, an IN, an
     * ANY, an EXISTS and a scalar subquery alike (a later join's ON reading an earlier join's table too). An unqualified
     * column in such a conjunct is refused by name: which side holds it decides.
     *
     * <p>IQ island, 2026-10-17 (the corpus lane's SQLStorm, 6 queries; measured on IQ 17.1): and only beside a conjunct
     * of no subquery that reads a column, of either side ({@code u.t_id = t.id}, {@code u.t_id > 0}, {@code t.id > 0}):
     * an ON of subquery conjuncts alone, or beside constant ones ({@code 1 = 1}), is IQ's error -1001047 too.
     */
    private static void outerJoinSubqueries(SqlAst.Join j, SqlAst.Expr on) {
        Set<String> nullSide = new java.util.HashSet<>();
        if (j.type() != SqlAst.JoinType.FULL) {
            for (SqlAst.FromItem leaf : leaves(j.type() == SqlAst.JoinType.LEFT ? j.right() : j.left())) {
                SqlAst.Ident name = correlation(leaf);
                if (name != null) {
                    nullSide.add(IqLexer.lower(name.text()));
                }
            }
        }
        List<SqlAst.Expr> conjuncts = new ArrayList<>();
        andConjuncts(on, conjuncts);
        boolean joined = conjuncts.stream().anyMatch(c -> !holdsSubquery(c) && readsColumn(c));
        for (SqlAst.Expr c : conjuncts) {
            if (!holdsSubquery(c)) {
                continue;
            }
            if (!joined) {
                throw SqlTranslationException.unsupported("a subquery in an outer join's ON beside no condition of"
                        + " a column without one is IQ's error -1001047 under its engine (subqueries are not supported"
                        + " in outer join ON clauses), refused before translating", c.pos());
            }
            Set<String> read = new java.util.HashSet<>();
            boolean[] unqualified = {false};
            tablesRead(c, Set.of(), read, unqualified);
            if (j.type() == SqlAst.JoinType.FULL || !nullSide.containsAll(read)) {
                throw SqlTranslationException.unsupported("a subquery in an outer join's ON that reads a table but the"
                        + " join's null-supplying side is IQ's error -1001047 under its engine (subqueries are not"
                        + " supported in outer join ON clauses), refused before translating", c.pos());
            }
            if (unqualified[0]) {
                throw SqlTranslationException.unsupported("an unqualified column beside a subquery in an outer join's"
                        + " ON is not supported yet: IQ reads the subquery only where its conjunct reads the join's"
                        + " null-supplying side alone, so the column's table decides", c.pos());
            }
            if (j.type() == SqlAst.JoinType.RIGHT) {
                // IQ reads it (measured); DuckDB cannot plan a subquery in a RIGHT JOIN's condition
                throw SqlTranslationException.unsupported("a subquery in a RIGHT JOIN's ON is not supported yet: lite's"
                        + " DuckDB lowering cannot plan it (write it as a LEFT JOIN)", c.pos());
            }
        }
        mergedDerivedSubqueries(j, conjuncts);
    }

    /**
     * IQ island, 2026-10-17 (the TPC-H corpus lane's LEFT JOIN TopSuppliers ts ON 1 = 1; measured on IQ 17.1): IQ
     * merges a plain derived table (no GROUP BY, aggregate, DISTINCT, row limit or window) on an outer join's
     * null-supplying side into an ON that reads no column of the preserved side ({@code 1 = 1}, {@code d.id > 0}); its
     * WHERE then is the ON's, and a subquery there -- a comparison's, an IN's, an EXISTS -- is IQ's error -1001047. An
     * ON reading the preserved side ({@code d.id = t.id}, {@code t.id > 0}) is answered, as is one of constants that
     * fails ({@code 1 = 0}); an ON of other constants is refused by name.
     */
    private static void mergedDerivedSubqueries(SqlAst.Join j, List<SqlAst.Expr> conjuncts) {
        if (j.type() == SqlAst.JoinType.FULL) {
            return;
        }
        boolean left = j.type() == SqlAst.JoinType.LEFT;
        if (!((left ? j.right() : j.left()) instanceof SqlAst.SubqueryFrom d) || !(d.query() instanceof SqlAst.Select ds)
                || ds.where() == null || !holdsSubquery(java.util.Objects.requireNonNull(ds.where())) || !merged(ds)) {
            return;
        }
        Set<String> kept = new java.util.HashSet<>();
        for (SqlAst.FromItem leaf : leaves(left ? j.left() : j.right())) {
            SqlAst.Ident name = correlation(leaf);
            if (name != null) {
                kept.add(IqLexer.lower(name.text()));
            }
        }
        boolean constants = true;
        for (SqlAst.Expr c : conjuncts) {
            if (holdsSubquery(c) || !readsColumn(c)) {
                continue;
            }
            constants = false;
            Set<String> read = new java.util.HashSet<>();
            boolean[] unqualified = {false};
            tablesRead(c, Set.of(), read, unqualified);
            if (unqualified[0] || read.stream().anyMatch(kept::contains)) {
                return;
            }
        }
        if (constants) {
            Boolean holds = constantsHold(conjuncts);
            if (holds == null) {
                throw SqlTranslationException.unsupported("an outer join's ON of constants beside a derived table whose"
                        + " WHERE holds a subquery is not supported yet: IQ merges the table into the ON where it"
                        + " holds (its -1001047) and answers where it fails", j.pos());
            }
            if (!holds) {
                return;
            }
        } else {
            // under an ON reading the null-supplying side, a scalar subquery is -1001047 and an EXISTS is answered
            // (measured); an IN, an ANY or ALL is not measured there
            List<SqlAst.Expr> subqueries = new ArrayList<>();
            subqueryExprs(java.util.Objects.requireNonNull(ds.where()), subqueries);
            if (subqueries.stream().allMatch(q -> q instanceof SqlAst.Exists)) {
                return;
            }
            if (!subqueries.stream().allMatch(q -> q instanceof SqlAst.ScalarSubquery)) {
                throw SqlTranslationException.unsupported("an IN, ANY or ALL subquery in the WHERE of a derived table on"
                        + " an outer join's null-supplying side, under an ON reading that side alone, is not supported"
                        + " yet: IQ merges the table into the ON (its -1001047 for a scalar subquery, an answer for an"
                        + " EXISTS)", java.util.Objects.requireNonNull(ds.where()).pos());
            }
        }
        throw SqlTranslationException.unsupported("a subquery in the WHERE of a plain derived table on an outer join's"
                + " null-supplying side, under an ON that reads no column of the preserved side, is IQ's error -1001047"
                + " under its engine (IQ merges the table's WHERE into the ON: subqueries are not supported in outer join"
                + " ON clauses), refused before translating", java.util.Objects.requireNonNull(ds.where()).pos());
    }

    /** Into {@code out}, the subquery expressions under {@code e}, outside its subqueries. */
    private static void subqueryExprs(SqlAst.Expr e, List<SqlAst.Expr> out) {
        if (subqueryOf(e) != null) {
            out.add(e);
            return;
        }
        for (SqlAst.Expr child : children(e)) {
            subqueryExprs(child, out);
        }
    }

    /** Whether IQ merges {@code s} into an outer join's ON ({@link #mergedDerivedSubqueries}): no GROUP BY, HAVING,
     *  aggregate, DISTINCT, row limit or window function. */
    private static boolean merged(SqlAst.Select s) {
        if (!s.groupBy().isEmpty() || s.having() != null || s.distinct() || s.limit() != null || s.offset() != null) {
            return false;
        }
        for (SqlAst.SelectItem i : s.items()) {
            if (i instanceof SqlAst.SelectExpr x && (!IqAggregates.aggregateCalls(x.expr()).isEmpty()
                    || hasWindowCall(x.expr()))) {
                return false;
            }
        }
        return true;
    }

    /** Whether conjuncts of integer constants compared ({@code 1 = 1}, {@code 2 > 1}) all hold; null where one is
     *  of another form. */
    private static @com.legend.base.Nullable Boolean constantsHold(List<SqlAst.Expr> conjuncts) {
        boolean all = true;
        for (SqlAst.Expr c : conjuncts) {
            if (!(c instanceof SqlAst.Binary b) || !(b.left() instanceof SqlAst.IntegerLit l)
                    || !(b.right() instanceof SqlAst.IntegerLit r)) {
                return null;
            }
            int cmp = l.value().compareTo(r.value());
            Boolean holds = switch (b.op()) {
                case EQ -> cmp == 0;
                case NE -> cmp != 0;
                case LT -> cmp < 0;
                case LE -> cmp <= 0;
                case GT -> cmp > 0;
                case GE -> cmp >= 0;
                default -> null;
            };
            if (holds == null) {
                return null;
            }
            all &= holds;
        }
        return all;
    }

    /**
     * IQ island, 2026-10-17 (found by verifying the corpus lane's IQ_ERROR claims: SQLStorm's LEFT JOIN ph ON ... AND
     * ph.CreationDate = (SELECT MAX(...)) WHERE ph.PostHistoryTypeId = 10; measured on IQ 17.1): whether a conjunct of
     * {@code conjuncts} (the query's WHERE's and its inner joins' ON's, {@link #rejecting}) or an expression its outer
     * query rejects the NULLs of ({@code held}, {@link #heldNonNull}) rejects the NULLs of {@code j}'s null-supplying
     * side, so that IQ's engine reads the outer join as the inner one it then is, a subquery in its ON taken as in any
     * join's. A conjunct that is a comparison with a column of that side, qualified, as one operand or in its arithmetic
     * ({@code u.id > 0}, {@code u.id + 1 > 0}, {@code u.id >= t.id}), {@code IS NOT NULL}, {@code [NOT] IN} a list,
     * {@code [NOT] BETWEEN} or {@code [NOT] LIKE} of such a column; no NULL constant beside it (under ansinull Off
     * {@code = NULL} is IS NULL). Not one: IS NULL, an OR, a COALESCE (IQ raises -1001047 under each: measured). A FULL
     * JOIN has no such side here.
     */
    private static boolean nullRejected(List<SqlAst.Expr> conjuncts, List<SqlAst.Expr> held, SqlAst.Join j) {
        if (j.type() == SqlAst.JoinType.FULL) {
            return false;
        }
        Set<String> side = new java.util.HashSet<>();
        for (SqlAst.FromItem leaf : leaves(j.type() == SqlAst.JoinType.LEFT ? j.right() : j.left())) {
            SqlAst.Ident name = correlation(leaf);
            if (name == null) {
                return false;
            }
            side.add(IqLexer.lower(name.text()));
        }
        Set<String> rejected = new java.util.HashSet<>();
        conjuncts.forEach(c -> rejectedColumns(c, side, null, rejected));
        held.forEach(e -> strictColumns(e, side, null, rejected));
        return !rejected.isEmpty();
    }

    /** The conjuncts that see {@code target}'s rows, a join's or a FROM item's, as {@code s} makes them: its WHERE's,
     *  and the ON's of the inner joins over {@code target} (measured on IQ 17.1: {@code LEFT JOIN u b ON ... JOIN u x
     *  ON x.id = b.id} rejects b's NULLs as a WHERE does); an inner join under it sees its rows before a join pads
     *  them. */
    private static List<SqlAst.Expr> rejecting(SqlAst.Select s, SqlAst.FromItem target) {
        List<SqlAst.Expr> out = new ArrayList<>();
        if (s.where() != null) {
            andConjuncts(java.util.Objects.requireNonNull(s.where()), out);
        }
        for (SqlAst.FromItem f : s.from()) {
            innerJoinsOver(f, target, out);
        }
        return out;
    }

    /** Whether {@code target} is {@code f} or under it; if so, the ON conjuncts of the inner joins on the way, into
     *  {@code out}. */
    private static boolean innerJoinsOver(SqlAst.FromItem f, SqlAst.FromItem target, List<SqlAst.Expr> out) {
        if (f == target) {
            return true;
        }
        if (f instanceof SqlAst.Join j && (innerJoinsOver(j.left(), target, out)
                || innerJoinsOver(j.right(), target, out))) {
            if (j.type() == SqlAst.JoinType.INNER && j.on() != null) {
                andConjuncts(java.util.Objects.requireNonNull(j.on()), out);
            }
            return true;
        }
        return false;
    }

    /**
     * IQ island, 2026-10-17 (found by verifying the corpus lane's IQ_ERROR claims: SQLStorm's CTE whose LEFT JOIN's ON
     * holds a subquery, joined on its column by an inner join; measured on IQ 17.1): IQ's engine carries the rejection
     * of a derived table's (or a common table expression's) column's NULLs, by its outer query's WHERE or inner join,
     * into the derived table, grouped or not, through nested ones: there it rejects the NULLs of what the column's item
     * computes, so a LEFT JOIN there is read as inner -- before a window function there numbers its rows (measured:
     * {@code ROW_NUMBER()} over the inner join's rows, where SQL's order numbers the outer join's). Per derived SELECT,
     * the items whose NULLs are so rejected; an unqualified column is the derived table's only where it is the outer
     * query's one FROM item; a derived table with a row limit is not entered (IQ's -1001030 there).
     */
    private static Map<SqlAst.Select, List<SqlAst.Expr>> heldNonNull(SqlAst.Statement statement) {
        Map<SqlAst.Select, List<SqlAst.Expr>> held = new IdentityHashMap<>();
        // the landing's corpus check (2026-10-17, SQLStorm's grouped CTE under a ROW_NUMBER, joined on its column;
        // measured on IQ 17.1): the rejection does not enter a grouped derived table a window above it reads -- its
        // NULL group stays, numbered (COUNT(*) OVER over a grouped table: SQL's count); a plain one it enters
        Set<SqlAst.Select> windowAbove = Collections.newSetFromMap(new IdentityHashMap<>());
        for (SqlAst.Statement q : queries(statement)) {
            if (!(q instanceof SqlAst.Select s)) {
                continue;
            }
            boolean above = windowAbove.contains(s) || expressions(s).stream().anyMatch(IqDialect::hasWindowCall);
            List<SqlAst.Expr> mine = held.getOrDefault(s, List.of());
            List<SqlAst.FromItem> all = new ArrayList<>();
            s.from().forEach(f -> all.addAll(leaves(f)));
            for (SqlAst.FromItem leaf : all) {
                if (!(leaf instanceof SqlAst.SubqueryFrom d) || d.alias() == null
                        || !(d.query() instanceof SqlAst.Select ds) || ds.limit() != null) {
                    continue;
                }
                if (above) {
                    windowAbove.add(ds);
                    if (!ds.groupBy().isEmpty() || expressions(ds).stream().anyMatch(IqDialect::hasAggregate)) {
                        continue;
                    }
                }
                String alias = IqLexer.lower(java.util.Objects.requireNonNull(d.alias()).text());
                String only = all.size() == 1 ? alias : null;
                Set<String> columns = new java.util.HashSet<>();
                rejecting(s, d).forEach(c -> rejectedColumns(c, Set.of(alias), only, columns));
                mine.forEach(e -> strictColumns(e, Set.of(alias), only, columns));
                List<SqlAst.Expr> items = new ArrayList<>();
                for (int i = 0; i < ds.items().size(); i++) {
                    String name = itemName(ds, i, d.columnAliases());
                    if (name != null && columns.contains(name) && ds.items().get(i) instanceof SqlAst.SelectExpr x) {
                        items.add(x.expr());
                    }
                }
                if (!items.isEmpty()) {
                    held.put(ds, items);
                }
            }
        }
        return held;
    }

    /** Into {@code out}, the columns (their names, lower case) of the tables {@code side} names whose NULLs conjunct
     *  {@code c} rejects ({@link #nullRejected}); an unqualified column is table {@code only}'s, where not null. */
    private static void rejectedColumns(SqlAst.Expr c, Set<String> side, @com.legend.base.Nullable String only,
            Set<String> out) {
        switch (c) {
            case SqlAst.Binary b when b.op() == SqlAst.BinaryOp.EQ || b.op() == SqlAst.BinaryOp.NE
                    || b.op() == SqlAst.BinaryOp.LT || b.op() == SqlAst.BinaryOp.LE
                    || b.op() == SqlAst.BinaryOp.GT || b.op() == SqlAst.BinaryOp.GE -> {
                if (!(b.left() instanceof SqlAst.NullLit) && !(b.right() instanceof SqlAst.NullLit)) {
                    strictColumns(b.left(), side, only, out);
                    strictColumns(b.right(), side, only, out);
                }
            }
            case SqlAst.IsNull n -> {
                if (n.negated()) {
                    sideColumn(n.operand(), side, only, out);
                }
            }
            case SqlAst.InList in -> {
                if (in.values().stream().noneMatch(v -> v instanceof SqlAst.NullLit)) {
                    sideColumn(in.operand(), side, only, out);
                }
            }
            case SqlAst.Between b -> {
                if (!(b.low() instanceof SqlAst.NullLit) && !(b.high() instanceof SqlAst.NullLit)) {
                    sideColumn(b.operand(), side, only, out);
                }
            }
            // the landing's review (2026-10-17): under ansinull Off NOT LIKE NULL holds for a NULL too (like)
            case SqlAst.Like l -> {
                if (!(l.pattern() instanceof SqlAst.NullLit) && !(l.escape() instanceof SqlAst.NullLit)) {
                    sideColumn(l.operand(), side, only, out);
                }
            }
            default -> { }
        }
    }

    /** Into {@code out}, {@code e}'s name where it is a column of one of the tables {@code side} names: qualified, or
     *  unqualified where {@code only} is one of them. */
    private static void sideColumn(SqlAst.Expr e, Set<String> side, @com.legend.base.Nullable String only,
            Set<String> out) {
        if (e instanceof SqlAst.ColumnRef r && (r.parts().size() == 2 && side.contains(IqLexer.lower(
                r.parts().get(0).text())) || r.parts().size() == 1 && only != null && side.contains(only))) {
            out.add(IqLexer.lower(r.parts().get(r.parts().size() - 1).text()));
        }
    }

    /** Into {@code out}, the columns of those tables that make {@code e} NULL where they are: the column, or arithmetic
     *  of it and no NULL constant. */
    private static void strictColumns(SqlAst.Expr e, Set<String> side, @com.legend.base.Nullable String only,
            Set<String> out) {
        sideColumn(e, side, only, out);
        // a + of strings too, though it concatenates a stored NULL as '' (D-S3): measured on IQ 17.1 (the landing's
        // review, 2026-10-17), b.label + 'x' = 'x' drops the rows the join supplies NULLs for; b.label || 'x' keeps them
        if (e instanceof SqlAst.Binary b && (b.op() == SqlAst.BinaryOp.PLUS || b.op() == SqlAst.BinaryOp.MINUS
                || b.op() == SqlAst.BinaryOp.TIMES || b.op() == SqlAst.BinaryOp.DIVIDE)
                && !(b.left() instanceof SqlAst.NullLit) && !(b.right() instanceof SqlAst.NullLit)) {
            strictColumns(b.left(), side, only, out);
            strictColumns(b.right(), side, only, out);
        }
    }

    /** IQ island, 2026-10-17: an outer join IQ's engine reads as an inner one ({@link #nullRejected}) as INNER. */
    @Override
    public SqlAst.JoinType joinType(SqlAst.Join join) {
        return innerJoins.contains(join) ? SqlAst.JoinType.INNER : join.type();
    }

    /** Whether {@code e} holds a subquery anywhere in it. */
    private static boolean holdsSubquery(SqlAst.Expr e) {
        if (subqueryOf(e) != null) {
            return true;
        }
        for (SqlAst.Expr child : children(e)) {
            if (holdsSubquery(child)) {
                return true;
            }
        }
        return false;
    }

    /** {@code e}'s conjuncts under AND, parentheses aside. */
    private static void andConjuncts(SqlAst.Expr e, List<SqlAst.Expr> out) {
        if (e instanceof SqlAst.Binary b && b.op() == SqlAst.BinaryOp.AND) {
            andConjuncts(b.left(), out);
            andConjuncts(b.right(), out);
        } else {
            out.add(e);
        }
    }

    /** The tables (by their lowercase names) {@code e} reads by qualified columns, inside its subqueries those of no
     *  FROM item of theirs (correlated); an unqualified column outside a subquery noted. A subquery's unqualified
     *  column is taken as its own. */
    private static void tablesRead(SqlAst.Expr e, Set<String> bound, Set<String> read, boolean[] unqualified) {
        if (e instanceof SqlAst.ColumnRef r) {
            if (r.parts().size() == 1) {
                unqualified[0] |= bound.isEmpty();
            } else {
                String q = IqLexer.lower(r.parts().get(r.parts().size() - 2).text());
                if (!bound.contains(q)) {
                    read.add(q);
                }
            }
            return;
        }
        SqlAst.Statement query = switch (e) {
            case SqlAst.ScalarSubquery q -> q.query();
            case SqlAst.Exists q -> q.query();
            case SqlAst.InSubquery q -> q.query();
            case SqlAst.Quantified q -> q.query();
            default -> null;
        };
        if (query != null) {
            Set<String> inner = new java.util.HashSet<>(bound);
            List<SqlAst.Statement> nested = queries(query);
            for (SqlAst.Statement q : nested) {
                if (q instanceof SqlAst.Select s) {
                    for (SqlAst.FromItem f : s.from()) {
                        for (SqlAst.FromItem leaf : leaves(f)) {
                            SqlAst.Ident name = correlation(leaf);
                            if (name != null) {
                                inner.add(IqLexer.lower(name.text()));
                            }
                        }
                    }
                }
            }
            if (inner.isEmpty()) {
                inner.add("");
            }
            for (SqlAst.Statement q : nested) {
                if (q instanceof SqlAst.Select s) {
                    for (SqlAst.Expr x : expressions(s)) {
                        tablesRead(x, inner, read, unqualified);
                    }
                }
            }
        }
        for (SqlAst.Expr child : children(e)) {
            tablesRead(child, bound, read, unqualified);
        }
    }

    /** The first subquery in {@code e}, refused with {@code refusal}. */
    private static void refuseSubquery(SqlAst.Expr e, String refusal) {
        if (subqueryOf(e) != null) {
            throw SqlTranslationException.unsupported(refusal, e.pos());
        }
        for (SqlAst.Expr child : children(e)) {
            refuseSubquery(child, refusal);
        }
    }

    /** IQ island, 2026-10-17 (the corpus lane's SEDE, measured on IQ 17.1 over 8 queries): an IN subquery that is a
     *  UNION or a UNION ALL is read under IQ's engine, where the research's flag list held that it restricts it;
     *  INTERSECT and EXCEPT are its -1001030 there, as at the top. Another subquery's set operation stays refused. */
    private static void unionOnly(SqlAst.Statement query, SqlAst.Pos pos) {
        if (query instanceof SqlAst.SetOperation s) {
            if (s.op() != SqlAst.SetOp.UNION) {
                throw SqlTranslationException.unsupported(s.op().name() + " is IQ's error -1001030 under its engine (a"
                        + " query over IQ tables), refused before translating", pos);
            }
            unionOnly(s.left(), pos);
            unionOnly(s.right(), pos);
        }
    }

    private static void noSetOperation(SqlAst.Statement query, SqlAst.Pos pos) {
        if (query instanceof SqlAst.SetOperation) {
            throw SqlTranslationException.unsupported("a set operation inside a subquery is not supported under IQ's"
                    + " engine, which restricts it (the research's flag list)", pos);
        }
    }

    // ------------------------------------------------------------------ the statement's expressions

    /** A select's expressions: its items', WHERE's, GROUP BY's, HAVING's, ORDER BY's, its joins' ON conditions (not
     *  its derived tables' or subqueries'). */
    static List<SqlAst.Expr> expressions(SqlAst.Select s) {
        List<SqlAst.Expr> all = new ArrayList<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x) {
                all.add(x.expr());
            }
        }
        if (s.where() != null) {
            all.add(java.util.Objects.requireNonNull(s.where()));
        }
        // IQ island, 2026-10-14 (M1c)
        all.addAll(s.groupBy());
        if (s.having() != null) {
            all.add(java.util.Objects.requireNonNull(s.having()));
        }
        s.orderBy().forEach(o -> all.add(o.expr()));
        for (SqlAst.FromItem f : s.from()) {
            List<SqlAst.Join> joins = new ArrayList<>();
            joins(f, joins);
            for (SqlAst.Join j : joins) {
                if (j.on() != null) {
                    all.add(java.util.Objects.requireNonNull(j.on()));
                }
            }
        }
        return all;
    }

    /** IQ island, 2026-10-16: {@code s}'s conditions -- its WHERE, its HAVING and its joins' ONs. */
    static List<SqlAst.Expr> conditions(SqlAst.Select s) {
        List<SqlAst.Expr> all = new ArrayList<>();
        if (s.where() != null) {
            all.add(java.util.Objects.requireNonNull(s.where()));
        }
        if (s.having() != null) {
            all.add(java.util.Objects.requireNonNull(s.having()));
        }
        for (SqlAst.FromItem f : s.from()) {
            List<SqlAst.Join> joins = new ArrayList<>();
            joins(f, joins);
            for (SqlAst.Join j : joins) {
                if (j.on() != null) {
                    all.add(java.util.Objects.requireNonNull(j.on()));
                }
            }
        }
        return all;
    }

    // ------------------------------------------------------------------ ROLLUP and CUBE

    /** {@code s}'s GROUP BY ROLLUP(...) or CUBE(...) (a GROUP BY ( ) is ROLLUP()), as {@link IqParser} reads it; null for
     *  none. */
    private static SqlAst.@com.legend.base.Nullable FunctionCall rollup(SqlAst.Select s) {
        return s.groupBy().size() == 1 && s.groupBy().get(0) instanceof SqlAst.FunctionCall f && f.name().size() == 1
                && !f.star() && (IqLexer.lower(f.name().get(0).text()).equals("rollup") || IqLexer.lower(f.name().get(0)
                .text()).equals("cube")) ? f : null;
    }

    /** Whether {@code e} is a call of GROUPING. */
    private static boolean isGrouping(SqlAst.Expr e) {
        return e instanceof SqlAst.FunctionCall f && f.name().size() == 1 && IqLexer.lower(f.name().get(0).text())
                .equals("grouping");
    }

    /**
     * IQ island, 2026-10-15 (checklist 2.16b, measured on IQ 17.1): {@code GROUP BY ROLLUP(k1, ..., kn)} -- or {@code k1,
     * ..., kn WITH ROLLUP} -- groups by k1..kn, by k1..kn-1, ..., and by nothing, the grand total (one row even over no
     * rows); {@code CUBE} by every subset of the keys; {@code GROUP BY ( )} by nothing. The statement is read as the
     * UNION ALL of one SELECT a grouping (a UNION under DISTINCT), each with the keys it does not group by NULL outside
     * its aggregates and GROUPING(k) 0 or 1, its ORDER BY and row limit the whole's (the ORDER BY by position: IQ's
     * engine orders a set operation by none of its names). A grouping by nothing with no aggregate keeps its one row by
     * {@code HAVING COUNT(*) >= 0}. Refused by name, as IQ raises: GROUPING of no key (-149); as not read: a ROLLUP or a
     * CUBE in a subquery, a derived table or a set operation's branch, a CUBE of more than 6 keys, SELECT *, a window
     * function beside it, an ORDER BY key that is none of the items, and a key rolled up inside an expression the
     * rewrite does not walk.
     */
    private static SqlAst.Statement rolledUp(SqlAst.Statement statement, Engine engine,
            Map<SqlAst.SelectExpr, SqlAst.SelectExpr> remade) {
        for (SqlAst.Statement q : queries(statement)) {
            if (q != statement && q instanceof SqlAst.Select s && rollup(s) != null) {
                throw SqlTranslationException.unsupported("ROLLUP or CUBE in a subquery, a derived table or a set"
                        + " operation's branch is not supported yet", s.groupBy().get(0).pos());
            }
        }
        if (!(statement instanceof SqlAst.Select s) || rollup(s) == null) {
            return statement;
        }
        SqlAst.FunctionCall g = java.util.Objects.requireNonNull(rollup(s));
        boolean cube = IqLexer.lower(g.name().get(0).text()).equals("cube");
        // IQ island, 2026-10-16 (measured on IQ 17.1): a key that is a select-list alias is its item's expression (an
        // alias before a column of its name: ROLLUP(sm) over id AS sm groups by id)
        List<SqlAst.Expr> written = g.args();
        List<SqlAst.Expr> keys = new ArrayList<>();
        for (SqlAst.Expr k : written) {
            keys.add(aliasedItem(s, k));
        }
        int n = keys.size();
        if (engine == Engine.IQ) {
            for (int i = 0; i < n; i++) {
                for (int j = i + 1; j < n; j++) {
                    if (IqAggregates.same(keys.get(i), keys.get(j))) {
                        // IQ island, 2026-10-16 (the landing's review, measured on IQ 17.1; SQL Anywhere answers)
                        throw SqlTranslationException.unsupported("a key twice in ROLLUP or CUBE (itself or its alias)"
                                + " is IQ's error -1001030 under its engine (GROUP BY ROLLUP/CUBE contains duplicate"
                                + " columns), refused before translating", written.get(j).pos());
                    }
                }
            }
        }
        aggregatesBesideRollup(s, cube, engine);
        if (cube && n > 6) {
            throw SqlTranslationException.unsupported("CUBE of more than 6 keys is not supported yet", g.pos());
        }
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.AllColumns a) {
                throw SqlTranslationException.unsupported("a select list of * with ROLLUP or CUBE is not supported yet", a.pos());
            }
            if (item instanceof SqlAst.SelectExpr x && hasWindowCall(x.expr())) {
                throw SqlTranslationException.unsupported("a window function beside ROLLUP or CUBE is not supported"
                        + " yet", x.expr().pos());
            }
        }
        // IQ island, 2026-10-16 (the landing's review): a key matched by its spelling, a column of a key's name written
        // otherwise (t.k over ROLLUP(k)) and a subquery (its correlated reads of a key) are refused by name, not as
        // IQ's -149, which IQ may not raise
        List<SqlAst.Expr> read = new ArrayList<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x) {
                read.add(x.expr());
            }
        }
        if (s.having() != null) {
            read.add(java.util.Objects.requireNonNull(s.having()));
        }
        for (SqlAst.Expr e : read) {
            SqlAst.ColumnRef apart = spelledApart(e, keys);
            if (apart != null) {
                throw SqlTranslationException.unsupported("a key of ROLLUP or CUBE written otherwise than in it is not"
                        + " supported yet: write it as the key is written", apart.pos());
            }
            if (readsQuery(e)) {
                throw SqlTranslationException.unsupported("a subquery beside ROLLUP or CUBE is not supported yet",
                        e.pos());
            }
        }
        // the whole's ORDER BY, by position
        List<SqlAst.SortItem> order = new ArrayList<>();
        for (SqlAst.SortItem o : s.orderBy()) {
            if (pastAnInt(o.expr())) {
                // a constant, which orders nothing (measured; the landing's review: a crash)
                continue;
            }
            order.add(new SqlAst.SortItem(new SqlAst.IntegerLit(BigInteger.valueOf(itemPosition(s, o.expr())),
                    o.expr().pos()), o.descending(), o.nullsFirst(), o.pos()));
        }
        // the groupings: ROLLUP's prefixes, longest first; CUBE's subsets
        List<List<Integer>> groupings = new ArrayList<>();
        if (cube) {
            for (int mask = (1 << n) - 1; mask >= 0; mask--) {
                List<Integer> set = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    if ((mask & (1 << i)) != 0) {
                        set.add(i);
                    }
                }
                groupings.add(set);
            }
        } else {
            for (int size = n; size >= 0; size--) {
                List<Integer> set = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    set.add(i);
                }
                groupings.add(set);
            }
        }
        boolean aggregated = false;
        for (SqlAst.Expr e : expressions(s)) {
            aggregated |= hasAggregate(e);
        }
        SqlAst.Statement union = null;
        for (List<Integer> set : groupings) {
            SqlAst.Select branch = grouping(s, keys, written, set, aggregated, remade);
            union = union == null ? branch : new SqlAst.SetOperation(SqlAst.SetOp.UNION, !s.distinct(), union, branch,
                    List.of(), null, null, s.pos());
        }
        if (union instanceof SqlAst.SetOperation o) {
            return new SqlAst.SetOperation(o.op(), o.all(), o.left(), o.right(), order, s.limit(), s.offset(), s.pos());
        }
        SqlAst.Select only = (SqlAst.Select) java.util.Objects.requireNonNull(union);
        return new SqlAst.Select(s.distinct(), only.items(), only.from(), only.where(), only.groupBy(), only.having(),
                only.windows(), s.orderBy(), s.limit(), s.offset(), s.pos());
    }

    /** Whether {@code e} holds a window function's call, outside its subqueries. */
    private static boolean hasWindowCall(SqlAst.Expr e) {
        return e instanceof SqlAst.Window || children(e).stream().anyMatch(IqDialect::hasWindowCall);
    }

    /** Whether {@code e} holds an aggregate the island reads (GROUPING aside), outside its subqueries. */
    private static boolean hasAggregate(SqlAst.Expr e) {
        return e instanceof SqlAst.FunctionCall f && IqAggregates.named(f) || children(e).stream().anyMatch(
                IqDialect::hasAggregate);
    }

    /** The position (from 1) of the select item ORDER BY key {@code k} names: a position, an alias, or an item's
     *  expression written alike; refused otherwise. */
    private static int itemPosition(SqlAst.Select s, SqlAst.Expr k) {
        if (k instanceof SqlAst.IntegerLit p) {
            return p.value().intValueExact();
        }
        for (int i = 0; i < s.items().size(); i++) {
            if (s.items().get(i) instanceof SqlAst.SelectExpr x && (x.alias() != null && k instanceof SqlAst.ColumnRef r
                    && r.parts().size() == 1 && IqLexer.lower(r.parts().get(0).text()).equals(IqLexer.lower(
                            java.util.Objects.requireNonNull(x.alias()).text())) || IqAggregates.same(x.expr(), k))) {
                return i + 1;
            }
        }
        throw SqlTranslationException.unsupported("an ORDER BY key that is none of the select items is not supported"
                + " with ROLLUP or CUBE yet", k.pos());
    }

    /** The select item {@code k} names when it is an alias of {@code s}'s select list (its expression), else {@code k}. */
    private static SqlAst.Expr aliasedItem(SqlAst.Select s, SqlAst.Expr k) {
        if (k instanceof SqlAst.ColumnRef r && r.parts().size() == 1) {
            for (SqlAst.SelectItem item : s.items()) {
                if (item instanceof SqlAst.SelectExpr x && x.alias() != null && IqLexer.lower(java.util.Objects
                        .requireNonNull(x.alias()).text()).equals(IqLexer.lower(r.parts().get(0).text()))) {
                    return x.expr();
                }
            }
        }
        return k;
    }

    /**
     * IQ island, 2026-10-16 (measured on IQ 17.1): the aggregates IQ's engine refuses beside ROLLUP or CUBE, its error
     * -1001030 -- PERCENTILE_CONT and PERCENTILE_DISC beside CUBE (SQL Anywhere's syntax error too; beside ROLLUP IQ
     * answers), MEDIAN and LIST beside either. Outside IQ's engine MEDIAN and LIST are refused by name (not measured).
     */
    private static void aggregatesBesideRollup(SqlAst.Select s, boolean cube, Engine engine) {
        for (SqlAst.Expr e : expressions(s)) {
            java.util.ArrayDeque<SqlAst.Expr> todo = new java.util.ArrayDeque<>();
            todo.push(e);
            while (!todo.isEmpty()) {
                SqlAst.Expr x = todo.pop();
                if (x instanceof SqlAst.FunctionCall f && f.name().size() == 1) {
                    String fn = IqLexer.lower(f.name().get(0).text());
                    if (cube && !f.withinGroup().isEmpty()) {
                        throw SqlTranslationException.invalid(f.name().get(0).text() + " beside GROUP BY CUBE is not"
                                + " supported" + code(-1001030, "QFA2A"), f.pos());
                    }
                    if (fn.equals("median") || fn.equals("list")) {
                        String what = f.name().get(0).text() + " beside ROLLUP or CUBE";
                        if (engine == Engine.IQ) {
                            throw SqlTranslationException.invalid(what + " is not supported" + code(-1001030, "QFA2A"),
                                    f.pos());
                        }
                        throw SqlTranslationException.unsupported(what + " is not supported yet", f.pos());
                    }
                }
                children(x).forEach(todo::push);
            }
        }
    }

    /** One grouping's SELECT ({@link #rolledUp}): {@code s} grouped by the keys of {@code set}, the others NULL
     *  ({@code written}: the keys as written, for GROUPING of an alias). */
    private static SqlAst.Select grouping(SqlAst.Select s, List<SqlAst.Expr> keys, List<SqlAst.Expr> written,
            List<Integer> set, boolean aggregated, Map<SqlAst.SelectExpr, SqlAst.SelectExpr> remade) {
        List<SqlAst.SelectItem> items = new ArrayList<>();
        for (SqlAst.SelectItem item : s.items()) {
            SqlAst.SelectExpr x = (SqlAst.SelectExpr) item;
            // a whole item that is a key not grouped by is NULL (the union types it by the other groupings)
            SqlAst.Expr e = rolledKey(x.expr(), keys, set) ? new SqlAst.NullLit(x.expr().pos())
                    : rolled(x.expr(), keys, written, set);
            SqlAst.SelectExpr now = new SqlAst.SelectExpr(e, x.alias(), x.pos(), x.text());
            remade.put(now, x);
            items.add(now);
        }
        SqlAst.Expr having = s.having() == null ? null : rolled(java.util.Objects.requireNonNull(s.having()), keys,
                written, set);
        if (set.isEmpty() && !aggregated) {
            // a grouping by nothing is one row
            SqlAst.Expr one = new SqlAst.Binary(SqlAst.BinaryOp.GE, new SqlAst.FunctionCall(List.of(new SqlAst.Ident(
                    "COUNT", false, s.pos())), List.of(), false, true, List.of(), List.of(), null, s.pos()),
                    new SqlAst.IntegerLit(BigInteger.ZERO, s.pos()), s.pos());
            having = having == null ? one : new SqlAst.Binary(SqlAst.BinaryOp.AND, having, one, s.pos());
        }
        List<SqlAst.Expr> groupBy = new ArrayList<>();
        set.forEach(i -> groupBy.add(keys.get(i)));
        return new SqlAst.Select(false, items, s.from(), s.where(), groupBy, having, s.windows(), List.of(), null,
                null, s.pos());
    }

    /** Whether {@code e} is a key {@code set} does not group by: one written as a key, no copy of which (ROLLUP(k, k),
     *  a key and its alias) the set holds (2026-10-16, the landing's review: a copy outside the set rolled a grouped
     *  key to NULL). */
    private static boolean rolledKey(SqlAst.Expr e, List<SqlAst.Expr> keys, List<Integer> set) {
        boolean key = false;
        for (int i = 0; i < keys.size(); i++) {
            if (IqAggregates.same(e, keys.get(i))) {
                if (set.contains(i)) {
                    return false;
                }
                key = true;
            }
        }
        return key;
    }

    /** {@code e} in a grouping by the keys of {@code set} ({@link #rolledUp}): a key not grouped a NULL of its type
     *  (IQ island, 2026-10-16: {@code MIN(NULLIF(k, k))}, an aggregate a grouping may read), GROUPING(k) 0 where k is
     *  grouped and 1 where not (k as written or as its alias's item); an aggregate as it is. */
    private static SqlAst.Expr rolled(SqlAst.Expr e, List<SqlAst.Expr> keys, List<SqlAst.Expr> written,
            List<Integer> set) {
        if (isGrouping(e)) {
            SqlAst.FunctionCall f = (SqlAst.FunctionCall) e;
            boolean key = false;
            boolean grouped = false;
            for (int i = 0; i < keys.size() && f.args().size() == 1; i++) {
                if (IqAggregates.same(f.args().get(0), keys.get(i)) || IqAggregates.same(f.args().get(0),
                        written.get(i))) {
                    key = true;
                    grouped |= set.contains(i);
                }
            }
            if (!key) {
                throw SqlTranslationException.invalid("GROUPING reads a value that is no key of the ROLLUP or the CUBE"
                        + code(-149, "53003"), f.pos());
            }
            return new SqlAst.IntegerLit(grouped ? BigInteger.ZERO : BigInteger.ONE, f.pos());
        }
        if (rolledKey(e, keys, set)) {
            SqlAst.Pos at = e.pos();
            SqlAst.Expr none = new SqlAst.FunctionCall(List.of(new SqlAst.Ident("NULLIF", false, at)), List.of(e, e),
                    false, false, List.of(), List.of(), null, at);
            return new SqlAst.FunctionCall(List.of(new SqlAst.Ident("MIN", false, at)), List.of(none), false, false,
                    List.of(), List.of(), null, at);
        }
        if (e instanceof SqlAst.FunctionCall f && IqAggregates.named(f) || !rolls(e, keys, set)) {
            return e;
        }
        return switch (e) {
            case SqlAst.Binary b -> new SqlAst.Binary(b.op(), rolled(b.left(), keys, written, set), rolled(b.right(), keys, written, set),
                    b.pos());
            case SqlAst.Unary u -> new SqlAst.Unary(u.op(), rolled(u.operand(), keys, written, set), u.pos());
            case SqlAst.Cast c -> new SqlAst.Cast(rolled(c.operand(), keys, written, set), c.type(), c.pos());
            case SqlAst.IsNull i -> new SqlAst.IsNull(rolled(i.operand(), keys, written, set), i.negated(), i.pos());
            case SqlAst.FunctionCall f when f.orderBy().isEmpty() && f.filter() == null && f.withinGroup().isEmpty() ->
                    new SqlAst.FunctionCall(f.name(), f.args().stream().map(a -> rolled(a, keys, written, set)).toList(),
                            f.distinct(), f.star(), f.orderBy(), f.withinGroup(), f.filter(), f.pos());
            case SqlAst.Case c -> new SqlAst.Case(c.operand() == null ? null : rolled(java.util.Objects.requireNonNull(
                    c.operand()), keys, written, set), c.whens().stream().map(w -> new SqlAst.When(rolled(w.condition(), keys,
                    written, set), rolled(w.result(), keys, written, set))).toList(), c.otherwise() == null ? null : rolled(
                    java.util.Objects.requireNonNull(c.otherwise()), keys, written, set), c.pos());
            default -> throw SqlTranslationException.unsupported("a key of ROLLUP or CUBE inside "
                    + e.getClass().getSimpleName() + " is not supported yet", e.pos());
        };
    }

    /** A column of {@code e}, outside its aggregates, of a column key's name and written otherwise than every such key;
     *  null for none. */
    private static SqlAst.@com.legend.base.Nullable ColumnRef spelledApart(SqlAst.Expr e, List<SqlAst.Expr> keys) {
        if (e instanceof SqlAst.FunctionCall f && IqAggregates.named(f)) {
            return null;
        }
        if (e instanceof SqlAst.ColumnRef r) {
            String name = IqLexer.lower(r.parts().get(r.parts().size() - 1).text());
            boolean named = false;
            boolean same = false;
            for (SqlAst.Expr k : keys) {
                if (k instanceof SqlAst.ColumnRef kr && IqLexer.lower(kr.parts().get(kr.parts().size() - 1).text())
                        .equals(name)) {
                    named = true;
                    same |= IqAggregates.same(r, k);
                }
            }
            return named && !same ? r : null;
        }
        for (SqlAst.Expr child : children(e)) {
            SqlAst.ColumnRef found = spelledApart(child, keys);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /** Whether {@code e} reads a key {@code set} does not group by, or GROUPING, outside its aggregates. */
    private static boolean rolls(SqlAst.Expr e, List<SqlAst.Expr> keys, List<Integer> set) {
        if (isGrouping(e) || rolledKey(e, keys, set)) {
            return true;
        }
        if (e instanceof SqlAst.FunctionCall f && IqAggregates.named(f)) {
            return false;
        }
        return children(e).stream().anyMatch(c -> rolls(c, keys, set));
    }

    // ------------------------------------------------------------------ NUMBER(*)

    /** Whether {@code e} is a call of {@code NUMBER(*)}. */
    private static boolean isNumber(SqlAst.Expr e) {
        return e instanceof SqlAst.FunctionCall f && f.star() && f.args().isEmpty() && f.name().size() == 1
                && IqLexer.lower(f.name().get(0).text()).equals("number");
    }

    /** Whether {@code e} holds a call of {@code NUMBER(*)}, outside its subqueries. */
    private static boolean hasNumber(SqlAst.Expr e) {
        return isNumber(e) || children(e).stream().anyMatch(IqDialect::hasNumber);
    }

    /** IQ's error for NUMBER(*) where IQ's engine does not number. */
    private static SqlTranslationException misplacedNumber(SqlAst.Pos pos, String where) {
        return SqlTranslationException.unsupported("NUMBER(*) " + where + " is IQ's error -864 (an illegal usage of the"
                + " NUMBER(*) function), refused before translating", pos);
    }

    /**
     * IQ island, 2026-10-15 (checklist 1.10, measured on IQ 17.1): {@code NUMBER(*)} numbers the rows of the result in
     * its order, from 1, before TOP and START AT keep theirs (TOP 3 START AT 2 shows 2, 3, 4): the statement read with
     * each call in its select list as {@code ROW_NUMBER() OVER (ORDER BY ...)} of the statement's ORDER BY keys, a
     * position or an alias as its item's expression (an UNSIGNED BIGINT, as IQ types it). Refused by name, as IQ raises
     * -864: one in WHERE, GROUP BY, HAVING or ORDER BY (an ORDER BY of its own item too), in a query with DISTINCT, in a
     * subquery, a derived table or a set operation's branch; as IQ raises -1001030, one inside a CASE or an IF; as not
     * read, one in an item without an alias (its name), and an ORDER BY position past a {@code *}.
     */
    private static SqlAst.Statement numbered(SqlAst.Statement statement, Set<SqlAst.Window> numbering,
            Map<SqlAst.SelectExpr, SqlAst.SelectExpr> remade) {
        for (SqlAst.Statement q : queries(statement)) {
            if (!(q instanceof SqlAst.Select s)) {
                continue;
            }
            boolean shown = s == statement;
            for (SqlAst.SelectItem item : s.items()) {
                if (item instanceof SqlAst.SelectExpr x && hasNumber(x.expr()) && !shown) {
                    throw misplacedNumber(x.expr().pos(), "in a subquery, a derived table or a set operation's branch");
                }
            }
            for (SqlAst.Expr e : expressions(s)) {
                boolean item = s.items().stream().anyMatch(i -> i instanceof SqlAst.SelectExpr x && x.expr() == e);
                if (!item && hasNumber(e)) {
                    throw misplacedNumber(e.pos(), "in WHERE, GROUP BY, HAVING, ORDER BY or a join's ON");
                }
            }
        }
        if (!(statement instanceof SqlAst.Select s) || s.items().stream().noneMatch(i -> i instanceof SqlAst.SelectExpr x
                && hasNumber(x.expr()))) {
            return statement;
        }
        if (s.distinct()) {
            throw misplacedNumber(s.pos(), "in a query with DISTINCT");
        }
        // the window's order: the statement's ORDER BY, each position and alias read as its item's expression; a
        // constant key orders nothing (IQ island, 2026-10-16, the landing's review: IQ numbers the rows of a constant
        // ORDER BY, or of none, in the order it shows them)
        List<SqlAst.SortItem> order = new ArrayList<>();
        for (SqlAst.SortItem key : s.orderBy()) {
            SqlAst.Expr k = key.expr();
            SqlAst.SelectExpr target = null;
            if (k instanceof SqlAst.IntegerLit position && !pastAnInt(position)) {
                if (s.items().stream().anyMatch(i -> i instanceof SqlAst.AllColumns)) {
                    throw SqlTranslationException.unsupported("NUMBER(*) with an ORDER BY position beside a * is not"
                            + " supported", k.pos());
                }
                if (position.value().signum() <= 0
                        || position.value().compareTo(BigInteger.valueOf(s.items().size())) > 0) {
                    // IQ's -152 (measured: 2147483647 past the items), as orderPositions raises later
                    throw orderRefused(k);
                }
                int at = position.value().intValueExact() - 1;
                target = s.items().get(at) instanceof SqlAst.SelectExpr x ? x : null;
            } else if (k instanceof SqlAst.ColumnRef ref && ref.parts().size() == 1) {
                for (SqlAst.SelectItem i : s.items()) {
                    if (i instanceof SqlAst.SelectExpr x && x.alias() != null && IqLexer.lower(java.util.Objects
                            .requireNonNull(x.alias()).text()).equals(IqLexer.lower(ref.parts().get(0).text()))) {
                        target = x;
                    }
                }
            }
            SqlAst.Expr by = target != null ? target.expr() : k;
            if (hasNumber(by)) {
                throw misplacedNumber(k.pos(), "in ORDER BY");
            }
            if (!constantKey(by)) {
                order.add(new SqlAst.SortItem(by, key.descending(), key.nullsFirst(), key.pos()));
            }
        }
        // one window for every NUMBER(*), the statement's ORDER BY's last key: the numbers and the order the rows are
        // shown in are one computation (IQ island, 2026-10-16, the landing's review: a ROW_NUMBER over the keys and
        // the statement's own sort, apart, numbered tied rows otherwise than they were shown)
        SqlAst.FunctionCall rowNumber = new SqlAst.FunctionCall(List.of(new SqlAst.Ident("row_number", false,
                s.pos())), List.of(), false, false, List.of(), List.of(), null, s.pos());
        SqlAst.Window number = new SqlAst.Window(rowNumber, new SqlAst.WindowSpec(null, List.of(), order, null,
                s.pos()), s.pos());
        numbering.add(number);
        List<SqlAst.SelectItem> items = new ArrayList<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x && hasNumber(x.expr())) {
                if (x.alias() == null) {
                    throw SqlTranslationException.unsupported("NUMBER(*) in an item without an alias is not supported:"
                            + " name it with AS", x.expr().pos());
                }
                SqlAst.SelectExpr now = new SqlAst.SelectExpr(numbers(x.expr(), number), x.alias(), x.pos(), x.text());
                remade.put(now, x);
                items.add(now);
            } else {
                items.add(item);
            }
        }
        List<SqlAst.SortItem> orderBy = new ArrayList<>(s.orderBy());
        orderBy.add(new SqlAst.SortItem(number, false, null, s.pos()));
        return new SqlAst.Select(s.distinct(), items, s.from(), s.where(), s.groupBy(), s.having(), s.windows(),
                orderBy, s.limit(), s.offset(), s.pos());
    }

    /** Whether a sort key orders nothing: no column, aggregate or subquery read in it. */
    private static boolean constantKey(SqlAst.Expr e) {
        List<SqlAst.ColumnRef> refs = new ArrayList<>();
        columnRefs(e, refs);
        return refs.isEmpty() && IqAggregates.aggregateCalls(e).isEmpty() && !readsQuery(e);
    }

    private static boolean readsQuery(SqlAst.Expr e) {
        return e instanceof SqlAst.ScalarSubquery || e instanceof SqlAst.Exists || e instanceof SqlAst.InSubquery
                || e instanceof SqlAst.Quantified || children(e).stream().anyMatch(IqDialect::readsQuery);
    }

    /** {@code e} with each NUMBER(*) as {@code number}, the statement's one {@code ROW_NUMBER() OVER (ORDER BY
     *  ...)}: inside arithmetic, a function's arguments and a CAST; inside a CASE or an IF IQ's -1001030, anywhere else
     *  not read (both refused). */
    private static SqlAst.Expr numbers(SqlAst.Expr e, SqlAst.Window number) {
        if (isNumber(e)) {
            return number;
        }
        if (!hasNumber(e)) {
            return e;
        }
        return switch (e) {
            case SqlAst.Binary b -> new SqlAst.Binary(b.op(), numbers(b.left(), number), numbers(b.right(), number),
                    b.pos());
            case SqlAst.Unary u -> new SqlAst.Unary(u.op(), numbers(u.operand(), number), u.pos());
            case SqlAst.Cast c -> new SqlAst.Cast(numbers(c.operand(), number), c.type(), c.pos());
            case SqlAst.FunctionCall f when f.orderBy().isEmpty() && f.filter() == null -> new SqlAst.FunctionCall(
                    f.name(), f.args().stream().map(a -> numbers(a, number)).toList(), f.distinct(), f.star(),
                    f.orderBy(), f.withinGroup(), f.filter(), f.pos());
            case SqlAst.Case c -> throw SqlTranslationException.unsupported("NUMBER(*) inside a CASE or an IF is IQ's"
                    + " error -1001030 (a CASE of a non-deterministic function), refused before translating", c.pos());
            default -> throw SqlTranslationException.unsupported("NUMBER(*) inside " + e.getClass().getSimpleName()
                    + " is not supported", e.pos());
        };
    }

    /** The first T-SQL outer join predicate ({@code *=}, {@code =*}) in {@code e}, outside its subqueries; null for none. */
    private static SqlAst.@com.legend.base.Nullable Operator tsqlOuterJoin(SqlAst.Expr e) {
        if (e instanceof SqlAst.Operator o) {
            return o;
        }
        for (SqlAst.Expr child : children(e)) {
            SqlAst.Operator o = tsqlOuterJoin(child);
            if (o != null) {
                return o;
            }
        }
        return null;
    }

    /** The expressions directly inside {@code e}, of the forms {@link IqParser} reads. */
    static List<SqlAst.Expr> children(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.Binary b -> List.of(b.left(), b.right());
            case SqlAst.Unary u -> List.of(u.operand());
            case SqlAst.IsNull n -> List.of(n.operand());
            case SqlAst.IsTruth t -> List.of(t.operand());
            case SqlAst.IsDistinct d -> List.of(d.left(), d.right());
            case SqlAst.Between b -> List.of(b.operand(), b.low(), b.high());
            case SqlAst.InList in -> {
                List<SqlAst.Expr> all = new ArrayList<>(List.of(in.operand()));
                all.addAll(in.values());
                yield all;
            }
            case SqlAst.Like l -> l.escape() == null ? List.of(l.operand(), l.pattern())
                    : List.of(l.operand(), l.pattern(), java.util.Objects.requireNonNull(l.escape()));
            // IQ island, 2026-10-14 (M1c): an aggregate's ORDER BY too
            case SqlAst.FunctionCall f -> {
                List<SqlAst.Expr> all = new ArrayList<>(f.args());
                f.orderBy().forEach(o -> all.add(o.expr()));
                // IQ island, 2026-10-15 (checklist 2.16): a percentile's WITHIN GROUP value
                f.withinGroup().forEach(o -> all.add(o.expr()));
                yield all;
            }
            case SqlAst.Case c -> {
                List<SqlAst.Expr> all = new ArrayList<>();
                if (c.operand() != null) {
                    all.add(java.util.Objects.requireNonNull(c.operand()));
                }
                for (SqlAst.When w : c.whens()) {
                    all.add(w.condition());
                    all.add(w.result());
                }
                if (c.otherwise() != null) {
                    all.add(java.util.Objects.requireNonNull(c.otherwise()));
                }
                yield all;
            }
            // a subquery's own expressions are its query's (queries), not its enclosing one's
            case SqlAst.ScalarSubquery q -> List.of();
            case SqlAst.Exists q -> List.of();
            case SqlAst.InSubquery q -> List.of(q.operand());
            case SqlAst.Quantified q -> List.of(q.left());
            // IQ island, 2026-10-14 (M1d): CAST and CONVERT
            case SqlAst.Cast c -> List.of(c.operand());
            case SqlAst.ColumnRef r -> List.of();
            case SqlAst.IntegerLit n -> List.of();
            case SqlAst.NumericLit n -> List.of();
            case SqlAst.StringLit n -> List.of();
            case SqlAst.NullLit n -> List.of();
            // IQ island, 2026-10-14 (M1e): a host variable
            case SqlAst.Param p -> List.of();
            // IQ island, 2026-10-15 (windows): a window call is no call of this query's (not an aggregate of its
            // grouping, no function of the catalog), but what it reads is (IqWindows.children)
            case SqlAst.Window w -> IqWindows.children(w);
            // IQ island, 2026-10-15 (checklist 0.6): a T-SQL outer join's predicate, *= or =*
            case SqlAst.Operator o -> o.left() == null ? List.of(o.right()) : List.of(java.util.Objects.requireNonNull(
                    o.left()), o.right());
            // exhaustive over IqParser's forms: a new form is read here before a walk can miss what is inside it
            default -> throw new IllegalStateException("the #IQ{...}# dialect does not walk " + e.getClass()
                    .getSimpleName() + " yet");
        };
    }

    /** Each comparison of a CASE's (an IF's) condition under {@code e} into {@code out}. */
    private static void caseConditions(SqlAst.Expr e, Set<SqlAst.Expr> out) {
        if (e instanceof SqlAst.Case c) {
            for (SqlAst.When w : c.whens()) {
                comparisons(w.condition(), out);
            }
        }
        for (SqlAst.Expr child : children(e)) {
            caseConditions(child, out);
        }
    }

    /** The comparisons a condition is made of (through AND, OR and NOT). */
    private static void comparisons(SqlAst.Expr condition, Set<SqlAst.Expr> out) {
        switch (condition) {
            case SqlAst.Binary b when b.op() == SqlAst.BinaryOp.AND || b.op() == SqlAst.BinaryOp.OR -> {
                comparisons(b.left(), out);
                comparisons(b.right(), out);
            }
            case SqlAst.Unary u when u.op() == SqlAst.UnaryOp.NOT -> comparisons(u.operand(), out);
            case SqlAst.IsTruth t -> comparisons(t.operand(), out);
            case SqlAst.Binary b -> out.add(b);
            default -> {
            }
        }
    }

    // ------------------------------------------------------------------ names

    /** IQ keeps an alias as written. */
    @Override
    public String fold(String text) {
        return text;
    }

    /** IQ's identifier rule: the one name equal to {@code id} without regard to ASCII case, quoted or not. */
    @Override
    public @com.legend.base.Nullable String resolve(SqlAst.Ident id, Iterable<String> names, String what) {
        String key = IqLexer.lower(id.text());
        List<String> found = new ArrayList<>();
        for (String n : names) {
            if (IqLexer.lower(n).equals(key) && !found.contains(n)) {
                found.add(n);
            }
        }
        if (found.size() > 1) {
            throw SqlTranslationException.invalid(what + " '" + id.text() + "' names " + String.join(" and ",
                    found.stream().map(n -> "'" + n + "'").toList()) + ", which IQ cannot tell apart: its names"
                    + " match without regard to case", id.pos());
        }
        return found.isEmpty() ? null : found.get(0);
    }

    @Override
    public String hint(SqlAst.Ident name, List<String> names) {
        return "";
    }

    /** IQ island, 2026-10-14 (review 5's M2, measured with a real variable): an alias spelled like a variable
     *  ({@code "@lo"}) is read before the variable {@code @lo}, in the select list and in WHERE; lite reads the
     *  variable, so such a read is refused by name. Any other alias read is read as IQ reads it ({@link IqAliases},
     *  the third landing). */
    private static void variableAliases(SqlAst.Select s) {
        Set<String> aliases = new java.util.HashSet<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x && x.alias() != null) {
                aliases.add(IqLexer.lower(java.util.Objects.requireNonNull(x.alias()).text()));
            }
        }
        if (aliases.isEmpty()) {
            return;
        }
        List<SqlAst.Param> params = new ArrayList<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x) {
                params(x.expr(), params);
            }
        }
        List<SqlAst.Expr> clauses = new ArrayList<>();
        if (s.where() != null) {
            clauses.add(java.util.Objects.requireNonNull(s.where()));
        }
        if (s.having() != null) {
            clauses.add(java.util.Objects.requireNonNull(s.having()));
        }
        clauses.addAll(s.groupBy());
        s.orderBy().forEach(o -> clauses.add(o.expr()));
        for (SqlAst.FromItem f : s.from()) {
            List<SqlAst.Join> joins = new ArrayList<>();
            joins(f, joins);
            for (SqlAst.Join j : joins) {
                if (j.on() != null) {
                    clauses.add(java.util.Objects.requireNonNull(j.on()));
                }
            }
        }
        clauses.forEach(e -> params(e, params));
        for (SqlAst.Param p : params) {
            String written = p.name();
            if (written != null && written.startsWith("@") && aliases.contains(IqLexer.lower(written))) {
                throw SqlTranslationException.unsupported("'" + written + "' names the select list's alias, which IQ"
                        + " reads before a variable of that name: reading an alias in place of a host variable is not"
                        + " supported yet; rename the alias", p.pos());
            }
        }
    }

    /** IQ island, 2026-10-14 (review 5's M2): the named parameters ({@code @name}, {@code :name}) under {@code e}, not
     *  inside a subquery. */
    private static void params(SqlAst.Expr e, List<SqlAst.Param> out) {
        if (e instanceof SqlAst.Param p) {
            out.add(p);
        }
        for (SqlAst.Expr child : children(e)) {
            params(child, out);
        }
    }

    static void columnRefs(SqlAst.Expr e, List<SqlAst.ColumnRef> out) {
        if (e instanceof SqlAst.ColumnRef ref) {
            out.add(ref);
        }
        for (SqlAst.Expr child : children(e)) {
            columnRefs(child, out);
        }
    }

    // ------------------------------------------------------------------ order and output names

    /** IQ's NULL is the lowest value. */
    @Override
    public boolean nullsFirst(boolean descending) {
        return !descending;
    }

    /** A column shows as its reference is written; in a grouped query of SQL Anywhere's front end, one that is a GROUP
     *  BY item written bare as that item is written (M1c, measured: {@code SELECT K ... GROUP BY k} shows {@code k};
     *  IQ island, 2026-10-16, measured on IQ 17.1: {@code K} under IQ's engine). */
    @Override
    public String columnName(SqlAst.Ident written, String label) {
        String key = naming.keyName(written);
        return key != null ? key : written.text();
    }

    /** IQ island, 2026-10-14 (review 7's B2): a column read from a query without FROM, {@code (SELECT id AS z)}, is
     *  named by that query's alias ({@link #defaultName(SqlAst.SelectExpr, Tables)}). */
    @Override
    public boolean namedAsColumn(SqlAst.SelectExpr item) {
        return !innerNames.containsKey(item);
    }

    /** IQ's name for an unnamed item read with no FROM item known ({@link #defaultName(SqlAst.SelectExpr, Tables)}):
     *  one over no column. */
    @Override
    public String defaultName(SqlAst.SelectExpr item) {
        return defaultName(item, ref -> null);
    }

    /** IQ's name for an unnamed item: its rendering of the item ({@link IqNames}, measured on SQL Anywhere 17, which
     *  IQ's front end is). Where that rendering is not known, the item is refused by name if the statement shows its
     *  name; an item no one sees (a subquery's) is named by its own text. */
    @Override
    public String defaultName(SqlAst.SelectExpr item, Tables tables) {
        SqlAst.Ident inner = innerNames.get(item);
        if (inner != null) {
            // IQ island, 2026-10-14 (review 7's B2, measured): (SELECT x AS a) is named a
            return inner.text();
        }
        SqlAst.Expr e = item.expr();
        while (e instanceof SqlAst.Unary u && u.op() == SqlAst.UnaryOp.PLUS) {
            e = u.operand();
        }
        // IQ island, 2026-10-14 (the third landing, measured): an alias the item reads shows as written (x + 1 over
        // an alias x is x+1, "my x" + 1 is "my x"+1)
        Set<String> read = aliasesRead.getOrDefault(item, Set.of());
        IqNames.Columns columns = (ref, inAggregate) -> {
            if (ref.parts().size() == 1 && read.contains(IqLexer.lower(ref.parts().get(0).text()))) {
                return aliasName(ref.parts().get(0).text());
            }
            // IQ island, 2026-10-16 (measured on IQ 17.1): a column named by a reserved word shows in double quotes
            // (t."date", t."order"; t.value, t.name bare)
            String column = ref.parts().get(ref.parts().size() - 1).text();
            String shown = IqLexer.reserved(column) ? "\"" + column + "\"" : column;
            if (ref.parts().size() == 3) {
                // IQ island, 2026-10-14 (the third landing, measured): owner.table.column shows as written (the
                // reader bound it to its table's column, IqAliases)
                return ref.parts().get(0).text() + "." + ref.parts().get(1).text() + "." + shown;
            }
            SqlAst.Ident table = tables.table(ref);
            if (table != null && derived.contains(IqLexer.lower(table.text()))) {
                // IQ island, 2026-10-17 (measured on IQ 17.1): a grouped or set-operation derived table's column by its
                // correlation name (d.s/2), a plain one's by its definition (derivedRenderings), in an aggregate too
                return setDerived.contains(IqLexer.lower(table.text())) ? (ref.parts().size() == 2
                        ? ref.parts().get(0).text() : table.text()) + "." + shown
                        : derivedRenderings.get(IqLexer.lower(table.text()) + "." + IqLexer.lower(column));
            }
            if (table == null || derived.contains(IqLexer.lower(table.text())) && !(inAggregate
                    && setDerived.contains(IqLexer.lower(table.text())))) {
                return null;
            }
            if (ref.parts().size() == 1) {
                return table.text() + "." + shown;
            }
            return ref.parts().get(0).text() + "." + shown;
        };
        // IQ island, 2026-10-14 (M1c): in a grouped query, a GROUP BY item's and an aggregate's parts as IQ reads them
        String key = e instanceof SqlAst.ColumnRef ref ? naming.keyName(ref.parts().get(ref.parts().size() - 1))
                : null;
        String name = key != null ? key : e instanceof SqlAst.ScalarSubquery q ? subqueryName(q.query())
                : IqNames.name(item.text(), settings, columns, naming.substitutes(item, settings, columns, tables),
                        chains.getOrDefault(item, IqNames.Chains.UNKNOWN));
        // IQ island, 2026-10-14 (M1e): an item over a host variable read as a Pure variable is not named
        name = readsVariable(e) ? null : name;
        if (!shown.contains(item)) {
            given.put(item, name);
            return name != null ? name : item.text() != null ? java.util.Objects.requireNonNull(item.text())
                    : "expression";
        }
        if (name == null) {
            throw SqlTranslationException.unsupported("this select item needs an alias (AS name): the name IQ gives an"
                    + " unaliased " + (item.text() == null ? "item" : "'" + item.text() + "'") + " is not known yet",
                    item.pos());
        }
        return name;
    }

    /** IQ island, 2026-10-14 (the third landing, measured): an alias read inside an unaliased item renders as written,
     *  in double quotes where it is no regular name ({@code "x y"+1}; {@code "x"} read as {@code x}); null where the
     *  rendering is not known (a name outside ASCII, or with a double quote). */
    private static @com.legend.base.Nullable String aliasName(String text) {
        boolean regular = !text.isEmpty() && !IqLexer.reserved(text);
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c > 127 || c == '"') {
                return null;
            }
            boolean letter = c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' || c == '@' || c == '#'
                    || c == '$';
            regular &= letter || i > 0 && c >= '0' && c <= '9';
        }
        return regular ? text : "\"" + text + "\"";
    }

    /** IQ island, 2026-10-14 (M1e): whether {@code e} reads a host variable as a variable of the enclosing Pure query
     *  ({@link #parameter}). */
    private boolean readsVariable(SqlAst.Expr e) {
        if (e instanceof SqlAst.Param p && variables.contains(p)) {
            return true;
        }
        for (SqlAst.Expr child : children(e)) {
            if (readsVariable(child)) {
                return true;
            }
        }
        return false;
    }

    /** The name of a parenthesized query shown as a select item: its one item's (its alias, its column's name, or
     *  the name it was given when translated); null where it is not known. */
    private @com.legend.base.Nullable String subqueryName(SqlAst.Statement query) {
        if (!(query instanceof SqlAst.Select s) || s.items().size() != 1
                || !(s.items().get(0) instanceof SqlAst.SelectExpr x)) {
            return null;
        }
        if (x.alias() != null) {
            return java.util.Objects.requireNonNull(x.alias()).text();
        }
        SqlAst.Expr e = x.expr();
        while (e instanceof SqlAst.Unary u && u.op() == SqlAst.UnaryOp.PLUS) {
            e = u.operand();
        }
        if (e instanceof SqlAst.ColumnRef ref) {
            return ref.parts().get(ref.parts().size() - 1).text();
        }
        return given.get(x);
    }

    // ------------------------------------------------------------------ types

    @Override
    public Kind kindOf(String pureType, @com.legend.base.Nullable String storeType) {
        int paren = pureType.indexOf('(');
        String base = paren < 0 ? pureType : pureType.substring(0, paren);
        return switch (base) {
            case "Integer" -> Kind.INTEGER;
            case "Float" -> Kind.FLOAT;
            case "Decimal" -> Kind.DECIMAL;
            case "Number" -> Kind.NUMBER;
            case "String" -> Kind.STRING;
            case "Boolean" -> Kind.BOOLEAN;
            case "StrictDate" -> Kind.DATE;
            case "DateTime" -> Kind.DATETIME;
            default -> Kind.OTHER;
        };
    }

    /** The column's store type, when it has one. */
    @Override
    public InputDialect.@com.legend.base.Nullable TypeDetail detailOf(String pureType,
            @com.legend.base.Nullable String storeType) {
        return storeType == null ? null : new IqType(storeType);
    }

    /** Under the IQ engine a CHAR(n) value is blank padded to n (the research's D-S1: the IQ manual's own examples and
     *  the TPC-H reports run on IQ show it). A BIT is IQ's integer 0 or 1 (the store holds Pure's Boolean): read as
     *  {@code if(b, 1, 0)}, NULL staying NULL. A store type the island does not read yet refuses the source by name. */
    @Override
    public SqlToPure.@com.legend.base.Nullable Tx read(String column, Tx stored, SqlAst.Pos pos) {
        if (!(stored.detail() instanceof IqType type)) {
            return null;
        }
        String family = type.family();
        if (referenced != null && !referenced.contains(IqLexer.lower(column)) && !starred.contains(pos)
                && (family.equals("CHAR") && type.size() >= 0 || family.equals("BIT"))) {
            // never seen: read as stored (the CHAR's pad and the BIT's integer change nothing the query sees; a NATURAL
            // join's shared columns compare by IQ's =, which ignores the pad)
            return null;
        }
        if (family.equals("CHAR")) {
            int length = type.size();
            if (length < 0) {
                throw SqlTranslationException.unsupported("the column '" + column + "' is of store type CHAR with no"
                        + " length, which the #IQ{...}# island does not read", pos);
            }
            if (engine != Engine.IQ) {
                return null;
            }
            Tx padded = SqlToPure.strict(Kind.STRING, List.of(stored), v -> SqlToPure.call("rpad", v.get(0),
                    new CInteger((long) length), new CString(" ")));
            return Tx.value(padded.vs(), Kind.STRING, padded.nullable(), type);
        }
        if (family.equals("BIT") && stored.kind() == Kind.BOOLEAN) {
            ValueSpecification bit = ifThen(stored.vs(), new CInteger(1L), stored.nullable()
                    ? ifThen(stored.falseWhen(), new CInteger(0L), SqlToPure.empty()) : new CInteger(0L));
            return Tx.value(bit, Kind.INTEGER, stored.nullable(), new IqType("BIT"));
        }
        if (!READ_AS_STORED.contains(family)) {
            throw SqlTranslationException.unsupported("the column '" + column + "' is of store type " + type.spelling()
                    + ", which the #IQ{...}# island does not read yet", pos);
        }
        return null;
    }

    /** An integer constant is an Integer of IQ's type by its value (a NUMERIC, Pure's Decimal, beyond a BIGINT); a
     *  numeric one a Decimal; a string a String (a CHAR of its length); a bare NULL IQ's SMALLINT NULL. */
    @Override
    public Tx literal(SqlAst.Expr constant) {
        BigDecimal number = constant instanceof SqlAst.IntegerLit n ? new BigDecimal(n.value())
                : constant instanceof SqlAst.NumericLit n ? n.value() : null;
        if (number != null && number.signum() != 0 && number.stripTrailingZeros().precision() > settings.precision()) {
            // the profile's database keeps only its PRECISION's leading digits of such a constant (measured)
            throw SqlTranslationException.unsupported("a number of more than " + settings.precision() + " significant"
                    + " digits is not supported under profile " + settings.spelling() + ": its database keeps only the"
                    + " leading ones", constant.pos());
        }
        return switch (constant) {
            case SqlAst.IntegerLit n -> n.value().bitLength() < Long.SIZE
                    ? Tx.value(SqlToPure.integer(n.value()), Kind.INTEGER, false, IqTypes.ofIntegerConstant(n.value()))
                    : Tx.value(new CDecimal(new BigDecimal(n.value())), Kind.DECIMAL, false,
                            IqTypes.ofIntegerConstant(n.value()));
            // at IQ's scale for it: its digits without trailing zeros (1.50 is NUMERIC(2,1) 1.5, 1.50E1 is 15)
            case SqlAst.NumericLit n -> Tx.value(new CDecimal(IqTypes.atScale(n.value())), Kind.DECIMAL, false,
                    IqTypes.ofNumericConstant(n.value()));
            case SqlAst.StringLit s -> Tx.value(new CString(s.value()), Kind.STRING, false, IqTypes.ofString(s.value()));
            case SqlAst.NullLit n -> Tx.value(SqlToPure.nullOf(Kind.INTEGER).vs(), Kind.NULL, true);
            default -> throw SqlTranslationException.unsupported("this constant is not IQ's", constant.pos());
        };
    }

    // ------------------------------------------------------------------ meanings

    @Override
    public Tx binary(SqlAst.Binary b, SqlToPure.At at) {
        return bounded(meaning(b, at), b);
    }

    private Tx meaning(SqlAst.Binary b, SqlToPure.At at) {
        return switch (b.op()) {
            // Kleene AND and OR through the shared chain reader (IQ island, 2026-10-14: the seventh line's
            // At.logical, PR #41): a chain of one operator read without recursion, each operand under its operator's
            // position, combined as a balanced tree -- the meaning of the two-operand arm it replaces
            case AND, OR -> at.logical(b);
            case EQ, NE, LT, LE, GT, GE -> {
                Tx l = at.tx(b.left());
                Tx r = at.tx(b.right());
                if (engine == Engine.IQ && simpleWhens.contains(b)) {
                    simpleWhen(b, l, r);
                }
                Tx compared = comparison(b.op(), l, r, b.left() instanceof SqlAst.NullLit,
                        b.right() instanceof SqlAst.NullLit, b, b.pos());
                yield selfComparison(b, l, r) ? itself(b.op(), l) : twoValued(b, compared);
            }
            case PLUS -> plus(b, at);
            case CONCAT -> {
                List<Tx> parts = new ArrayList<>();
                List<SqlAst.Expr> leaves = operands(b, SqlAst.BinaryOp.CONCAT);
                for (SqlAst.Expr e : leaves) {
                    parts.add(at.tx(e));
                }
                yield concatenation(parts, leaves, b, "||");
            }
            case MINUS, TIMES, DIVIDE, MODULO -> arithmetic(b, at.tx(b.left()), at.tx(b.right()));
            case POWER -> throw SqlTranslationException.unsupported("IQ has no power operator", b.pos());
        };
    }

    /**
     * IQ island, 2026-10-17 (the differential fuzzer's fifth round; measured on IQ 17.1): under IQ's engine a simple
     * CASE's operand and a WHEN's value of different kinds -- a number and a string either way, a number or a string
     * and a date -- are its -1009145 ("In CASE expression, incompatible data type"), whether or not a row reaches it
     * ({@code CASE sm WHEN '1'}), where a comparison converts; a constant string that is no number beside a number is
     * -157 or -1009145 by IQ's plan, refused by name. A date operand converts a string, as a comparison does.
     */
    private static void simpleWhen(SqlAst.Binary b, Tx operand, Tx value) {
        boolean numberOperand = numeric(operand.kind());
        boolean numberValue = numeric(value.kind());
        boolean dateValue = value.kind() == Kind.DATE || value.kind() == Kind.DATETIME;
        boolean mixed = numberOperand && (value.kind() == Kind.STRING || dateValue)
                || operand.kind() == Kind.STRING && (numberValue || dateValue);
        if (!mixed) {
            return;
        }
        if (numberOperand && b.right() instanceof SqlAst.StringLit l && !iqNumber(l.value())) {
            throw SqlTranslationException.unsupported("a simple CASE's WHEN of a string that is no number, beside a"
                    + " number, is IQ's -157 or -1009145 by its plan under its engine: not supported", b.right().pos());
        }
        throw SqlTranslationException.invalid("Data type conversion is not possible: a simple CASE compares "
                + kindName(operand.kind()) + " with " + kindName(value.kind()) + " under IQ's engine"
                + code(-1009145, "QFA2E"), b.right().pos());
    }

    private static boolean numeric(Kind k) {
        return k == Kind.INTEGER || k == Kind.DECIMAL || k == Kind.FLOAT || k == Kind.NUMBER;
    }

    /** IQ island, 2026-10-17 (the differential fuzzer's IF ch = ch THEN ch ELSE '' ENDIF; measured on IQ 17.1): under
     *  IQ's engine an expression compared with itself, written alike ({@code v = v}, {@code ABS(v) = ABS(v)}, {@code
     *  g.v = v} where both read one column), is two-valued outside a search condition -- an IF's or a CASE's
     *  condition, a truth test's operand: {@code =}, {@code <=} and {@code >=} hold where it is not NULL, {@code <>},
     *  {@code <} and {@code >} never; reached through AND, OR and NOT from a WHERE, an ON or a HAVING it stays
     *  three-valued (NOT (v = v) keeps no row there). One that reads no column, a subquery or a host variable is read
     *  as any other. */
    private boolean selfComparison(SqlAst.Binary b, Tx l, Tx r) {
        return engine == Engine.IQ && !filters.contains(b) && (IqAggregates.same(b.left(), b.right())
                || IqAggregates.sameUnqualified(b.left(), b.right()) && l.vs().equals(r.vs()))
                && readsColumn(b.left()) && !readsQuery(b.left()) && !readsParameter(b.left());
    }

    /** {@link #selfComparison}'s truth: TRUE where {@code x} is not NULL for an order that holds of equals, else FALSE;
     *  never UNKNOWN. */
    private static Tx itself(SqlAst.BinaryOp op, Tx x) {
        if (op == SqlAst.BinaryOp.NE || op == SqlAst.BinaryOp.LT || op == SqlAst.BinaryOp.GT) {
            return Tx.condition(new CBoolean(false), new CBoolean(true), false);
        }
        if (!x.nullable()) {
            return Tx.condition(new CBoolean(true), new CBoolean(false), false);
        }
        return Tx.condition(SqlToPure.call("isNotEmpty", SqlToPure.valueOf(x)), SqlToPure.call("isEmpty",
                SqlToPure.valueOf(x)), false);
    }

    /** The comparison operators. */
    private static final Set<SqlAst.BinaryOp> COMPARISONS = Set.of(SqlAst.BinaryOp.EQ, SqlAst.BinaryOp.NE,
            SqlAst.BinaryOp.LT, SqlAst.BinaryOp.LE, SqlAst.BinaryOp.GT, SqlAst.BinaryOp.GE);

    private static boolean readsParameter(SqlAst.Expr e) {
        return e instanceof SqlAst.Param || children(e).stream().anyMatch(IqDialect::readsParameter);
    }

    /** Two WINDOW definitions of one name: SQL Anywhere reads the first
     *  (lite/windows/named_window_twice_the_first_counts, measured on IQ 17). */
    @Override
    public boolean oneWindowPerName() {
        return false;
    }

    /** {@code -x}, {@code +x}, {@code NOT c}: a minus before an integer constant is a constant typed by its value; one
     *  before any other integer negates it in its type (a BIT's is a SMALLINT), checked; a minus before a numeric
     *  constant negates it exactly; a plus changes no number and no string. */
    @Override
    public Tx unary(SqlAst.Unary u, SqlToPure.At at) {
        Tx x = at.tx(u.operand());
        if (u.op() == SqlAst.UnaryOp.NOT) {
            return SqlToPure.not(SqlToPure.truth(x, "NOT", u.pos()));
        }
        if (x.kind() == Kind.NULL && engine == Engine.IQ && u.op() == SqlAst.UnaryOp.MINUS) {
            // IQ island, 2026-10-15 (measured on IQ 17.1): -NULL is no arithmetic under IQ's engine
            throw SqlTranslationException.unsupported("- of an untyped NULL is IQ's error -1009145 under its engine,"
                    + " refused before translating", u.pos());
        }
        if (x.kind() == Kind.NULL) {
            return typedNull("SMALLINT");
        }
        if (u.op() == SqlAst.UnaryOp.PLUS) {
            // IQ island, 2026-10-17 (the corpus lane's '+' || ... spelled + 'x'; measured on IQ 17.1, its engine's and
            // its front end's alike): a plus before a string is the string, of its type
            if (x.kind().numeric() || x.kind() == Kind.STRING) {
                return x;
            }
            throw SqlTranslationException.unsupported("a sign before " + kindName(x.kind()) + " (IQ converts it to"
                    + " NUMERIC) is not supported yet", u.pos());
        }
        if (u.operand() instanceof SqlAst.IntegerLit n) {
            Tx negated = SqlToPure.strict(x.kind(), List.of(x), v -> SqlToPure.call("minus", v.get(0)));
            return Tx.value(negated.vs(), x.kind(), false, IqTypes.ofNegatedConstant(n.value()));
        }
        if (u.operand() instanceof SqlAst.NumericLit) {
            Tx negated = SqlToPure.strict(x.kind(), List.of(x), v -> SqlToPure.call("minus", v.get(0)));
            return Tx.value(negated.vs(), x.kind(), false, x.detail());
        }
        if (x.kind() == Kind.FLOAT) {
            // a DOUBLE's negation is exact, and never leaves its range
            doubleOperand(x, u.operand(), "-");
            Tx negated = SqlToPure.strict(Kind.FLOAT, List.of(x), v -> SqlToPure.call("minus", v.get(0)));
            // IQ island, 2026-10-15 (measured on IQ 17.1): a REAL's is a REAL
            return Tx.value(negated.vs(), Kind.FLOAT, negated.nullable(), new IqType(IqFunctions.real(x) ? "REAL"
                    : "DOUBLE"));
        }
        if (decimal(x)) {
            // a NUMERIC's negation keeps its type (TYPING §6), exactly; under IQ's engine too (IQ island, 2026-10-15,
            // D-N1, measured on IQ 17.1: -nm is a NUMERIC(10,2))
            int[] type = numericType(x, u.operand(), "-");
            Tx negated = SqlToPure.strict(Kind.DECIMAL, List.of(x), v -> SqlToPure.call("minus", v.get(0)));
            return Tx.value(negated.vs(), Kind.DECIMAL, negated.nullable(), numeric(type[0], type[1]));
        }
        IqType t = integerOperand(x, u.operand(), "-");
        IqType result = new IqType(IqTypes.negated(t.family()));
        BigInteger lo = t.hi().negate();
        BigInteger hi = t.lo().negate();
        return bounded(checkedInteger(u, "-", result, lo, hi, List.of(x), List.of(t), constants(List.of(u.operand()),
                List.of(x)), false, v -> SqlToPure.call("minus", v.get(0))), u);
    }

    /** {@code +}: two strings join (as {@code ||} does), two integers add; a string with anything else is IQ's NUMERIC
     *  addition of the string, refused until the conversions land. A run of {@code +} over strings is one join. */
    private Tx plus(SqlAst.Binary b, SqlToPure.At at) {
        List<SqlAst.Expr> leaves = operands(b, SqlAst.BinaryOp.PLUS);
        List<Tx> parts = new ArrayList<>();
        boolean strings = true;
        boolean anyString = false;
        boolean stringsOrNull = true;
        for (SqlAst.Expr e : leaves) {
            Tx t = at.tx(e);
            parts.add(t);
            strings &= t.kind() == Kind.STRING;
            anyString |= t.kind() == Kind.STRING;
            stringsOrNull &= t.kind() == Kind.STRING || t.kind() == Kind.NULL;
        }
        // IQ island, 2026-10-15 (measured on IQ 17.1): under IQ's engine a string + an untyped NULL joins them, the
        // NULL as nothing (vc + NULL is vc), as || does
        if (strings || engine == Engine.IQ && anyString && stringsOrNull) {
            return concatenation(parts, leaves, b, "+");
        }
        if (anyString) {
            throw SqlTranslationException.unsupported("+ of a string and a value that is no string is IQ's NUMERIC"
                    + " addition (the string converted, an untyped NULL included): not supported yet", b.pos());
        }
        return arithmetic(b, at.tx(b.left()), at.tx(b.right()));
    }

    /** The operands of a run of {@code op} (both sides: {@code ||} and string {@code +} are associative). */
    private static List<SqlAst.Expr> operands(SqlAst.Expr e, SqlAst.BinaryOp op) {
        if (e instanceof SqlAst.Binary b && b.op() == op) {
            List<SqlAst.Expr> all = new ArrayList<>(operands(b.left(), op));
            all.addAll(operands(b.right(), op));
            return all;
        }
        return List.of(e);
    }

    /** IQ's {@code ||} (and {@code +} of strings): the values as text, joined; a NULL among them is read as '', and the
     *  result is NULL only when every one is (the research's D-S3). An integer joins as its digits, a date by the
     *  profile's format, under IQ's engine a NUMERIC as its text (D-C8); any other value ({@code ||} writes doubles by
     *  IQ's format) is refused by name. */
    private Tx concatenation(List<Tx> parts, List<SqlAst.Expr> leaves, SqlAst.Binary b, String operator) {
        List<Tx> present = new ArrayList<>();
        for (int i = 0; i < parts.size(); i++) {
            Tx part = parts.get(i);
            if (part.kind() == Kind.NULL) {
                continue;
            }
            // IQ island, 2026-10-14 (M1e): a date or a timestamp in the profile's DATE_FORMAT or TIMESTAMP_FORMAT;
            // IQ island, 2026-10-15 (D-C8, measured on IQ 17.1): under IQ's engine a NUMERIC as its text ('0.01x')
            Tx p = part.kind() == Kind.DATE || part.kind() == Kind.DATETIME
                    ? IqDateTime.text(part, settings, operator, b.pos())
                    : engine == Engine.IQ && part.kind() == Kind.DECIMAL
                    ? IqFunctions.iqNumericAsText(part, leaves.get(i), operator) : part;
            boolean integer = p.kind() == Kind.INTEGER && p.detail() instanceof IqType t && t.isInteger();
            if (p.kind() != Kind.STRING && !integer) {
                throw SqlTranslationException.unsupported(operator + " of " + kindName(p.kind()) + " (IQ writes it as"
                        + " text by its own format) is not supported yet", b.pos());
            }
            present.add(p);
        }
        IqType text = new IqType("VARCHAR");
        if (present.isEmpty()) {
            return Tx.value(SqlToPure.nullOf(Kind.STRING).vs(), Kind.STRING, true, text);
        }
        boolean nullable = true;
        List<ValueSpecification> texts = new ArrayList<>();
        ValueSpecification allNull = null;
        for (Tx p : present) {
            nullable &= p.nullable();
            ValueSpecification v = SqlToPure.valueOf(p);
            texts.add(text(p, v));
            ValueSpecification isNull = SqlToPure.call("isEmpty", v);
            allNull = allNull == null ? isNull : SqlToPure.call("and", allNull, isNull);
        }
        ValueSpecification joined = texts.size() == 1 ? texts.get(0) : AppliedFunction.infixRun("plus", texts);
        if (!nullable) {
            if (leaves.stream().allMatch(l -> l instanceof SqlAst.StringLit)) {
                // IQ island, 2026-10-17: the text a concatenation of literals alone spells, which IQ folds as it plans
                // (dateOf reads it so)
                literalJoins.put(joined, String.join("", leaves.stream().map(l -> ((SqlAst.StringLit) l).value())
                        .toList()));
            }
            return Tx.value(joined, Kind.STRING, false, text);
        }
        return Tx.value(ifThen(java.util.Objects.requireNonNull(allNull), SqlToPure.empty(), joined), Kind.STRING,
                true, text);
    }

    /** A part of a join as text: a string as it is, an integer as its digits, a NULL as ''. */
    private static ValueSpecification text(Tx p, ValueSpecification v) {
        if (p.kind() == Kind.STRING) {
            return p.nullable() ? SqlToPure.call("coalesce", v, new CString("")) : v;
        }
        return p.nullable() ? ifThen(SqlToPure.call("isEmpty", v), new CString(""),
                SqlToPure.call("toString", SqlToPure.call("toOne", v))) : SqlToPure.call("toString", v);
    }

    /** {@code l op r} for {@code + - * / %} of two numbers: a DOUBLE's ({@link #doubleArithmetic}) when either is
     *  one, a NUMERIC's ({@link #decimalArithmetic}) when either is one, else an integer's
     *  ({@link #integerArithmetic}). */
    private Tx arithmetic(SqlAst.Binary b, Tx l, Tx r) {
        if (engine == Engine.IQ && (l.kind() == Kind.NULL || r.kind() == Kind.NULL)) {
            // IQ island, 2026-10-15 (measured on IQ 17.1): under IQ's engine an untyped NULL is no arithmetic operand
            // ("must be a valid arithmetic expression"); SQL Anywhere's front end answers NULL
            throw SqlTranslationException.unsupported(operator(b.op()) + " of an untyped NULL is IQ's error -1009145"
                    + " under its engine, refused before translating", b.pos());
        }
        SqlAst.Expr x = engine == Engine.IQ ? identityOperand(b) : null;
        SqlAst.Expr zero = x == null ? null : x == b.left() ? b.right() : b.left();
        // IQ island, 2026-10-17 (the corpus lane's SEDE: AVG(x + 0.0); measured on IQ 17.1): a zero written with a
        // point is folded only in the type a shown item takes (it + 0.0 shown is an INTEGER); inside another
        // expression it is a NUMERIC of scale 0 (AVG(it + 0.0) of scale 21, (it + 0.0) / (sm + 0.0) a NUMERIC quotient)
        if (zero != null && integerLiteral(zero) == null && !shownValues.contains(b)) {
            x = null;
        }
        if (x != null) {
            Tx value = x == b.left() ? l : r;
            if (value.detail() instanceof IqType type && IDENTITY_FAMILIES.contains(type.family())) {
                // IQ island, 2026-10-17 (the differential fuzzer; measured on IQ 17.1): under IQ's engine x + 0, 0 + x
                // and x - 0 are x for a SMALLINT, an INTEGER or a DOUBLE (0 + it an INTEGER where 1 + it is a BIGINT,
                // 0.0 + it an INTEGER), not for a BIGINT, a NUMERIC or a BIT; not in GROUP BY, whose key it then is
                // of no select item written alike (IqAggregates.foldedOutOfGroup)
                for (SqlAst.Select s : grouped.values()) {
                    IqAggregates.foldedOutOfGroup(s, b, x);
                }
                return value;
            }
        }
        if (engine == Engine.IQ && (l.kind() == Kind.DATE || l.kind() == Kind.DATETIME || r.kind() == Kind.DATE
                || r.kind() == Kind.DATETIME)) {
            // IQ island, 2026-10-15 (measured on IQ 17.1): a date moved by days, two dates' days apart
            return new IqFunctions(this).dateArithmetic(b, l, r);
        }
        if (l.kind() == Kind.FLOAT || r.kind() == Kind.FLOAT) {
            return doubleArithmetic(b, l, r);
        }
        if (decimal(l) || decimal(r)) {
            return decimalArithmetic(b, l, r);
        }
        return integerArithmetic(b, l, r);
    }

    /** The types whose {@code x + 0} IQ's engine reads as {@code x} ({@link #arithmetic}). */
    private static final Set<String> IDENTITY_FAMILIES = Set.of("SMALLINT", "INTEGER", "DOUBLE");

    /** {@code x} of {@code x + 0}, {@code 0 + x} or {@code x - 0} (a zero written as a number) where {@code x} reads a
     *  column; null for any other expression. */
    private static SqlAst.@com.legend.base.Nullable Expr identityOperand(SqlAst.Binary b) {
        BigDecimal right = numberLiteral(b.right());
        if ((b.op() == SqlAst.BinaryOp.PLUS || b.op() == SqlAst.BinaryOp.MINUS) && right != null
                && right.signum() == 0 && readsColumn(b.left())) {
            return b.left();
        }
        BigDecimal left = numberLiteral(b.left());
        if (b.op() == SqlAst.BinaryOp.PLUS && left != null && left.signum() == 0 && readsColumn(b.right())) {
            return b.right();
        }
        return null;
    }

    private static String operator(SqlAst.BinaryOp op) {
        return switch (op) {
            case PLUS -> "+";
            case MINUS -> "-";
            case TIMES -> "*";
            case DIVIDE -> "/";
            case MODULO -> "%";
            default -> throw new IllegalArgumentException("not arithmetic: " + op);
        };
    }

    /** Whether {@code t} is an exact number IQ types NUMERIC: a numeric constant or column, an integer constant beyond
     *  an INT's range. */
    private static boolean decimal(Tx t) {
        if (t.detail() instanceof IqType type && (type.family().equals("NUMERIC") || type.family().equals("DECIMAL"))) {
            return true;
        }
        return t.kind() == Kind.DECIMAL || t.kind() == Kind.NUMBER;
    }

    /** A DATE as the TIMESTAMP of its midnight, as IQ converts one beside a TIMESTAMP (measured on IQ 17.1). */
    private static Tx atMidnight(Tx date) {
        return Tx.value(SqlToPure.strict(Kind.DATETIME, List.of(date), x -> IqFunctions.midnight(x.get(0))).vs(),
                Kind.DATETIME, date.nullable(), IqFunctions.dated("TIMESTAMP", IqFunctions.latestYear(date)));
    }

    /** IQ island, 2026-10-15: a COUNT's integer (the island's UNSIGNED BIGINT: IQ's 20-digit integer of a COUNT, or of
     *  one beside a signed integer) as the BIGINT lite computes it in, of the range known of it: a whole number of rows,
     *  0 to a BIGINT's largest, where nothing narrower is known. */
    static IqType countRange(IqType type) {
        return IqType.integer("BIGINT", type.low() != null ? type.lo() : BigInteger.ZERO, type.high() != null
                ? type.hi() : IqType.BIGINT_MAX, true, type.loose());
    }

    /** Whether {@code t} is a number of a kind the island reads: an integer, a NUMERIC or a DOUBLE. */
    private static boolean number(Tx t) {
        return t.kind() == Kind.INTEGER || t.kind() == Kind.FLOAT || decimal(t);
    }

    /** IQ's NUMERIC type of precision {@code p} and scale {@code s}. */
    private static IqType numeric(int p, int s) {
        return new IqType("NUMERIC(" + p + "," + s + ")");
    }

    /** The NUMERIC(p, s) an operand of NUMERIC arithmetic enters it as ({p, s}; the research's TYPING §1, §4,
     *  measured): a NUMERIC its own; an integer literal (signed or not) the NUMERIC of its digits; any other integer
     *  NUMERIC(10,0), a BIGINT NUMERIC(20,0) -- precisions at least SQL Anywhere's (a folded constant's may be fewer),
     *  which the island reads only to refuse a result IQ caps or lite cannot hold, so never a wrong answer. Any other
     *  value is refused by name. */
    private static int[] numericType(Tx t, SqlAst.Expr e, String operator) {
        if (t.detail() instanceof IqType type) {
            String f = type.family();
            if ((f.equals("NUMERIC") || f.equals("DECIMAL")) && type.size() >= 0) {
                String sp = type.spelling();
                int comma = sp.indexOf(',');
                int scale = comma < 0 ? 0 : Integer.parseInt(sp.substring(comma + 1, sp.indexOf(')')).strip());
                return new int[] {type.size(), scale};
            }
            if (type.isInteger()) {
                BigInteger literal = integerLiteral(e);
                if (literal != null) {
                    return new int[] {Math.max(1, literal.abs().toString().length()), 0};
                }
                return new int[] {f.equals("BIGINT") ? 20 : 10, 0};
            }
            if (type.spelling().equals("UNSIGNED BIGINT")) {
                // IQ island, 2026-10-15 (the corpus lane; measured on IQ 17.1): a COUNT enters as a NUMERIC(20,0)
                // (COUNT(*) * 1.5 a NUMERIC(22,1), COUNT(id) + 0.25 a NUMERIC(23,2))
                return new int[] {20, 0};
            }
            if (t.kind() == Kind.INTEGER) {
                throw SqlTranslationException.unsupported(operator + " of a " + type.spelling() + " and a NUMERIC is"
                        + " not supported yet", e.pos());
            }
        }
        throw SqlTranslationException.unsupported(operator + " of " + kindName(t.kind()) + " and a NUMERIC is not"
                + " supported yet" + (t.kind() == Kind.STRING ? " (IQ converts the string to NUMERIC)" : ""), e.pos());
    }

    /** An integer literal's value, signs applied ({@code 5}, {@code -5}, {@code +5}); null for any other expression. */
    private static @com.legend.base.Nullable BigInteger integerLiteral(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.IntegerLit n -> n.value();
            case SqlAst.Unary u when u.op() == SqlAst.UnaryOp.MINUS -> {
                BigInteger v = integerLiteral(u.operand());
                yield v == null ? null : v.negate();
            }
            case SqlAst.Unary u when u.op() == SqlAst.UnaryOp.PLUS -> integerLiteral(u.operand());
            default -> null;
        };
    }

    /**
     * {@code l op r} where either is a NUMERIC: SQL Anywhere's front end's exact arithmetic (the research's TYPING §4,
     * measured on SQL Anywhere 17). The result is a NUMERIC(p, s): {@code + -} scale max(s1, s2), precision
     * max(p1 - s1, p2 - s2) + s + 1; {@code *} precision p1 + p2, scale s1 + s2 -- the scale the databases' own exact
     * arithmetic gives too, each operand widened to a 38-digit decimal first so that neither computes it narrower. With
     * an untyped NULL it is a DOUBLE NULL (measured). Refused by name: under the IQ engine (its decimal types are the
     * research's D-N1, not yet verified); a result the profile's PRECISION caps (SQL Anywhere then rounds it to a
     * smaller scale); one of more than 38 digits (lite's decimals hold 38). {@code /} and {@code %}: {@link #quotient}
     * (IQ island, 2026-10-14).
     */
    private Tx decimalArithmetic(SqlAst.Binary b, Tx l, Tx r) {
        String operator = operator(b.op());
        // IQ island, 2026-10-15 (the research's D-N1, measured on IQ 17.1): under IQ's engine + - * and % type and
        // compute as SQL Anywhere's front end does (nm + 1 a NUMERIC(11,2), nm * nm a NUMERIC(20,4), nm % 3 of scale 2,
        // the dividend's sign); / is its own (iqQuotient)
        if (engine == Engine.IQ && b.op() == SqlAst.BinaryOp.DIVIDE && l.kind() != Kind.NULL
                && r.kind() != Kind.NULL) {
            return iqQuotient(b, l, r);
        }
        if ((l.kind() == Kind.NULL || r.kind() == Kind.NULL) && b.op() == SqlAst.BinaryOp.MODULO) {
            // IQ island, 2026-10-14 (natives): NULL % 1.5 is a NUMERIC(2,1) NULL (measured), not a DOUBLE
            throw SqlTranslationException.unsupported("% of an untyped NULL and a NUMERIC is not supported yet: IQ"
                    + " types it NUMERIC by the NULL's type", b.pos());
        }
        if (l.kind() == Kind.NULL || r.kind() == Kind.NULL) {
            // an untyped NULL with a NUMERIC is a DOUBLE (measured; / too)
            return doubleNull();
        }
        if (b.op() == SqlAst.BinaryOp.DIVIDE || b.op() == SqlAst.BinaryOp.MODULO) {
            return quotient(b, l, r, operator);
        }
        // IQ island, 2026-10-17 (the corpus judge's NUMERIC places; measured on IQ 17.1): under IQ's engine an integer
        // column enters with its own type's digits (100.0 * sm a NUMERIC(8,0), sm + 1.0 a NUMERIC(6,0), bg + 1 a
        // NUMERIC(20,0)), as its quotient's operands do (iqNumericType); SQL Anywhere's front end's are NUMERIC(10,0)
        // and NUMERIC(20,0)
        int[] lt = engine == Engine.IQ ? iqNumericType(l, b.left(), operator) : numericType(l, b.left(), operator);
        int[] rt = engine == Engine.IQ ? iqNumericType(r, b.right(), operator) : numericType(r, b.right(), operator);
        int p;
        int s;
        if (b.op() == SqlAst.BinaryOp.TIMES) {
            p = lt[0] + rt[0];
            s = lt[1] + rt[1];
        } else {
            s = Math.max(lt[1], rt[1]);
            p = Math.max(lt[0] - lt[1], rt[0] - rt[1]) + s + 1;
        }
        if (p > settings.precision()) {
            throw SqlTranslationException.unsupported("this " + operator + " is a NUMERIC(" + p + "," + s + "), more"
                    + " digits than profile " + settings.spelling() + "'s PRECISION (" + settings.precision() + "): its"
                    + " database cuts it to a smaller scale, which is not supported", b.pos());
        }
        String past = null;
        if (p > 38) {
            if (engine != Engine.IQ || s > 37) {
                throw SqlTranslationException.unsupported("this " + operator + " is a NUMERIC(" + p + "," + s + "),"
                        + " more than the 38 digits lite's decimals hold", b.pos());
            }
            // IQ island, 2026-10-17 (the TPC-H corpus lane's 80 a * (1 - b) * (1 + c) of NUMERIC(15,2)s, IQ's
            // NUMERIC(47,6)): past 38 digits IQ's type holds what lite's 38 do at its scale; lite computes it there and
            // refuses at run time a value that may not fit (its size estimated in doubles, before the database
            // computes it), typing the value it holds
            past = REFUSED_AT_RUN_TIME + "this " + operator + " of more than " + (38 - s) + " digits before its point is"
                    + " not supported: its NUMERIC(" + p + "," + s + ") needs more than the 38 digits lite's decimals hold";
            p = 38;
        }
        String guardedPast = past;
        int guardScale = s;
        String fn = b.op() == SqlAst.BinaryOp.TIMES ? "times" : b.op() == SqlAst.BinaryOp.PLUS ? "plus" : "minus";
        boolean lInteger = !decimal(l);
        boolean rInteger = !decimal(r);
        // a product of more than 18 digits: one operand a 38-digit decimal, so that DuckDB does not compute it in 64
        // bits (it raises an overflow there past 18 digits); an integer becomes one anyway. IQ island, 2026-10-14
        // (review 3's m1): a sum or a difference of more than 18 digits widens both (DuckDB aligns the two scales in
        // the narrower one's width: 99999999999999999.5 + 1.00 overflowed DECIMAL(18,2))
        boolean times = b.op() == SqlAst.BinaryOp.TIMES;
        boolean widen = p > 18 && (times ? !lInteger && !rInteger : true);
        // IQ island, 2026-10-15 (the census's scale suite): under IQ's engine a + - * of operands that cannot raise
        // merges their NULL guards, and keeps its own apart, so a sum of NUMERIC measures stays linear in size
        boolean merge = cannotRaise(b.left()) && cannotRaise(b.right());
        Tx result = SqlToPure.strict(Kind.DECIMAL, List.of(l, r), merge || List.of(l, r).stream().filter(Tx::nullable)
                .count() == 1, v -> {
                    ValueSpecification value = AppliedFunction.infixRun(fn,
                            List.of(lInteger ? SqlToPure.call("toDecimal", v.get(0)) : widen ? wide(v.get(0)) : v.get(0),
                                    rInteger ? SqlToPure.call("toDecimal", v.get(1)) : widen && !times ? wide(v.get(1))
                                            : v.get(1)));
                    if (guardedPast == null) {
                        return value;
                    }
                    ValueSpecification la = SqlToPure.call("abs", SqlToPure.call("toFloat", v.get(0)));
                    ValueSpecification ra = SqlToPure.call("abs", SqlToPure.call("toFloat", v.get(1)));
                    ValueSpecification size = AppliedFunction.infixRun(times ? "times" : "plus", List.of(la, ra));
                    return IqFunctions.refusedPast(SqlToPure.call("greaterThanEqual", size,
                            new com.legend.protocol.spec.CFloat(0.99 * Math.pow(10, 38 - guardScale))), guardedPast, value,
                            SqlToPure.call("toOne", SqlToPure.call("cast", SqlToPure.empty(),
                                    SqlToPure.typeArg("Decimal"))));
                });
        return new Tx(result.vs(), Kind.DECIMAL, result.nullable(), null, numeric(p, s), result.guard(),
                result.body());
    }

    /**
     * {@code l / r} and {@code l % r} where either is a NUMERIC, in SQL Anywhere's front end (IQ island, 2026-10-14; the
     * research's TYPING §4, measured on SQL Anywhere 17). {@code /}: a NUMERIC(p1 + p2 + 1, s1 + p2 - s2 + 1), where
     * the profile's PRECISION P caps p it is P and the scale shrinks by as much (but not below min(s, SCALE)); its value
     * is {@link IqFunctions#quotient}'s. {@code %}: a NUMERIC((p2 - s2) + s, s), s = max(s1, s2), the remainder after
     * the quotient cut toward zero ({@link IqFunctions#remainder}). A zero divisor is IQ's error -628. Refused by name,
     * where IQ's answer is not lite's: a quotient anywhere but a shown select item nothing else reads (SQL Anywhere
     * rounds it only where it shows it: {@link #shownValues}); a quotient whose capped type may not hold it (SQL
     * Anywhere shows '?'); a capped remainder, and one SQL Anywhere computes in more digits than its PRECISION (it
     * loses the last ones); a quotient
     * whose operand's type is not read exactly -- SQL Anywhere types a constant integer expression by its folded value
     * ({@code (2+3)/1.5} divides a NUMERIC(1,0)), which this reader does not compute ({@link #exactlyTyped}); a step of
     * more than lite's 38 digits.
     */
    private Tx quotient(SqlAst.Binary b, Tx l, Tx r, String operator) {
        boolean divide = b.op() == SqlAst.BinaryOp.DIVIDE;
        if (divide && !shownValues.contains(b)) {
            throw SqlTranslationException.unsupported("a NUMERIC / is supported only as a select item of a SELECT"
                    + " without DISTINCT that nothing else in the statement reads (by its alias or position, or written"
                    + " again): SQL Anywhere rounds the quotient to its scale only where it shows it, and an operand, a"
                    + " CAST, a comparison or a key reads it with more digits ((10.0/3)*3 is 10.00), which this reader"
                    + " does not compute yet", b.pos());
        }
        if (divide && (!exactlyTyped(b.left()) || !exactlyTyped(b.right()))) {
            throw SqlTranslationException.unsupported("this / is not supported yet: SQL Anywhere types an operand of"
                    + " it that is a constant integer expression (a derived table's column defined by a constant too),"
                    + " or a function or CASE, by rules this reader does not follow yet, and the quotient's scale"
                    + " depends on it", b.pos());
        }
        int[] lt = numericType(l, b.left(), operator);
        int[] rt = numericType(r, b.right(), operator);
        int p;
        int s;
        if (divide) {
            p = lt[0] + rt[0] + 1;
            s = lt[1] + rt[0] - rt[1] + 1;
            if (p > settings.precision()) {
                int cut = s - (p - settings.precision());
                if (Math.min(s, settings.scale()) > cut) {
                    throw SqlTranslationException.unsupported("this / is a NUMERIC(" + p + "," + s + ") cut to"
                            + " profile " + settings.spelling() + "'s PRECISION (" + settings.precision() + ") at a"
                            + " scale that may not hold the quotient (SQL Anywhere then shows '?'), which is not"
                            + " supported", b.pos());
                }
                s = cut;
                p = settings.precision();
            }
        } else {
            s = Math.max(lt[1], rt[1]);
            p = rt[0] - rt[1] + s;
            if (p > settings.precision() || lt[0] - lt[1] + s > settings.precision()) {
                throw SqlTranslationException.unsupported("this % needs more digits than profile "
                        + settings.spelling() + "'s PRECISION (" + settings.precision() + "): its database computes"
                        + " it in fewer, which is not supported", b.pos());
            }
        }
        boolean fits = divide ? IqFunctions.quotientFits(lt[1], lt[0] - lt[1], rt[1], rt[0] - rt[1], p, s)
                : IqFunctions.remainderFits(lt[1], lt[0] - lt[1], rt[1], rt[0] - rt[1]);
        if (!fits) {
            throw SqlTranslationException.unsupported("this " + operator + " of a NUMERIC(" + lt[0] + "," + lt[1]
                    + ") and a NUMERIC(" + rt[0] + "," + rt[1] + ") needs a step of more than the 38 digits lite's"
                    + " decimals hold", b.pos());
        }
        BigDecimal divisor = numberLiteral(b.right());
        // IQ island, 2026-10-14 (the options framework): under divide_by_zero_error Off a zero divisor gives NULL
        boolean nullOnZero = !settings.divideByZeroError() && (divisor == null || divisor.signum() == 0)
                && zeroGuarded(b, operator);
        if (nullOnZero && divisor != null) {
            return Tx.value(SqlToPure.nullOf(Kind.DECIMAL).vs(), Kind.DECIMAL, true, numeric(p, s));
        }
        boolean lInteger = !decimal(l);
        boolean rInteger = !decimal(r);
        int fp = p;
        int fs = s;
        Tx result = SqlToPure.strict(Kind.DECIMAL, List.of(l, r), v -> {
            ValueSpecification x = lInteger ? SqlToPure.call("toDecimal", v.get(0)) : wide(v.get(0));
            ValueSpecification y = rInteger ? SqlToPure.call("toDecimal", v.get(1)) : wide(v.get(1));
            if (divisor != null && divisor.signum() == 0) {
                return raiseDecimal();
            }
            ValueSpecification value = divide
                    ? IqFunctions.quotient(x, lt[1], lt[0] - lt[1], y, rt[1], rt[0] - rt[1], fp, fs)
                    : IqFunctions.remainder(x, lt[1], lt[0] - lt[1], y, rt[1], rt[0] - rt[1]);
            return divisor != null ? value : ifThen(SqlToPure.call("equal", y, new CInteger(0L)), nullOnZero
                    ? SqlToPure.nullOf(Kind.DECIMAL).vs() : raiseDecimal(), value);
        });
        return Tx.value(result.vs(), Kind.DECIMAL, result.nullable() || nullOnZero, numeric(p, s));
    }

    /**
     * {@code l / r} where either is a NUMERIC, under IQ's engine (IQ island, 2026-10-15; the research's D-N1, measured
     * on IQ 17.1): a NUMERIC of scale max(6, s1 + (p2 - s2) + 1) -- the divisor's digits before its point counted as
     * IQ counts them, a constant's by its value (100: 3), a SMALLINT's 5, an INTEGER's 10, a BIGINT's 19, a BIT's 1,
     * a COUNT's 20 -- its value the quotient cut toward zero there (-0.5 / 3 is -.166666), wherever it is read; a zero
     * divisor is IQ's error. IQ reads an integer column's digits from its data (a column of 1 to 7 divides as one
     * digit, D-N3), lite from its type. Refused by name: a step of more than lite's 38 digits.
     */
    private Tx iqQuotient(SqlAst.Binary b, Tx l, Tx r) {
        int[] lt = iqNumericType(l, b.left());
        int[] rt = iqNumericType(r, b.right());
        int s = Math.max(6, lt[1] + rt[0] - rt[1] + 1);
        int whole1 = lt[0] - lt[1];
        int whole2 = rt[0] - rt[1];
        // IQ island, 2026-10-15 (the corpus lane: COUNT(id) * 100.0 / COUNT(*), IQ's NUMERIC(44,21), in 168 of the
        // corpora's queries): where the type's steps need more than lite's 38 digits, the quotient is computed for a
        // dividend of the most digits they hold, and a row whose dividend has more is refused there, at run time
        int held = whole1;
        // not by a divisor that is a decimal of scale 0: it may be one of IQ's whole NUMERICs, which IQ divides whole
        // (SUM(v) * 100 / SUM(SUM(v)) OVER () is its BIGINT 23, where lite's window SUM is a decimal)
        boolean guardable = !(decimal(r) && rt[1] == 0);
        while (held > 0 && !IqFunctions.truncatedDivisionFits(lt[1], held, rt[1], whole2, s)) {
            held = guardable ? held - 1 : 0;
        }
        if (held == 0) {
            throw SqlTranslationException.unsupported("this / of a NUMERIC(" + lt[0] + "," + lt[1] + ") and a NUMERIC("
                    + rt[0] + "," + rt[1] + ") is a NUMERIC of scale " + s + ", a step of which needs more than the 38"
                    + " digits lite's decimals hold", b.pos());
        }
        int dividendDigits = held;
        String past = held < whole1 ? REFUSED_AT_RUN_TIME + "a / whose dividend has more than " + held + " digits"
                + " before its point is not supported: its NUMERIC(" + (whole1 + rt[1] + s + 1) + "," + s + ") needs"
                + " more than the 38 digits lite's decimals hold" : null;
        BigDecimal divisor = numberLiteral(b.right());
        boolean nullOnZero = !settings.divideByZeroError() && (divisor == null || divisor.signum() == 0)
                && zeroGuarded(b, "/");
        if (nullOnZero && divisor != null) {
            return Tx.value(SqlToPure.nullOf(Kind.DECIMAL).vs(), Kind.DECIMAL, true, numeric(whole1 + s + 1, s));
        }
        boolean lInteger = !decimal(l);
        boolean rInteger = !decimal(r);
        Tx result = SqlToPure.strict(Kind.DECIMAL, List.of(l, r), v -> {
            ValueSpecification x = lInteger ? SqlToPure.call("toDecimal", v.get(0)) : wide(v.get(0));
            ValueSpecification y = rInteger ? SqlToPure.call("toDecimal", v.get(1)) : wide(v.get(1));
            if (divisor != null && divisor.signum() == 0) {
                return raiseDecimal();
            }
            ValueSpecification value = IqFunctions.truncatedDivision(x, lt[1], dividendDigits, y, rt[1], s);
            if (past != null) {
                value = IqFunctions.refusedPast(SqlToPure.call("greaterThanEqual", SqlToPure.call("abs", x),
                        new CDecimal(BigDecimal.TEN.pow(dividendDigits))), past, value, raiseDecimal());
            }
            return divisor != null ? value : ifThen(SqlToPure.call("equal", new CInteger(0L), y), nullOnZero
                    ? SqlToPure.nullOf(Kind.DECIMAL).vs() : raiseDecimal(), value);
        });
        return Tx.value(result.vs(), Kind.DECIMAL, result.nullable() || nullOnZero, numeric(whole1 + rt[1] + s + 1,
                s));
    }

    /** An operand of IQ's engine's NUMERIC quotient as a NUMERIC(p, s) ({@link #iqQuotient}): a NUMERIC its own, an
     *  integer literal its digits, an integer column its type's (SMALLINT 5, INTEGER 10, BIGINT 19, BIT 1), a COUNT
     *  (IQ's UNSIGNED BIGINT) 20. */
    private static int[] iqNumericType(Tx t, SqlAst.Expr e) {
        return iqNumericType(t, e, "/");
    }

    /** {@link #iqNumericType} of an operand of {@code operator}. */
    private static int[] iqNumericType(Tx t, SqlAst.Expr e, String operator) {
        if (t.detail() instanceof IqType type) {
            if (type.spelling().equals("UNSIGNED BIGINT")) {
                return new int[] {20, 0};
            }
            if (type.isInteger() && integerLiteral(e) == null) {
                return new int[] {switch (type.family()) {
                    case "BIT" -> 1;
                    case "SMALLINT" -> 5;
                    case "INTEGER" -> 10;
                    case "BIGINT" -> 19;
                    default -> throw new IllegalStateException("an integer family IQ divides by: " + type.family());
                }, 0};
            }
        }
        return numericType(t, e, operator);
    }

    /** A number literal's written value, signs applied ({@code 1.5}, {@code -2}); null for any other expression. */
    private static @com.legend.base.Nullable BigDecimal numberLiteral(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.IntegerLit n -> new BigDecimal(n.value());
            case SqlAst.NumericLit n -> n.value();
            case SqlAst.Unary u when u.op() == SqlAst.UnaryOp.MINUS -> {
                BigDecimal v = numberLiteral(u.operand());
                yield v == null ? null : v.negate();
            }
            case SqlAst.Unary u when u.op() == SqlAst.UnaryOp.PLUS -> numberLiteral(u.operand());
            default -> null;
        };
    }

    /** A Decimal that raises when computed: an empty value read as one (IQ's error, here -628). */
    private static ValueSpecification raiseDecimal() {
        return SqlToPure.call("toOne", SqlToPure.call("cast", SqlToPure.empty(), SqlToPure.typeArg("Decimal")));
    }

    /**
     * Whether SQL Anywhere's front end types {@code e}, an operand of a NUMERIC quotient, as this reader does
     * ({@link #numericType}; measured): a number constant by its digits, a column by its type, a CAST to NUMERIC by its
     * written precision (or a CAST of a column to an integer type, by that type), and arithmetic of those by TYPING
     * §4's formulas ({@code (1.5 + 2.25) / 7} divides a
     * NUMERIC(4,2)), signs included -- but not a constant integer expression, which it folds and types by its value's
     * digits ({@code (2+3) / 1.5} divides a NUMERIC(1,0), where this reader's type is an INTEGER's NUMERIC(10,0)), nor
     * a derived table's column defined by one ({@link #folded}), nor a function, a CASE or any other form (not
     * measured).
     */
    private boolean exactlyTyped(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.IntegerLit n -> true;
            case SqlAst.NumericLit n -> true;
            // IQ island, 2026-10-14 (the merge): not a derived table's column SQL Anywhere may type as a folded
            // constant ({@link #folded})
            case SqlAst.ColumnRef c -> !folded.contains(IqLexer.lower(c.parts().get(c.parts().size() - 1).text()));
            case SqlAst.Unary u when u.op() != SqlAst.UnaryOp.NOT -> u.operand() instanceof SqlAst.IntegerLit
                    || u.operand() instanceof SqlAst.NumericLit || exactlyTyped(u.operand()) && !wholeConstant(u);
            case SqlAst.Binary b -> switch (b.op()) {
                case PLUS, MINUS, TIMES, DIVIDE, MODULO -> !wholeConstant(b) && exactlyTyped(b.left())
                        && exactlyTyped(b.right());
                default -> false;
            };
            case SqlAst.Cast c -> (c.type().name().equals("NUMERIC") || c.type().name().equals("DECIMAL"))
                    && !c.type().modifiers().isEmpty() || INTEGER_TYPES.contains(c.type().name()) && readsColumn(c);
            default -> false;
        };
    }

    /** The integer types a CAST of a column enters a quotient as by its type (measured: a BIGINT as NUMERIC(20,0), a
     *  SMALLINT and an INTEGER as NUMERIC(10,0)); a CAST of a constant SQL Anywhere folds. */
    private static final Set<String> INTEGER_TYPES = Set.of("SMALLINT", "INTEGER", "BIGINT");

    /** Whether {@code e} reads a column somewhere. */
    private static boolean readsColumn(SqlAst.Expr e) {
        List<SqlAst.ColumnRef> refs = new ArrayList<>();
        columnRefs(e, refs);
        return !refs.isEmpty();
    }

    /** Whether {@code e} reads the rows -- a column, an aggregate, a window function, a subquery -- so that no
     *  database folds it as a constant (IQ island, 2026-10-15, the corpus lane: CAST(COUNT(*) AS DOUBLE) / 3). */
    private static boolean readsRows(SqlAst.Expr e) {
        if (readsColumn(e) || holdsSubquery(e)) {
            return true;
        }
        java.util.ArrayDeque<SqlAst.Expr> todo = new java.util.ArrayDeque<>(List.of(e));
        while (!todo.isEmpty()) {
            SqlAst.Expr x = todo.pop();
            if (x instanceof SqlAst.Window || x instanceof SqlAst.FunctionCall f && IqAggregates.named(f)) {
                return true;
            }
            children(x).forEach(todo::push);
        }
        return false;
    }

    /** Whether {@code e} is arithmetic of integer constants alone, which SQL Anywhere folds before typing it. */
    private static boolean wholeConstant(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.IntegerLit n -> true;
            case SqlAst.Unary u when u.op() != SqlAst.UnaryOp.NOT -> wholeConstant(u.operand());
            case SqlAst.Binary b -> switch (b.op()) {
                case PLUS, MINUS, TIMES, DIVIDE, MODULO -> wholeConstant(b.left()) && wholeConstant(b.right());
                default -> false;
            };
            default -> false;
        };
    }

    /** A decimal as a 38-digit decimal of its own scale: it plus a 38-digit zero of scale 0. */
    static ValueSpecification wide(ValueSpecification x) {
        return AppliedFunction.infixRun("plus", List.of(x, SqlToPure.call("toDecimal", new CInteger(0L))));
    }

    /** The arithmetic under {@code e} that an enclosing + - * reads, or a / as its dividend (not its divisor: x / inf
     *  is 0, which would hide the divisor's overflow), into {@code out}; without recursion. */
    private static void chained(SqlAst.Expr e, Set<SqlAst.Expr> out) {
        java.util.ArrayDeque<SqlAst.Expr> todo = new java.util.ArrayDeque<>();
        todo.push(e);
        while (!todo.isEmpty()) {
            SqlAst.Expr x = todo.pop();
            if (x instanceof SqlAst.Binary b) {
                boolean chain = b.op() == SqlAst.BinaryOp.PLUS || b.op() == SqlAst.BinaryOp.MINUS
                        || b.op() == SqlAst.BinaryOp.TIMES;
                if ((chain || b.op() == SqlAst.BinaryOp.DIVIDE) && arithmetic(b.left())) {
                    out.add(b.left());
                }
                if (chain && arithmetic(b.right())) {
                    out.add(b.right());
                }
            }
            for (SqlAst.Expr c : children(x)) {
                todo.push(c);
            }
        }
    }

    /** The IN, LIKE and comparison predicates search condition {@code c} reaches through AND, OR and NOT alone, into
     *  {@code out}. */
    private static void filters(SqlAst.Expr c, Set<SqlAst.Expr> out) {
        java.util.ArrayDeque<SqlAst.Expr> todo = new java.util.ArrayDeque<>();
        todo.push(c);
        while (!todo.isEmpty()) {
            SqlAst.Expr x = todo.pop();
            switch (x) {
                case SqlAst.Binary b when b.op() == SqlAst.BinaryOp.AND || b.op() == SqlAst.BinaryOp.OR -> {
                    todo.push(b.left());
                    todo.push(b.right());
                }
                case SqlAst.Unary u when u.op() == SqlAst.UnaryOp.NOT -> todo.push(u.operand());
                case SqlAst.InList in -> out.add(in);
                case SqlAst.Like l -> out.add(l);
                case SqlAst.Binary b when COMPARISONS.contains(b.op()) -> out.add(b);
                default -> {
                }
            }
        }
    }

    private static boolean arithmetic(SqlAst.Expr e) {
        return e instanceof SqlAst.Binary b && (b.op() == SqlAst.BinaryOp.PLUS || b.op() == SqlAst.BinaryOp.MINUS
                || b.op() == SqlAst.BinaryOp.TIMES || b.op() == SqlAst.BinaryOp.DIVIDE);
    }

    /** IQ island, 2026-10-15: whether {@code e} cannot raise under IQ's engine -- a column, a constant, + - * of such
     *  (integers widen unchecked, D-N3; a NUMERIC's 38 digits are refused past; a DOUBLE's overflow is NULL): so an
     *  operation over it may merge its operands' NULL guards ({@link SqlToPure#strict}) without losing an error. */
    private boolean cannotRaise(SqlAst.Expr e) {
        if (engine != Engine.IQ) {
            return false;
        }
        return switch (e) {
            case SqlAst.ColumnRef r -> true;
            case SqlAst.IntegerLit n -> true;
            case SqlAst.NumericLit n -> true;
            case SqlAst.NullLit n -> true;
            case SqlAst.Unary u -> u.op() != SqlAst.UnaryOp.NOT && cannotRaise(u.operand());
            case SqlAst.Binary b -> (b.op() == SqlAst.BinaryOp.PLUS || b.op() == SqlAst.BinaryOp.MINUS
                    || b.op() == SqlAst.BinaryOp.TIMES) && cannotRaise(b.left()) && cannotRaise(b.right());
            default -> false;
        };
    }

    /** A DOUBLE NULL. */
    private static Tx doubleNull() {
        return Tx.value(SqlToPure.nullOf(Kind.FLOAT).vs(), Kind.FLOAT, true, new IqType("DOUBLE"));
    }

    /** The largest finite DOUBLE: a result beyond it is IQ's error -158. */
    private static final double DOUBLE_MAX = Double.MAX_VALUE;

    /**
     * {@code l op r} where either is a DOUBLE: SQL Anywhere's IEEE double arithmetic, which the databases compute alike
     * ({@code + - * /} of two doubles round the same everywhere), the other operand converted to a DOUBLE first (an
     * integer of any of IQ's types; a NUMERIC under SQL Anywhere's front end). A result beyond the largest DOUBLE is
     * IQ's error -158 and a zero divisor IQ's -628, where DuckDB would answer an infinity: each is checked, so every
     * database raises. With an untyped NULL it is a DOUBLE NULL. Refused by name: {@code %} (IQ types it NUMERIC), a
     * string (IQ converts it to NUMERIC), a NUMERIC under the IQ engine (the research's D-N1). A REAL takes part as
     * its DOUBLE, two REALs give a REAL (IQ island, 2026-10-15).
     */
    private Tx doubleArithmetic(SqlAst.Binary b, Tx l, Tx r) {
        String operator = operator(b.op());
        if (b.op() == SqlAst.BinaryOp.MODULO) {
            throw SqlTranslationException.unsupported("% of a DOUBLE is not supported yet: IQ's result is a NUMERIC",
                    b.pos());
        }
        if (l.kind() == Kind.NULL || r.kind() == Kind.NULL) {
            return doubleNull();
        }
        boolean lf = doubleOperand(l, b.left(), operator);
        boolean rf = doubleOperand(r, b.right(), operator);
        // IQ island, 2026-10-15 (the corpus lane; measured on IQ 17.1): in SQL Anywhere's front end two REALs give a
        // REAL, the DOUBLE result rounded to single precision (+ - * / of two singles in double precision round once,
        // exactly as single precision does: CAST(1.1 AS REAL) * CAST(3 AS REAL) is 3.3000001907348633); under IQ's
        // engine a DOUBLE, unrounded (1.5's REAL times 1.1's is 1.6500000357627869); a REAL with any other number is the
        // DOUBLE of its value (CAST(1.1 AS REAL) * 3 is 3.3000000715255737)
        boolean reals = engine != Engine.IQ && IqFunctions.real(l) && IqFunctions.real(r);
        // IQ island, 2026-10-14 (the options framework): under divide_by_zero_error Off a zero divisor gives NULL
        BigDecimal literal = numberLiteral(b.right());
        boolean nullOnZero = b.op() == SqlAst.BinaryOp.DIVIDE && !settings.divideByZeroError()
                && (literal == null || literal.signum() == 0) && zeroGuarded(b, operator);
        if (nullOnZero && literal != null) {
            return doubleNull();
        }
        boolean iq = engine == Engine.IQ;
        SqlAst.BinaryOp op = b.op();
        // IQ island, 2026-10-14 (review 3's m4): a product or quotient that underflows to 0 is 0 in SQL Anywhere and
        // DuckDB, and Postgres's error; unless a constant operand rules it out, it is computed so that no step
        // underflows ({@link #unflowedProduct}, {@link #unflowedQuotient})
        BigDecimal lc = numberLiteral(b.left());
        BigDecimal rc = numberLiteral(b.right());
        BigDecimal one = BigDecimal.ONE;
        boolean guarded = op == SqlAst.BinaryOp.TIMES ? !(lc != null && lc.abs().compareTo(one) >= 0
                || rc != null && rc.abs().compareTo(one) >= 0 || lc != null && lc.signum() == 0
                || rc != null && rc.signum() == 0)
                : op == SqlAst.BinaryOp.DIVIDE && !(rc != null && rc.abs().compareTo(one) <= 0
                || lc != null && lc.signum() == 0);
        if (guarded && !readsRows(b)) {
            // Postgres folds an expression of no column, every branch of the guard with it
            throw SqlTranslationException.unsupported("this " + operator + " of DOUBLEs that read no column may"
                    + " underflow to 0, which IQ answers and Postgres raises as an error (it folds the expression"
                    + " whole): not supported", b.pos());
        }
        // a guard is computed only where both operands read a column: Postgres folds an operand of no column in every
        // branch of it, where a scaled constant could overflow; with one such operand the product is as before (an
        // underflow there is recorded, not guarded)
        boolean guard = guarded && readsRows(b.left()) && readsRows(b.right());
        // IQ island, 2026-10-15 (the census's scale suite): under IQ's engine a value beyond the largest DOUBLE is NULL,
        // tested once where a chain of + - * ends (an infinity or a NaN within it stays one to the end, where abs(v)
        // exceeds the largest on DuckDB and Postgres alike); its operands' NULL guards merge where they cannot raise,
        // so a sum of DOUBLE measures stays linear in size
        boolean pending = !reals && iq && chained.contains(b) && op != SqlAst.BinaryOp.DIVIDE;
        boolean merge = iq && op != SqlAst.BinaryOp.DIVIDE && cannotRaise(b.left()) && cannotRaise(b.right());
        Tx result = SqlToPure.strict(Kind.FLOAT, List.of(l, r), merge || List.of(l, r).stream().filter(Tx::nullable)
                .count() == 1, v -> {
            ValueSpecification x = lf ? v.get(0) : SqlToPure.call("toFloat", v.get(0));
            ValueSpecification y = rf ? v.get(1) : SqlToPure.call("toFloat", v.get(1));
            ValueSpecification value = switch (op) {
                case PLUS -> AppliedFunction.infixRun("plus", List.of(x, y));
                case MINUS -> AppliedFunction.infixRun("minus", List.of(x, y));
                case TIMES -> guard ? unflowedProduct(x, y) : AppliedFunction.infixRun("times", List.of(x, y));
                default -> guard ? unflowedQuotient(x, y) : SqlToPure.call("divide", x, y);
            };
            if (reals) {
                value = IqFunctions.single(value);
            }
            ValueSpecification finite = pending ? value : ifThen(SqlToPure.call("greaterThan", SqlToPure.call("abs",
                    value), new com.legend.protocol.spec.CFloat(DOUBLE_MAX)), iq ? SqlToPure.nullOf(Kind.FLOAT).vs()
                    : floatRaise(), value);
            return op == SqlAst.BinaryOp.DIVIDE ? ifThen(SqlToPure.call("equal", y,
                    new com.legend.protocol.spec.CFloat(0.0)), nullOnZero ? SqlToPure.nullOf(Kind.FLOAT).vs()
                    : floatRaise(), finite) : finite;
        });
        if (pending) {
            // the body never empty where the guard is false: carried apart for the next operation to merge
            return new Tx(result.vs(), Kind.FLOAT, result.nullable(), null, new IqType("DOUBLE"), result.guard(),
                    result.body());
        }
        return Tx.value(result.vs(), Kind.FLOAT, result.nullable() || nullOnZero || iq, new IqType(reals ? "REAL"
                : "DOUBLE"));
    }

    private static ValueSpecification fl(double d) {
        return new com.legend.protocol.spec.CFloat(d);
    }

    private static ValueSpecification times2(ValueSpecification a, ValueSpecification b) {
        return AppliedFunction.infixRun("times", List.of(a, b));
    }

    /**
     * IQ island, 2026-10-14 (review 3's m4): {@code x * y} of two doubles, no step of it underflowing to 0 (Postgres
     * raises there, where SQL Anywhere and DuckDB answer 0): where either is 0, or at least 1 in magnitude, or both at
     * least 2^-537, the product is no underflow; else the smaller (below 2^-537) is scaled by 2^1074 (exactly) and
     * multiplied by the other: p = x * y * 2^1074 is no underflow, and x * y is 0 where |p| <= 1/2 (it rounds to 0),
     * p * 2^-537 * 2^-537 otherwise.
     */
    static ValueSpecification unflowedProduct(ValueSpecification x, ValueSpecification y) {
        ValueSpecification ax = SqlToPure.call("abs", x);
        ValueSpecification ay = SqlToPure.call("abs", y);
        double up = Math.scalb(1.0, 537);
        double down = Math.scalb(1.0, -537);
        ValueSpecification safe = SqlToPure.call("or", SqlToPure.call("or", SqlToPure.call("equal", x, fl(0.0)),
                SqlToPure.call("equal", y, fl(0.0))), SqlToPure.call("or", SqlToPure.call("or",
                SqlToPure.call("greaterThanEqual", ax, fl(1.0)), SqlToPure.call("greaterThanEqual", ay, fl(1.0))),
                SqlToPure.call("and", SqlToPure.call("greaterThanEqual", ax, fl(down)),
                        SqlToPure.call("greaterThanEqual", ay, fl(down)))));
        // the smaller of the two (below 2^-537) scaled by 2^1074, then times the other: x * y * 2^1074, no step past
        // the largest double or to 0 (the lowering writes a product of products as one chain, left to right)
        ValueSpecification smallFirst = times2(times2(times2(x, fl(up)), fl(up)), y);
        ValueSpecification otherFirst = times2(times2(times2(y, fl(up)), fl(up)), x);
        ValueSpecification p = ifThen(SqlToPure.call("lessThan", ax, ay), smallFirst, otherFirst);
        ValueSpecification tiny = ifThen(SqlToPure.call("lessThanEqual", SqlToPure.call("abs", p), fl(0.5)), fl(0.0),
                times2(times2(p, fl(down)), fl(down)));
        return ifThen(safe, times2(x, y), tiny);
    }

    /**
     * IQ island, 2026-10-14 (review 3's m4): {@code x / y} of two doubles (y not 0), no step of it underflowing to 0:
     * where |y| <= 1, or |x| >= |y| * 2^-1022, the quotient is no underflow; else x is scaled by 2^537 (exactly), the
     * quotient is 0 where |x * 2^537| <= |y| * 2^-538 (it rounds to 0), (x * 2^537) / y * 2^-537 otherwise.
     */
    static ValueSpecification unflowedQuotient(ValueSpecification x, ValueSpecification y) {
        ValueSpecification ax = SqlToPure.call("abs", x);
        ValueSpecification ay = SqlToPure.call("abs", y);
        ValueSpecification xs = times2(x, fl(Math.scalb(1.0, 537)));
        ValueSpecification tiny = ifThen(SqlToPure.call("lessThanEqual", SqlToPure.call("abs", xs), times2(ay,
                fl(Math.scalb(1.0, -538)))), fl(0.0), times2(SqlToPure.call("divide", xs, y), fl(Math.scalb(1.0,
                -537))));
        return ifThen(SqlToPure.call("lessThanEqual", ay, fl(1.0)), SqlToPure.call("divide", x, y),
                ifThen(SqlToPure.call("greaterThanEqual", ax, times2(ay, fl(Math.scalb(1.0, -1022)))),
                        SqlToPure.call("divide", x, y), tiny));
    }

    /** A Float that raises when computed: an empty value read as one. */
    static ValueSpecification floatRaise() {
        return SqlToPure.call("toOne", SqlToPure.call("cast", SqlToPure.empty(), SqlToPure.typeArg("Float")));
    }

    /** Checks an operand of DOUBLE arithmetic: whether it is a DOUBLE already (else an integer or, under SQL
     *  Anywhere's front end, a NUMERIC, which is converted); anything else is refused by name. */
    private boolean doubleOperand(Tx t, SqlAst.Expr e, String operator) {
        if (t.kind() == Kind.FLOAT) {
            if (t.detail() instanceof IqType type && !type.family().equals("DOUBLE") && !type.family().equals("REAL")) {
                throw SqlTranslationException.unsupported(operator + " of a " + type.spelling() + " is not supported"
                        + " yet", e.pos());
            }
            return true;
        }
        if (decimal(t)) {
            // a NUMERIC with a DOUBLE is a DOUBLE, under IQ's engine as in SQL Anywhere's front end (IQ island,
            // 2026-10-15, measured on IQ 17.1: dbl + nm is a DOUBLE)
            return false;
        }
        if (t.kind() == Kind.INTEGER && t.detail() instanceof IqType type && (type.isInteger()
                || engine == Engine.IQ && type.spelling().equals("UNSIGNED BIGINT"))) {
            // IQ island, 2026-10-15 (the corpus lane; measured on IQ 17.1): a COUNT's integer too (CAST(SUM(id) AS
            // DOUBLE) / COUNT(*) is the DOUBLE 4.0)
            return false;
        }
        throw SqlTranslationException.unsupported(operator + " of a DOUBLE and " + kindName(t.kind()) + " is not"
                + " supported yet" + (t.kind() == Kind.STRING ? " (IQ converts the string to NUMERIC)" : ""), e.pos());
    }

    /** {@code l op r} for {@code + - * / %} of two integers, IQ's way ({@link IqTypes}): the result's type, a check
     *  where the result may leave it, each operand widened to 64 bits first unless it is a constant or already wide; a
     *  NULL constant gives a NULL of the type IQ gives the other operand with itself. */
    private Tx integerArithmetic(SqlAst.Binary b, Tx l, Tx r) {
        String operator = switch (b.op()) {
            case PLUS -> "+";
            case MINUS -> "-";
            case TIMES -> "*";
            case DIVIDE -> "/";
            case MODULO -> "%";
            default -> throw new IllegalArgumentException("not integer arithmetic: " + b.op());
        };
        boolean leftNull = l.kind() == Kind.NULL;
        boolean rightNull = r.kind() == Kind.NULL;
        if (leftNull && rightNull) {
            return typedNull(b.op() == SqlAst.BinaryOp.TIMES ? "INTEGER" : "SMALLINT");
        }
        if (leftNull || rightNull) {
            IqType other = integerOperand(leftNull ? r : l, leftNull ? b.right() : b.left(), operator);
            String family = IqTypes.arithmetic(b.op(), other.family(), other.family());
            if (family == null) {
                throw resultRefused(operator, other.family(), other.family(), b.pos());
            }
            return typedNull(family);
        }
        IqType lt = integerOperand(l, b.left(), operator);
        IqType rt = integerOperand(r, b.right(), operator);
        String family = IqTypes.arithmetic(b.op(), lt.family(), rt.family());
        if (family == null && engine == Engine.IQ && b.op() == SqlAst.BinaryOp.MODULO && lt.isInteger()
                && rt.isInteger() && !lt.family().equals("BIT") && !rt.family().equals("BIT")) {
            // IQ island, 2026-10-15 (D-N3, measured on IQ 17.1): a BIGINT's remainder under IQ's engine is a
            // scale-0 NUMERIC of the remainder's value (bg % 7 is NUMERIC(20,0) 0), read as the integer it is
            family = "BIGINT";
        }
        if (family == null) {
            throw resultRefused(operator, lt.family(), rt.family(), b.pos());
        }
        BigInteger[] range = IqTypes.range(b.op(), lt, rt);
        SqlAst.BinaryOp op = b.op();
        // a divisor that may be zero raises IQ's -628 (the lowering's guarded error)
        boolean zeroDivisor = (op == SqlAst.BinaryOp.DIVIDE || op == SqlAst.BinaryOp.MODULO)
                && rt.lo().signum() <= 0 && rt.hi().signum() >= 0;
        // IQ island, 2026-10-14 (the options framework): under divide_by_zero_error Off a zero divisor gives NULL
        boolean nullOnZero = zeroDivisor && !settings.divideByZeroError() && zeroGuarded(b, operator);
        if (nullOnZero && numberLiteral(b.right()) != null) {
            return typedNull(family);
        }
        return checkedInteger(b, operator, new IqType(family), range[0], range[1], List.of(l, r),
                List.of(lt, rt), constants(List.of(b.left(), b.right()), List.of(l, r)), zeroDivisor && !nullOnZero,
                nullOnZero ? 1 : -1, v -> switch (op) {
                    case PLUS -> AppliedFunction.infixRun("plus", v);
                    case MINUS -> AppliedFunction.infixRun("minus", v);
                    case TIMES -> AppliedFunction.infixRun("times", v);
                    case DIVIDE -> SqlToPure.wholeQuotient(v.get(0), v.get(1));
                    // Pure's rem is a Number: the integer it is, as one
                    default -> SqlToPure.call("cast", SqlToPure.call("rem", v.get(0), v.get(1)),
                            SqlToPure.typeArg("Integer"));
                });
    }

    /**
     * An integer operation's translation: strict in its operands (each widened to 64 bits unless it is a constant or
     * already wide), checked against {@code type}'s range unless {@code lo..hi} (the range its operands allow) lies in
     * it. A BIGINT result is not checked: the database computes it in 64 bits and raises its own overflow first --
     * but over a value it may compute wider ({@link IqType#wider}, IQ island, 2026-10-14, review 5's B1).
     * <ul>
     *   <li>A result that can only leave the range is IQ's error now when every operand is a constant
     *       ({@code constant}: literals, signs and arithmetic of literals, never NULL), as IQ folds one; otherwise
     *       the check is emitted and raises only where the operation is computed (never on a NULL): under a row
     *       limit, for the rows IQ computes it for ({@link #rowLimitItems}).</li>
     * </ul>
     */
    private Tx checkedInteger(SqlAst.Expr node, String operator, IqType type, BigInteger lo, BigInteger hi,
            List<Tx> operands, List<IqType> types, boolean constant, boolean zeroDivisor,
            java.util.function.Function<List<ValueSpecification>, ValueSpecification> build) {
        return checkedInteger(node, operator, type, lo, hi, operands, types, constant, zeroDivisor, -1, build);
    }

    /** {@link #checkedInteger}, NULL where the operand at {@code zeroGuard} (when 0 or more) is 0 (IQ island,
     *  2026-10-14, the options framework: divide_by_zero_error Off). */
    private Tx checkedInteger(SqlAst.Expr node, String operator, IqType declared, BigInteger lo, BigInteger hi,
            List<Tx> operands, List<IqType> types, boolean constant, boolean zeroDivisor, int zeroGuard,
            java.util.function.Function<List<ValueSpecification>, ValueSpecification> build) {
        SqlAst.Pos pos = node.pos();
        IqType widened = null;
        if (engine == Engine.IQ && lo.compareTo(IqType.BIGINT_MIN) >= 0 && hi.compareTo(IqType.BIGINT_MAX) <= 0) {
            // IQ island, 2026-10-15 (the research's D-N3, measured on IQ 17.1): IQ's engine widens -- no integer
            // operation raises -158; its result is of the narrowest type that holds it (IQ reads its operands' ranges
            // from their data -- id + 1 over ids 1 to 7 is a SMALLINT, sm + 1 an INTEGER, it + 1 a BIGINT --, lite from
            // their types: the same integers, of a type no narrower). Past a BIGINT, IQ answers a NUMERIC: there lite
            // keeps its BIGINT, which the database's own overflow guards.
            widened = new IqType(lo.compareTo(IqType.SMALLINT_MIN) >= 0 && hi.compareTo(IqType.SMALLINT_MAX) <= 0
                    ? "SMALLINT" : lo.compareTo(IqType.INTEGER_MIN) >= 0 && hi.compareTo(IqType.INTEGER_MAX) <= 0
                    ? "INTEGER" : "BIGINT");
        }
        IqType type = widened != null ? widened : declared;
        boolean certainlyOut = lo.compareTo(type.max()) > 0 || hi.compareTo(type.min()) < 0;
        if (certainlyOut && constant) {
            throw SqlTranslationException.invalid("the value of this " + operator + " is out of range for its type, "
                    + type.family() + overflowCode(), pos);
        }
        boolean leaves = lo.compareTo(type.min()) < 0 || hi.compareTo(type.max()) > 0;
        // IQ island, 2026-10-14 (review 5's B1): a BIGINT result over a value the database may compute wider than 64
        // bits (a host variable's, a constant near 2^63) is checked here, the database's own overflow never coming
        boolean loose = false;
        for (IqType t : types) {
            loose |= t.wider();
        }
        boolean check = leaves && (!type.family().equals("BIGINT") || loose);
        boolean wide = false;
        List<Boolean> widen = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            IqType t = types.get(i);
            // a constant (one value, never NULL) is written as itself; anything else is widened unless it is wide
            boolean w = !t.wide() && !(t.exact() && !operands.get(i).nullable());
            widen.add(w);
            wide |= w || t.wide();
        }
        if (!wide && widened != null && (lo.compareTo(IqType.INTEGER_MIN) < 0
                || hi.compareTo(IqType.INTEGER_MAX) > 0)) {
            // IQ island, 2026-10-17 (the differential fuzzer; measured on IQ 17.1): under IQ's engine 2147483647 * 2
            // is the BIGINT 4294967294; of constants alone (the database types each by itself, in 32 bits) one is
            // widened so that the database computes in 64
            widen.set(0, true);
            wide = true;
        }
        // IQ island, 2026-10-15 (D-N3): under IQ's engine an operation that cannot raise merges every operand's NULL
        // guard: none of its operands raises where another is NULL either (an integer operation raises only for a
        // zero divisor, which IQ's engine answers no rows for, its defect), so a chain stays linear in size
        boolean merge = widened != null && !check && !zeroDivisor && zeroGuard < 0;
        Tx result = SqlToPure.strict(Kind.INTEGER, operands, merge || operands.stream().filter(Tx::nullable).count()
                == 1, v -> {
            List<ValueSpecification> args = new ArrayList<>();
            for (int i = 0; i < v.size(); i++) {
                // floor: Pure's integer of a number, the database's 64-bit integer (exact below 2^53)
                args.add(widen.get(i) ? SqlToPure.call("floor", v.get(i)) : v.get(i));
            }
            ValueSpecification core = build.apply(args);
            ValueSpecification checked = check ? inRange(core, type.min(), type.max()) : core;
            // 0 == x, the constant first: Pure's == binds tighter than arithmetic, and the printer writes x - 3 == 0
            // for equal(x - 3, 0), which reads back as x - (3 == 0) (upstream's composer's rule, ported verbatim)
            return zeroGuard < 0 ? checked : ifThen(SqlToPure.call("equal", new CInteger(0L), v.get(zeroGuard)),
                    SqlToPure.nullOf(Kind.INTEGER).vs(), checked);
        });
        BigInteger low = lo.max(type.min());
        BigInteger high = hi.min(type.max());
        if (low.compareTo(high) > 0) {
            // every value it computes raises: NULL is all it answers
            low = type.min();
            high = type.max();
        }
        IqType out = IqType.integer(type.family(), low, high, wide, loose);
        if (zeroGuard < 0) {
            // IQ island, 2026-10-14 (the seventh line's merged guards, PR #41; the scale suite's cliff 2): the strict
            // operation's NULL test is carried apart from its body, so the next strict operation ORs the guards rather
            // than testing this whole value again -- sound, as the body is never empty where the guard is false
            // (inRange raises or answers the value)
            return new Tx(result.vs(), Kind.INTEGER, result.nullable(), null, out, result.guard(), result.body());
        }
        return Tx.value(result.vs(), Kind.INTEGER, true, out);
    }

    /**
     * Whether {@code b}'s zero divisor, under divide_by_zero_error Off, is NULL by a guard the database computes (IQ
     * island, 2026-10-14, the options framework): a literal divisor (a zero one is NULL outright) or one that reads a
     * column. A divisor of constants and variables only is refused by name: a database folds the whole operation of
     * constants before the guard is read (Postgres raises on {@code CASE WHEN 0 = 0 THEN NULL ELSE 1 / 0 END}).
     */
    private static boolean zeroGuarded(SqlAst.Binary b, String operator) {
        if (numberLiteral(b.right()) != null || readsColumn(b.right())) {
            return true;
        }
        throw SqlTranslationException.unsupported("this " + operator + " under divide_by_zero_error Off is not"
                + " supported: its divisor reads no column and is no literal, and a database folds the operation"
                + " before the guard that gives NULL for a zero divisor is read", b.pos());
    }

    /** Whether every one of {@code exprs} is a constant IQ folds (a literal, a sign or arithmetic of literals) whose
     *  translation is never NULL. */
    private static boolean constants(List<SqlAst.Expr> exprs, List<Tx> values) {
        for (int i = 0; i < exprs.size(); i++) {
            if (values.get(i).nullable() || !folded(exprs.get(i))) {
                return false;
            }
        }
        return true;
    }

    /** Whether {@code e} is a number IQ folds: a literal, a sign or arithmetic of literals. */
    private static boolean folded(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.IntegerLit n -> true;
            case SqlAst.NumericLit n -> true;
            case SqlAst.Unary u -> u.op() != SqlAst.UnaryOp.NOT && folded(u.operand());
            case SqlAst.Binary b -> switch (b.op()) {
                case PLUS, MINUS, TIMES, DIVIDE, MODULO -> folded(b.left()) && folded(b.right());
                default -> false;
            };
            default -> false;
        };
    }

    // ------------------------------------------------------------------ the size of a translation

    /** The most nodes one value's Pure may have written out (each repeated operand counted at each use): a
     *  checked operation repeats its operands (its NULL test, its range test, its value), so nested ones grow
     *  fourfold per level and a quotient twentyfold; past this, the translation is refused rather than built. A sum of
     *  six SMALLINT columns (about 17,000 nodes) is about 120 KB of SQL; a quotient's SQL is the densest, about 20
     *  bytes a node. */
    static final long LARGEST = 20_000;

    /** How many nodes of Pure written out per node of the SQL read a translation may hold past {@link #LARGEST}
     *  (IQ island, 2026-10-14, the scale suite's cliff 3): a linear construct (an IN list, a long AND/OR, a CASE of
     *  many WHENs) writes a few nodes a node of SQL however long it is; a checked operation repeating its operands
     *  multiplies per level, which is what the bound is for. */
    static final long RATIO = 50;

    /** {@code t}, {@code node}'s translation, unless its Pure written out exceeds {@link #LARGEST} nodes and
     *  {@link #RATIO} times {@code node}'s own size: refused by name. */
    static Tx bounded(Tx t, SqlAst.Expr node) {
        long size = written(t);
        if (size > LARGEST && size > RATIO * sqlSize(node)) {
            throw tooLarge(node.pos(), size);
        }
        return t;
    }

    /** The nodes of {@code e}'s SQL (its subqueries one node each), counted without recursion. */
    private static long sqlSize(SqlAst.Expr e) {
        long n = 0;
        java.util.ArrayDeque<SqlAst.Expr> todo = new java.util.ArrayDeque<>();
        todo.push(e);
        while (!todo.isEmpty()) {
            SqlAst.Expr x = todo.pop();
            n++;
            for (SqlAst.Expr c : children(x)) {
                todo.push(c);
            }
        }
        return n;
    }

    /** {@code t}'s Pure written out: the larger of its value's and its falsity's. */
    private static long written(Tx t) {
        long size = size(t.vs(), new IdentityHashMap<>());
        if (t.whenFalse() != null) {
            size = Math.max(size, size(t.falseWhen(), new IdentityHashMap<>()));
        }
        return size;
    }

    /** {@code t}, unless its Pure written out exceeds {@link #LARGEST} nodes: refused by name (where no SQL node is at
     *  hand to measure against). */
    static Tx bounded(Tx t, SqlAst.Pos pos) {
        long size = written(t);
        if (size > LARGEST) {
            throw tooLarge(pos, size);
        }
        return t;
    }

    private static SqlTranslationException tooLarge(SqlAst.Pos pos, long size) {
        return SqlTranslationException.pastSizeBound("this expression nests too many operations IQ checks for overflow"
                + " or a zero divisor to be translated: each repeats its operands in lite's SQL, which would hold "
                + size + " nodes, more than " + LARGEST + "; split it into fewer operations (or compute it over BIGINT"
                + " values, which need no check)", pos);
    }

    /** What {@link #reads} finds: no column, a column, a query (a lambda: a subquery's). */
    private static final int READS_NOTHING = 0;
    private static final int READS_COLUMN = 1;
    private static final int READS_QUERY = 2;

    /** Whether {@code vs} reads a column ({@link #READS_COLUMN}), holds a lambda ({@link #READS_QUERY}), or reads
     *  neither ({@link #READS_NOTHING}: constants and variables alone). */
    private static int reads(ValueSpecification vs) {
        List<ValueSpecification> children = switch (vs) {
            case AppliedFunction f -> f.parameters();
            case com.legend.protocol.spec.AppliedProperty p -> {
                yield null;
            }
            case com.legend.protocol.spec.LambdaFunction l -> {
                yield null;
            }
            case com.legend.protocol.spec.PureCollection c -> c.values();
            default -> List.of();
        };
        if (children == null) {
            return vs instanceof com.legend.protocol.spec.LambdaFunction ? READS_QUERY : READS_COLUMN;
        }
        int most = READS_NOTHING;
        for (ValueSpecification c : children) {
            most = Math.max(most, reads(c));
        }
        return most;
    }

    /** The nodes of {@code vs} written out: a node shared by several parents counts once for each. */
    private static long size(ValueSpecification vs, Map<ValueSpecification, Long> memo) {
        Long known = memo.get(vs);
        if (known != null) {
            return known;
        }
        long n = 1;
        List<ValueSpecification> children = switch (vs) {
            case AppliedFunction f -> f.parameters();
            case com.legend.protocol.spec.AppliedProperty p -> List.of(p.receiver());
            case com.legend.protocol.spec.LambdaFunction l -> l.body();
            case com.legend.protocol.spec.PureCollection c -> c.values();
            default -> List.of();
        };
        for (ValueSpecification c : children) {
            n = Math.min(Long.MAX_VALUE / 2, n + size(c, memo));
        }
        memo.put(vs, n);
        return n;
    }

    /** {@code x}, or IQ's error -158 when it lies outside {@code min..max}: an empty value read as one raises, as
     *  {@code SqlToPure}'s narrow integer casts do. */
    private static ValueSpecification inRange(ValueSpecification x, BigInteger min, BigInteger max) {
        return ifThen(SqlToPure.call("or", SqlToPure.call("lessThan", x, SqlToPure.integer(min)),
                SqlToPure.call("greaterThan", x, SqlToPure.integer(max))), raise(), x);
    }

    /** A value that raises when computed: an empty value read as one, typed Integer. */
    static ValueSpecification raise() {
        return SqlToPure.call("toOne", SqlToPure.call("cast", SqlToPure.empty(), SqlToPure.typeArg("Integer")));
    }

    /** {@code if(test, |then, |otherwise)}. */
    static ValueSpecification ifThen(ValueSpecification test, ValueSpecification then,
            ValueSpecification otherwise) {
        return SqlToPure.call("if", test, SqlToPure.thunk(then), SqlToPure.thunk(otherwise));
    }

    /** A NULL of the integer type {@code family}. */
    private static Tx typedNull(String family) {
        return Tx.value(SqlToPure.nullOf(Kind.INTEGER).vs(), Kind.INTEGER, true, new IqType(family));
    }

    /** The IQ integer type of an operand of {@code operator}: anything else is refused by name. */
    private IqType integerOperand(Tx t, SqlAst.Expr e, String operator) {
        if (t.kind() == Kind.INTEGER && t.detail() instanceof IqType type) {
            if (type.isInteger()) {
                return type;
            }
            if (engine == Engine.IQ && type.spelling().equals("UNSIGNED BIGINT")) {
                // IQ island, 2026-10-15 (D-N3, measured on IQ 17.1): a COUNT under IQ's engine, a whole number of
                // rows, widens as any integer does there (COUNT(*) + 1 is the NUMERIC(20,0) 8 over seven rows); one
                // beside a signed integer keeps the range the island knows of it (iqNumericUnify)
                return countRange(type);
            }
            if (type.family().equals("NUMERIC") || type.family().equals("DECIMAL")) {
                throw numericRefused(operator, e.pos());
            }
            throw SqlTranslationException.unsupported(operator + " of a " + type.spelling() + " is not supported yet"
                    + " (IQ's result is an unsigned or NUMERIC type)", e.pos());
        }
        if (t.kind() == Kind.DECIMAL || t.kind() == Kind.NUMBER) {
            throw numericRefused(operator, e.pos());
        }
        if (t.kind() == Kind.FLOAT) {
            throw SqlTranslationException.unsupported(operator + " of a DOUBLE is not supported yet", e.pos());
        }
        if (t.kind() == Kind.INTEGER) {
            throw SqlTranslationException.unsupported(operator + " of an integer whose IQ type is not known is not"
                    + " supported", e.pos());
        }
        throw SqlTranslationException.unsupported(operator + " of " + kindName(t.kind()) + " is not supported yet (IQ"
                + " converts " + (t.kind() == Kind.STRING ? "a string to NUMERIC" : "it by its own rules") + ")",
                e.pos());
    }

    private static SqlTranslationException numericRefused(String operator, SqlAst.Pos pos) {
        return SqlTranslationException.unsupported(operator + " of a NUMERIC is not supported yet: IQ's NUMERIC result"
                + " types are the research's D-N1, not yet verified", pos);
    }

    private static SqlTranslationException resultRefused(String operator, String l, String r, SqlAst.Pos pos) {
        return SqlTranslationException.unsupported(l + " " + operator + " " + r + " is not supported yet: IQ types it"
                + " NUMERIC or unsigned", pos);
    }

    /** A comparison: with a NULL constant UNKNOWN (IS [NOT] NULL for = and != under ansinull Off, but for a CASE's or
     *  an IF's condition in SQL Anywhere's front end), else of two values of one kind. */
    private Tx comparison(SqlAst.BinaryOp op, Tx l, Tx r, boolean leftNull, boolean rightNull,
            SqlAst.@com.legend.base.Nullable Expr at, SqlAst.Pos pos) {
        if (leftNull || rightNull) {
            boolean rewritten = !settings.ansinull() && (op == SqlAst.BinaryOp.EQ || op == SqlAst.BinaryOp.NE)
                    && !(engine == Engine.SA && at != null && caseConditions.contains(at));
            if (rewritten) {
                ValueSpecification v = SqlToPure.valueOf(leftNull ? r : l);
                Tx isNull = Tx.condition(SqlToPure.call("isEmpty", v), SqlToPure.call("isNotEmpty", v), false);
                return op == SqlAst.BinaryOp.EQ ? isNull : SqlToPure.not(isNull);
            }
            return Tx.condition(new CBoolean(false), new CBoolean(false), true);
        }
        return compared(op, l, r, pos);
    }

    /** {@code l op r} for two values that are not NULL constants, by IQ's conversions (the research's TYPING §9,
     *  measured on SQL Anywhere 17): two strings as strings; a string with a number as NUMERIC ({@link #numberOf}); a
     *  string with a date or a timestamp as a timestamp ({@link #dateOf}); an exact number with a DOUBLE as a DOUBLE
     *  (the database's own comparison of the two converts the exact one); numbers, dates, timestamps among their
     *  own. */
    private Tx compared(SqlAst.BinaryOp op, Tx l, Tx r, SqlAst.Pos pos) {
        Kind lk = l.kind();
        Kind rk = r.kind();
        if (lk == Kind.STRING && rk == Kind.STRING) {
            return strings(op, l, r, pos);
        }
        if (lk == Kind.STRING || rk == Kind.STRING) {
            boolean left = lk == Kind.STRING;
            Kind other = left ? rk : lk;
            Tx text = left ? l : r;
            Tx converted = other.numeric() ? numberOf(text, other, left ? r : l, pos)
                    : other == Kind.DATE || other == Kind.DATETIME ? dateOf(text, left ? r : l, pos) : null;
            if (converted == null) {
                throw SqlTranslationException.unsupported("comparing " + kindName(lk) + " with " + kindName(rk)
                        + " is not supported yet", pos);
            }
            return compared(op, left ? converted : l, left ? r : converted, pos);
        }
        if (lk == Kind.DATE && rk == Kind.DATETIME || lk == Kind.DATETIME && rk == Kind.DATE) {
            // IQ island, 2026-10-15 (the corpus lane: ts >= CAST(... AS DATE) in 227 of SQLStorm's and SEDE's
            // queries; measured on IQ 17.1, its front end's and its engine's alike): a DATE compared with a TIMESTAMP
            // is the TIMESTAMP of its midnight (2000-02-29 00:00:00 = DATE 2000-02-29; BETWEEN and IN too)
            return compared(op, lk == Kind.DATE ? atMidnight(l) : l, rk == Kind.DATE ? atMidnight(r) : r, pos);
        }
        boolean comparable = lk.numeric() && rk.numeric() && lk != Kind.NUMBER && rk != Kind.NUMBER
                || lk == Kind.DATE && rk == Kind.DATE || lk == Kind.DATETIME && rk == Kind.DATETIME;
        boolean dated = lk == Kind.DATE || lk == Kind.DATETIME || rk == Kind.DATE || rk == Kind.DATETIME;
        if (!comparable && engine == Engine.IQ && dated && (lk.numeric() || rk.numeric())) {
            // IQ island, 2026-10-15 (the corpus lane; measured on IQ 17.1): a date or a timestamp against a number
            // (ts > 2008, ts >= ts - dt) is IQ's -1001013 under its engine
            throw SqlTranslationException.unsupported("comparing " + kindName(lk) + " with " + kindName(rk) + " is IQ's"
                    + " error -1001013 under its engine (an invalid data type comparison in a predicate), refused"
                    + " before translating", pos);
        }
        if (!comparable) {
            throw SqlTranslationException.unsupported("comparing " + kindName(lk) + " with " + kindName(rk)
                    + " is not supported yet (IQ's conversions between kinds are a later slice's)", pos);
        }
        int[] left = digits(l);
        int[] right = digits(r);
        if (left != null && right != null && Math.max(left[0], right[0]) + Math.max(left[1], right[1]) > 38) {
            boolean rightWide = right[1] > left[1];
            int[] narrow = rightWide ? left : right;
            int[] wide = rightWide ? right : left;
            boolean ordering = op == SqlAst.BinaryOp.LT || op == SqlAst.BinaryOp.LE || op == SqlAst.BinaryOp.GT
                    || op == SqlAst.BinaryOp.GE;
            if (ordering && Math.max(narrow[0], wide[0] + 1) + narrow[1] <= 38 && wide[0] + wide[1] <= 38) {
                // IQ island, 2026-10-17 (the TPC-H corpus lane's 257): an ordering against a value of fewer places is
                // the same against the other at those places, rounded up or down by the side and the operator
                // (x > y is x > floor(y) for an x of s places), which needs no decimal holding both scales
                boolean up = rightWide ? op == SqlAst.BinaryOp.GE || op == SqlAst.BinaryOp.LT
                        : op == SqlAst.BinaryOp.GT || op == SqlAst.BinaryOp.LE;
                Tx w = rightWide ? r : l;
                Tx placed = SqlToPure.strict(Kind.DECIMAL, List.of(w), v -> IqFunctions.placesToward(v.get(0), wide[1],
                        wide[0], narrow[1], up));
                Tx at = Tx.value(placed.vs(), Kind.DECIMAL, placed.nullable(), new IqType("NUMERIC("
                        + (wide[0] + 1 + narrow[1]) + "," + narrow[1] + ")"));
                return SqlToPure.compare(op, rightWide ? l : at, rightWide ? at : r);
            }
            // the database compares the two as one decimal holding both, which lite's 38 digits cannot
            throw SqlTranslationException.unsupported("comparing these two numbers needs a decimal of more than 38"
                    + " digits, which lite does not hold", pos);
        }
        return SqlToPure.compare(op, l, r);
    }

    /** An exact number's digits before and after its point, by its IQ type; null for any other value. */
    private static int @com.legend.base.Nullable [] digits(Tx t) {
        if (!(t.detail() instanceof IqType type)) {
            return null;
        }
        if (type.isInteger()) {
            return new int[] {switch (type.family()) {
                case "BIT" -> 1;
                case "SMALLINT" -> 5;
                case "INTEGER" -> 10;
                case "BIGINT" -> 19;
                default -> throw new IllegalStateException("not an integer type: " + type.family());
            }, 0};
        }
        String f = type.family();
        if ((f.equals("NUMERIC") || f.equals("DECIMAL")) && type.size() >= 0) {
            String sp = type.spelling();
            int comma = sp.indexOf(',');
            int scale = comma < 0 ? 0 : Integer.parseInt(sp.substring(comma + 1, sp.indexOf(')')).strip());
            return new int[] {type.size() - scale, scale};
        }
        return null;
    }

    /** Two strings: = and != ignore trailing blanks (BLANK PADDING ON): equal when one starts with the other and the
     *  two are equal with their spaces taken out (so the longer one's rest is spaces only, exactly); an ordering, and
     *  any comparison in a case-insensitive database, compares the two by IQ's collation ({@link #collated}). */
    private Tx strings(SqlAst.BinaryOp op, Tx l, Tx r, SqlAst.Pos pos) {
        boolean equality = op == SqlAst.BinaryOp.EQ || op == SqlAst.BinaryOp.NE;
        if (!equality || !settings.caseSensitive() || !settings.blankPadding()) {
            // IQ island, 2026-10-14 (natives): orderings and a sqla17 profile's comparisons, by IQ's collation
            String fn = switch (op) {
                case EQ, NE -> "equal";
                case LT -> "lessThan";
                case LE -> "lessThanEqual";
                case GT -> "greaterThan";
                case GE -> "greaterThanEqual";
                default -> throw new IllegalArgumentException("not a comparison: " + op);
            };
            Tx test = SqlToPure.strictTest(List.of(l, r), v -> SqlToPure.call(fn, collated(v.get(0)),
                    collated(v.get(1))));
            return op == SqlAst.BinaryOp.NE ? SqlToPure.not(test) : test;
        }
        Tx equal = SqlToPure.strictTest(List.of(l, r), v -> {
            ValueSpecification prefix = SqlToPure.call("or", SqlToPure.call("startsWith", v.get(0), v.get(1)),
                    SqlToPure.call("startsWith", v.get(1), v.get(0)));
            ValueSpecification spaceless = SqlToPure.call("equal",
                    SqlToPure.call("replace", v.get(0), new CString(" "), new CString("")),
                    SqlToPure.call("replace", v.get(1), new CString(" "), new CString("")));
            return SqlToPure.call("and", prefix, spaceless);
        });
        return op == SqlAst.BinaryOp.EQ ? equal : SqlToPure.not(equal);
    }

    /**
     * The value IQ compares a string {@code s} by in the profile's database (the research's SEMANTICS §3.3, OPTIONS
     * §9; measured on SQL Anywhere 17): in a case-insensitive ({@code sqla17}) database its ASCII letters folded to
     * lower case -- the order is the folded strings' ({@code '_' < 'a'}, {@code 'Z' > '['}), and a letter outside ASCII
     * is not folded ({@code 'é' <> 'É'}); in a blank-padding (IQ) database without its trailing spaces, as IQ orders
     * strings ({@code 'a' = 'a  '}, {@code 'a' < 'a\t'}, {@code 'a  ' < 'a\x01'}). Both are composed of existing
     * natives ({@link IqFunctions#asciiFold}, {@link IqFunctions#trailingSpacesDropped}); the databases then compare
     * the results byte by byte (DuckDB's and the embedded Postgres's, in the C locale, order as IQ's binary
     * collations do: UTF-8's byte order is the code points' and so ISO-8859-1's). For whoever orders or groups
     * strings (ORDER BY, DISTINCT, GROUP BY, MIN and MAX): the key is this value; the value shown is the row's own,
     * and among rows of one key IQ shows the first it meets (measured), the plan's choice.
     */
    ValueSpecification collated(ValueSpecification s) {
        ValueSpecification x = settings.caseSensitive() ? s : IqFunctions.asciiFold(s, false);
        return settings.blankPadding() ? IqFunctions.trailingSpacesDropped(x) : x;
    }

    /**
     * A string compared with a number, as SQL Anywhere's front end reads it: as a NUMERIC of the profile's PRECISION
     * and SCALE (the research's TYPING §9, measured): blanks around it ignored, nothing but blanks 0, else an optional
     * sign, digits with an optional point, an optional exponent. Read here for a constant string only, written with at
     * most the profile's SCALE places and 38 digits, so that its NUMERIC is its own value exactly. Refused by name:
     * a DOUBLE (its NUMERIC is not read yet); a string that is not a constant, or spelled otherwise. IQ island,
     * 2026-10-15 (the corpus lane: 123 queries -- PostTypeId = '1', YEAR(ts) = '2026'; measured on IQ 17.1): under
     * IQ's engine no such comparison is read -- a column or a computed number against a string, a string column
     * against a number, in WHERE, IN, BETWEEN and a CASE's test alike, is its error -1001013 (a string that is no
     * number its front end's -157 first), named before translating.
     */
    private Tx numberOf(Tx text, Kind other, Tx number, SqlAst.Pos pos) {
        if (engine == Engine.IQ) {
            SqlTranslationException e = text.vs() instanceof CString s ? unconvertible(s.value(), number, pos) : null;
            hold(e != null ? e : SqlTranslationException.unsupported("comparing a string with a number is IQ's error -1001013 under"
                    + " its engine (an invalid data type comparison in a predicate), refused before translating", pos));
            return number;
        }
        if (other == Kind.FLOAT) {
            throw SqlTranslationException.unsupported("comparing a string with a DOUBLE is not supported yet: IQ"
                    + " converts both to NUMERIC", pos);
        }
        if (!(text.vs() instanceof CString s) || text.nullable()) {
            throw SqlTranslationException.unsupported("comparing a string that is not a constant with a number is not"
                    + " supported yet: IQ converts the string to NUMERIC", pos);
        }
        BigDecimal v = numericText(s.value());
        if (v == null) {
            throw SqlTranslationException.unsupported("comparing the string '" + s.value() + "' with a number is not"
                    + " supported: IQ reads it as a NUMERIC by rules not read for this spelling yet", pos);
        }
        BigDecimal d = IqTypes.atScale(v);
        int places = d.scale();
        int whole = Math.max(d.precision() - d.scale(), 0);
        if (places > settings.scale() || whole > settings.precision() - settings.scale() || whole + places > 38) {
            throw SqlTranslationException.unsupported("comparing the string '" + s.value() + "' with a number is not"
                    + " supported under profile " + settings.spelling() + ": as its NUMERIC(" + settings.precision()
                    + "," + settings.scale() + ") it would be rounded or out of range, or need more than lite's 38"
                    + " digits", pos);
        }
        // IQ island, 2026-10-14 (the options framework, found by its precision cases; measured on SQL Anywhere 17 at
        // every PRECISION and SCALE): compared with a column of a NUMERIC(p, s), the string is that NUMERIC first --
        // rounded to s places ('0.005' is 0.01 against a NUMERIC(10,2) column) -- where an integer column, and an
        // expression of any type, compares it at PRECISION and SCALE: a string of more places, or more whole digits,
        // than such a column holds is refused by name
        int[] numeric = number.vs() instanceof com.legend.protocol.spec.AppliedProperty
                && number.detail() instanceof IqType type && (type.family().equals("NUMERIC")
                || type.family().equals("DECIMAL")) ? digits(number) : null;
        if (numeric != null && (places > numeric[1] || whole > numeric[0])) {
            throw SqlTranslationException.unsupported("comparing the string '" + s.value() + "' with a NUMERIC("
                    + (numeric[0] + numeric[1]) + "," + numeric[1] + ") column is not supported: IQ reads the string"
                    + " as a value of the column's NUMERIC first, rounded, which is not read here", pos);
        }
        return Tx.value(new CDecimal(d), Kind.DECIMAL, false, IqTypes.ofNumericConstant(d));
    }

    /** IQ island, 2026-10-17 (found by verifying the corpus lane's IQ_ERROR claims: SEDE's n >= 'x', n an INTEGER;
     *  measured on IQ 17.1 over 40 spellings and every number type): under IQ's engine a string constant compared with
     *  a column of an integer type (BIT, SMALLINT, INTEGER, BIGINT) is converted to a number first, so one that is no
     *  number ({@link #iqNumber}) is its -157 -- before any -1001013 of the statement, wherever each stands. Compared
     *  with a NUMERIC or DOUBLE column, or with an expression, it is not converted: -1001013. */
    private static @com.legend.base.Nullable SqlTranslationException unconvertible(String value, Tx number,
            SqlAst.Pos pos) {
        if (!iqNumber(value) && number.vs() instanceof com.legend.protocol.spec.AppliedProperty
                && number.detail() instanceof IqType t && t.isInteger()) {
            return SqlTranslationException.unsupported("comparing the string '" + value + "' with an integer column is"
                    + " IQ's error -157 under its engine (it cannot convert the string to a number), refused before"
                    + " translating" + code(-157, "53018"), pos);
        }
        return null;
    }

    /** Whether IQ's engine converts {@code text} to a number (measured on IQ 17.1): spaces around it, an optional
     *  sign, digits with an optional point (none needed on either side: '', '+', '.' and '-' are numbers), an optional
     *  exponent of {@code e} and digits with an optional sign ('e5' is one, '5e' is not); nothing else (a tab, a sign
     *  apart from its digits, a comma, a second point). */
    static boolean iqNumber(String text) {
        int i = 0;
        int n = text.length();
        while (i < n && text.charAt(i) == ' ') {
            i++;
        }
        while (n > i && text.charAt(n - 1) == ' ') {
            n--;
        }
        if (i < n && (text.charAt(i) == '+' || text.charAt(i) == '-')) {
            i++;
        }
        while (i < n && Character.isDigit(text.charAt(i)) && text.charAt(i) < 128) {
            i++;
        }
        if (i < n && text.charAt(i) == '.') {
            i++;
            while (i < n && Character.isDigit(text.charAt(i)) && text.charAt(i) < 128) {
                i++;
            }
        }
        if (i < n && (text.charAt(i) == 'e' || text.charAt(i) == 'E')) {
            i++;
            if (i < n && (text.charAt(i) == '+' || text.charAt(i) == '-')) {
                i++;
            }
            int digits = i;
            while (i < n && Character.isDigit(text.charAt(i)) && text.charAt(i) < 128) {
                i++;
            }
            if (i == digits) {
                return false;
            }
        }
        return i == n;
    }

    /** IQ island, 2026-10-17: an error IQ raises after any -157 of the statement ({@link #unconvertible}), held until
     *  the statement is translated ({@link #translated}); the first held is raised. */
    void hold(SqlTranslationException e) {
        SqlTranslationException first = held[0];
        // -157 comes before -1001013 wherever each stands (measured on IQ 17.1)
        if (first == null || codeOf(String.valueOf(e.getMessage())) == -157
                && codeOf(String.valueOf(first.getMessage())) != -157) {
            held[0] = e;
        }
    }

    @Override
    public void translated() {
        Recursive r = recursive[0];
        if (r != null && !r.last()) {
            throw r.error();
        }
        SqlTranslationException p = placement[0];
        SqlTranslationException h = held[0];
        if (p != null && h != null && codeOf(java.util.Objects.requireNonNullElse(h.getMessage(), "")) == -157
                && !limitInView(p)) {
            // IQ island, 2026-10-17 (measured on IQ 17.1): a conversion's -157 before a subquery's placement and
            // ARRAY_AGG's -1001030, after a row limit's in a derived table
            throw h;
        }
        if ((p != null || held[0] != null) && !unknownCalls.isEmpty()) {
            // the landing's review (2026-10-17): a call of no function of IQ's is -265 before either
            SqlAst.FunctionCall unknown = unknownCalls.get(0);
            throw IqFunctions.noSuchFunction(unknown.name().get(unknown.name().size() - 1).text(), unknown.pos());
        }
        if (p != null) {
            throw p;
        }
        SqlTranslationException e = held[0];
        if (e != null) {
            throw e;
        }
        if (r != null) {
            throw r.error();
        }
        SqlTranslationException run = executed[0];
        if (run != null) {
            throw run;
        }
    }

    /** The checks of a statement's subqueries' places under IQ's engine (and of GROUP BY's under either): the
     *  placement error one names, held ({@link #placement}), or null; a refusal is thrown. IQ island, 2026-10-17 (the
     *  claim verification: SQLStorm's and SEDE's -143, -149 and -265 where the island named a subquery's placement
     *  error; measured on IQ 17.1): IQ raises those first, so the statement is translated on, and this is raised where
     *  it fails otherwise or once it is translated. */
    private static @com.legend.base.Nullable SqlTranslationException placements(SqlAst.Statement statement,
            Engine chosen, Set<SqlAst.Join> innerHere, Set<SqlAst.FunctionCall> placeheldHere) {
        if (chosen == Engine.IQ) {
            // its refusals are no claims; the calls it reads as NULL strings are known before any claim is held
            unreadUnknownFunctions(statement, placeheldHere);
        }
        SqlTranslationException held = null;
        try {
            groupBySubqueries(statement, chosen);
            if (chosen == Engine.IQ) {
                arrayAggregates(statement, placeheldHere);
                // IQ island, 2026-10-15 (windows): a window function in a condition's subquery (its -1001030), after a
                // subquery in an outer join's ON (-1001047) or in ORDER BY (-1005004), before the other placements
                // (IQ island, 2026-10-17, the TPC-DS corpus lane; measured on IQ 17.1)
                SqlTranslationException window = null;
                try {
                    IqWindows.statement(statement, chosen);
                } catch (SqlTranslationException e) {
                    if (!namesIqsError(e)) {
                        throw e;
                    }
                    window = e;
                }
                if (window == null) {
                    appliedQueries(statement);
                    limitedSubqueries(statement);
                }
                try {
                    subqueryPlaces(statement, innerHere);
                } catch (SqlTranslationException e) {
                    if (window == null || beforeWindows(e)) {
                        throw e;
                    }
                }
                if (window != null) {
                    throw window;
                }
                iqEngineRestrictions(statement);
            }
        } catch (SqlTranslationException e) {
            if (!namesIqsError(e)) {
                throw e;
            }
            held = e;
        }
        return held;
    }

    /**
     * IQ island, 2026-10-17 (the JOB corpus lane's 553; measured on IQ 17.1): ARRAY_AGG under IQ's engine is its
     * -1001030 (an aggregate it does not support), raised after the names (-143), argument counts (-154), grouping
     * (-149) and conversions (-157), and before a subquery's placement (-1005004); not where a column no one reads
     * holds it ({@code placeheld}, {@link #unreadUnknownFunctions}).
     */
    private static void arrayAggregates(SqlAst.Statement statement, Set<SqlAst.FunctionCall> placeheld) {
        for (SqlAst.Statement q : queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.Expr e : readExpressions(s)) {
                    List<SqlAst.FunctionCall> arrays = new ArrayList<>();
                    arrayAggregates(e, arrays);
                    for (SqlAst.FunctionCall a : arrays) {
                        if (!placeheld.contains(a)) {
                            throw SqlTranslationException.invalid("ARRAY_AGG is an aggregate IQ's engine does not"
                                    + " support" + code(-1001030, "QFA2A"), a.pos());
                        }
                    }
                }
            }
        }
    }

    /** Into {@code out}, the ARRAY_AGG calls in {@code e}, outside its subqueries. */
    private static void arrayAggregates(SqlAst.Expr e, List<SqlAst.FunctionCall> out) {
        if (e instanceof SqlAst.FunctionCall f && f.name().size() == 1
                && IqLexer.lower(f.name().get(0).text()).equals("array_agg")) {
            out.add(f);
        }
        for (SqlAst.Expr child : children(e)) {
            arrayAggregates(child, out);
        }
    }

    /** IQ island, 2026-10-17: a call of no function of IQ's in the statement (not one read as a NULL string,
     *  {@link #placeheld}) is its -265 before any later error -- a placement's, a conversion's, a type check's -- as
     *  a placement error held ({@link #placement}) is; both come after a name's (-142, -143, -144), a grouping's
     *  (-149, -150) and an argument count's (-154) error. */
    @Override
    public SqlTranslationException failed(SqlTranslationException e) {
        int code = codeOf(java.util.Objects.requireNonNullElse(e.getMessage(), ""));
        Recursive r = recursive[0];
        if (r != null && !r.last()) {
            // IQ island, 2026-10-17 (measured on IQ 17.1): -706 and -922 before all but these ({@link #recursion})
            return code == 0 || r.before().contains(code) ? e : r.error();
        }
        if (code == -142 || code == -143 || code == -144 || code == -149 || code == -150 || code == -154
                || code == -265 || code == 0 && placement[0] == null) {
            return e;
        }
        SqlAst.FunctionCall unknown = unknownCalls.isEmpty() ? null : unknownCalls.get(0);
        if (unknown != null) {
            return IqFunctions.noSuchFunction(unknown.name().get(unknown.name().size() - 1).text(), unknown.pos());
        }
        SqlTranslationException p = placement[0];
        if (code == -157 && p != null && !limitInView(p)) {
            // IQ island, 2026-10-17 (measured on IQ 17.1): see translated()
            return e;
        }
        return p != null ? p : e;
    }

    /** Whether a held placement error is a row limit's in a subquery or a derived table ({@link #limitedSubqueries}),
     *  which IQ raises before a conversion's -157 (the other placements after it: measured on IQ 17.1). */
    private static boolean limitInView(SqlTranslationException p) {
        return java.util.Objects.requireNonNullElse(p.getMessage(), "").contains("TOP, FIRST or START AT in a"
                + " subquery or a derived table");
    }

    /** The IQ code a message names ("(IQ: SQLCODE -n", "is IQ's error -n"); 0 for none. */
    private static int codeOf(String message) {
        for (String marker : List.of("SQLCODE -", "IQ's error -")) {
            int i = message.indexOf(marker);
            if (i >= 0) {
                int start = i + marker.length() - 1;
                int end = start + 1;
                while (end < message.length() && Character.isDigit(message.charAt(end))) {
                    end++;
                }
                if (end > start + 1) {
                    return Integer.parseInt(message.substring(start, end));
                }
            }
        }
        return 0;
    }

    /** Whether {@code e} names IQ's own error, refused before translating (a claim, not a refusal of the island's). */
    /** Whether a subquery's placement error is one IQ raises before a window function's in a subquery: an outer
     *  join's ON's (-1001047), ORDER BY's (-1005004; measured on IQ 17.1). */
    private static boolean beforeWindows(SqlTranslationException e) {
        String m = java.util.Objects.requireNonNullElse(e.getMessage(), "");
        int code = codeOf(m);
        return code == -1001047 || code == -1005004 && m.contains("ORDER BY");
    }

    private static boolean namesIqsError(SqlTranslationException e) {
        String m = java.util.Objects.requireNonNullElse(e.getMessage(), "");
        return m.contains("is IQ's error -") && m.contains("refused before translating") || m.contains("(IQ: SQLCODE -");
    }

    /** A string's NUMERIC value as SQL Anywhere's front end reads one in a comparison (measured): spaces around it
     *  ignored, nothing but spaces 0, else {@code [+-] digits [. digits] [e [+-] digits]} with digits on at least one
     *  side of the point; null for any other spelling (a tab, a sign apart from its digits, a bare point, ...). */
    static @com.legend.base.Nullable BigDecimal numericText(String text) {
        int start = 0;
        int end = text.length();
        while (start < end && text.charAt(start) == ' ') {
            start++;
        }
        while (end > start && text.charAt(end - 1) == ' ') {
            end--;
        }
        if (start == end) {
            return BigDecimal.ZERO;
        }
        int i = start;
        if (text.charAt(i) == '+' || text.charAt(i) == '-') {
            i++;
        }
        int digitsBefore = 0;
        while (i < end && text.charAt(i) >= '0' && text.charAt(i) <= '9') {
            i++;
            digitsBefore++;
        }
        int digitsAfter = 0;
        if (i < end && text.charAt(i) == '.') {
            i++;
            while (i < end && text.charAt(i) >= '0' && text.charAt(i) <= '9') {
                i++;
                digitsAfter++;
            }
        }
        if (digitsBefore + digitsAfter == 0) {
            return null;
        }
        if (i < end && (text.charAt(i) == 'e' || text.charAt(i) == 'E')) {
            i++;
            if (i < end && (text.charAt(i) == '+' || text.charAt(i) == '-')) {
                i++;
            }
            int exponent = 0;
            while (i < end && text.charAt(i) >= '0' && text.charAt(i) <= '9') {
                i++;
                exponent++;
            }
            if (exponent == 0 || exponent > 4) {
                return null;
            }
        }
        if (i != end) {
            return null;
        }
        String number = text.substring(start, end);
        return new BigDecimal(number.startsWith("+") ? number.substring(1) : number);
    }

    /**
     * A string compared with a date or a timestamp, as IQ reads it: a TIMESTAMP (the research's TYPING §9, measured;
     * a DATE is compared as its midnight). Read here for a constant string in ISO spelling --
     * {@code yyyy-mm-dd}, optionally a space and {@code hh:nn}, {@code :ss}, {@code .ffffff} (at most six places) --
     * which every profile's date order reads alike; with a DATE only at midnight, where the two compare as dates. Any
     * other constant is refused by name (IQ reads many more spellings, by the profile's date order). IQ island,
     * 2026-10-17 (the corpus lane's {@code ts BETWEEN '2000-01-01' AND '2100-12-31' + ' 23:59:59'}; measured on IQ
     * 17.1): under IQ's engine a string computed of constants alone is {@code CAST(s AS TIMESTAMP)} (over 14
     * spellings, the same rows and the same errors), beside a DATE {@code CAST(s AS DATE)} ({@link
     * IqFunctions#compared}); one reading a column is
     * IQ's -1001013 (an invalid data type comparison in a predicate), and one holding a subquery is not measured: both
     * refused by name. In SQL Anywhere's front end a string that is not a constant is refused.
     */
    private Tx dateOf(Tx text, Tx date, SqlAst.Pos pos) {
        if (!(text.vs() instanceof CString s) || text.nullable()) {
            if (engine == Engine.IQ) {
                int reads = reads(text.vs());
                if (reads == READS_COLUMN) {
                    throw SqlTranslationException.unsupported("comparing a date with a string that reads a column is"
                            + " IQ's error -1001013 under its engine (an invalid data type comparison in a"
                            + " predicate), refused before translating", pos);
                }
                if (reads == READS_QUERY) {
                    throw SqlTranslationException.unsupported("comparing a date with a string a subquery computes is"
                            + " not supported yet", pos);
                }
                String folded = literalJoins.get(text.vs());
                if (folded != null) {
                    // IQ island, 2026-10-17 (the corpus lane's 'x' + '-' + 'x' + ... + 'T00:00:00.000', IQ's -1009145
                    // over no rows): IQ folds a string of literals as it plans, so it is read as the literal it is
                    return dateOf(Tx.value(new CString(folded), Kind.STRING, false, IqTypes.ofString(folded)), date,
                            pos);
                }
                return new IqFunctions(this).compared(text, date.kind() == Kind.DATE);
            }
            throw SqlTranslationException.unsupported("comparing a string that is not a constant with a date is not"
                    + " supported yet in SQL Anywhere's front end: it converts the string to TIMESTAMP", pos);
        }
        if (IqDateTime.read(s.value(), settings, pos) == null && IqDateTime.unconvertible(s.value(), settings)) {
            // IQ island, 2026-10-17 (the corpus lane's SEDE, 55 queries; measured on IQ 17.1): numbers ending in a
            // four-digit year ('01/01/2018') are no date under DATE_ORDER YMD -- in a comparison (BETWEEN and IN
            // too), IQ's engine's -157 beside a DATE column, its -1009145 beside a TIMESTAMP or a computed DATE
            // (CAST(ts AS DATE)); SQL Anywhere's front end's -157
            // IQ island, 2026-10-17 (the corpus lane's judge): which code IQ reports is its order's -- another error
            // of the statement may come first (a DATEPART of a string's -1001011), and a BETWEEN over a computed DATE
            // in a grouped query is -157 -- so the error names both, and claims neither as IQ's
            boolean column = date.kind() == Kind.DATE && date.vs() instanceof com.legend.protocol.spec.AppliedProperty;
            String likely = engine == Engine.IQ && !column ? "-1009145" : "-157";
            SqlTranslationException e = SqlTranslationException.invalid("cannot convert '" + s.value() + "' to a "
                    + (date.kind() == Kind.DATE ? "date" : "timestamp") + " under DATE_ORDER YMD: IQ's error where it"
                    + " converts it (" + likely + " here as a rule, -157 or -1009145 by the place it converts it)", pos);
            if (engine == Engine.IQ) {
                // IQ island, 2026-10-17 (the claim verification: SEDE's DATEFROMPARTS, EOMONTH beside one; measured on
                // IQ 17.1): its engine converts as it plans, after it resolves the statement's names and functions
                hold(e);
                return date;
            }
            throw e;
        }
        return dateConstant(s.value(), date.kind(), pos, "comparing");
    }

    /** The date constant string {@code value} is in IQ's spellings (IqDateTime), as a DATE ({@code other} a DATE: of
     *  a midnight only) or a TIMESTAMP; {@code what} heads the refusal of one not read. */
    private Tx dateConstant(String value, Kind other, SqlAst.Pos pos, String what) {
        // IQ island, 2026-10-14 (M1e): IQ's spellings by the profile's DATE_ORDER (IqDateTime)
        String[] parts = IqDateTime.read(value, settings, pos);
        if (parts == null && what.equals("comparing") && engine == Engine.IQ
                && value.chars().noneMatch(Character::isDigit) && !(value.isEmpty() && other == Kind.DATE)) {
            // IQ island, 2026-10-16 (measured on IQ 17.1; the corpus lane: SEDE's 'x' and '' compared with dates): a
            // string of no digit is no date in any of IQ's spellings -- compared with a TIMESTAMP its engine's
            // -1009145, with a DATE -157, whether or not a row reaches the comparison ('' with a DATE is IQ's -1009145
            // and SQL Anywhere's -157: refused by name below)
            String code = other == Kind.DATE ? code(-157, "53018") : code(-1009145, "QFA2E");
            // held as the conversion above is: a later name's or function's error comes first
            hold(SqlTranslationException.invalid("cannot convert '" + value + "' to a " + (other == Kind.DATE ? "date"
                    : "timestamp") + code, pos));
            return Tx.value(SqlToPure.nullOf(other).vs(), other, true);
        }
        if (parts == null) {
            throw SqlTranslationException.unsupported(what + " the string '" + value + "' with a date is not"
                    + " supported: it is no date spelling the island reads yet (IqDateTime)", pos);
        }
        String time = parts[1];
        // IQ island, 2026-10-17 (measured on IQ 17.1): compared with a DATE, a column's or a computed one, IQ's engine
        // reads the string as a DATE, its time dropped (dt = '2026-03-04 12:00' holds for 2026-03-04); SQL Anywhere's
        // front end as a TIMESTAMP, the DATE at its midnight ({@link #compared})
        boolean asDate = time.equals("00:00:00") || what.equals("comparing") && engine == Engine.IQ;
        if (other == Kind.DATE && !asDate && !what.equals("comparing")) {
            throw SqlTranslationException.unsupported(what + " a date with a timestamp of a time other than"
                    + " midnight is not supported yet", pos);
        }
        if (other == Kind.DATE && asDate) {
            // IQ island, 2026-10-14 (the merge of M1e): a constant's year carried as every date constant's
            // (review 4's M2, IqFunctions.dated)
            return Tx.value(new CDate(PureDateLiteral.parse(parts[0]), parts[0], null), Kind.DATE, false,
                    IqFunctions.dated("DATE", Integer.parseInt(parts[0].substring(0, 4))));
        }
        String body = parts[0] + "T" + time;
        return Tx.value(new CDate(PureDateLiteral.parse(body), body, null), Kind.DATETIME, false,
                IqFunctions.dated("TIMESTAMP", Integer.parseInt(parts[0].substring(0, 4))));
    }

    // IQ island, 2026-10-14 (M1e): a date read from a string -- M1a's isoDate and isoTimestamp, with review 3's B3 and
    // review 4's M2 -- is IqDateTime's (IQ's spellings by DATE_ORDER and NEAREST_CENTURY)

    /** The last year whose timestamps keep their hours and minutes in SQL Anywhere 17 (measured, review 4's M2: from
     *  7911-01-01 on they are dropped, by a constant's reading and by DATEADD's arithmetic alike). */
    static final int LAST_TIMED_YEAR = 7910;

    /** IQ island, 2026-10-15 (the corpus lane: DATEADD(YEAR, -1, GETDATE()) in 225 of SQLStorm's queries, refused as
     *  a timestamp that may pass {@link #LAST_TIMED_YEAR}): the latest year a reading of the clock (GETDATE, NOW,
     *  CURRENT TIMESTAMP, TODAY, CURRENT DATE) may be in -- a bound on when this code runs, not a value it computes;
     *  the database reads the clock. (Measured on IQ 17.1, the same day: its front end drops a timestamp's hours and
     *  minutes from 7911-01-01 on, as SQL Anywhere 17's does; its engine keeps them, its client fetch does not.) */
    static final int CLOCK_LATEST_YEAR = 3000;

    // ------------------------------------------------------------------ predicates

    /** {@code x [NOT] BETWEEN low AND high}: {@code x >= low AND x <= high}, each by IQ's comparison (its
     *  conversions, its blank padding), in three-valued logic (a NULL bound leaves the other to decide FALSE). */
    @Override
    public Tx between(SqlAst.Between b, SqlToPure.At at) {
        boolean iq = engine == Engine.IQ;
        boolean lowIsX = IqAggregates.same(b.operand(), b.low()) && constantBound(b.high());
        boolean highIsX = IqAggregates.same(b.operand(), b.high()) && constantBound(b.low());
        if (iq && lowIsX == highIsX && (b.low() instanceof SqlAst.NullLit || b.high() instanceof SqlAst.NullLit)) {
            // IQ island, 2026-10-15 (measured on IQ 17.1): under IQ's engine a NULL bound leaves every row UNKNOWN
            // (sm NOT BETWEEN NULL AND 5 holds for none), where SQL Anywhere's lets the other bound decide FALSE
            return Tx.condition(new CBoolean(false), new CBoolean(false), true);
        }
        Tx x = at.tx(b.operand());
        BigDecimal lowConstant = numberLiteral(b.low());
        BigDecimal highConstant = numberLiteral(b.high());
        if (iq && lowConstant != null && highConstant != null && lowConstant.compareTo(highConstant) > 0
                && (x.kind() == Kind.INTEGER || x.kind() == Kind.DECIMAL || x.kind() == Kind.FLOAT)) {
            // IQ island, 2026-10-17 (the differential fuzzer; measured on IQ 17.1): under IQ's engine an empty range of
            // constants is FALSE for a number, whatever it is -- a NULL too (NOT (sm BETWEEN 5 AND 1) holds for a NULL)
            Tx empty = Tx.condition(new CBoolean(false), new CBoolean(true), false);
            return bounded(b.negated() ? SqlToPure.not(empty) : empty, b);
        }
        boolean xNull = b.operand() instanceof SqlAst.NullLit;
        Tx lowValue = at.tx(b.low());
        Tx highValue = at.tx(b.high());
        if (iq && lowIsX != highIsX && !xNull) {
            // IQ island, 2026-10-17 (the differential fuzzer's three rounds; measured on IQ 17.1): under IQ's engine
            // x BETWEEN x AND c (and BETWEEN c AND x) is x IS NOT NULL AND x <= c -- FALSE, not UNKNOWN, for a NULL x
            // (NOT (sm BETWEEN sm AND 1) holds there); beside a column bound it stays three-valued
            Tx other = comparison(lowIsX ? SqlAst.BinaryOp.LE : SqlAst.BinaryOp.GE, x, lowIsX ? highValue : lowValue,
                    false, (lowIsX ? b.high() : b.low()) instanceof SqlAst.NullLit, null, b.pos());
            ValueSpecification present = x.nullable() ? SqlToPure.call("isNotEmpty", SqlToPure.valueOf(x))
                    : new CBoolean(true);
            ValueSpecification absent = x.nullable() ? SqlToPure.call("isEmpty", SqlToPure.valueOf(x))
                    : new CBoolean(false);
            Tx folded = Tx.condition(SqlToPure.call("and", present, other.vs()), SqlToPure.call("or", absent,
                    other.falseWhen()), other.nullable());
            return bounded(b.negated() ? SqlToPure.not(folded) : folded, b);
        }
        Tx low = comparison(SqlAst.BinaryOp.GE, x, lowValue, xNull, b.low() instanceof SqlAst.NullLit, null,
                b.pos());
        Tx high = comparison(SqlAst.BinaryOp.LE, x, highValue, xNull, b.high() instanceof SqlAst.NullLit, null,
                b.pos());
        ValueSpecification fails = SqlToPure.call("or", low.falseWhen(), high.falseWhen());
        if (iq) {
            // ... and so does a bound of no column that is NULL (a host variable's); a column's NULL leaves the other
            // bound to decide FALSE only where that bound reads no column (IQ island, 2026-10-17, the differential
            // fuzzer, measured on IQ 17.1: NOT (id BETWEEN 100 AND sm) and NOT (id BETWEEN w AND 3 + 0) hold where sm
            // or w is NULL; id BETWEEN w AND v, and BETWEEN w AND j - 5, are UNKNOWN there, whatever v or j - 5 is)
            List<SqlAst.Expr> bounds = List.of(b.low(), b.high());
            List<Tx> values = List.of(lowValue, highValue);
            boolean[] reads = new boolean[2];
            for (int i = 0; i < 2; i++) {
                List<SqlAst.ColumnRef> refs = new ArrayList<>();
                columnRefs(bounds.get(i), refs);
                reads[i] = !refs.isEmpty();
            }
            for (int i = 0; i < 2; i++) {
                if (values.get(i).nullable() && (!reads[i] || reads[1 - i])) {
                    fails = SqlToPure.call("and", fails, SqlToPure.call("isNotEmpty", SqlToPure.valueOf(values.get(i))));
                }
            }
        }
        Tx test = Tx.condition(SqlToPure.call("and", low.vs(), high.vs()), fails, low.nullable() || high.nullable());
        return bounded(b.negated() ? SqlToPure.not(test) : test, b);
    }

    /** A BETWEEN bound IQ folds beside the operand ({@link #between}): a constant (a number, a string, NULL, or a
     *  CAST of a string). */
    private static boolean constantBound(SqlAst.Expr e) {
        return numberLiteral(e) != null || e instanceof SqlAst.StringLit || e instanceof SqlAst.NullLit
                || e instanceof SqlAst.Cast c && c.operand() instanceof SqlAst.StringLit;
    }

    /** {@code x [NOT] IN (v, ...)}: {@code x = v} for each, by IQ's comparison, in three-valued OR -- TRUE where one
     *  holds, FALSE where every one fails, else UNKNOWN; a NULL in the list (or a NULL x) is UNKNOWN, never IS NULL,
     *  whatever ansinull says (measured). */
    @Override
    public Tx inList(SqlAst.InList in, SqlToPure.At at) {
        if (in.operand() instanceof SqlAst.NullLit) {
            return Tx.condition(new CBoolean(false), new CBoolean(false), true);
        }
        if (engine == Engine.IQ && !settings.ansinull() && filters.contains(in)
                && in.values().stream().anyMatch(v -> v instanceof SqlAst.NullLit)) {
            // IQ island, 2026-10-15 (measured on IQ 17.1): under IQ's engine with ansinull Off (a TDS login) a NULL in
            // a search condition's list matches a NULL value, two-valued: sm IN (1, NULL) is sm = 1 OR sm IS NULL, NOT
            // IN its negation; in an IF's condition it is UNKNOWN (measured), as the general reading has it
            Tx x = at.tx(in.operand());
            List<ValueSpecification> holds = new ArrayList<>();
            holds.add(x.nullable() ? SqlToPure.call("isEmpty", SqlToPure.valueOf(x)) : new CBoolean(false));
            for (SqlAst.Expr v : in.values()) {
                if (!(v instanceof SqlAst.NullLit)) {
                    holds.add(compared(SqlAst.BinaryOp.EQ, x, at.tx(v), v.pos()).vs());
                }
            }
            ValueSpecification found = balanced("or", holds);
            Tx test = Tx.condition(found, SqlToPure.call("not", found), false);
            return bounded(in.negated() ? SqlToPure.not(test) : test, in);
        }
        Tx fast = literalList(in, at);
        if (fast != null) {
            return in.negated() ? SqlToPure.not(fast) : fast;
        }
        Tx x = at.tx(in.operand());
        List<ValueSpecification> holds = new ArrayList<>();
        List<ValueSpecification> fails = new ArrayList<>();
        boolean nullable = false;
        for (SqlAst.Expr v : in.values()) {
            if (v instanceof SqlAst.NullLit) {
                nullable = true;
                fails.add(new CBoolean(false));
                continue;
            }
            Tx eq = compared(SqlAst.BinaryOp.EQ, x, at.tx(v), v.pos());
            holds.add(eq.vs());
            fails.add(eq.falseWhen());
            nullable |= eq.nullable();
        }
        if (holds.isEmpty()) {
            return Tx.condition(new CBoolean(false), new CBoolean(false), true);
        }
        Tx test = Tx.condition(balanced("or", holds), balanced("and", fails), nullable);
        return bounded(in.negated() ? SqlToPure.not(test) : test, in);
    }

    /**
     * {@code x IN (v, ...)} over integer constants (an integer {@code x}) or string constants (a string {@code x}), with
     * at most NULLs besides: Pure's {@code in}, one node a value and SQL's own {@code IN (...)} (IQ island, 2026-10-14,
     * the scale suite's cliff 3: the general reading writes about ten nodes a value and met the node cap at 2,500).
     * Its meaning is the general reading's: {@link SqlToPure#strictTest} -- UNKNOWN for a NULL {@code x} -- of IQ's
     * equality, a string compared by its {@link #collated} key (a constant with no trailing blank in a case-sensitive
     * database is its own key, written as it is); a NULL in the list: TRUE where found, else UNKNOWN, never FALSE.
     * Null when the list is not of that form (the general reading).
     */
    private @com.legend.base.Nullable Tx literalList(SqlAst.InList in, SqlToPure.At at) {
        boolean ints = true;
        boolean strings = true;
        boolean nullInList = false;
        int constants = 0;
        for (SqlAst.Expr v : in.values()) {
            if (v instanceof SqlAst.NullLit) {
                nullInList = true;
                continue;
            }
            constants++;
            ints &= v instanceof SqlAst.IntegerLit;
            strings &= v instanceof SqlAst.StringLit;
        }
        if (constants < 2 || !ints && !strings) {
            return null;
        }
        Tx x = at.tx(in.operand());
        if (ints ? x.kind() != Kind.INTEGER : x.kind() != Kind.STRING) {
            return null;
        }
        List<Tx> operands = new ArrayList<>();
        operands.add(x);
        List<SqlAst.Expr> values = new ArrayList<>();
        for (SqlAst.Expr v : in.values()) {
            if (!(v instanceof SqlAst.NullLit)) {
                Tx c = at.tx(v);
                if (c.nullable() || c.kind() != x.kind()) {
                    return null;
                }
                operands.add(c);
                values.add(v);
            }
        }
        boolean integers = ints;
        Tx test = SqlToPure.strictTest(operands, v -> {
            if (integers) {
                return SqlToPure.call("in", v.get(0), new PureCollection(v.subList(1, v.size())));
            }
            List<ValueSpecification> keys = new ArrayList<>();
            for (int i = 1; i < v.size(); i++) {
                boolean own = settings.caseSensitive() && !((SqlAst.StringLit) values.get(i - 1)).value().endsWith(" ");
                keys.add(own ? v.get(i) : collated(v.get(i)));
            }
            return SqlToPure.call("in", collated(v.get(0)), new PureCollection(keys));
        });
        return nullInList ? Tx.condition(test.vs(), new CBoolean(false), true) : test;
    }

    /** {@code fn} (and, or) of {@code terms} as a balanced tree: no deeper than the logarithm of their count. */
    private static ValueSpecification balanced(String fn, List<ValueSpecification> terms) {
        if (terms.size() == 1) {
            return terms.get(0);
        }
        int half = terms.size() / 2;
        return SqlToPure.call(fn, balanced(fn, terms.subList(0, half)), balanced(fn, terms.subList(half,
                terms.size())));
    }

    /** {@code x IS [NOT] DISTINCT FROM y}: never UNKNOWN; two NULLs are not distinct, a NULL and a value are, two
     *  values are as IQ's {@code =} says (its conversions, its blank padding: measured). */
    @Override
    public Tx isDistinct(SqlAst.IsDistinct d, SqlToPure.At at) {
        boolean leftNull = d.left() instanceof SqlAst.NullLit;
        boolean rightNull = d.right() instanceof SqlAst.NullLit;
        ValueSpecification same;
        if (leftNull && rightNull) {
            same = new CBoolean(true);
        } else if (leftNull || rightNull) {
            same = SqlToPure.call("isEmpty", SqlToPure.valueOf(at.tx(leftNull ? d.right() : d.left())));
        } else {
            Tx l = at.tx(d.left());
            Tx r = at.tx(d.right());
            Tx eq = compared(SqlAst.BinaryOp.EQ, l, r, d.pos());
            same = l.nullable() || r.nullable() ? SqlToPure.call("or", SqlToPure.call("and",
                    SqlToPure.call("isEmpty", SqlToPure.valueOf(l)), SqlToPure.call("isEmpty",
                            SqlToPure.valueOf(r))), eq.vs()) : eq.vs();
        }
        Tx notDistinct = Tx.condition(same, SqlToPure.call("not", same), false);
        return bounded(d.negated() ? notDistinct : SqlToPure.not(notDistinct), d);
    }

    /**
     * {@code x [NOT] LIKE pattern [ESCAPE c]} without IQ's {@code [set]} classes: {@code %} any run, {@code _} one
     * character (a line feed too), every other character itself, case-sensitively (an IQ database's CASE RESPECT), and
     * trailing blanks significant (measured: LIKE does not ignore them, as {@code =} does). There is no default escape;
     * {@code ESCAPE c} makes {@code c%}, {@code c_}, {@code c[} and {@code cc} literal. A string or an integer (its
     * digits) is matched; a NULL operand, pattern or escape is UNKNOWN. A {@code sqla17} profile's database matches
     * without regard to the ASCII letters' case (measured: {@code 'ABC' LIKE 'a%'}, not {@code 'É' LIKE 'é'}): each
     * letter of the pattern a class of its two cases (IQ island, 2026-10-14, natives). Refused by name: under the IQ
     * engine an operand reading a CHAR(n)
     * column (its blank pad matched or not is the research's D-S5, not yet verified); a pattern or an escape that is
     * not a constant; a {@code [} that is not escaped (a {@code ]} alone is itself, measured); an escape that is not
     * one character, or before any other character (IQ's run-time errors -629 and -630).
     */
    @Override
    public Tx like(SqlAst.Like l, SqlToPure.At at) {
        if (engine == Engine.IQ && !settings.ansinull() && filters.contains(l) && l.pattern() instanceof SqlAst.NullLit
                && l.escape() == null) {
            // IQ island, 2026-10-15 (measured on IQ 17.1): under IQ's engine with ansinull Off LIKE NULL is FALSE for
            // every row, a NULL value's too (NOT LIKE NULL holds for all)
            Tx no = Tx.condition(new CBoolean(false), new CBoolean(true), false);
            return l.negated() ? SqlToPure.not(no) : no;
        }
        Tx unknown = Tx.condition(new CBoolean(false), new CBoolean(false), true);
        if (l.operand() instanceof SqlAst.NullLit || l.pattern() instanceof SqlAst.NullLit
                || l.escape() instanceof SqlAst.NullLit) {
            return unknown;
        }
        Tx operand = at.tx(l.operand());
        if (engine == Engine.IQ && operand.kind() != Kind.STRING) {
            // IQ island, 2026-10-15 (measured on IQ 17.1): under IQ's engine LIKE over a number or a date is
            // "Invalid data type comparison in predicate"
            if ((operand.kind() == Kind.DATE || operand.kind() == Kind.DATETIME) && l.pattern() instanceof SqlAst
                    .StringLit p && IqDateTime.read(p.value(), settings, p.pos()) == null && !likeDate(p.value())) {
                // IQ island, 2026-10-17 (the corpus lane's CreationDate LIKE '%2015%'; measured on IQ 17.1): it
                // converts the pattern to the date's type first, so a pattern that is no date is its -1009145
                hold(SqlTranslationException.unsupported("LIKE over " + kindName(operand.kind()) + " of a pattern"
                        + " that is no date is IQ's error -1009145 under its engine (the pattern converted to the"
                        + " date's type), refused before translating", l.pattern().pos()));
                return unknown;
            }
            SqlTranslationException e = l.pattern() instanceof SqlAst.StringLit p && p.value().indexOf('%') < 0
                    && p.value().indexOf('_') < 0 && p.value().indexOf('[') < 0 ? unconvertible(p.value(), operand,
                    p.pos()) : null;
            // IQ island, 2026-10-17 (measured on IQ 17.1): a pattern without a wildcard is converted as an
            // equality's string is ({@link #unconvertible}: LIKE '2.....' is its -157, LIKE 'x%' its -1001013)
            hold(e != null ? e : SqlTranslationException.unsupported("LIKE over " + kindName(operand.kind()) + " is IQ's error"
                    + " -1001013 under its engine (an invalid comparison), refused before translating",
                    l.operand().pos()));
            return unknown;
        }
        // IQ island, 2026-10-14 (M1e): a date or a timestamp is matched as its text in the profile's format (measured)
        if (operand.kind() == Kind.DATE || operand.kind() == Kind.DATETIME) {
            operand = IqDateTime.text(operand, settings, "LIKE", l.operand().pos());
        }
        // IQ island, 2026-10-15 (the research's D-S5, measured on IQ 17.1): under IQ's engine LIKE matches a CHAR(n)
        // column's value blank padded ('ab   ' LIKE 'ab' is false, LIKE 'ab%' and LIKE 'a____' true), which is the
        // value lite reads there (D-S1)
        boolean integer = operand.kind() == Kind.INTEGER && operand.detail() instanceof IqType t && t.isInteger();
        if (operand.kind() != Kind.STRING && !integer) {
            throw SqlTranslationException.unsupported("LIKE over " + kindName(operand.kind()) + " is not supported"
                    + " yet: IQ matches its text, written by its own format", l.operand().pos());
        }
        if (!(l.pattern() instanceof SqlAst.StringLit pattern)) {
            char escape = l.escape() instanceof SqlAst.StringLit e && e.value().length() == 1 ? e.value().charAt(0)
                    : 0;
            Tx result = bounded(SqlToPure.strictTest(List.of(operand, runtimePattern(l, at)), v -> runtimeLike(
                    integer ? SqlToPure.call("toString", v.get(0)) : v.get(0), v.get(1), escape)), l);
            return l.negated() ? SqlToPure.not(result) : result;
        }
        if (engine == Engine.IQ && unclosedSet(pattern.value(), l.escape())) {
            // IQ island, 2026-10-17 (the corpus lane's LIKE '%[%'; measured on IQ 17.1, over no rows too, NOT LIKE
            // and in a CASE alike): its engine cannot compile a pattern whose [ no ] closes; its front end matches
            // nothing by one
            throw SqlTranslationException.unsupported("a LIKE pattern with a [ no ] closes is IQ's error -1001047 under"
                    + " its engine (it cannot compile the pattern), refused before translating", pattern.pos());
        }
        List<String> parts = likeParts(pattern, l.escape(), true, settings.latin1());
        boolean caseless = !settings.caseSensitive();
        for (int i = 1; caseless && i < parts.size(); i += 2) {
            if (parts.get(i).startsWith("[")) {
                throw SqlTranslationException.unsupported("LIKE's [set] classes in a case-insensitive database are not"
                        + " supported yet", pattern.pos());
            }
        }
        java.util.function.Function<List<ValueSpecification>, ValueSpecification> test = v -> {
            ValueSpecification value = integer ? SqlToPure.call("toString", v.get(0)) : v.get(0);
            return likeTest(value, parts, caseless);
        };
        Tx result = SqlToPure.strictTest(List.of(operand), test);
        return bounded(l.negated() ? SqlToPure.not(result) : result, l);
    }

    /**
     * IQ island, 2026-10-14 (the third landing; measured on SQL Anywhere 17): a LIKE pattern that is not a constant
     * (a column, {@code '%' || :x || '%'}), its translation: a string, matched as a constant one is ({@link #like})
     * where the database's matching is IQ's -- an IQ database's (case-sensitive), without ESCAPE, over no CHAR(n)
     * under the IQ engine (D-S5). Refused by name otherwise.
     */
    private Tx runtimePattern(SqlAst.Like l, SqlToPure.At at) {
        if (l.escape() != null && !(l.escape() instanceof SqlAst.StringLit e && e.value().length() == 1
                && "%_[".indexOf(e.value().charAt(0)) < 0)) {
            throw SqlTranslationException.unsupported("LIKE with a pattern that is not a constant string is not"
                    + " supported yet with an ESCAPE other than a constant of one character that is no wildcard",
                    l.pattern().pos());
        }
        String why = !settings.caseSensitive()
                ? "in a database that matches without regard to case (profile " + settings.spelling() + ": its"
                + " letters' cases are ASCII's only)" : null;
        if (why != null) {
            throw SqlTranslationException.unsupported("LIKE with a pattern that is not a constant string is not"
                    + " supported yet " + why, l.pattern().pos());
        }
        Tx pattern = at.tx(l.pattern());
        if (pattern.kind() != Kind.STRING) {
            throw SqlTranslationException.unsupported("LIKE with a pattern that is not a constant string is not"
                    + " supported yet over " + kindName(pattern.kind()), l.pattern().pos());
        }
        return pattern;
    }

    /** The regular expression's characters a pattern's character is written escaped in. */
    private static final String REGEX_SPECIAL = ".[]{}()*+?^$|";

    /**
     * {@code value LIKE pattern}, {@code pattern} computed by the database: the regular expression it is, built there
     * -- every character itself (a backslash and the regular expression's own escaped), {@code %} any run, {@code _}
     * one character (a line feed too) -- matched whole. A pattern holding a {@code [} (IQ's set class, measured: SQL
     * Anywhere matches {@code [b]} as a set and an unclosed one as UNKNOWN) or longer than {@link #LONGEST_PATTERN}
     * characters (SQL Anywhere's -606 for some values) raises, never answers otherwise than IQ: (IQ island,
     * 2026-10-14, review 7's m3) by name ({@link #LIKE_PATTERN_RAISE}).
     */
    private static ValueSpecification runtimeLike(ValueSpecification value, ValueSpecification pattern,
            char escape) {
        // IQ island, 2026-10-15: with ESCAPE c, each c-pair, read left to right as IQ reads it (cc, c%, c_, c[), is a
        // placeholder first -- a control character below a blank -- and the literal it stands for last; a pattern
        // holding a placeholder already, or a c left over (before any other character: IQ's error -630), raises
        ValueSpecification p = pattern;
        String[] pairs = {"", "%", "_", "["};
        if (escape != 0) {
            for (int i = 0; i < pairs.length; i++) {
                String pair = String.valueOf(escape) + (i == 0 ? String.valueOf(escape) : pairs[i]);
                p = SqlToPure.call("replace", p, new CString(pair), new CString(String.valueOf((char) (1 + i))));
            }
        }
        ValueSpecification left = p;
        p = SqlToPure.call("replace", p, new CString("\\"), new CString("\\\\"));
        for (int i = 0; i < REGEX_SPECIAL.length(); i++) {
            String c = String.valueOf(REGEX_SPECIAL.charAt(i));
            p = SqlToPure.call("replace", p, new CString(c), new CString("\\" + c));
        }
        p = SqlToPure.call("replace", SqlToPure.call("replace", p, new CString("%"), new CString(".*")),
                new CString("_"), new CString("."));
        if (escape != 0) {
            for (int i = 0; i < pairs.length; i++) {
                String literal = i == 0 ? String.valueOf(escape) : pairs[i];
                String regex = REGEX_SPECIAL.indexOf(literal.charAt(0)) >= 0 || literal.equals("\\") ? "\\" + literal
                        : literal;
                p = SqlToPure.call("replace", p, new CString(String.valueOf((char) (1 + i))), new CString(regex));
            }
        }
        ValueSpecification regex = AppliedFunction.infixRun("plus", List.of(new CString("(?s)"), p));
        ValueSpecification refused = SqlToPure.call("or", SqlToPure.call("contains", escape != 0 ? left : pattern,
                new CString("[")), SqlToPure.call("greaterThan", SqlToPure.call("length", pattern),
                        SqlToPure.integer(BigInteger.valueOf(LONGEST_PATTERN))));
        if (escape != 0) {
            for (int i = 0; i < pairs.length; i++) {
                refused = SqlToPure.call("or", refused, SqlToPure.call("contains", pattern, new CString(
                        String.valueOf((char) (1 + i)))));
            }
            refused = SqlToPure.call("or", refused, SqlToPure.call("contains", left, new CString(
                    String.valueOf(escape))));
        }
        return ifThen(refused, SqlToPure.call("fail", new CString(LIKE_PATTERN_RAISE)), SqlToPure.call("matches",
                value, regex));
    }

    /** The error a LIKE pattern the database computes raises where its matching is not lite's
     *  ({@link #runtimeLike}). */
    static final String LIKE_PATTERN_RAISE = IqDialect.REFUSED_AT_RUN_TIME + "a LIKE pattern computed in the query"
            + " that holds a [ (IQ's set class) or is longer than " + IqDialect.LONGEST_PATTERN + " characters is not"
            + " supported: IQ matches the set, and SQL Anywhere raises its error -606 for some values of a longer one";

    /** The longest LIKE pattern SQL Anywhere 17 always matches, in characters (an ISO-8859-1 database's bytes): with
     *  a longer one it raises its error -606 for some values and not others (measured; IQ's own limit depends on the
     *  pattern, up to 4095). */
    static final int LONGEST_PATTERN = 126;

    /** IQ island, 2026-10-17 (the claim verification's SEDE CreationDate LIKE '2019-06%'; measured on IQ 17.1): a
     *  pattern IQ's engine converts to a date before it raises its -1001013 -- its trailing {@code %}s dropped, a year
     *  of four digits and at most a month and a day after it, each of one or two digits behind a {@code -}, {@code /},
     *  {@code .} or {@code _} ('2019%', '2019-06%', '2019_06%', '2019-06-01%'); '%2019%' and '2019-06-%' are no date
     *  there, its -1009145. */
    private static boolean likeDate(String pattern) {
        int end = pattern.length();
        while (end > 0 && pattern.charAt(end - 1) == '%') {
            end--;
        }
        if (end < 4) {
            return false;
        }
        int i = 0;
        for (; i < 4; i++) {
            if (!isAsciiDigit(pattern.charAt(i))) {
                return false;
            }
        }
        for (int part = 0; part < 2 && i < end; part++) {
            char sep = pattern.charAt(i);
            if (sep != '-' && sep != '/' && sep != '.' && sep != '_') {
                return false;
            }
            int start = ++i;
            while (i < end && i - start < 2 && isAsciiDigit(pattern.charAt(i))) {
                i++;
            }
            if (i == start) {
                return false;
            }
        }
        return i == end;
    }

    private static boolean isAsciiDigit(char c) {
        return c >= '0' && c <= '9';
    }

    /** Whether {@code p} holds a {@code [} (not escaped) that no {@code ]} after it closes. */
    private static boolean unclosedSet(String p, SqlAst.@com.legend.base.Nullable Expr esc) {
        char escape = esc instanceof SqlAst.StringLit e && e.value().length() == 1 ? e.value().charAt(0) : 0;
        for (int i = 0; i < p.length(); i++) {
            char c = p.charAt(i);
            if (escape != 0 && c == escape) {
                i++;
            } else if (c == '[') {
                int close = p.indexOf(']', i + 1);
                if (close < 0) {
                    return true;
                }
                i = close;
            }
        }
        return false;
    }

    /** A constant pattern as literal runs between wildcards: run, wildcard, run, ..., run; with {@code escape} (null
     *  for none) a constant of one character, which makes the {@code %}, {@code _}, {@code [} or escape after it
     *  literal. What IQ raises an error for is refused by name, and so is a {@code [set]} class when {@code sets}. */
    static List<String> likeParts(SqlAst.StringLit pattern, SqlAst.@com.legend.base.Nullable Expr esc,
            boolean sets, boolean latin1) {
        char escape = 0;
        if (esc != null) {
            if (!(esc instanceof SqlAst.StringLit e) || e.value().length() != 1) {
                throw SqlTranslationException.unsupported("an ESCAPE other than a constant of one character is not"
                        + " supported (IQ raises its error -629 for one of another length)", esc.pos());
            }
            escape = e.value().charAt(0);
            if (escape == '%' || escape == '_' || escape == '[') {
                // IQ island, 2026-10-14 (review 3's B2, measured): SQL Anywhere keeps a wildcard that is also the
                // escape a wildcard ('%%' ESCAPE '%' matches every string) and reads '[' as a set's start
                throw SqlTranslationException.unsupported("an ESCAPE that is '" + escape + "', a wildcard of LIKE's"
                        + " own, is not supported: IQ reads it as the wildcard, not as the escape", esc.pos());
            }
        }
        String p = pattern.value();
        if (p.length() > LONGEST_PATTERN) {
            throw SqlTranslationException.unsupported("a LIKE pattern of more than " + LONGEST_PATTERN + " characters"
                    + " is not supported (SQL Anywhere raises its error -606 for some values; IQ's limit depends on the"
                    + " pattern)",
                    pattern.pos());
        }
        List<String> parts = new ArrayList<>();
        StringBuilder run = new StringBuilder();
        for (int i = 0; i < p.length(); i++) {
            char c = p.charAt(i);
            if (escape != 0 && c == escape) {
                char next = i + 1 < p.length() ? p.charAt(i + 1) : 0;
                if (next != '%' && next != '_' && next != '[' && next != escape) {
                    throw SqlTranslationException.unsupported("an escape before " + (next == 0 ? "the pattern's end"
                            : "'" + next + "'") + " is not supported (IQ raises its error -630)", pattern.pos());
                }
                run.append(next);
                i++;
            } else if (c == '%' || c == '_') {
                parts.add(run.toString());
                parts.add(String.valueOf(c));
                run.setLength(0);
            } else if (c == '[' && sets) {
                // IQ island, 2026-10-15 (the corpus lane: '%[^0-9]%' and kin in 39 queries; measured on IQ 17.1):
                // a set of one character, a wildcard part of its own, as the regular expression's class it is
                int close = p.indexOf(']', i + 1);
                String set = close < 0 ? null : likeSet(p.substring(i + 1, close), escape, latin1);
                if (set == null) {
                    throw SqlTranslationException.unsupported("this LIKE [set] is not supported yet: an empty set, a"
                            + " range that descends, the escape inside a set, and a character past U+FFFF (or past"
                            + " U+00FF in an ISO-8859-1 database) are not read", pattern.pos());
                }
                parts.add(run.toString());
                parts.add(set);
                run.setLength(0);
                i = close;
            } else {
                run.append(c);
            }
        }
        parts.add(run.toString());
        return parts;
    }

    /**
     * A LIKE set's members ({@code abc}, {@code a-z}, {@code ^0-9}) as a regular expression's class, or null where
     * they are not read. IQ island, 2026-10-17 (the corpus lane: 13 queries; measured on IQ 17.1, its engine's and its
     * front end's alike, CaseSensitive On, UTF8BIN): every character is a member as itself -- {@code %}, {@code _},
     * {@code [}, a backslash and every mark too -- but a {@code ^} first, which negates the set, and {@code a-b}, the
     * characters from a to b by their code points ({@code [あ-ん]}, {@code [é-ÿ]}, {@code [*-<]}); a {@code -} first
     * (after the {@code ^}) or last is itself ({@code [-+]}, {@code [a-z-]}). Not read: an empty set, a range that
     * descends (IQ's is empty), the escape, a character past U+FFFF, and one past U+00FF in an ISO-8859-1 database.
     */
    private static @com.legend.base.Nullable String likeSet(String members, char escape, boolean latin1) {
        int from = members.startsWith("^") ? 1 : 0;
        if (members.length() == from) {
            return null;
        }
        StringBuilder out = new StringBuilder(from == 1 ? "[^" : "[");
        int last = members.length() - 1;
        for (int k = from; k < members.length(); k++) {
            char c = members.charAt(k);
            if (!member(c, escape, latin1)) {
                return null;
            }
            if (k + 2 <= last && members.charAt(k + 1) == '-' && (k > from || c != '-')) {
                char to = members.charAt(k + 2);
                if (!member(to, escape, latin1) || to < c) {
                    return null;
                }
                out.append(classChar(c)).append('-').append(classChar(to));
                k += 2;
            } else {
                out.append(classChar(c));
            }
        }
        return out.append(']').toString();
    }

    /** Whether {@code c} is a LIKE set's member lite reads ({@link #likeSet}). */
    private static boolean member(char c, char escape, boolean latin1) {
        return c != escape && !Character.isSurrogate(c) && c >= ' ' && (!latin1 || c <= 0xFF);
    }

    /** {@code c} as itself in a regular expression's class: the class's own marks escaped. */
    private static String classChar(char c) {
        return "\\]^-[".indexOf(c) >= 0 ? "\\" + c : String.valueOf(c);
    }

    /** A pattern's test, by its shape: no wildcard is equality; a trailing, leading or enclosing {@code %} alone is
     *  startsWith, endsWith or contains; anything else the equivalent regular expression, whose {@code .} matches a
     *  line feed too ({@code (?s)}), as IQ's {@code _} does. {@code caseless}: always the regular expression, each
     *  ASCII letter a class of its two cases. */
    static ValueSpecification likeTest(ValueSpecification value, List<String> parts, boolean caseless) {
        List<String> wild = new ArrayList<>();
        for (int i = 1; i < parts.size(); i += 2) {
            wild.add(parts.get(i));
        }
        boolean onlyPercent = !wild.contains("_") && wild.stream().noneMatch(w -> w.startsWith("["));
        if (caseless) {
            StringBuilder regex = new StringBuilder("(?s)^");
            for (int i = 0; i < parts.size(); i++) {
                String part = parts.get(i);
                regex.append(i % 2 == 1 ? part.equals("%") ? ".*" : "." : IqFunctions.caselessPattern(part));
            }
            return SqlToPure.call("matches", value, new CString(regex.append('$').toString()));
        }
        if (wild.isEmpty()) {
            return SqlToPure.call("equal", value, new CString(parts.get(0)));
        }
        if (onlyPercent && wild.size() == 1 && parts.get(2).isEmpty() && !parts.get(0).isEmpty()) {
            return SqlToPure.call("startsWith", value, new CString(parts.get(0)));
        }
        if (onlyPercent && wild.size() == 1 && parts.get(0).isEmpty() && !parts.get(2).isEmpty()) {
            return SqlToPure.call("endsWith", value, new CString(parts.get(2)));
        }
        if (onlyPercent && wild.size() == 2 && parts.get(0).isEmpty() && parts.get(4).isEmpty()
                && !parts.get(2).isEmpty()) {
            return SqlToPure.call("contains", value, new CString(parts.get(2)));
        }
        StringBuilder regex = new StringBuilder("(?s)^");
        for (int i = 0; i < parts.size(); i++) {
            String part = parts.get(i);
            if (i % 2 == 1) {
                regex.append(part.equals("%") ? ".*" : part.equals("_") ? "." : part);
            } else {
                for (int k = 0; k < part.length(); k++) {
                    char c = part.charAt(k);
                    if ("\\.[]{}()*+?^$|".indexOf(c) >= 0) {
                        regex.append('\\');
                    }
                    regex.append(c);
                }
            }
        }
        regex.append('$');
        return SqlToPure.call("matches", value, new CString(regex.toString()));
    }

    // ------------------------------------------------------------------ SQL Anywhere's own relations

    /** The rows {@code sa_rowgenerator()} gives when its bounds are not written: 0 to 100. */
    private static final long ROWGENERATOR_START = 0;
    private static final long ROWGENERATOR_END = 100;

    /**
     * SQL Anywhere's own relations, which IQ's front end reads ({@link #frontEndSource}; measured on SQL Anywhere 17):
     * <ul>
     *   <li>{@code DUMMY} ({@code SYS.DUMMY}): one row, its one column {@code dummy_col} the INTEGER 0.</li>
     *   <li>{@code sa_rowgenerator([start [, end [, step]]])}: a column {@code row_num} of INTEGERs, start, start +
     *       step, ... while not beyond end (start 0, end 100 and step 1 when not written); none when start is beyond
     *       end or the step is not positive. Its arguments are integer constants of an INTEGER's range (outside it,
     *       IQ's error -158); any other argument, NULL included, is refused by name.</li>
     * </ul>
     */
    /** IQ island, 2026-10-14 (M1e): a table named without {@code tb()}, bound to the model's ({@link IqBindings}, the
     *  study's O2). */
    @Override
    public com.legend.protocol.spec.@com.legend.base.Nullable ValueSpecification table(SqlAst.TableName t,
            SqlToPure.Sources sources) {
        return IqBindings.table(t, SqlToPure.tables(sources));
    }

    /** IQ island, 2026-10-14 (M1e): a host variable, {@code @name} or {@code :name}, as the enclosing Pure query's
     *  variable ({@link IqBindings}, the study's O3); {@code @name} as a column of its name in scope when there is one.
     *  IQ island, 2026-10-14 (review 6's M1, measured on SQL Anywhere 17): under a TDS profile (tsql_variables On) a
     *  client binds {@code @name} as a placeholder, which reads the bound value even where a column of its name is in
     *  scope, while a connection's variable reads the column: which one an island's {@code @name} is there is not
     *  decided, so such an {@code @name} is refused by name (an alias of its name is refused under every profile,
     *  {@link #variableAliases}). */
    @Override
    public Tx parameter(SqlAst.Param p, SqlToPure.At at, SqlToPure.Sources sources) {
        if (settings.tsqlVariables() && IqBindings.readsColumn(p, at)) {
            throw SqlTranslationException.unsupported("'" + p.name() + "' names a column in scope: under the "
                    + settings.spelling() + " profile (a TDS login, tsql_variables On) a client binds " + p.name()
                    + " as a placeholder, which reads the bound value, while a connection's variable of that name"
                    + " reads the column first; which one is meant is not decided: rename the column, or write :"
                    + java.util.Objects.requireNonNull(p.name()).substring(1) + " for the variable", p.pos());
        }
        Tx t = IqBindings.parameter(p, at, sources);
        if (!IqBindings.readsColumn(p, at)) {
            variables.add(p);
        }
        return t;
    }

    /** IQ island, 2026-10-14 (review 5's M3, measured on SQL Anywhere 17): an unqualified {@code DUMMY} (and
     *  {@code db..DUMMY}) reads the user's own table of that name before SYS.DUMMY. When the model holds one (a table
     *  of the island's user, DBA, named DUMMY as IQ matches names) the name is refused by name: the statement's engine
     *  is chosen before the model is read (a DUMMY is SQL Anywhere's front end's, a table IQ's). */
    @Override
    public SqlToPure.@com.legend.base.Nullable Relation tableSource(SqlAst.TableName t, SqlToPure.Sources sources) {
        if (dummy(t.name()) && IqBindings.owner(t.name()) == null) {
            for (SqlToPure.CatalogTable c : SqlToPure.tables(sources)) {
                if (IqBindings.usersTable(c) && IqLexer.lower(c.table()).equals("dummy")) {
                    throw SqlTranslationException.unsupported("'" + t.name().get(t.name().size() - 1).text() + "'"
                            + " names the model's table " + c.database() + "." + c.table() + " of the island's user, "
                            + IqBindings.USER + ", which IQ reads before its own SYS.DUMMY: write " + IqBindings.USER
                            + ".DUMMY (or tb('...')) for the model's table, or SYS.DUMMY for IQ's", t.pos());
                }
            }
        }
        return source(t);
    }

    @Override
    public SqlToPure.@com.legend.base.Nullable Relation source(SqlAst.FromItem item) {
        if (item instanceof SqlAst.TableName t && dummy(t.name())) {
            String one = "\ndummy_col\n0\n";
            return new SqlToPure.Relation(com.legend.protocol.spec.TdsLiteral.of(one, "#TDS" + one + "#", null),
                    List.of(new SqlToPure.RelationColumn("dummy_col", Kind.INTEGER, false,
                            IqType.integer("INTEGER", BigInteger.ZERO, BigInteger.ZERO, false))));
        }
        if (!(item instanceof SqlAst.TableFunction f) || !rowGenerator(f.name())) {
            return null;
        }
        if (f.args().size() > 3) {
            throw SqlTranslationException.invalid("wrong number of arguments to sa_rowgenerator" + code(-154, "37505"),
                    f.pos());
        }
        long[] bounds = {ROWGENERATOR_START, ROWGENERATOR_END, 1};
        for (int i = 0; i < f.args().size(); i++) {
            SqlAst.Arg a = f.args().get(i);
            BigInteger v = a.name() == null ? integerLiteral(a.value()) : null;
            if (v == null) {
                throw SqlTranslationException.unsupported("an argument of sa_rowgenerator other than an integer"
                        + " constant is not supported yet", a.value().pos());
            }
            if (v.compareTo(IqType.INTEGER_MIN) < 0 || v.compareTo(IqType.INTEGER_MAX) > 0) {
                throw SqlTranslationException.invalid("sa_rowgenerator's argument " + v + " is out of range for its"
                        + " type, INTEGER" + overflowCode(), a.value().pos());
            }
            bounds[i] = v.longValue();
        }
        long start = bounds[0];
        long end = bounds[1];
        long step = bounds[2];
        boolean rows = step > 0 && start <= end;
        // range's stop is exclusive: the constant after end, written as one (end + 1 computed by a database in its
        // literal's 32 bits would overflow at an INTEGER's largest); no rows: a range of none
        ValueSpecification range = rows ? SqlToPure.call("range", new CInteger(start), new CInteger(end + 1),
                new CInteger(step)) : SqlToPure.call("range", new CInteger(0L), new CInteger(0L), new CInteger(1L));
        ValueSpecification relation = SqlToPure.call("flatten", range, new com.legend.protocol.spec.ColSpec("row_num",
                null, null, null));
        BigInteger lo = BigInteger.valueOf(rows ? start : 0);
        BigInteger hi = BigInteger.valueOf(rows ? end : 0);
        // the column is Pure's flatten's, of multiplicity [0..1]: nullable to the translation, though never NULL
        // IQ island, 2026-10-14 (M1d): and a BIGINT in lite's databases (range's), so computed in 64 bits already
        return new SqlToPure.Relation(relation, List.of(new SqlToPure.RelationColumn("row_num", Kind.INTEGER, true,
                IqType.integer("INTEGER", lo, hi, true))));
    }

    /** IQ's wording of a kind in the translator's refusals that name a value's type (D11, 2026-10-18): its own
     *  ({@link #kindName}). */
    @Override
    public String typeName(Kind kind) {
        return kindName(kind);
    }

    /** A string constant the translator compares with a value of another kind (a simple CASE's operand, D11,
     *  2026-10-18): refused by name. IQ converts the string by its own rules, which the island reads in its own
     *  comparisons ({@link #binary}) but not in the translator's; until 2026-10-18 the translator read it by
     *  Postgres's rules, which no IQ test reached. */
    @Override
    public Tx readString(SqlAst.StringLit s, Kind kind) {
        throw SqlTranslationException.unsupported("comparing the string '" + s.value() + "' with " + kindName(kind)
                + " in a simple CASE is not supported yet", s.pos());
    }

    private static String kindName(Kind k) {
        return switch (k) {
            case BOOLEAN -> "a condition";
            case STRING -> "a string";
            case DATE -> "a date";
            case DATETIME, ZONED, ZONED_TIME -> "a timestamp";
            case TIME -> "a time";
            case DURATION -> "a duration";
            case NULL -> "NULL";
            case INTEGER, WHOLE, FLOAT, DECIMAL, NUMBER, INEXACT -> "a number";
            case JSON, OTHER -> "a value of this type";
        };
    }

    // ------------------------------------------------------------------ joins, subqueries and set operations (M1b)

    /** IQ island, 2026-10-15 (checklist 0.6, measured on IQ 17.1): {@code *=} and {@code =*} are T-SQL outer joins
     *  under tsql_outer_joins On (the real IQ 17.1's PUBLIC value); Off, {@link #parse} refuses them as IQ's -1099. */
    @Override
    public boolean readsTsqlOuterJoins() {
        return settings.tsqlOuterJoins();
    }

    /** IQ's NATURAL JOIN keeps both sides' columns (measured: an unqualified shared column is ambiguous, IQ's error
     *  -144), joining on their equality by IQ's {@code =}. */
    @Override
    public boolean mergesNaturalJoin() {
        return false;
    }

    /** {@code x op ANY | ALL (query)}, {@code x IN (query)}: each row's comparison is IQ's ({@link #compared}: its
     *  conversions, its blank padding). */
    @Override
    public Tx quantifiedTest(SqlAst.BinaryOp op, SqlAst.Expr left, Tx each, SqlAst.Pos pos, SqlToPure.At at) {
        return comparison(op, at.tx(left), each, false, false, null, pos);
    }

    /**
     * A set operation's column takes one type by IQ's unification ({@link #unify}: the research's TYPING §8, which
     * UNION shares), where the island proves it: integers the smallest type holding both (measured: SMALLINT with
     * BIGINT is BIGINT), strings, dates, timestamps, doubles alike; an untyped NULL with an integer (its NULL is a
     * SMALLINT's). IQ island, 2026-10-15 (measured on IQ 17.1), under IQ's engine: a NUMERIC with an integer or a
     * NUMERIC is a NUMERIC of the most digits before the point and the largest scale (NUMERIC(10,2) with INTEGER is
     * NUMERIC(12,2), with BIGINT NUMERIC(21,2)), with a DOUBLE a DOUBLE, an integer with a DOUBLE a DOUBLE, a DATE with
     * a TIMESTAMP a TIMESTAMP, and an untyped NULL takes the other side's type; each side's values converted
     * ({@link #setValue}). Refused by name: those on SQL Anywhere's front end, and every other mix of kinds (IQ's
     * error -1001020 for a string with a number).
     */
    @Override
    public SqlToPure.ColumnType setColumn(SqlAst.SetOperation s, int i, SqlToPure.ColumnType left,
            SqlToPure.ColumnType right) {
        String op = s.op().name();
        if (engine == Engine.IQ && s.op() != SqlAst.SetOp.UNION) {
            // IQ island, 2026-10-15 (measured on IQ 17.1): IQ's engine runs no INTERSECT or EXCEPT over its tables
            // (its -1001030); the front end's, of no IQ table, does
            throw SqlTranslationException.unsupported(op + " is IQ's error -1001030 under its engine (a query over IQ"
                    + " tables), refused before translating", s.pos());
        }
        boolean leftNull = left.kind() == Kind.NULL;
        boolean rightNull = right.kind() == Kind.NULL;
        if ((leftNull || rightNull) && s.op() != SqlAst.SetOp.UNION) {
            throw SqlTranslationException.unsupported(op + " over an untyped NULL in column " + (i + 1) + " is not"
                    + " supported yet: lite's row match would not find it equal to a NULL", s.pos());
        }
        if (leftNull || rightNull) {
            SqlToPure.ColumnType other = leftNull ? right : left;
            if (other.kind() == Kind.NULL) {
                // IQ island, 2026-10-15 (measured on IQ 17.1): under IQ's engine a UNION types its columns over all
                // its branches, so NULL with NULL stays untyped for a later branch (NULL, NULL, a DATE is a DATE);
                // the outermost UNION's all-NULL column is a VARCHAR(4)
                if (engine == Engine.IQ) {
                    return innerSets.contains(s) ? new SqlToPure.ColumnType(Kind.NULL, true, null)
                            : new SqlToPure.ColumnType(Kind.STRING, true, new IqType("VARCHAR(4)"));
                }
                return new SqlToPure.ColumnType(Kind.INTEGER, true, IqType.integer("SMALLINT", IqType.SMALLINT_MIN,
                        IqType.SMALLINT_MAX, false));
            }
            if (engine == Engine.IQ && (other.kind() != Kind.INTEGER || !(other.detail() instanceof IqType o)
                    || !o.isInteger())) {
                // IQ island, 2026-10-15 (measured on IQ 17.1): the NULL takes the other side's type
                return new SqlToPure.ColumnType(other.kind(), true, other.detail());
            }
            if (other.kind() != Kind.INTEGER || !(other.detail() instanceof IqType t) || !t.isInteger()) {
                throw SqlTranslationException.unsupported(op + " of a NULL and " + kindName(other.kind()) + " in"
                        + " column " + (i + 1) + " is not supported yet: IQ types the NULL by its own rules", s.pos());
            }
            // the NULL's type is a SMALLINT's; its values are the other side's
            return new SqlToPure.ColumnType(Kind.INTEGER, true, IqType.integer(IqTypes.unified("SMALLINT",
                    t.family()), t.lo(), t.hi(), t.wide(), t.wider()));
        }
        if (engine == Engine.IQ && s.op() == SqlAst.SetOp.UNION && (left.kind() != right.kind()
                || left.kind() == Kind.DECIMAL || left.kind() == Kind.NUMBER)) {
            // IQ island, 2026-10-15 (measured on IQ 17.1): numbers and dates unified as a CASE's are
            SqlToPure.Common common = unify(op, List.of(new SqlAst.NullLit(s.pos()), new SqlAst.NullLit(s.pos())),
                    List.of(placeholder(left), placeholder(right)), s.pos());
            return new SqlToPure.ColumnType(common.kind(), left.nullable() || right.nullable(), common.detail());
        }
        if (left.kind() == Kind.DECIMAL || left.kind() == Kind.NUMBER || right.kind() == Kind.DECIMAL
                || right.kind() == Kind.NUMBER) {
            throw SqlTranslationException.unsupported(op + " of a NUMERIC in column " + (i + 1) + " is not supported"
                    + " yet: IQ's NUMERIC result types are the research's D-N1, and lite's two sides would need one"
                    + " scale", s.pos());
        }
        Tx l = Tx.value(new CString(""), left.kind(), left.nullable(), left.detail());
        Tx r = Tx.value(new CString(""), right.kind(), right.nullable(), right.detail());
        SqlToPure.Common common = unify(op, List.of(new SqlAst.NullLit(s.pos()), new SqlAst.NullLit(s.pos())),
                List.of(l, r), s.pos());
        if (common.kind() == Kind.STRING && !s.all() && !keys.own(s, i) && s.op() != SqlAst.SetOp.UNION) {
            // IQ island, 2026-10-14 (the merge): IQ tells the rows apart by its collation and shows the first of each
            // group of equal keys it meets ({@link IqKeys}); a UNION shows one value of each (setKey; IQ island,
            // 2026-10-15), INTERSECT and EXCEPT are refused
            throw SqlTranslationException.unsupported(op + " over strings in column " + (i + 1) + " is supported only"
                    + " where every value is its own collation key (RTRIM, TRIM, a constant" + (settings.caseSensitive()
                    ? "" : ", LOWER") + "): IQ tells " + (settings.caseSensitive() ? "'a' and 'a '" : "'A' and 'a'")
                    + " apart as one value and shows the first it meets, which is its plan's choice", s.pos());
        }
        // IQ island, 2026-10-14 (review 4's m1): EXCEPT's values are the left's; UNION's and INTERSECT's may be NULL
        // where either side's may (INTERSECT's are the left's rows, which lite's lowering reads as they are: typed as
        // not NULL because the right's are not, an ORDER BY's collation key of RTRIM(vc) INTERSECT 'Hello' failed)
        return new SqlToPure.ColumnType(common.kind(), s.op() == SqlAst.SetOp.EXCEPT ? left.nullable()
                : left.nullable() || right.nullable(), common.detail());
    }

    /** A set operation's side's column as a value of its type, for {@link #unify} to type (never computed). */
    private static Tx placeholder(SqlToPure.ColumnType t) {
        return Tx.value(new CString(""), t.kind(), t.nullable(), t.detail());
    }

    /** IQ island, 2026-10-15: a UNION's side's values converted to the column's type ({@link #setColumn}): an untyped
     *  NULL a NULL of that type, a number or a date as {@link #unify} converts a CASE's. */
    @Override
    public @com.legend.base.Nullable Tx setValue(SqlAst.SetOperation s, int i, Tx side, SqlToPure.ColumnType other,
            boolean left) {
        if (engine != Engine.IQ || s.op() != SqlAst.SetOp.UNION) {
            return null;
        }
        if (side.kind() == Kind.NULL) {
            if (other.kind() == Kind.NULL && !innerSets.contains(s)) {
                return Tx.value(SqlToPure.nullOf(Kind.STRING).vs(), Kind.STRING, true, new IqType("VARCHAR(4)"));
            }
            if (other.kind() == Kind.NULL || other.kind() == Kind.INTEGER) {
                return null;
            }
            if (other.kind() == Kind.DECIMAL || other.kind() == Kind.NUMBER) {
                // a NULL of the column's own Decimal type (the other side's is brought to its scale): Pure types the
                // never-taken branch, so the databases see one NUMERIC, not a NUMERIC of 18 places
                int scale = iqNumericType(placeholder(other), new SqlAst.NullLit(s.pos()))[1];
                ValueSpecification zero = SqlToPure.call("toDecimal", new CInteger(0L));
                return Tx.value(ifThen(new com.legend.protocol.spec.CBoolean(true, null),
                        SqlToPure.nullOf(Kind.DECIMAL).vs(), scale > 0 ? IqFunctions.atScale(zero, scale) : zero),
                        Kind.DECIMAL, true, other.detail());
            }
            return Tx.value(SqlToPure.nullOf(other.kind()).vs(), other.kind(), true, other.detail());
        }
        boolean numeric = side.kind() == Kind.DECIMAL || side.kind() == Kind.NUMBER;
        if (other.kind() == Kind.NULL && !numeric || side.kind() == other.kind() && !numeric) {
            // (a NUMERIC beside a NULL is brought to its scale as unify brings it: one Decimal type with the NULL's)
            return null;
        }
        Tx o = placeholder(other);
        SqlToPure.Common common = unify(s.op().name(), List.of(new SqlAst.NullLit(s.pos()),
                new SqlAst.NullLit(s.pos())), left ? List.of(side, o) : List.of(o, side), s.pos());
        Tx mine = common.values().get(left ? 0 : 1);
        return mine == side ? null : mine;
    }

    // ------------------------------------------------------------------ one type for alternatives

    /**
     * IQ's unification (the research's TYPING §8: CASE, IF, ISNULL and COALESCE share it), for the pairs the island
     * proves: integers take the smallest type holding them all (BIT with another gives the other); strings a VARCHAR
     * when one is, else a CHAR of the longest; dates with dates, timestamps with timestamps, doubles with doubles; an
     * untyped NULL takes the others' type, and all NULL is a SMALLINT. Under the IQ engine a CHAR result whose values
     * differ in length is refused: whether IQ pads such a result as it pads a CHAR(n) column (the research's D-S1) is
     * not decided. A NUMERIC (the research's D-N1) and every mix of kinds (IQ converts: a string with a number to
     * NUMERIC, with a date to TIMESTAMP) are refused by name.
     */
    @Override
    public SqlToPure.Common unify(String what, List<SqlAst.Expr> exprs, List<Tx> values, SqlAst.Pos pos) {
        values.forEach(v -> bounded(v, pos));
        int branches = exprs.size();
        if (what.equals("CASE") && branches >= 3 && IqParser.nullValue(exprs.get(branches - 1))
                && IqParser.nullValue(exprs.get(branches - 2)) && !exprs.stream().allMatch(IqParser::nullValue)) {
            // IQ island, 2026-10-14 (review 3's M5): the reader drops a last NULL branch whose condition cannot raise
            // (IqParser.caseExpression); one it keeps would meet the compiler's wrong multiplicity stamp of a CASE whose
            // last branches are all NULL (the compiler line's, recorded); (review 4's m2) a NULL of any type alike
            throw SqlTranslationException.unsupported("a CASE whose last WHEN gives NULL after a condition that can"
                    + " raise (a comparison, arithmetic, a call), or beside a NULL of another type, is not supported"
                    + " yet: write it without that WHEN", exprs.get(branches - 2).pos());
        }
        if (engine != Engine.IQ) {
            // IQ island, 2026-10-17 (the research's D-N1 in SQL Anywhere's front end, measured on SQL Anywhere 17):
            // a NUMERIC beside another number unifies as under IQ's engine, an integer by numericType's widths
            SqlToPure.Common numbers = iqNumericUnify(what, exprs, values, pos);
            if (numbers != null) {
                return numbers;
            }
        }
        if (engine == Engine.IQ) {
            SqlToPure.Common numbers = iqNumericUnify(what, exprs, values, pos);
            if (numbers != null) {
                return numbers;
            }
            // IQ island, 2026-10-15 (measured on IQ 17.1): a DATE beside a TIMESTAMP is the TIMESTAMP of its midnight
            if (values.stream().anyMatch(v -> v.kind() == Kind.DATE)
                    && values.stream().anyMatch(v -> v.kind() == Kind.DATETIME)
                    && values.stream().allMatch(v -> v.kind() == Kind.DATE || v.kind() == Kind.DATETIME
                    || v.kind() == Kind.NULL)) {
                List<Tx> timestamps = new ArrayList<>();
                for (Tx v : values) {
                    timestamps.add(v.kind() != Kind.DATE ? v : atMidnight(v));
                }
                return unify(what, exprs, timestamps, pos);
            }
            // IQ island, 2026-10-15 (the corpus lane: COALESCE(LastActivityDate, '2000-01-01') and kin; measured on IQ
            // 17.1): a constant date string beside a DATE or a TIMESTAMP is read as one (COALESCE(ts, '1900-01-01') is
            // the TIMESTAMP of its midnight, COALESCE(dt, '1900-01-01') a DATE); a string IQ reads as no date is its
            // -1001014, refused by name here
            boolean dated = values.stream().anyMatch(v -> v.kind() == Kind.DATE || v.kind() == Kind.DATETIME);
            boolean strings = values.stream().anyMatch(v -> v.kind() == Kind.STRING);
            if (dated && strings && values.stream().allMatch(v -> v.kind() == Kind.DATE || v.kind() == Kind.DATETIME
                    || v.kind() == Kind.NULL || v.kind() == Kind.STRING && v.vs() instanceof CString && !v.nullable())) {
                Kind target = values.stream().anyMatch(v -> v.kind() == Kind.DATETIME) ? Kind.DATETIME : Kind.DATE;
                List<Tx> read = new ArrayList<>();
                for (int i = 0; i < values.size(); i++) {
                    Tx v = values.get(i);
                    read.add(v.kind() != Kind.STRING ? v : dateConstant(((CString) v.vs()).value(), target,
                            exprs.get(i).pos(), what + " of"));
                }
                return unify(what, exprs, read, pos);
            }
        }
        Kind kind = null;
        String family = null;
        List<Integer> charLengths = new ArrayList<>();
        BigInteger lo = null;
        BigInteger hi = null;
        boolean wide = true;
        boolean loose = false;
        for (Tx v : values) {
            if (v.kind() == Kind.NULL) {
                continue;
            }
            if (!(v.detail() instanceof IqType t)) {
                throw SqlTranslationException.unsupported(what + " of a value whose IQ type is not known is not"
                        + " supported", pos);
            }
            if (kind != null && kind != v.kind()) {
                throw SqlTranslationException.unsupported(what + " of " + kindName(kind) + " and " + kindName(v.kind())
                        + " is not supported yet (IQ converts one to the other's type by its own rules)", pos);
            }
            kind = v.kind();
            switch (v.kind()) {
                case INTEGER -> {
                    if (!t.isInteger()) {
                        throw t.family().equals("NUMERIC") ? numericRefused(what, pos)
                                : SqlTranslationException.unsupported(what + " of a " + t.spelling() + " is not"
                                        + " supported yet", pos);
                    }
                    family = family == null ? t.family() : IqTypes.unified(family, t.family());
                    lo = lo == null ? t.lo() : lo.min(t.lo());
                    hi = hi == null ? t.hi() : hi.max(t.hi());
                    wide &= t.wide();
                    loose |= t.wider();
                }
                case STRING -> {
                    String f = t.family();
                    if (f.equals("CHAR")) {
                        charLengths.add(t.size());
                        family = family == null ? "CHAR" : family;
                    } else if (f.equals("VARCHAR") || f.equals("LONG VARCHAR")) {
                        // IQ island, 2026-10-17 (the corpus lane's JOB: COALESCE(LIST(...), '{}'); measured on IQ
                        // 17.1): a LONG VARCHAR (a LIST's) beside strings is a VARCHAR
                        family = "VARCHAR";
                    } else {
                        throw SqlTranslationException.unsupported(what + " of a " + t.spelling() + " is not supported"
                                + " yet", pos);
                    }
                }
                case DATE, DATETIME -> family = v.kind() == Kind.DATE ? "DATE" : "TIMESTAMP";
                case FLOAT -> {
                    if (!t.family().equals("DOUBLE")) {
                        throw SqlTranslationException.unsupported(what + " of a " + t.spelling() + " is not supported"
                                + " yet", pos);
                    }
                    family = "DOUBLE";
                }
                case DECIMAL, NUMBER -> throw numericRefused(what, pos);
                default -> throw SqlTranslationException.unsupported(what + " of " + kindName(v.kind()) + " is not"
                        + " supported yet", pos);
            }
        }
        if (kind == null && engine == Engine.IQ) {
            // IQ island, 2026-10-15 (measured on IQ 17.1): under IQ's engine every value an untyped NULL is a
            // VARCHAR(4)'s, as a UNION's all-NULL column is (D-N7)
            IqType text = new IqType("VARCHAR(4)");
            List<Tx> nulls = new ArrayList<>();
            for (Tx v : values) {
                nulls.add(Tx.value(SqlToPure.nullOf(Kind.STRING).vs(), Kind.STRING, true, text));
            }
            return new SqlToPure.Common(nulls, Kind.STRING, text);
        }
        if (kind == null) {
            // every value an untyped NULL: IQ's SMALLINT NULL
            kind = Kind.INTEGER;
            family = "SMALLINT";
            lo = IqType.SMALLINT_MIN;
            hi = IqType.SMALLINT_MAX;
            wide = false;
        }
        IqType type;
        if (kind == Kind.STRING && "CHAR".equals(family)) {
            int longest = Collections.max(charLengths);
            // IQ island, 2026-10-15 (D-S1, measured on IQ 17.1): under IQ's engine strings of different lengths unify
            // to a VARCHAR, each value as it is (CASE ... 'Yes' ELSE 'No' is 'No', two characters)
            type = new IqType((engine == Engine.IQ && charLengths.stream().anyMatch(n -> n != longest) ? "VARCHAR("
                    : "CHAR(") + longest + ")");
        } else if (kind == Kind.INTEGER) {
            type = IqType.integer(java.util.Objects.requireNonNull(family), java.util.Objects.requireNonNull(lo),
                    java.util.Objects.requireNonNull(hi), wide, loose);
        } else if (kind == Kind.DATE || kind == Kind.DATETIME) {
            // IQ island, 2026-10-14 (review 4's M2): the latest year of any of them (IqFunctions.latestYear)
            Integer latest = 1;
            for (Tx v : values) {
                Integer y = v.kind() == Kind.NULL ? Integer.valueOf(1) : IqFunctions.latestYear(v);
                latest = latest == null || y == null ? null : Math.max(latest, y);
            }
            type = IqFunctions.dated(java.util.Objects.requireNonNull(family), latest);
        } else {
            type = new IqType(java.util.Objects.requireNonNull(family));
        }
        List<Tx> unified = new ArrayList<>();
        for (Tx v : values) {
            unified.add(v.kind() == Kind.NULL ? Tx.value(SqlToPure.nullOf(kind).vs(), kind, true, type) : v);
        }
        return new SqlToPure.Common(unified, kind, type);
    }

    /**
     * IQ's engine's unification of numbers where one is a NUMERIC (IQ island, 2026-10-15; the research's D-N1, measured
     * on IQ 17.1: COALESCE(nm, 0) is a NUMERIC(10,2) and its 0 is 0.00), and SQL Anywhere's front end's (2026-10-17,
     * measured on SQL Anywhere 17: its integers sized as {@link #numericType} sizes them, a COUNT and a REAL not read): with a DOUBLE a DOUBLE; else a NUMERIC whose
     * scale is the largest of the values' and whose digits before the point are the most of theirs (an integer's its
     * type's: SMALLINT 5, INTEGER 10, BIGINT 19, BIT 1; a literal's its own), each value converted to it. Null where no
     * value is a NUMERIC or another kind is among them (the general unification then refuses or reads it). Refused by
     * name: a NUMERIC of more than lite's 38 digits.
     */
    private SqlToPure.@com.legend.base.Nullable Common iqNumericUnify(String what, List<SqlAst.Expr> exprs,
            List<Tx> values, SqlAst.Pos pos) {
        boolean numeric = false;
        boolean doubles = false;
        boolean integers = false;
        boolean counts = false;
        boolean reals = false;
        for (Tx v : values) {
            if (v.kind() == Kind.NULL) {
                continue;
            }
            if (decimal(v)) {
                numeric = true;
            } else if (v.kind() == Kind.FLOAT && v.detail() instanceof IqType t && t.family().equals("DOUBLE")) {
                doubles = true;
            } else if (IqFunctions.real(v)) {
                reals = true;
            } else if (v.kind() == Kind.INTEGER && v.detail() instanceof IqType t && t.isInteger()) {
                integers = true;
            } else if (v.kind() == Kind.INTEGER && v.detail() instanceof IqType t && t.spelling().equals(
                    "UNSIGNED BIGINT")) {
                counts = true;
            } else {
                return null;
            }
        }
        if (engine != Engine.IQ && (counts || reals)) {
            // in SQL Anywhere's front end a COUNT and a REAL beside other numbers are not measured
            return null;
        }
        // IQ island, 2026-10-15 (the corpus lane: COALESCE(COUNT(...), 0) in 1,100 of SQLStorm's queries; measured on
        // IQ 17.1): COUNT's UNSIGNED BIGINT (a ranking's too) beside UNSIGNED BIGINTs alone is one; beside a signed
        // integer IQ shows a NUMERIC(20,0) (COALESCE(COUNT(*), 0), CASE ... THEN COUNT(*) ELSE 0 END) that computes as
        // an integer does (its / divides whole: COALESCE(COUNT(*), 0) / 2 is 3, 10 / NULLIF(COUNT(*), 0) is 1, where
        // CAST(7 AS NUMERIC(20,0)) / 2 is 3.50), so lite reads it as one, of 64 bits, as D-N3 reads a COUNT; beside a
        // NUMERIC a NUMERIC of its 20 digits (COALESCE(COUNT(*), 0.5) a NUMERIC(21,1)), beside a DOUBLE a DOUBLE
        if (counts && !numeric && !doubles) {
            IqType type = new IqType("UNSIGNED BIGINT");
            if (integers) {
                // IQ's integer of 20 digits (a NUMERIC(20,0) it divides whole: COUNT(id) * 1.0 / NULLIF(COUNT(*), 0)
                // is a NUMERIC(42,21), as by COUNT(*) itself), so the island's UNSIGNED BIGINT, of the range it knows
                BigInteger lo = BigInteger.ZERO;
                BigInteger hi = IqType.BIGINT_MAX;
                boolean loose = false;
                for (Tx v : values) {
                    if (v.detail() instanceof IqType t && t.isInteger()) {
                        lo = lo.min(t.lo());
                        hi = hi.max(t.hi());
                        loose |= t.wider();
                    } else if (v.detail() instanceof IqType t && t.low() != null) {
                        lo = lo.min(t.lo());
                        hi = hi.max(t.hi());
                    }
                }
                type = IqType.integer("UNSIGNED BIGINT", lo, hi, true, loose);
            }
            List<Tx> unified = new ArrayList<>();
            for (Tx v : values) {
                unified.add(v.kind() == Kind.NULL ? Tx.value(SqlToPure.nullOf(Kind.INTEGER).vs(), Kind.INTEGER, true,
                        type) : v);
            }
            return new SqlToPure.Common(unified, Kind.INTEGER, type);
        }
        // IQ island, 2026-10-15 (the corpus lane; measured on IQ 17.1): REALs alone are a REAL; a REAL beside any other
        // number a DOUBLE (COALESCE(CAST(x AS REAL), 0), a UNION of a REAL and an integer)
        if (reals && !numeric && !doubles && !integers && !counts) {
            IqType type = new IqType("REAL");
            List<Tx> unified = new ArrayList<>();
            for (Tx v : values) {
                unified.add(v.kind() == Kind.NULL ? Tx.value(SqlToPure.nullOf(Kind.FLOAT).vs(), Kind.FLOAT, true, type)
                        : v);
            }
            return new SqlToPure.Common(unified, Kind.FLOAT, type);
        }
        doubles |= reals;
        // IQ island, 2026-10-15 (measured on IQ 17.1): an integer beside a DOUBLE is a DOUBLE too
        if (!numeric && !(doubles && (integers || counts || reals))) {
            return null;
        }
        if (doubles) {
            IqType type = new IqType("DOUBLE");
            List<Tx> unified = new ArrayList<>();
            for (Tx v : values) {
                unified.add(v.kind() == Kind.NULL ? Tx.value(SqlToPure.nullOf(Kind.FLOAT).vs(), Kind.FLOAT, true, type)
                        : v.kind() == Kind.FLOAT ? v : SqlToPure.strict(Kind.FLOAT, List.of(v),
                        x -> SqlToPure.call("toFloat", x.get(0))));
            }
            return new SqlToPure.Common(unified, Kind.FLOAT, type);
        }
        int whole = 0;
        int scale = 0;
        // IQ island, 2026-10-15 (the corpus lane: COALESCE(AVG(n), 0) of COUNTs, IQ's NUMERIC(51,16)): what lite holds
        // of each value -- at most 38 digits at its own scale, whatever IQ's precision -- converted to the common scale
        int held = 0;
        for (int i = 0; i < values.size(); i++) {
            Tx v = values.get(i);
            if (v.kind() == Kind.NULL) {
                continue;
            }
            // SQL Anywhere's front end (measured on SQL Anywhere 17, 2026-10-17): an integer below a BIGINT -- a BIT, a
            // SMALLINT -- as a NUMERIC(10,0), a BIGINT a NUMERIC(20,0) (numericType), where IQ's engine sizes each
            int[] ps = engine == Engine.IQ ? iqNumericType(v, exprs.get(i)) : numericType(v, exprs.get(i), what);
            whole = Math.max(whole, ps[0] - ps[1]);
            scale = Math.max(scale, ps[1]);
            held = Math.max(held, Math.min(ps[0] - ps[1], 38 - ps[1]));
        }
        if (held + scale > 38) {
            throw SqlTranslationException.unsupported(what + " of these numbers is a NUMERIC(" + (whole + scale) + ","
                    + scale + "), more than the 38 digits lite's decimals hold", pos);
        }
        IqType type = numeric(whole + scale, scale);
        int s = scale;
        List<Tx> unified = new ArrayList<>();
        for (Tx v : values) {
            if (v.kind() == Kind.NULL) {
                unified.add(Tx.value(SqlToPure.nullOf(Kind.DECIMAL).vs(), Kind.DECIMAL, true, type));
                continue;
            }
            boolean integer = !decimal(v);
            Tx converted = SqlToPure.strict(Kind.DECIMAL, List.of(v), x -> {
                ValueSpecification d = integer ? SqlToPure.call("toDecimal", x.get(0)) : x.get(0);
                return s > 0 ? IqFunctions.atScale(d, s) : d;
            });
            unified.add(Tx.value(converted.vs(), Kind.DECIMAL, converted.nullable(), type));
        }
        return new SqlToPure.Common(unified, Kind.DECIMAL, type);
    }

    // ------------------------------------------------------------------ the catalog

    /** IQ's scalar functions the catalog holds, each with how many arguments it takes. */
    private enum Catalog {
        ISNULL(2, Integer.MAX_VALUE), COALESCE(2, Integer.MAX_VALUE), IFNULL(2, 3), NULLIF(2, 2);

        final int min;
        final int max;

        Catalog(int min, int max) {
            this.min = min;
            this.max = max;
        }

        /** The function {@code name} names, matched as IQ matches names (ASCII case folded); null for none. */
        static @com.legend.base.Nullable Catalog named(String name) {
            String key = IqLexer.lower(name);
            for (Catalog c : values()) {
                if (IqLexer.lower(c.name()).equals(key)) {
                    return c;
                }
            }
            return null;
        }
    }

    /** IQ's scalar functions read so far: ISNULL and COALESCE (one function in IQ), IFNULL, NULLIF. Any other name is
     *  refused by name (by the translator: "function f(...) is not supported yet"). */
    @Override
    public InputDialect.@com.legend.base.Nullable FunctionRule function(String name) {
        Catalog fn = Catalog.named(name);
        if (fn == null && IqFunctions.NO_SUCH_FUNCTION.contains(IqLexer.lower(name))) {
            // IQ island, 2026-10-17 (measured on IQ 17.1): its -265, once its arguments are read -- a grouped query's
            // column outside the GROUP BY is its -149 first (STRING_AGG(v, ',') ... GROUP BY k)
            return (f, args, at) -> {
                if (placeheld.contains(f)) {
                    // a column IQ drops before it resolves its functions (unreadUnknownFunctions): read nowhere
                    return Tx.value(SqlToPure.call("cast", SqlToPure.empty(), SqlToPure.typeArg("String")),
                            Kind.STRING, true);
                }
                throw IqFunctions.noSuchFunction(name, f.pos());
            };
        }
        if (fn == null) {
            // IQ island, 2026-10-14 (M1d): the rest of the catalog
            return IqFunctions.rule(this, name);
        }
        return switch (fn) {
            case ISNULL -> (f, args, at) -> bounded(coalesce("ISNULL", f, args), f);
            case COALESCE -> (f, args, at) -> bounded(coalesce("COALESCE", f, args), f);
            case IFNULL -> (f, args, at) -> bounded(ifnull(f, args), f);
            case NULLIF -> (f, args, at) -> bounded(nullif(f, args), f);
        };
    }

    private static void arguments(String fn, SqlAst.FunctionCall f, List<Tx> args, int min, int max) {
        if (args.size() < min || args.size() > max) {
            throw SqlTranslationException.invalid("wrong number of arguments to " + fn + code(-154, "37505"),
                    f.pos());
        }
    }

    /** {@code ISNULL(a, b, ...)} and {@code COALESCE(a, b, ...)}: the first that is not NULL, as their unified type.
     *  The arguments after the first that is never NULL are never read. */
    private Tx coalesce(String fn, SqlAst.FunctionCall f, List<Tx> arguments) {
        arguments(fn, f, arguments, 2, Integer.MAX_VALUE);
        SqlToPure.Common common = unify(fn, f.args(), arguments, f.pos());
        List<Tx> args = new ArrayList<>();
        for (int i = 0; i < common.values().size(); i++) {
            Tx a = common.values().get(i);
            if (arguments.get(i).kind() == Kind.NULL) {
                continue;
            }
            args.add(a);
            if (!a.nullable()) {
                break;
            }
        }
        if (args.isEmpty()) {
            return Tx.value(SqlToPure.nullOf(common.kind()).vs(), common.kind(), true, common.detail());
        }
        if (args.size() == 1) {
            Tx only = args.get(0);
            return Tx.value(SqlToPure.valueOf(only), common.kind(), only.nullable(), common.detail());
        }
        boolean nullable = true;
        for (Tx a : args) {
            nullable &= a.nullable();
        }
        ValueSpecification tail = SqlToPure.valueOf(args.get(args.size() - 1));
        int i = args.size() - 1;
        while (i > 0) {
            int take = Math.min(3, i);
            List<ValueSpecification> group = new ArrayList<>();
            for (int k = i - take; k < i; k++) {
                group.add(SqlToPure.valueOf(args.get(k)));
            }
            group.add(tail);
            tail = SqlToPure.call("coalesce", group.toArray(new ValueSpecification[0]));
            i -= take;
        }
        return Tx.value(tail, common.kind(), nullable, common.detail());
    }

    /** IQ's {@code IFNULL(a, b)}: {@code b} when {@code a} is NULL, else NULL (not COALESCE), typed as {@code b};
     *  {@code IFNULL(a, b, c)}: {@code b} when {@code a} is NULL, else {@code c}, as their unified type. */
    private Tx ifnull(SqlAst.FunctionCall f, List<Tx> args) {
        arguments("IFNULL", f, args, 2, 3);
        Tx a = args.get(0);
        SqlToPure.Common common = args.size() == 3
                ? unify("IFNULL", f.args().subList(1, 3), args.subList(1, 3), f.pos())
                : unify("IFNULL", f.args().subList(1, 2), List.of(args.get(1)), f.pos());
        Tx then = common.values().get(0);
        Tx otherwise = args.size() == 3 ? common.values().get(1)
                : Tx.value(SqlToPure.nullOf(common.kind()).vs(), common.kind(), true, common.detail());
        if (a.kind() == Kind.NULL) {
            return Tx.value(SqlToPure.valueOf(then), common.kind(), then.nullable(), common.detail());
        }
        if (!a.nullable()) {
            return Tx.value(SqlToPure.valueOf(otherwise), common.kind(), otherwise.nullable(), common.detail());
        }
        ValueSpecification vs = ifThen(SqlToPure.call("isEmpty", SqlToPure.valueOf(a)), SqlToPure.valueOf(then),
                SqlToPure.valueOf(otherwise));
        return Tx.value(vs, common.kind(), then.nullable() || otherwise.nullable(), common.detail());
    }

    /** IQ's {@code NULLIF(a, b)}: NULL when {@code a = b}, else {@code a}, typed as {@code a}; {@code b} is read as
     *  {@code a}'s type (an integer outside its range is IQ's error -158). A CHAR {@code a} in SQL Anywhere's front end
     *  (the conversion may cut {@code b}) and every pair of kinds IQ converts are refused by name. IQ island,
     *  2026-10-17 (the corpus lane's TPC-DS; measured on IQ 17.1): under IQ's engine a CHAR {@code a} is compared as
     *  {@code =} compares, blank-padded, {@code b} not cut (NULLIF(ch, 'ab   Q') of 'ab   ' is 'ab   '). */
    private Tx nullif(SqlAst.FunctionCall f, List<Tx> args) {
        arguments("NULLIF", f, args, 2, 2);
        Tx a = args.get(0);
        Tx b = args.get(1);
        if (a.kind() == Kind.NULL) {
            return typedNull("SMALLINT");
        }
        if (!(a.detail() instanceof IqType at)) {
            throw SqlTranslationException.unsupported("NULLIF of a value whose IQ type is not known is not supported",
                    f.pos());
        }
        if (b.kind() == Kind.NULL) {
            return a;
        }
        if (a.kind() == Kind.STRING && at.family().equals("CHAR") && engine != Engine.IQ) {
            throw SqlTranslationException.unsupported("NULLIF of a CHAR is not supported in SQL Anywhere's front end"
                    + " yet: it reads the second argument as the CHAR, which may cut it", f.pos());
        }
        boolean signedIntegers = a.kind() == Kind.INTEGER && at.isInteger() && b.kind() == Kind.INTEGER
                && b.detail() instanceof IqType bt && bt.isInteger();
        if (engine == Engine.IQ && number(a) && number(b) && !signedIntegers) {
            // IQ island, 2026-10-15 (the corpus lane: NULLIF(x, 0) under a quotient in BIRD's and SQLStorm's queries;
            // the research's D-N1, measured on IQ 17.1): of numbers one of which is a NUMERIC, a DOUBLE or a COUNT, the
            // type COALESCE gives the two (NULLIF(id, 1.5) a NUMERIC(11,1), NULLIF(nm, 0) nm's NUMERIC(10,2),
            // NULLIF(nm, dbl) a DOUBLE, NULLIF(COUNT(*), 0) a NUMERIC(20,0)): NULL where the two are equal in it,
            // else the first as it
            SqlToPure.Common common = iqNumericUnify("NULLIF", f.args(), List.of(a, b), f.pos());
            if (common != null) {
                Tx first = common.values().get(0);
                Tx equal = comparison(SqlAst.BinaryOp.EQ, first, common.values().get(1), false, false, null, f.pos());
                return Tx.value(ifThen(equal.vs(), SqlToPure.nullOf(common.kind()).vs(), SqlToPure.valueOf(first)),
                        common.kind(), true, common.detail());
            }
        }
        if (a.kind() == Kind.INTEGER) {
            IqType bt = integerOperand(b, f.args().get(1), "NULLIF");
            if (!at.isInteger()) {
                throw numericRefused("NULLIF", f.pos());
            }
            if (bt.lo().compareTo(at.min()) < 0 || bt.hi().compareTo(at.max()) > 0) {
                // the second argument read as the first's type: a value outside its range is IQ's -158
                b = checkedInteger(f, "NULLIF", new IqType(at.family()), bt.lo(), bt.hi(), List.of(b), List.of(bt),
                        constants(List.of(f.args().get(1)), List.of(b)), false, v -> v.get(0));
            }
        } else if ((a.kind() == Kind.DATE || a.kind() == Kind.DATETIME) && b.kind() == Kind.STRING
                && b.vs() instanceof CString date && !b.nullable()
                && IqDateTime.read(date.value(), settings, f.pos()) != null) {
            // IQ island, 2026-10-17 (the corpus lane's SQLStorm: NULLIF(MAX(CreationDate), '1970-01-01')): a date and a
            // constant date string, compared as = compares them (dateOf)
        } else if ((a.kind() == Kind.DATE || a.kind() == Kind.DATETIME) && b.vs() instanceof CString text
                && !b.nullable() && (engine == Engine.IQ || text.value().isEmpty())) {
            // IQ island, 2026-10-17 (measured on IQ 17.1): a date and a string that is no date -- under IQ's engine
            // its error -1001014, whether or not a row reaches it ('' and 'x' alike: it converts the constant as it
            // plans); in its front end NULLIF(d, '') is d
            if (engine != Engine.IQ) {
                return a;
            }
            throw SqlTranslationException.invalid("NULLIF of a date and the string '" + text.value() + "', which is"
                    + " no date" + code(-1001014, "QFA14"), f.pos());
        } else if (a.kind() != b.kind() || a.kind() == Kind.DECIMAL || a.kind() == Kind.NUMBER) {
            throw SqlTranslationException.unsupported("NULLIF of " + kindName(a.kind()) + " and " + kindName(b.kind())
                    + " is not supported yet", f.pos());
        }
        Tx equal = comparison(SqlAst.BinaryOp.EQ, a, b, false, false, null, f.pos());
        ValueSpecification vs = ifThen(equal.vs(), SqlToPure.nullOf(a.kind()).vs(), SqlToPure.valueOf(a));
        return Tx.value(vs, a.kind(), true, a.detail());
    }

    /** CAST and a style-less CONVERT: IqFunctions' (M1d). */
    @Override
    public Tx cast(SqlAst.Cast c, SqlToPure.At at) {
        return new IqFunctions(this).cast(c, at);
    }

    /** IQ island, 2026-10-14 (review 7's m1): whether a query of the statement has an ORDER BY. */
    boolean ordered() {
        return !ordered.isEmpty();
    }

    // ------------------------------------------------------------------ grouping (M1c)

    /** IQ reads a select-list alias in GROUP BY and HAVING before a column of its name
     *  ({@link IqAggregates#grouping}). */
    @Override
    public SqlAst.Select grouping(SqlAst.Select s) {
        SqlAst.Select read = grouped.get(s);
        return read != null ? read : IqAggregates.grouping(s);
    }

    /** An integer constant in GROUP BY is a constant to IQ, never a select-list position (measured: -149). */
    @Override
    public boolean groupsByPosition() {
        return false;
    }

    /** IQ's aggregates the island reads: COUNT, SUM, AVG, MIN, MAX, MEDIAN, LIST ({@link IqAggregates}). */
    @Override
    public boolean isAggregate(String name) {
        return IqAggregates.isAggregate(name);
    }

    @Override
    public SqlToPure.Aggregation aggregate(SqlAst.FunctionCall f, SqlToPure.Grouping at) {
        return IqAggregates.aggregate(f, at, this);
    }

    /** IQ island, 2026-10-15 (windows): IQ's window functions ({@link IqWindows}). */
    @Override
    public SqlToPure.WindowFunction window(SqlAst.FunctionCall f, SqlToPure.Windowing at) {
        return IqWindows.window(f, at, this);
    }

    /**
     * A key over strings, by IQ's collation ({@link #collated}; IQ island, 2026-10-14, the merge of M1c and the natives
     * slice; measured on SQL Anywhere 17). ORDER BY sorts by the collation key (the order of rows whose keys tie is the
     * plan's, in SQL Anywhere as in lite's databases); GROUP BY and DISTINCT group by it, and where the query shows the
     * key's value it shows one value of each group ({@link #representative}; IQ island, 2026-10-15: IQ shows the first
     * it meets, its plan's choice, so any value of the group is an answer IQ gives). A set operation reads strings only
     * where no two different values share a key ({@link IqKeys}): there the values themselves are the keys. Anything
     * else over strings is refused by name; a value of an unknown kind or JSON is no key.
     */
    @Override
    public java.util.function.@com.legend.base.Nullable UnaryOperator<ValueSpecification> key(String clause, Tx value,
            SqlAst.@com.legend.base.Nullable Expr expr, SqlAst.Pos pos) {
        Kind kind = value.kind();
        if (kind == Kind.OTHER || kind == Kind.JSON) {
            throw SqlTranslationException.unsupported(clause + " over a value of this type is not supported yet", pos);
        }
        if (kind != Kind.STRING) {
            return null;
        }
        switch (clause) {
            // IQ island, 2026-10-15 (windows): a window's PARTITION BY partitions by the key (it shows no value of it)
            case "ORDER BY", "PARTITION BY" -> {
                return expr != null && keys.own(expr) ? null : key(value.nullable());
            }
            case "GROUP BY" -> {
                return expr != null && keys.distinct(expr) ? null : key(value.nullable());
            }
            default -> {
                // a set operation's columns are checked by setColumn
                return expr != null && keys.distinct(expr) || !clause.equals("DISTINCT") ? null : key(value.nullable());
            }
        }
    }

    /**
     * A GROUP BY key over strings the query shows the value of, and a DISTINCT's string, show one value of each group
     * (IQ island, 2026-10-15; the user's rule: which spelling of a group IQ shows is its plan's choice, so any is an
     * answer IQ could give). A key whose values are their own keys ({@link IqKeys#distinct}) is grouped by as it is
     * and needs none.
     */
    @Override
    public boolean representative(String clause, Tx value, SqlAst.@com.legend.base.Nullable Expr expr) {
        return value.kind() == Kind.STRING && (clause.equals("DISTINCT") || clause.equals("UNION")
                || clause.equals("GROUP BY") && (expr == null || keys.shown(expr)));
    }

    /** A UNION's column of strings whose values may share a collation key ({@link IqKeys#own}) is keyed by the
     *  collation and shows one value of each group (IQ island, 2026-10-15, the user's rule); any other none. */
    @Override
    public java.util.function.@com.legend.base.Nullable UnaryOperator<ValueSpecification> setKey(
            SqlAst.SetOperation s, int i, Tx value) {
        return s.op() == SqlAst.SetOp.UNION && value.kind() == Kind.STRING && !keys.own(s, i) ? key(value.nullable())
                : null;
    }

    /** The collation key of a string that may be NULL ({@code nullable}): NULL's is NULL. */
    private java.util.function.UnaryOperator<ValueSpecification> key(boolean nullable) {
        return x -> SqlToPure.valueOf(SqlToPure.strict(Kind.STRING, List.of(Tx.value(x, Kind.STRING, nullable)),
                v -> collated(v.get(0))));
    }

    /** IQ island, 2026-10-14 (the merge): the statement's strings' keys, for the aggregates ({@link IqAggregates}). */
    IqKeys keys() {
        return keys;
    }

    /** IQ island, 2026-10-14 (the merge): whether {@code e} is shown as it is (a select item of a SELECT without
     *  DISTINCT that nothing else in the statement reads: review 4's B2, {@link #shownOnly}): a NUMERIC quotient (an
     *  AVG's) is rounded only there. */
    boolean shownValue(SqlAst.Expr e) {
        return shownValues.contains(e);
    }

    // ------------------------------------------------------------------ row limits

    /**
     * A row limit's count, as {@link IqParser} reads one: TOP's ({@code LIMIT}), or START AT's less one
     * ({@code OFFSET}). It is a constant: integers and {@code + - *}, which IQ types and checks as it does any constant
     * arithmetic (a result out of its type's range is IQ's error -158), and which the planner folds. A literal
     * {@code TOP 0}, a negative TOP and a START AT below 1 are IQ's error -674 (the research's D-Q1: a TOP expression
     * worth 0 gives no rows). Without ORDER BY the rows a limit keeps are its plan's choice, in IQ as in lite's
     * database: any is an answer IQ could give (IQ island, 2026-10-15).
     */
    @Override
    public ValueSpecification rowCount(SqlAst.Expr count, String clause) {
        boolean offset = clause.equals("OFFSET");
        String iq = offset ? "START AT" : "TOP";
        if (!offset && count instanceof SqlAst.IntegerLit n && n.value().signum() == 0) {
            throw SqlTranslationException.invalid("a literal TOP 0 is IQ's error" + code(-674, "09W07"),
                    count.pos());
        }
        IqType type = constant(count);
        if (type.hi().signum() < 0) {
            throw SqlTranslationException.invalid(iq + (offset ? " counts from 1" : " cannot be negative")
                    + code(-674, "09W07"), count.pos());
        }
        if (!limits.containsKey(count)) {
            throw new IllegalStateException("a row limit the statement does not hold: " + count);
        }
        // IQ island, 2026-10-15 (the user's rule: any answer IQ could give is right): without ORDER BY the rows kept
        // are the plan's choice, IQ's as the database's -- read as written
        return pure(count);
    }

    /** The start of each error the island raises in the database by name (IQ island, 2026-10-14, review 7's m3): a
     *  refusal whose test the database computes ({@link #LIKE_PATTERN_RAISE}), read by the parity harness as a
     *  refusal. */
    public static final String REFUSED_AT_RUN_TIME = "the #IQ{...}# island refuses this at run time: ";

    /** A row limit's constant, typed and checked as IQ types constant arithmetic: its type, whose range is its
     *  value. */
    private static IqType constant(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.IntegerLit n -> IqTypes.ofIntegerConstant(n.value());
            case SqlAst.Binary b -> {
                IqType l = constant(b.left());
                IqType r = constant(b.right());
                String family = java.util.Objects.requireNonNull(IqTypes.arithmetic(b.op(), l.family(), r.family()));
                IqType type = new IqType(family);
                BigInteger[] range = IqTypes.range(b.op(), l, r);
                if (range[0].compareTo(type.min()) < 0 || range[1].compareTo(type.max()) > 0) {
                    throw SqlTranslationException.invalid("the row limit's arithmetic is out of range for "
                            + family + overflowCode(), b.pos());
                }
                yield IqType.integer(family, range[0], range[1], false);
            }
            default -> throw new IllegalStateException("not a row limit's count: " + e);
        };
    }

    /** A row limit's constant as Pure arithmetic, for the planner to fold. */
    private static ValueSpecification pure(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.IntegerLit n -> SqlToPure.integer(n.value());
            case SqlAst.Binary b -> AppliedFunction.infixRun(b.op() == SqlAst.BinaryOp.PLUS ? "plus"
                    : b.op() == SqlAst.BinaryOp.MINUS ? "minus" : "times", List.of(pure(b.left()), pure(b.right())));
            default -> throw new IllegalStateException("not a row limit's count: " + e);
        };
    }

    /** IQ island, 2026-10-16 (the landing's review; measured on IQ 17.1 and SQL Anywhere 17,
     *  {@code lite/corpus_gaps.yaml}'s order_by cases): an integer past an INT's range is no position but a constant,
     *  which orders nothing. */
    @Override
    public boolean ordersNothing(SqlAst.Expr key) {
        return pastAnInt(key);
    }

    private static boolean pastAnInt(SqlAst.Expr key) {
        return key instanceof SqlAst.IntegerLit n && n.value().bitLength() > 31;
    }

    /** IQ island, 2026-10-16 (the user's decision; measured on IQ 17.1 and SQL Anywhere 17, the TOP cases of
     *  {@code lite/corpus_gaps.yaml}): a statement with TOP, FIRST or START AT computes the items its ORDER BY does not
     *  read for the rows it keeps, so one that can raise raises for those rows only. Under IQ's engine, the rows up to
     *  its last, those START AT skips too; in SQL Anywhere's front end (and in SQL Anywhere 17), the rows it returns. */
    @Override
    public RowLimitItems rowLimitItems(SqlAst.Select s) {
        return engine == Engine.IQ ? RowLimitItems.ROWS_UP_TO_THE_LAST : RowLimitItems.ROWS_RETURNED;
    }

    /** IQ island, 2026-10-15 (the corpus lane's judged runs: sede-51725, sede-52800; measured on IQ 17.1): an
     *  ORDER BY's unqualified name reads a select item only by the item's explicit alias -- ORDER BY id over SELECT
     *  t.id, where t and u both hold id, is IQ's -144, over SELECT t.id AS id t's id; any other name is a column of the
     *  FROM clause. A star's outputs are columns too. */
    @Override
    public boolean ordersByOutput(SqlAst.Select s, String output) {
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x && x.alias() != null
                    && IqLexer.lower(x.alias().text()).equals(IqLexer.lower(output))) {
                return true;
            }
        }
        return false;
    }

    // ------------------------------------------------------------------ IQ's refusals, in this dialect's words

    @Override
    public String message(Problem problem, Object... args) {
        return switch (problem) {
            case NO_SUCH_QUALIFIED_COLUMN -> "no column '" + args[1] + "' in '" + args[0] + "'" + code(-143, "52003");
            case NO_SUCH_COLUMN -> "no column '" + args[0] + "' in the query's tables" + code(-143, "52003");
            case AMBIGUOUS_COLUMN -> "the column '" + args[0] + "' is in more than one of the query's tables: qualify"
                    + " it" + code(-144, "52002");
            case AMBIGUOUS_TABLE, DUPLICATE_TABLE -> "two of the query's tables are named '" + args[0] + "'"
                    + code(-139, "52012");
            case NO_SUCH_TABLE -> "no table or correlation name '" + args[0] + "' in the query" + code(-142, "52W02");
            // IQ island, 2026-10-14 (the third landing, measured): IQ's codes for these three -- -149 in the select
            // list and HAVING, -854 in ORDER BY (as IqAggregates' own check)
            // IQ island, 2026-10-17 (the corpus lane's SUM(COUNT(*)); measured on IQ 17.1): its -156
            case NESTED_AGGREGATE -> "an aggregate's argument holds an aggregate (aggregate calls cannot be nested)"
                    + code(-156, "42W08");
            case NOT_GROUPED -> args.length > 1 && outerGroupRefs.contains(args[1]) ? "'" + args[0] + "', read in a"
                    + " subquery, is inside a GROUP BY expression but no GROUP BY item itself: an outer reference of"
                    + " a scalar subquery must appear in GROUP BY" + code(-1005028, "QTA28")
                    : "'" + args[0] + "' is neither grouped nor aggregated: it must be a GROUP BY item, or"
                    + " inside an aggregate" + (args.length > 1 && orderRefs.contains(args[1]) ? code(-854, "42W65")
                    : args.length > 1 && orderFirst.contains(args[1]) ? "; so is a column the ORDER BY reads, which IQ"
                    + " reports first" + code(-854, "42W65") : code(-149, "53003"));
            case DUPLICATE_USING_COLUMN -> "the column '" + args[0] + "' is twice in the join's " + args[1] + " side";
            case NO_SUCH_USING_COLUMN -> "the join's " + args[1] + " side has no column '" + args[0] + "'";
            case NO_SUCH_WINDOW -> "no window named '" + args[0] + "'";
            case DISTINCT_ORDER_KEY -> "the ORDER BY key is none of the SELECT DISTINCT's items" + code(-854, "42W65");
            case SET_WIDTHS -> setWidths(String.valueOf(args[0])) + code(-153, "53026");
        };
    }
}
