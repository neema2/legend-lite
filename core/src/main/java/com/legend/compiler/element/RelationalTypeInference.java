// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.element;
import com.legend.builtin.DynaFn;
import com.legend.compiler.StoreLookups;

import com.legend.model.DatabaseDefinition;
import com.legend.model.RelationalDataType;
import com.legend.model.RelationalOperation;

import java.util.ArrayList;
import java.util.List;

/**
 * The SQL type of a relational operation element — the compiler's typing
 * rule for mapping expressions and view column expressions (the engine's
 * {@code inferRelationalType} / {@code getSafeType} spec,
 * relationalExtension.pure:111-190 and :1906-2010). The metamodel store
 * STAMPS the result on every relational-operation row it seeds
 * ({@code relational_elements.type_id}), the include-closure precedent: the
 * engine recurses over the tree per query; ours reads the compile-time
 * fact as a row. Moved from the legacy walk ({@code MetamodelWalk.inferOp},
 * group F burn 2026-09-02) — the walk keeps a delegating shim for the
 * constructed-instance tests until those instances are rows too.
 *
 * <p>Null = the element has no inferable type (the engine's
 * {@code failOnMatchFailure=false} branch) — a seeded row then has no
 * type row, and {@code inferRelationalType} over it is EMPTY, loud in
 * every {@code toOne()} the corpus writes after it.
 *
 * <p>A view column read through its expression is EXPANDING while its expression is typed: a chain of view columns
 * that reaches one of them again (a view-on-view cycle) is refused, a {@code ModelException} worded as
 * {@link ViewSignatures}' own cycle refusal (2026-11-13; before,
 * the walk recursed until {@code StackOverflowError}). The set is of the view column records
 * themselves, by identity, and a column leaves it once typed: two reads of one column side by side
 * ({@code concat(V.a, V.a)}) are no cycle.
 */
public final class RelationalTypeInference {

    private RelationalTypeInference() {
    }

    /** The type of {@code op}; {@code db} scopes bare column references
     * (a view's column expressions read their own database), {@code ctx}
     * resolves an op's own {@code [db]} qualifier (mapping expressions
     * carry it). */
    public static @com.legend.base.Nullable RelationalDataType infer(RelationalOperation op,
            @com.legend.base.Nullable DatabaseDefinition db,
            @com.legend.base.Nullable StoreLookups ctx) {
        return infer(op, db, ctx, java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>()));
    }

    private static @com.legend.base.Nullable RelationalDataType infer(RelationalOperation op,
            @com.legend.base.Nullable DatabaseDefinition db,
            @com.legend.base.Nullable StoreLookups ctx,
            java.util.Set<DatabaseDefinition.ViewDefinition.ViewColumnMapping> expanding) {
        return switch (op) {
            case RelationalOperation.ColumnRef c ->
                    columnType(db, ctx, c.databaseName(), c.table(), c.column(), expanding);
            // a function's name is matched exactly, as the engine's map get matches it (relationalExtension.pure:114,
            // `$dfToReturnFunctionMap->get($d.name)`): each arm spells the map's key; a name spelled otherwise (ABS, Sum)
            // has no rule, no type. Until 2026-11-14 lower-cased first
            case RelationalOperation.FunctionCall f -> switch (f.name()) {
                // aggregates carry their argument's type (engine
                // inferDynaFunctionReturnType: max/min/sum/avg family); max
                // and min over SEVERAL arguments are the safe type over all
                // of them when one exists, else the first's -- the engine's
                // two rules (relationalExtension.pure:1151-1197); until
                // 2026-11-13 the first's alone
                case "max", "min" -> {
                    List<RelationalDataType> types = new ArrayList<>(f.args().size());
                    for (var arg : f.args()) {
                        types.add(infer(arg, db, ctx, expanding));
                    }
                    RelationalDataType first = types.isEmpty() ? null : types.get(0);
                    if (types.size() <= 1) {
                        yield first;
                    }
                    // an argument with no type leaves no type: the engine's rule maps every argument through the
                    // failOnMatchFailure=true inferRelationalType (relationalExtension.pure:120-123; its assert
                    // :116), which fails on it (2026-11-15; until then it dropped out, typing the rest). The safe
                    // type is the engine's fold (2026-11-13)
                    if (types.contains(null)) {
                        yield null;
                    }
                    RelationalDataType safe = safeTypeOf(types);
                    yield safe != null ? safe : first;
                }
                case "distinct" ->
                        f.args().isEmpty() ? null
                                : infer(f.args().get(0), db, ctx, expanding);
                // sum by its first argument's raw kind, the engine's five rules (relationalExtension.pure:1717-1741):
                // TINYINT, SMALLINT and INTEGER an INTEGER, BIGINT a BIGINT, a DECIMAL or a NUMERIC its own kind with its
                // precision capped at 31, FLOAT and DOUBLE a DOUBLE; any other kind (REAL among them) matches no rule, no
                // type. Until 2026-11-13 its first argument's type, a FLOAT or REAL one DOUBLE (the
                // fix's first version read four rules, its fifth missed, and typed a FLOAT or DOUBLE nothing)
                case "sum" -> {
                    RelationalDataType at = f.args().isEmpty() ? null
                            : infer(f.args().get(0), db, ctx, expanding);
                    yield at == null ? null : switch (kind(at)) {
                        case TINYINT, SMALLINT, INTEGER -> new RelationalDataType.Integer_();
                        case BIGINT -> new RelationalDataType.BigInt();
                        case DECIMAL, NUMERIC -> capped(at);
                        case FLOAT, DOUBLE -> new RelationalDataType.Double_();
                        case REAL, BIT, VARCHAR, CHAR, BINARY, VARBINARY, TIMESTAMP, DATE, TIME, TIMESTAMP_NS, DISTINCT,
                                OTHER, SEMISTRUCTURED, ARRAY, OBJECT -> null;
                    };
                }
                // average and divide are DOUBLE whatever their arguments, which the engine's one rule each does not read
                // (relationalExtension.pure:291-300, :693-702); until 2026-11-13 average kept its first argument's type,
                // a FLOAT or REAL one DOUBLE, and divide was the safe type over its arguments
                case "average", "divide" -> new RelationalDataType.Double_();
                // count is BIGINT at any arity (relationalExtension.pure:543-551); until 2026-11-13 INTEGER
                case "count" -> new RelationalDataType.BigInt();
                // boolean dynafunctions spell BIT (engine H2). like has no rule in the engine's map, so no type
                // (relationalExtension.pure's getDynaFunctionTypeInferenceMap, asked of the engine: EMPTY); until
                // 2026-11-14 BIT, a rule only lite had
                case "and", "or", "not", "equal", "notEqual", "in",
                        "isNull", "isNotNull", "greaterThan", "lessThan",
                        "greaterThanEqual", "lessThanEqual", "isEmpty",
                        "isNotEmpty", "startsWith", "endsWith",
                        "contains" -> new RelationalDataType.Bit();
                // sqlNull carries the engine's OTHER type
                case "sqlNull" -> new RelationalDataType.Other();
                // engine inferDynaFunctionReturnType: sqlTrue/sqlFalse
                // are BIT literals (ledger cluster 56)
                case "sqlTrue", "sqlFalse" -> new RelationalDataType.Bit();
                // substring, left, right, trim, ltrim and rtrim are a VARCHAR sized by their first argument's
                // CHAR or VARCHAR (relationalExtension.pure:1051-1059, :1121-1129, :1509-1517, :1547-1555,
                // :1647-1655, :1853-1861: a match with those two arms alone, so another first argument has no type);
                // until 2026-11-13 they kept their first argument's type
                case "substring", "left", "right", "trim", "ltrim", "rtrim" -> {
                    RelationalDataType at = f.args().isEmpty() ? null
                            : infer(f.args().get(0), db, ctx, expanding);
                    yield at instanceof RelationalDataType.Varchar v ? new RelationalDataType.Varchar(v.size())
                            : at instanceof RelationalDataType.Char_ c ? new RelationalDataType.Varchar(c.size())
                            : null;
                }
                // toUpper and toLower keep their first argument's type (:1793-1801, :1843-1851); upper and lower have no
                // rule in the engine's map, so no type (until 2026-11-13 they kept their first argument's type)
                case "toUpper", "toLower" ->
                        f.args().isEmpty() ? null
                                : infer(f.args().get(0), db, ctx, expanding);
                // length and indexOf are INTEGER whatever their arguments (relationalExtension.pure:1061-1069,
                // :961-969). position, a registered dynafunction that returns an integer, is INTEGER as the database
                // returns it, though the engine's map has no rule for it (register row S500, 2026-11-15); charindex and
                // locate are not registered dynafunctions and have no rule (asked of the engine: EMPTY); until
                // 2026-11-14 INTEGER, rules only lite had
                case "length", "indexOf", "position" -> new RelationalDataType.Integer_();
                // sub over numeric operands is the database's promotion over every argument, as plus is (register row
                // S500, 2026-11-15); over others the engine's rule, its first argument's type when its first two have
                // one raw kind, else no rule (relationalExtension.pure:1637-1645), as their safe type where one is
                // possible ({@link #operator}). Until 2026-11-13 the safe type over all its arguments, with an INTEGER
                // default
                case "sub" -> operator(DynaFn.SUB, f.args(), db, ctx, expanding);
                // joinStrings is a VARCHAR of the larger of 4000 and its first argument's CHAR or VARCHAR size; any other
                // first argument, or none, matches no rule, no type (relationalExtension.pure:1031-1039, asked of the
                // engine). A joinStrings(...) a Database text writes is a DynaFunction, typed by the map: the
                // inferRelationalType arm for the JoinStrings OPERATION (:173, its strings', prefix's, separator's and
                // suffix's sizes summed) types an element only the engine's query translation builds (pureToSQLQuery),
                // never a parsed one. Until 2026-11-14 VARCHAR(4000) whatever its arguments
                case "joinStrings" -> {
                    RelationalDataType at = f.args().isEmpty() ? null : infer(f.args().get(0), db, ctx, expanding);
                    yield at != null && isCharOrVarchar(at) ? new RelationalDataType.Varchar(Math.max(4000, size(at)))
                            : null;
                }
                // string concatenation SUMS the CHAR and VARCHAR operands' sizes (the engine's one rule, getSize over
                // every operand, relationalExtension.pure:463-471). An operand with no type leaves no type: the
                // engine's map is failOnMatchFailure=true (:120-123), which fails on it (2026-11-15; until then it
                // dropped out). An operand of another kind is concatenated as its text, so the result is still a
                // VARCHAR, where the engine's rule (every operand a CHAR or VARCHAR) has none: its size is the string
                // operands' alone, a floor, as before 2026-11-13 (register row S500).
                // group_concat has no rule in the engine's map (no type: until 2026-11-13 it was typed as a
                // concat)
                case "concat" -> {
                    List<RelationalDataType> types = everyType(f.args(), db, ctx, expanding);
                    if (types == null) {
                        yield null;
                    }
                    int size = 0;
                    for (RelationalDataType t : types) {
                        size += isCharOrVarchar(t) ? size(t) : 0;
                    }
                    yield new RelationalDataType.Varchar(size);
                }
                // case(c1, v1, ..., else): the engine's safe type over the value branches and the last
                // argument (relationalExtension.pure:402-411: the branches at 1, 3, ... below 2 * floor((n - 1) / 2),
                // then the last). Where none is possible the engine ASSERTS, whatever failOnMatchFailure is ('Type
                // inference not supported yet! Dyna function: case'): its inference fails where the engine runs it,
                // at plan generation (relationalMappingExecution.pure:205-208, a TDS column's source type, :96, :101),
                // never when the database compiles: there each view column is a VARCHAR at first pass
                // (HelperRelationalBuilder.java:478-480), which a #>{DB.V}# accessor exposes as Varchar
                // (RelationalCompilerExtension.java:954-964), the inference not run. Lite
                // runs the inference when the model compiles (ViewSignatures, KnowledgeLayer) and when it seeds the
                // metamodel (RelationalOpRows), where a failure would refuse or break what the engine compiles: it
                // answers no type there, a form lite cannot spell (2026-11-13; register row S501). A branch with no
                // type leaves no type, as the engine's failOnMatchFailure=true inference fails on it (:120-123;
                // until 2026-11-15 it dropped out). Numeric branches alone (an OTHER, a NULL, among them) are the
                // database's promotion over all of them, as plus is (register row S500, 2026-11-15; the engine's safe
                // type has none for a BIGINT and a FLOAT, or for a REAL and a REAL)
                case "case" -> {
                    List<RelationalDataType> types = new ArrayList<>();
                    for (int i = 1; i < f.args().size(); i += 2) {
                        types.add(infer(f.args().get(i), db, ctx, expanding));
                    }
                    if (f.args().size() % 2 == 1) {
                        types.add(infer(f.args().get(f.args().size() - 1), db, ctx, expanding));
                    }
                    if (types.contains(null)) {
                        yield null;
                    }
                    RelationalDataType numeric = promotion(types);
                    yield numeric != null ? numeric : safeTypeOf(types);
                }
                // the numeric operators plus, minus and times over numeric operands are the database's promotion
                // over every argument ({@link #operator}): what the database returns, which legend-engine's rules
                // misstate -- plus and minus
                // over several arguments are their FIRST argument's type when a safe type is possible, else no rule
                // (relationalExtension.pure:1408-1436, :1209-1218), so plus(INTEGER, FLOAT) is the engine's INTEGER
                // where the database returns 1.5; times is the safe type of its first two arguments, the rest unread
                // (:1773-1782). Lite diverges (register row S500, 2026-11-15). Over a string or date operand, the
                // engine's rule ({@link #operator}). divide is DOUBLE, above.
                case "plus" -> {
                    if (f.args().size() == 1) {
                        // plus over one argument widens it, the engine's rule (relationalExtension.pure:1408-1436):
                        // wider than the database's answer, never a misstated value
                        RelationalDataType at = infer(f.args().get(0), db, ctx, expanding);
                        yield at == null ? null : switch (kind(at)) {
                            case TINYINT, SMALLINT -> new RelationalDataType.Integer_();
                            case INTEGER, BIGINT -> new RelationalDataType.BigInt();
                            case DECIMAL, NUMERIC -> capped(at);
                            case FLOAT, DOUBLE -> new RelationalDataType.Double_();
                            case REAL, BIT, VARCHAR, CHAR, BINARY, VARBINARY, TIMESTAMP, DATE, TIME, TIMESTAMP_NS,
                                    DISTINCT, OTHER, SEMISTRUCTURED, ARRAY, OBJECT -> null;
                        };
                    }
                    yield operator(DynaFn.PLUS, f.args(), db, ctx, expanding);
                }
                case "minus", "times" -> operator(DynaFn.of(f.name()).orElseThrow(), f.args(), db, ctx, expanding);
                // ---- the 30 rules the seeded Database texts use (2026-11-14; until then no rule, no type).
                // Each is legend-engine
                // 4.145.0's getDynaFunctionTypeInferenceMap entry (relationalExtension.pure, the lines cited), and every
                // arm's answer was ASKED of the engine, not only read: the pinned release's own generated
                // inferRelationalType over each case, the
                // same 123 cases through the legend-lsp daemon too. Where the engine's inference fails (a toOne over no
                // type, an at() past the arguments, a match with no arm), lite answers no type, as for every inference the
                // engine fails where it is asked.
                //
                // One type whatever the arguments, which the rule never reads (so an argument no rule types, or none at
                // all, leaves it typed): bitAnd :321-329, bitShiftLeft :351-359, bitXor :371-379, dayOfMonth :633-641,
                // dayOfYear :663-671, mod :1219-1227 (the map's second mod, :1479-1487, the same rule), monthNumber
                // :1259-1267, sign :1587-1595, weekOfYear :1883-1891 an INTEGER; cbrt :415-423, log :1101-1109, sin
                // :1597-1605, sqrt :1687-1695 a DOUBLE; datePart :623-631, firstDayOfMonth :763-771, firstDayOfQuarter
                // :773-781, firstDayOfWeek :813-821, firstDayOfYear :823-831 a DATE; firstHourOfDay :833-841,
                // firstMinuteOfHour :843-851, firstSecondOfMinute :853-861 a TIMESTAMP; convertVarchar128 :503-511 a
                // VARCHAR(128), md5 :1165-1173 a VARCHAR(32), sha256 :1577-1585 a VARCHAR(64)
                case "bitAnd", "bitShiftLeft", "bitXor", "dayOfMonth", "dayOfYear", "mod", "monthNumber", "sign",
                        "weekOfYear" -> new RelationalDataType.Integer_();
                case "cbrt", "log", "sin", "sqrt" -> new RelationalDataType.Double_();
                case "datePart", "firstDayOfMonth", "firstDayOfQuarter", "firstDayOfWeek", "firstDayOfYear" ->
                        new RelationalDataType.Date_();
                case "firstHourOfDay", "firstMinuteOfHour", "firstSecondOfMinute" -> new RelationalDataType.Timestamp();
                case "convertVarchar128" -> new RelationalDataType.Varchar(128);
                case "md5" -> new RelationalDataType.Varchar(32);
                case "sha256" -> new RelationalDataType.Varchar(64);
                // abs is its one argument's type, whatever its kind (:193-201, `$params->toOne()->inferRelationalType()`):
                // with no argument or several the engine's toOne fails, no type
                case "abs" -> f.args().size() == 1 ? infer(f.args().get(0), db, ctx, expanding) : null;
                // add's two rules over its first two arguments, the rest unread (:213-225; the first rule that matches
                // answers): when a safe type of the two is possible and is a VARCHAR, a VARCHAR of their two sizes summed
                // (getSize reads a CHAR's or a VARCHAR's alone: an OTHER, as sqlNull(), fails it, no type); else when one
                // is possible, the safe type; else no rule. Each of the two must be typed (the engine's toOne), and there
                // must be two (its at(1))
                case "add" -> {
                    if (f.args().size() < 2) {
                        yield null;
                    }
                    RelationalDataType t0 = infer(f.args().get(0), db, ctx, expanding);
                    RelationalDataType t1 = infer(f.args().get(1), db, ctx, expanding);
                    if (t0 == null || t1 == null) {
                        yield null;
                    }
                    RelationalDataType safe = safeTypeOf(List.of(t0, t1));
                    if (!(safe instanceof RelationalDataType.Varchar)) {
                        yield safe;
                    }
                    yield isCharOrVarchar(t0) && isCharOrVarchar(t1) ? new RelationalDataType.Varchar(size(t0) + size(t1))
                            : null;
                }
                // coalesce is the safe type over every argument when one is possible, else no rule (:453-461, its
                // `$params->map(p | $p->inferRelationalType())`, failOnMatchFailure true): an argument no rule types fails
                // the engine's inference, so no type, as for every rule here that reads it (max, min, case, concat,
                // plus, minus, times, sub) -- and no argument at all fails its fold's head, no type
                case "coalesce" -> {
                    List<RelationalDataType> types = everyType(f.args(), db, ctx, expanding);
                    yield types == null ? null : safeTypeOf(types);
                }
                // if is the safe type of its second and third arguments when one is possible, else no rule (:941-949): the
                // condition and any fourth argument unread; each of the two must be typed (toOne), and there must be three
                case "if" -> {
                    if (f.args().size() < 3) {
                        yield null;
                    }
                    RelationalDataType t1 = infer(f.args().get(1), db, ctx, expanding);
                    RelationalDataType t2 = infer(f.args().get(2), db, ctx, expanding);
                    yield t1 == null || t2 == null ? null : safeTypeOf(List.of(t1, t2));
                }
                // lpad is a VARCHAR sized by its second argument when that is an integer literal (:1131-1139: a match
                // with that one arm), its first argument unread; any other second argument (a column, a string, a float,
                // a call) or none fails the match, no type. Lite's grammar reads every integer constant as a Long; one
                // past an int is past the engine's own parser too (it reads an integer constant with Integer.parseInt,
                // RelationalParseTreeWalker.visitConstant) and past a lite VARCHAR's size: no type
                case "lpad" -> {
                    Object v = f.args().size() >= 2 && f.args().get(1) instanceof RelationalOperation.Literal l
                            ? l.value() : null;
                    yield v instanceof Integer i ? new RelationalDataType.Varchar(i)
                            : v instanceof Long g && g == g.intValue() ? new RelationalDataType.Varchar(g.intValue())
                            : null;
                }
                // replace is a VARCHAR of twice its first argument's CHAR or VARCHAR size (:1489-1497); any other first
                // argument, or none, fails the match, no type. Java's int doubling, as concat's sum (the engine's Integer
                // does not wrap: a size past 2^30 is the form lite cannot spell)
                case "replace" -> {
                    RelationalDataType at = f.args().isEmpty() ? null : infer(f.args().get(0), db, ctx, expanding);
                    yield at != null && isCharOrVarchar(at) ? new RelationalDataType.Varchar(size(at) * 2) : null;
                }
                default -> null;
            };
            case RelationalOperation.JoinNavigation j -> {
                if (j.terminal() == null) {
                    yield null;
                }
                // the nav's own [DB] qualifier scopes its TERMINAL (the
                // terminal ColumnRef is stored db-relative — adjudication
                // ledger cluster 8: dropping it left the column untyped
                // and concat inferred VARCHAR(n+0))
                DatabaseDefinition sub = db;
                if (j.databaseName() != null && ctx != null) {
                    var jdb = ctx.findDatabase(j.databaseName()).orElse(null);
                    if (jdb != null) {
                        sub = jdb;
                    }
                }
                yield infer(j.terminal(), sub, ctx, expanding);
            }
            case RelationalOperation.Group g -> infer(g.inner(), db, ctx, expanding);
            case RelationalOperation.Literal l -> switch (l.value()) {
                case String str ->
                        new RelationalDataType.Varchar(str.length());
                case Integer ignored -> new RelationalDataType.Integer_();
                case Long ignored -> new RelationalDataType.Integer_();
                // a float literal is FLOAT (relationalExtension.pure:148); until 2026-11-13 DOUBLE
                case Double ignored -> new RelationalDataType.Float_();
                default -> null;
            };
            // boolean expressions spell BIT (engine H2 boolean SQL type)
            case RelationalOperation.Comparison ignored ->
                    new RelationalDataType.Bit();
            case RelationalOperation.BooleanOp ignored ->
                    new RelationalDataType.Bit();
            case RelationalOperation.IsNull ignored ->
                    new RelationalDataType.Bit();
            case RelationalOperation.IsNotNull ignored ->
                    new RelationalDataType.Bit();
            default -> null;
        };
    }

    /** plus, minus, times and sub over several arguments (and minus, times and sub over one). An argument with no type
     *  leaves no type, whichever arguments the engine's rule reads (register row S500, 2026-11-15; until then it
     *  dropped out and the rest were typed). Numeric operands alone (an OTHER, a NULL, among them) are the database's
     *  {@link #promotion}, the ruled divergence. Any other operand is typed by the engine's rule, as its safe type
     *  where the engine's answer is its first argument's (a type at least as wide, as lite typed it until
     *  2026-11-13):
     *  <ul>
     *    <li>plus and minus: over several, their safe type when one is possible, else no rule
     *        (relationalExtension.pure:1408-1436, :1209-1218: the first's), so plus over a VARCHAR(10) and a
     *        VARCHAR(20) is a VARCHAR(20);</li>
     *    <li>times: the safe type of its first two when one is possible, the rest unread, else no rule (:1773-1782);
     *        </li>
     *    <li>sub: when its first two have one raw kind, their safe type, or the first's where the kind has no safe
     *        partner (a TIME), else no rule (:1637-1645: the first's).</li>
     *  </ul>
     *  So {@code sub(DATE, DATE)} and {@code minus(TIMESTAMP, TIMESTAMP)} keep their kind, and a numeric operand
     *  against a string or a date has no type, as the engine's safe type has none. */
    private static @com.legend.base.Nullable RelationalDataType operator(DynaFn op, List<RelationalOperation> args,
            @com.legend.base.Nullable DatabaseDefinition db, @com.legend.base.Nullable StoreLookups ctx,
            java.util.Set<DatabaseDefinition.ViewDefinition.ViewColumnMapping> expanding) {
        List<RelationalDataType> types = everyType(args, db, ctx, expanding);
        if (types == null) {
            return null;
        }
        RelationalDataType numeric = promotion(types);
        if (numeric != null) {
            return numeric;
        }
        boolean several = types.size() > 1;
        return switch (op) {
            case PLUS, MINUS -> several ? safeTypeOf(types) : null;
            case TIMES -> several ? safeTypeOf(types.subList(0, 2)) : null;
            case SUB -> {
                if (!several || kind(types.get(0)) != kind(types.get(1))) {
                    yield null;
                }
                RelationalDataType safe = safeTypeOf(types.subList(0, 2));
                yield safe != null ? safe : types.get(0);
            }
            default -> throw new IllegalStateException("not an operator: " + op);
        };
    }

    /** The database's numeric promotion over every type (the operators and case, register row S500, 2026-11-15),
     *  when every type is numeric or an OTHER (a NULL, which takes the others' type) and one is numeric; null
     *  otherwise. Any approximate operand (FLOAT, DOUBLE, REAL) makes a DOUBLE, but operands of one approximate kind
     *  keep it (2026-11-15): two FLOATs a FLOAT, the engine's answer; two REALs a REAL, lite's until 2026-11-13 (the
     *  engine has none). Else a DECIMAL or NUMERIC operand is the answer, two of them a DECIMAL of the larger integer
     *  digits plus the larger scale; else the widest integer kind. */
    private static @com.legend.base.Nullable RelationalDataType promotion(List<RelationalDataType> types) {
        RelationalDataType acc = null;
        for (RelationalDataType t : types) {
            if (t instanceof RelationalDataType.Other) {
                continue;
            }
            if (!isNumeric(t)) {
                return null;
            }
            acc = acc == null ? t : promote(acc, t);
        }
        return acc;
    }

    private static boolean isNumeric(RelationalDataType t) {
        return switch (kind(t)) {
            case TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, REAL, DECIMAL, NUMERIC -> true;
            case BIT, VARCHAR, CHAR, BINARY, VARBINARY, TIMESTAMP, DATE, TIME, TIMESTAMP_NS, DISTINCT, OTHER,
                    SEMISTRUCTURED, ARRAY, OBJECT -> false;
        };
    }

    private static RelationalDataType promote(RelationalDataType a, RelationalDataType b) {
        Kind ka = kind(a);
        Kind kb = kind(b);
        if (approximate(ka) || approximate(kb)) {
            return ka == kb ? a : new RelationalDataType.Double_();
        }
        boolean da = ka == Kind.DECIMAL || ka == Kind.NUMERIC;
        boolean dbx = kb == Kind.DECIMAL || kb == Kind.NUMERIC;
        if (da != dbx) {
            return da ? a : b;  // an integer operand never widens a DECIMAL or NUMERIC
        }
        if (da) {
            int scale = Math.max(scale(a), scale(b));
            int precision = Math.max(integerDigits(a), integerDigits(b)) + scale;
            return new RelationalDataType.Decimal(precision, scale);
        }
        return INTEGER_WIDTH.indexOf(ka) >= INTEGER_WIDTH.indexOf(kb) ? a : b;
    }

    private static boolean approximate(Kind k) {
        return k == Kind.FLOAT || k == Kind.DOUBLE || k == Kind.REAL;
    }

    private static final List<Kind> INTEGER_WIDTH = List.of(Kind.TINYINT, Kind.SMALLINT, Kind.INTEGER, Kind.BIGINT);

    /** Every argument's type, in order, each typed (the engine's {@code map} over {@code inferRelationalType()}, which
     *  is failOnMatchFailure true); null when any has none, as the engine's inference fails there (coalesce's rule,
     *  2026-11-14). */
    private static @com.legend.base.Nullable List<RelationalDataType> everyType(List<RelationalOperation> args,
            @com.legend.base.Nullable DatabaseDefinition db, @com.legend.base.Nullable StoreLookups ctx,
            java.util.Set<DatabaseDefinition.ViewDefinition.ViewColumnMapping> expanding) {
        List<RelationalDataType> types = new ArrayList<>(args.size());
        boolean untyped = false;
        for (var arg : args) {
            RelationalDataType t = infer(arg, db, ctx, expanding);
            untyped |= t == null;
            types.add(t);
        }
        return untyped ? null : types;
    }

    /** A CHAR or a VARCHAR: the kinds the engine's getSize and its size matches read (:2005-2011). */
    private static boolean isCharOrVarchar(@com.legend.base.Nullable RelationalDataType t) {
        return t instanceof RelationalDataType.Char_ || t instanceof RelationalDataType.Varchar;
    }

    /** plus's and sum's DECIMAL and NUMERIC rules (relationalExtension.pure:1408-1436, :1717-1741): the kind kept, its
     *  precision {@code min([31, precision])}, its scale as it is. Asked of a DECIMAL or NUMERIC alone. */
    private static RelationalDataType capped(RelationalDataType t) {
        if (t instanceof RelationalDataType.Decimal d) {
            return new RelationalDataType.Decimal(Math.min(31, d.precision()), d.scale());
        }
        if (t instanceof RelationalDataType.Numeric n) {
            return new RelationalDataType.Numeric(Math.min(31, n.precision()), n.scale());
        }
        throw new IllegalStateException("a " + kind(t) + " asked for a DECIMAL's or NUMERIC's capped precision");
    }

    // ---- the engine's safe type (2026-11-13): relationalExtension.pure:1926-2033, exactly

    /** A relational type's raw kind: the engine's datatype class, which its safeTypeMap and sub's rule compare. */
    enum Kind {
        TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, REAL, BIT, VARCHAR, CHAR, BINARY, VARBINARY, DECIMAL,
        NUMERIC, TIMESTAMP, DATE, TIME, TIMESTAMP_NS, DISTINCT, OTHER, SEMISTRUCTURED, ARRAY, OBJECT
    }

    static Kind kind(RelationalDataType t) {
        return switch (t) {
            case RelationalDataType.TinyInt ignored -> Kind.TINYINT;
            case RelationalDataType.SmallInt ignored -> Kind.SMALLINT;
            case RelationalDataType.Integer_ ignored -> Kind.INTEGER;
            case RelationalDataType.BigInt ignored -> Kind.BIGINT;
            case RelationalDataType.Float_ ignored -> Kind.FLOAT;
            case RelationalDataType.Double_ ignored -> Kind.DOUBLE;
            case RelationalDataType.Real ignored -> Kind.REAL;
            case RelationalDataType.Bit ignored -> Kind.BIT;
            case RelationalDataType.Varchar ignored -> Kind.VARCHAR;
            case RelationalDataType.Char_ ignored -> Kind.CHAR;
            case RelationalDataType.Binary ignored -> Kind.BINARY;
            case RelationalDataType.Varbinary ignored -> Kind.VARBINARY;
            case RelationalDataType.Decimal ignored -> Kind.DECIMAL;
            case RelationalDataType.Numeric ignored -> Kind.NUMERIC;
            case RelationalDataType.Timestamp ignored -> Kind.TIMESTAMP;
            case RelationalDataType.Date_ ignored -> Kind.DATE;
            case RelationalDataType.Time ignored -> Kind.TIME;
            case RelationalDataType.TimestampNs ignored -> Kind.TIMESTAMP_NS;
            case RelationalDataType.Distinct ignored -> Kind.DISTINCT;
            case RelationalDataType.Other ignored -> Kind.OTHER;
            case RelationalDataType.SemiStructured ignored -> Kind.SEMISTRUCTURED;
            case RelationalDataType.Array ignored -> Kind.ARRAY;
            case RelationalDataType.Object_ ignored -> Kind.OBJECT;
        };
    }

    /** The engine's safeTypeMap (relationalExtension.pure:2014-2033): a kind's safe partners. A kind it does not list
     *  (REAL, BINARY, VARBINARY, TIME, TIMESTAMP_NS, DISTINCT, SEMISTRUCTURED, ARRAY, OBJECT) has none, itself
     *  included. */
    private static List<Kind> partners(Kind k) {
        return switch (k) {
            case TINYINT -> List.of(Kind.TINYINT, Kind.SMALLINT, Kind.INTEGER, Kind.FLOAT, Kind.BIGINT, Kind.DOUBLE,
                    Kind.NUMERIC, Kind.DECIMAL);
            case SMALLINT -> List.of(Kind.SMALLINT, Kind.INTEGER, Kind.FLOAT, Kind.BIGINT, Kind.DOUBLE, Kind.NUMERIC,
                    Kind.DECIMAL);
            case INTEGER -> List.of(Kind.INTEGER, Kind.FLOAT, Kind.BIGINT, Kind.DOUBLE, Kind.NUMERIC, Kind.DECIMAL);
            case BIGINT -> List.of(Kind.BIGINT, Kind.DOUBLE, Kind.NUMERIC, Kind.DECIMAL);
            case FLOAT -> List.of(Kind.FLOAT, Kind.DOUBLE, Kind.NUMERIC, Kind.DECIMAL);
            case DOUBLE -> List.of(Kind.DOUBLE, Kind.NUMERIC, Kind.DECIMAL);
            case NUMERIC -> List.of(Kind.NUMERIC, Kind.DECIMAL);
            case DECIMAL -> List.of(Kind.DECIMAL, Kind.NUMERIC);
            case CHAR, VARCHAR -> List.of(Kind.CHAR, Kind.VARCHAR);
            case DATE -> List.of(Kind.DATE);
            case TIMESTAMP -> List.of(Kind.TIMESTAMP);
            case BIT -> List.of(Kind.BIT);
            case OTHER -> List.of(Kind.TINYINT, Kind.SMALLINT, Kind.INTEGER, Kind.FLOAT, Kind.BIGINT, Kind.DOUBLE,
                    Kind.NUMERIC, Kind.DECIMAL, Kind.CHAR, Kind.VARCHAR, Kind.DATE, Kind.TIMESTAMP, Kind.BIT, Kind.OTHER);
            case REAL, BINARY, VARBINARY, TIME, TIMESTAMP_NS, DISTINCT, SEMISTRUCTURED, ARRAY, OBJECT -> List.of();
        };
    }

    /** {@code safeTypeMap->get(t1).values->contains(t2)}, by raw kind. */
    private static boolean lists(RelationalDataType t1, RelationalDataType t2) {
        return partners(kind(t1)).contains(kind(t2));
    }

    /** The engine's isSafeTypePossible(type1, type2) (:1954-1965). */
    static boolean isSafeTypePossible(RelationalDataType t1, RelationalDataType t2) {
        return lists(t1, t2) || lists(t2, t1);
    }

    /** The engine's getSafeType(type1, type2) (:1967-1985): type2 when type1 lists it, else type1; a CHAR or VARCHAR
     *  safe type is a VARCHAR of the larger size (an OTHER sized 0); a NUMERIC or DECIMAL one a DECIMAL of the larger
     *  integer digits plus the larger scale (getDecimalScale and getDecimalIntegerPrecision: 0 for any other kind).
     *  Asked only where {@link #isSafeTypePossible} holds. */
    static RelationalDataType getSafeType(RelationalDataType t1, RelationalDataType t2) {
        RelationalDataType safe = lists(t1, t2) ? t2 : t1;
        return switch (kind(safe)) {
            case CHAR, VARCHAR -> new RelationalDataType.Varchar(Math.max(size(t1), size(t2)));
            case NUMERIC, DECIMAL -> {
                int scale = Math.max(scale(t1), scale(t2));
                int precision = Math.max(integerDigits(t1), integerDigits(t2)) + scale;
                yield new RelationalDataType.Decimal(precision, scale);
            }
            case TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, REAL, BIT, BINARY, VARBINARY, TIMESTAMP, DATE, TIME,
                    TIMESTAMP_NS, DISTINCT, OTHER, SEMISTRUCTURED, ARRAY, OBJECT -> safe;
        };
    }

    /** The engine's list fold (isSafeTypePossible / getSafeType over types, :1926-1951): from the first type, each next
     *  one against the accumulated safe type ({@code getSafeType(next, acc)}); null when a step has none. A type that
     *  is null (no inferred type) leaves none: the engine's {@code map} over the failOnMatchFailure=true
     *  inferRelationalType fails on it (relationalExtension.pure:120-123, its assert :116; until 2026-11-15 it
     *  dropped out); no types, null. */
    static @com.legend.base.Nullable RelationalDataType safeTypeOf(List<RelationalDataType> types) {
        RelationalDataType acc = null;
        for (RelationalDataType t : types) {
            if (t == null) {
                return null;
            }
            if (acc == null) {
                acc = t;
            } else if (isSafeTypePossible(t, acc)) {
                acc = getSafeType(t, acc);
            } else {
                return null;
            }
        }
        return acc;
    }

    /** A CHAR's or VARCHAR's size, an OTHER's 0 (getSafeType's own rule); no other kind meets a CHAR or VARCHAR safe
     *  type (only CHAR, VARCHAR and OTHER list them). */
    private static int size(RelationalDataType t) {
        if (t instanceof RelationalDataType.Char_ c) {
            return c.size();
        }
        if (t instanceof RelationalDataType.Varchar v) {
            return v.size();
        }
        if (t instanceof RelationalDataType.Other) {
            return 0;
        }
        throw new IllegalStateException("a " + kind(t) + " met a CHAR or VARCHAR safe type: the safeTypeMap lists none");
    }

    /** getDecimalScale (:1987-1994): a NUMERIC's or DECIMAL's scale, any other kind's none (0, the engine's arm). */
    private static int scale(RelationalDataType t) {
        if (t instanceof RelationalDataType.Numeric n) {
            return n.scale();
        }
        if (t instanceof RelationalDataType.Decimal d) {
            return d.scale();
        }
        return 0;
    }

    /** getDecimalIntegerPrecision (:1996-2003): a NUMERIC's or DECIMAL's integer digits, any other kind's none (0). */
    private static int integerDigits(RelationalDataType t) {
        if (t instanceof RelationalDataType.Numeric n) {
            return n.precision() - n.scale();
        }
        if (t instanceof RelationalDataType.Decimal d) {
            return d.precision() - d.scale();
        }
        return 0;
    }

    /** The declared type of {@code table.column}: in the op's OWN database
     * when its {@code [db]} names one, else the scoping db — through the
     * one table lookup (include closure, schema-qualified spellings, exact
     * names); a VIEW's column resolves THROUGH its column expression
     * (view-on-view). The op's own name is tried first as spelled: a
     * legacy surface (the metamodel seed's input) still carries an
     * UNQUALIFIED [db], which resolves to nothing and falls to the scope. */
    private static @com.legend.base.Nullable RelationalDataType columnType(
            @com.legend.base.Nullable DatabaseDefinition scope,
            @com.legend.base.Nullable StoreLookups ctx,
            @com.legend.base.Nullable String opDb,
            String table, String column,
            java.util.Set<DatabaseDefinition.ViewDefinition.ViewColumnMapping> expanding) {
        DatabaseDefinition db = scope;
        if (opDb != null && ctx != null) {
            db = ctx.findDatabase(opDb).orElse(db);
        }
        if (db == null || ctx == null) {
            return null;
        }
        String dbFqn = db.qualifiedName();
        var td = ctx.findTableDefinition(dbFqn, table);
        if (td.isPresent()) {
            for (var c : td.get().columns()) {
                if (c.name().equals(column)) {
                    return c.dataType();
                }
            }
            return null;
        }
        var view = ctx.findView(dbFqn, table);
        if (view.isPresent()) {
            for (var cm : view.get().columnMappings()) {
                if (cm.name().equals(column)) {
                    if (!expanding.add(cm)) {
                        throw new com.legend.error.ModelException(
                                com.legend.error.LegendCompileException.Phase.MODEL,
                                "view '" + view.get().name() + "' of '" + dbFqn + "' reads its own columns"
                                + " (cyclic view-on-view chain)");
                    }
                    try {
                        return infer(cm.expression(), db, ctx, expanding);
                    } finally {
                        expanding.remove(cm);
                    }
                }
            }
        }
        return null;
    }
}
