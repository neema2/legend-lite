// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

import java.util.List;
import java.util.Objects;

/**
 * R2's SQL-side canonical scalar render (docs/CANONICAL_FORM_SPEC.md
 * §2): the DATABASE computes the byte-channel text — tenet #1, the
 * render IS the semantic work; Java's only remaining act on a verdict
 * is comparing two DB-computed byte strings. The kind comes from the
 * STAMP (types drive construction — never runtime sniffing), and every
 * rule mirrors the host reference render ({@code CanonicalForm}), the
 * pair the divergence census holds together.
 *
 * <p>Returns null for kinds the SQL channel does not (yet) claim —
 * the caller falls back to the host lattice and the decline is
 * counted, never silent.
 */
public final class CanonicalRenderSql {

    private CanonicalRenderSql() {
    }

    /** Canonical VARCHAR of a scalar value expression, by STAMPED kind. */
    /** Canon LEAF spellings live in {@link LiteralSpelling} (F10
     * proper slice 1 — one grammar owner); this name survives as the
     * verdict lane's entry. */
    public static @com.legend.Nullable SqlExpr scalarCanon(SqlExpr v, Type t) {
        return LiteralSpelling.leaf(v, t);
    }

    /**
     * V11 — wrap a lowered side plan so the canon rides the SAME query
     * ({@code SELECT value, canon(value) FROM (plan) side}): one
     * execution produces values and canon texts together, deleting the
     * double-execution obligation. Declines (recorded on the rider,
     * plan returned unchanged) when the plan is not a single-column
     * scalar shape or no candidate kind is claimed.
     *
     * <p>An unrefined NUMBER root projects one candidate column per
     * fine kind (the plan's OutputCol type is stamp-derived and cannot
     * name the member — V6-round-2 circularity); the verdict layer
     * selects by runtime value kind. canonicalOrder (assertSameElements)
     * sorts rows by the canon text IN THE DATABASE — declined for
     * multi-candidate sides (no single ordering key exists).
     */
    /** The wrap outcome: a wrapped plan with its candidate kinds, or a
     * decline reason with the plan unchanged. Lowering stays pure of
     * the exec layer (Invariant 6h) — the driver records this on its
     * rider. */
    public record CanonWrap(com.legend.sql.SqlQuery plan,
            List<Type> kinds, boolean many, int literalIndex,
            @com.legend.Nullable String declineReason) {

        static CanonWrap decline(com.legend.sql.SqlQuery plan,
                String reason) {
            return new CanonWrap(plan, List.of(), false, -1, reason);
        }
    }

    public static CanonWrap wrapWithCanon(com.legend.sql.SqlQuery plan,
            com.legend.compiler.element.type.ExprType rootInfo,
            boolean canonicalOrder,
            com.legend.compiler.element.@com.legend.Nullable EqualityKeys
                    instanceKeys) {
        return wrapWithCanon(plan, rootInfo, canonicalOrder, instanceKeys,
                true);
    }

    /** Is a side of this type a NAME-VALUED one — a TYPE value (String,
     * Car, {@code $col.type}: a metamodel type classifier) or a tracked
     * ELEMENT value (a Mapping, a Database)? Such values travel as their
     * bare simple names (the Lowerer's convention for both) and compare
     * by that name — the canon is the name itself, unquoted, disjoint
     * from a string. {@code tracksClassifier} = the model's own answer
     * for element classes. */
    public static boolean nameValued(Type t,
            java.util.function.Predicate<String> tracksClassifier) {
        String fqn = com.legend.compiler.element.EqualityKeys.fqnOf(t);
        return fqn != null
                && (com.legend.compiler.element.type.PlatformTypes.isTypeClassifier(fqn)
                        || tracksClassifier.test(fqn));
    }

    /** {@code literalChannel} false = the canon-exec tunnel's MIDDLE
     * rung: a typed side re-wraps WITHOUT its literal candidate (a
     * stamp-derived column type can lie about the wire — witness the
     * BLOB byte carrier under a STRING stamp — and the unbindable
     * literal must not demote the whole side to the host; the bare
     * candidates still byte-decide). Literal-ONLY sides (Any/JSON)
     * have no bare channel and are unaffected by the flag. */
    public static CanonWrap wrapWithCanon(com.legend.sql.SqlQuery plan,
            com.legend.compiler.element.type.ExprType rootInfo,
            boolean canonicalOrder,
            com.legend.compiler.element.@com.legend.Nullable EqualityKeys
                    instanceKeys,
            boolean literalChannel) {
        return wrapWithCanon(plan, rootInfo, canonicalOrder, instanceKeys, literalChannel, false);
    }

    /** {@code nameValued} = the side is a type / element value ({@link #nameValued}). */
    public static CanonWrap wrapWithCanon(com.legend.sql.SqlQuery plan,
            com.legend.compiler.element.type.ExprType rootInfo,
            boolean canonicalOrder,
            com.legend.compiler.element.@com.legend.Nullable EqualityKeys
                    instanceKeys,
            boolean literalChannel, boolean nameValued) {
        return wrapWithCanon(plan, rootInfo, canonicalOrder, instanceKeys, literalChannel,
                nameValued, null);
    }

    /** {@code enumFrame} = the pair's declared enumeration framing this
     * side when it is untyped (Any) or an abstract Enum: the wire's NAME
     * spells as {@code Enumeration.NAME}. */
    public static CanonWrap wrapWithCanon(com.legend.sql.SqlQuery plan,
            com.legend.compiler.element.type.ExprType rootInfo,
            boolean canonicalOrder,
            com.legend.compiler.element.@com.legend.Nullable EqualityKeys
                    instanceKeys,
            boolean literalChannel, boolean nameValued,
            @com.legend.Nullable String enumFrame) {
        if (plan.outputs().size() != 1) {
            return CanonWrap.decline(plan, "non-scalar plan shape: "
                    + plan.outputs().size() + " columns");
        }
        Type t = rootInfo.type();
        com.legend.sql.OutputCol valueCol = plan.outputs().get(0);
        SqlExpr valueRef = SqlExpr.Column.of(null, valueCol);
        List<Type> candidates;
        List<SqlExpr> canons = new java.util.ArrayList<>();
        int literalIndex = -1;
        String instFqn = com.legend.compiler.element.EqualityKeys.fqnOf(t);
        if (valueCol.type() == SqlType.Scalar.LITERAL) {
            // F10 slice 2 — the KIND-FAITHFUL CARRIER: the cell already
            // IS the canonical pure-literal spelling (LiteralSpelling
            // wrote it at construction), so the canon is the identity
            // and the ONE candidate is the literal channel.
            return new CanonWrap(new com.legend.sql.SqlSelect(
                    List.of(new com.legend.sql.SqlSelect.Projection(
                                    valueRef, valueCol.name(), valueCol),
                            new com.legend.sql.SqlSelect.Projection(
                                    new SqlExpr.Cast(valueRef,
                                            SqlType.Scalar.VARCHAR),
                                    "__canon0",
                                    new com.legend.sql.OutputCol("__canon0",
                                            SqlType.Scalar.VARCHAR, true))),
                    false,
                    new com.legend.sql.SqlSource.Subselect(plan, "side",
                            null),
                    null, List.of(), null, null,
                    canonicalOrder
                            ? List.of(new com.legend.sql.SqlSelect.SortKey(
                                    new SqlExpr.Cast(valueRef,
                                            SqlType.Scalar.VARCHAR),
                                    true, null, null))
                            : List.of(),
                    null, null,
                    List.of(valueCol, new com.legend.sql.OutputCol(
                            "__canon0", SqlType.Scalar.VARCHAR, true))),
                    List.of(t),
                    rootInfo.multiplicity().requireBounded("canon side")
                            .upper() == null
                            || rootInfo.multiplicity()
                                    .requireBounded("canon side").upper() > 1,
                    0, null);
        }
        boolean jsonCol = valueCol.type() == SqlType.Scalar.JSON;
        if (jsonCol || (t instanceof Type.ClassType anyCt
                && com.legend.compiler.element.type.PlatformTypes
                        .isAny(anyCt))) {
            // F10 v1 — an ANY-stamped side, or ANY side riding the JSON
            // carrier (the carrier is the FACT — PureSql's own doctrine:
            // the JSON decision follows the LOWERED shape, never the
            // pure type alone; witness the Number-stamped mixed lists).
            // A JSON cell dispatches the pure-literal canon on its
            // RUNTIME type in the database (anyJsonCanon); an Any stamp
            // over a PLAIN column (a let-bound scalar erased to Any —
            // witness testLetWithParam's VARCHAR 'echo') renders the
            // literal of the COLUMN's kind, a wire fact. The ONE
            // candidate IS the literal channel. Trees mark and the
            // verdict layer declines on sight.
            SqlExpr lit;
            if (enumFrame != null) {
                lit = framedEnumCanon(jsonCol
                        ? SqlExpr.Call.of(SqlFn.VARIANT_GET, valueRef, new SqlExpr.StringLit("$"))
                        : valueRef, enumFrame);
            } else if (jsonCol) {
                lit = anyJsonCanon(valueRef);
            } else {
                Type colKind = Type.kindOfSqlType(valueCol.type());
                SqlExpr lc = colKind == null ? null
                        : literalCanon(valueRef, colKind);
                if (lc == null) {
                    return CanonWrap.decline(plan,
                            "any-carrier: " + valueCol.type());
                }
                lit = lc;
            }
            candidates = List.of(t);
            canons.add(new SqlExpr.Cast(lit, SqlType.Scalar.VARCHAR));
            literalIndex = 0;
        } else if (instFqn != null
                && com.legend.compiler.element.type.PlatformTypes
                        .isMapCarrier(t)) {
            // F12 — the engine's OWN map rule (EqualityUtilities.
            // mapEquals): equal key SETS then per-key values,
            // order-INSENSITIVE — the canon is the SORTED entry list,
            // so order-insensitivity is byte-decidable.
            SqlExpr c = mapCanon(valueRef, valueCol.type(), instFqn);
            if (c == null) {
                return CanonWrap.decline(plan,
                        "map-key-shape: " + valueCol.type());
            }
            candidates = List.of(t);
            canons.add(new SqlExpr.Cast(c, SqlType.Scalar.VARCHAR));
        } else if (t instanceof Type.EnumType aet
                && "meta::pure::metamodel::type::Enum".equals(aet.fqn())) {
            candidates = List.of(t);
            canons.add(framedEnumCanon(valueRef, enumFrame != null ? enumFrame : aet.fqn()));
            literalIndex = 0;
        } else if (instFqn != null && nameValued) {
            // a TYPE / ELEMENT value: the wire holds its bare simple name —
            // the canon IS the name (unquoted: never equal to a string)
            candidates = List.of(t);
            canons.add(new SqlExpr.Cast(valueRef, SqlType.Scalar.VARCHAR));
            literalIndex = 0;
        } else if (instFqn != null
                && com.legend.compiler.element.type.PlatformTypes.isNil(t)) {
            // the []-born BOTTOM type: a Nil side is the EMPTY value —
            // zero rows, the canon column is never read (the frame's
            // empty rule renders '[]'); claim with a placeholder
            candidates = List.of(t);
            canons.add(new SqlExpr.NullLit());
            // …and it IS the literal channel: the one NULL row is dropped
            // at the frame, so a grid peer judges "no rows" in the database
            literalIndex = 0;
        } else if (instFqn != null && valueCol.type() instanceof SqlType.Struct cst
                && hasCanonField(cst)) {
            // F10 proper — a constructed instance carries its canon on the
            // wire (__canon, stamped at its construction site from its
            // own fields and its class's keys): the side's canon IS it
            candidates = List.of(t);
            canons.add(new SqlExpr.Cast(SqlExpr.StructGet.of(valueRef,
                    com.legend.compiler.element.ClassLayouts.SYNTHETIC_CANON),
                    SqlType.Scalar.VARCHAR));
        } else if (instFqn != null) {
            // X5 — a KEYED instance side: the canon is the key-property
            // render (EqualityUtilities compares keyed classes by key
            // properties only), compiled from the model. The DRIVER
            // resolved the key tree — lowering stays model-free
            // (Invariant 6h, same inversion as CanonWrap itself).
            if (instanceKeys == null) {
                // F13 — SYNTHETIC IDENTITY: a keyless class's engine
                // equality is INSTANCE IDENTITY; the identity-bearing
                // layout carries it as the __id field (minted per
                // construction site), so the canon is the identity
                // itself — '_type' + '_id', JSON-framed like the keyed
                // canon. A side whose layout carries no __id (Any/
                // variant wire trees, layoutless classes) stays a
                // counted decline.
                SqlExpr idc = identityCanon(valueRef, instFqn,
                        valueCol.type());
                if (idc == null) {
                    return CanonWrap.decline(plan,
                            "keyless-instance: " + instFqn);
                }
                candidates = List.of(t);
                canons.add(new SqlExpr.Cast(idc, SqlType.Scalar.VARCHAR));
            } else {
                SqlExpr c = instanceCanon(valueRef, instanceKeys,
                        valueCol.type());
                if (c == null) {
                    return CanonWrap.decline(plan, "instance-key-shape: "
                            + instFqn + " layout=" + valueCol.type());
                }
                candidates = List.of(t);
                // the ROOT canon is byte text (nested levels stay
                // JSON-typed so they embed structurally)
                canons.add(new SqlExpr.Cast(c, SqlType.Scalar.VARCHAR));
            }
        } else {
            // an unrefined NUMBER side whose wire is a concrete numeric SQL
            // type takes that kind (the engine reads a cell by its result-set
            // type — Equality.effectiveKind's rule); only an unknown wire
            // keeps the three candidates
            Type wireNumeric = t == Type.Primitive.NUMBER ? Type.kindOfSqlType(valueCol.type()) : null;
            List<Type> bare = t == Type.Primitive.NUMBER
                    ? (wireNumeric == Type.Primitive.INTEGER || wireNumeric == Type.Primitive.FLOAT
                            || wireNumeric == Type.Primitive.DECIMAL
                            ? List.of(wireNumeric)
                            : List.of(Type.Primitive.INTEGER, Type.Primitive.FLOAT,
                                    Type.Primitive.DECIMAL))
                    : List.of(t);
            for (Type k : bare) {
                // JUDGING_TWO_MODES §1: a Float-DECLARED side converts to
                // DOUBLE once before its canon is spelled; an unrefined
                // Number side keeps every candidate on the wire kind
                SqlExpr cell = t == Type.Primitive.FLOAT
                        ? LiteralSpelling.declaredDouble(valueRef) : valueRef;
                SqlExpr c = scalarCanon(cell, k);
                if (c == null) {
                    return CanonWrap.decline(plan, "unclaimed kind: " + k);
                }
                canons.add(c);
            }
            if (canonicalOrder && canons.size() > 1) {
                return CanonWrap.decline(plan,
                        "canonical-order over an unrefined Number side");
            }
            // F10 v1 — the LITERAL candidate (pure-literal spelling,
            // ALWAYS LAST): the channel an Any-involving pair compares
            // in (the verdict layer selects it only then). Single-kind
            // sides only — an unrefined Number's literal is ambiguous.
            candidates = new java.util.ArrayList<>(bare);
            // guarded by the COLUMN kind: a column outside the literal
            // vocabulary (BLOB — the byte wire) must not poison the
            // wrapped query with an unbindable candidate (witness
            // testRepeatStringNoString: replace(BLOB,..) rode the
            // canon-exec tunnel and DEMOTED a bare-decided pair)
            if (literalChannel && bare.size() == 1
                    && Type.kindOfSqlType(valueCol.type()) != null) {
                SqlExpr lit = literalCanon(valueRef, t);
                if (lit != null) {
                    canons.add(new SqlExpr.Cast(lit, SqlType.Scalar.VARCHAR));
                    candidates.add(t);
                    literalIndex = canons.size() - 1;
                }
            }
            if (literalIndex < 0 && System.getenv("LEGEND_LITE_DUMP_SQL") != null) {
                // the SQL dump's companion: WHY a typed side has no literal candidate
                System.err.println("[canon] no literal candidate: kind=" + t
                        + " column=" + valueCol.type() + " literalChannel=" + literalChannel
                        + " bare=" + bare.size());
            }
        }
        var mult = rootInfo.multiplicity().requireBounded("canon side");
        boolean many = mult.upper() == null || mult.upper() > 1;
        List<com.legend.sql.SqlSelect.Projection> projections =
                new java.util.ArrayList<>();
        projections.add(new com.legend.sql.SqlSelect.Projection(
                valueRef, valueCol.name(), valueCol));
        for (int i = 0; i < canons.size(); i++) {
            projections.add(new com.legend.sql.SqlSelect.Projection(
                    canons.get(i), "__canon" + i,
                    new com.legend.sql.OutputCol("__canon" + i,
                            SqlType.Scalar.VARCHAR, true)));
        }
        List<com.legend.sql.SqlSelect.SortKey> sort = canonicalOrder
                ? List.of(new com.legend.sql.SqlSelect.SortKey(
                        canons.get(0), true, null, null))
                : List.of();
        return new CanonWrap(new com.legend.sql.SqlSelect(projections,
                false,
                new com.legend.sql.SqlSource.Subselect(plan, "side", null),
                null, List.of(), null, null, sort, null, null, List.of()),
                candidates, many, literalIndex, null);
    }

    /** The ABSTRACT-Enum / untyped side's canon under a FRAMING enumeration:
     * the wire holds the NAME (an EnumValueMapping's .enum, toDomainValue, a
     * row cell read through the Any carrier); the pair's declared enumeration
     * — or the abstract classifier itself when both sides are abstract (two
     * such sides compare by name, the host's rule) — spells it
     * {@code Enumeration.NAME}. */
    private static SqlExpr framedEnumCanon(SqlExpr nameText, String enumeration) {
        return new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.CONCAT,
                new SqlExpr.StringLit(enumeration + "."),
                new SqlExpr.Cast(nameText, SqlType.Scalar.VARCHAR)), SqlType.Scalar.VARCHAR);
    }

    /** V7 §8 leg 1 — the GRID canon wrap outcome: the plan with a
     * per-ROW canonical text appended as the LAST column, or a decline
     * with the plan unchanged. */
    public record TdsWrap(com.legend.sql.SqlQuery plan,
            @com.legend.Nullable String declineReason) {

        static TdsWrap decline(com.legend.sql.SqlQuery plan,
                String reason) {
            return new TdsWrap(plan, reason);
        }
    }

    /** The row-canon cell separator: the unit-separator control
     * character, reserved — a STRING cell containing it poisons its
     * row canon to NULL (a counted decline downstream), never a
     * silent mis-split. */
    // U+001D (group separator). NOT U+001F: that is RaisedErrors.SENTINEL, the
    // raise envelope's mark — H2 embeds the executed statement in its error
    // messages, so a separator literal in the canon text would be read as an
    // envelope and the real message lost (leg 3.4 step 2 catch, 2026-09-20)
    public static final String TDS_CELL_SEP = "\u001D";

    /** The grid wrap's appended columns: the per-ROW canon (last), and
     * one per-CELL canon per column before it (leg 3.1b). */
    public static final String ROW_CANON = "__rowcanon";
    public static final String CELL_CANON = "__cell";

    /** V7 §8 leg 1 (fusion-spike F2, user-ratified) — wrap a TABULAR
     * plan so every row carries its canonical text: per-cell
     * PURE-LITERAL spellings ({@link LiteralSpelling#literal} — the
     * six disjoint forms, the same grammar the value peer's literal
     * channel spells, so grid cells and literal-list elements meet in
     * ONE spelling) joined by {@link #TDS_CELL_SEP}; a NULL cell
     * spells the golden convention's bare {@code TDSNull} — DISJOINT
     * from a real string 'TDSNull', which spells QUOTED. Declines
     * (named, counted) on late-bound schemas, plan/schema width
     * mismatches (pivot, struct flattening), and unclaimed cell
     * kinds. */
    public static TdsWrap wrapTdsCanon(com.legend.sql.SqlQuery plan,
            Type.@com.legend.Nullable RelationType schema) {
        if (schema == null) {
            return TdsWrap.decline(plan, "tds-canon: no schema view");
        }
        if (schema.isLateBound()) {
            return TdsWrap.decline(plan, "tds-canon: late-bound schema");
        }
        if (plan.outputs().size() != schema.columns().size()
                || plan.outputs().isEmpty()) {
            return TdsWrap.decline(plan, "tds-canon: plan/schema width "
                    + plan.outputs().size() + "/" + schema.columns().size());
        }
        // ASSERT-BOUNDARY determinism (user design, 2026-08-29): results
        // under assertion are deterministically ordered even when the
        // test forgot to sort — positional reads over an unordered
        // relation are otherwise undefined. Always on: a FEATURE of the
        // assert surface, and both verdict channels read one ordered
        // relation by construction. ScanOrder is the one key owner.
        plan = com.legend.sql.ScanOrder.stabilize(plan);
        // VALUE-READ decode IN SQL (Java-eval retirement, disagree-9
        // burn close): a grid under assertion is being READ AS PURE
        // VALUES, so its wire cells conform to the engine's decode
        // (nine-digit temporals / scale-canonical decimals) in the
        // FETCH itself — the executor's label-driven unwrap hands BOTH
        // verdict channels the decoded value and the host twin
        // (AssertVerdicts.valueRead) is deleted. The raw TDS lane
        // (toCSV/row-string renders) never passes through this wrap.
        if (plan instanceof com.legend.sql.SqlSelect ps) {
            plan = Fold.conformValueEgress(ps,
                    LiteralSpelling.ValueLane.GRID_FETCH);
        }
        // leg 3.1b: every cell's canon is ALSO projected on its own
        // (__cell<i>) so the database-mode cell-pool verdict reads cells
        // without splitting the row canon (no string_split on any dialect)
        List<SqlExpr> cellCanons = new java.util.ArrayList<>();
        for (int i = 0; i < plan.outputs().size(); i++) {
            com.legend.sql.OutputCol col = plan.outputs().get(i);
            Type kind = schema.columns().get(i).type();
            // the engine's boundary rule (dataTypeTransformer; the same rule
            // Equality.effectiveKind applies): a NUMERIC declaration converts
            // the wire cell; any other declaration keeps the WIRE's kind — a
            // String-declared property mapped to an INT column delivers the
            // Integer 11 and spells bare, never quoted (mapping::tree, the
            // four rows the H2 grid canon lost; leg 3.1b)
            // (a Boolean or temporal declaration converts too — the
            // transformer's Boolean and parseDate arms — so only the STRING
            // declaration is the identity)
            if (kind == Type.Primitive.STRING || kind == Type.Primitive.NUMBER) {
                // (an unrefined NUMBER declaration likewise: the wire's fine
                // kind is the cell's — the engine reads by result-set type)
                // the slot IS the wire (docs/WIRE_SLOT_HOMEWORK_2026_09_19.md)
                Type wire = Type.kindOfSqlType(col.type());
                if (wire != null) {
                    kind = wire;
                }
            }
            // an ENUM cell spells as pure's enum literal (Enumeration.NAME
            // — LiteralSpelling.literal's enum arm; the column's declared
            // enumeration is static): disjoint from a string golden, so
            // no byte compare fabricates equality or inequality.
            SqlExpr ref = SqlExpr.Column.of(null, col);
            // (cells arrive DECODED — the fetch conformance above; the
            // literal spelling reads the text carrier directly.)
            // JUDGING_TWO_MODES §1: a Float-DECLARED grid column converts to
            // DOUBLE once before its canon is spelled.
            // a TYPE-valued column (TDSColumn.type : Type; a rawType read):
            // the cell holds the type's simple name — the canon IS the name,
            // unquoted (never a string's)
            String declFqn = com.legend.compiler.element.EqualityKeys.fqnOf(
                    schema.columns().get(i).type());
            boolean typeValued = declFqn != null
                    && com.legend.compiler.element.type.PlatformTypes.isTypeClassifier(declFqn);
            SqlExpr lit = typeValued
                    ? new SqlExpr.Cast(ref, SqlType.Scalar.VARCHAR)
                    : LiteralSpelling.literal(
                            kind == Type.Primitive.FLOAT
                                    ? LiteralSpelling.declaredDouble(ref) : ref,
                            kind);
            if (lit == null) {
                return TdsWrap.decline(plan,
                        "tds-canon: unclaimed cell kind "
                                + kind.typeName());
            }
            SqlExpr cell = SqlExpr.Call.of(SqlFn.COALESCE,
                    new SqlExpr.Cast(lit, SqlType.Scalar.VARCHAR),
                    new SqlExpr.StringLit("TDSNull"));
            // the reserved separator POISONS the row canon to NULL (a
            // counted decline at the verdict frame — never a silent
            // mis-split); the guard sits OUTSIDE the COALESCE so a
            // poisoned cell is a null ROW CANON, never a fake TDSNull.
            // Only string kinds can carry the separator, so only they
            // pay the guard; NULL string cells short to TDSNull first
            // (STRPOS over NULL would poison every null cell).
            if (kind == Type.Primitive.STRING) {
                cell = new SqlExpr.Case(List.of(
                        new SqlExpr.Case.When(
                                SqlExpr.Call.of(SqlFn.IS_NULL, ref),
                                new SqlExpr.StringLit("TDSNull")),
                        new SqlExpr.Case.When(
                                SqlExpr.Call.of(SqlFn.GREATER,
                                        SqlExpr.Call.of(SqlFn.STRPOS, ref,
                                                new SqlExpr.StringLit(
                                                        TDS_CELL_SEP)),
                                        new SqlExpr.IntLit(0)),
                                new SqlExpr.NullLit())), cell);
            }
            cellCanons.add(cell);
        }
        // TWO levels: the inner select spells each cell canon ONCE (__cell<i>);
        // the outer passes everything through and joins the row canon from
        // those COLUMNS. Spelled in one select, the row canon repeated every
        // cell expression (a Float cell's canon is ~4 KB): the wrap carried
        // each cell twice (2026-09-23, columnValueDifferenceTest).
        List<com.legend.sql.SqlSelect.Projection> inner = new java.util.ArrayList<>();
        List<com.legend.sql.OutputCol> innerOuts = new java.util.ArrayList<>();
        for (com.legend.sql.OutputCol col : plan.outputs()) {
            inner.add(new com.legend.sql.SqlSelect.Projection(
                    SqlExpr.Column.of(null, col), col.name(), col));
            innerOuts.add(col);
        }
        for (int i = 0; i < cellCanons.size(); i++) {
            com.legend.sql.OutputCol out = new com.legend.sql.OutputCol(CELL_CANON + i,
                    SqlType.Scalar.VARCHAR, true);
            inner.add(new com.legend.sql.SqlSelect.Projection(
                    cellCanons.get(i), CELL_CANON + i, out));
            innerOuts.add(out);
        }
        com.legend.sql.SqlSelect cells = new com.legend.sql.SqlSelect(inner,
                false,
                new com.legend.sql.SqlSource.Subselect(plan, "side", null),
                null, List.of(), null, null, List.of(), null, null,
                List.copyOf(innerOuts));
        List<com.legend.sql.SqlSelect.Projection> projections =
                new java.util.ArrayList<>();
        for (com.legend.sql.OutputCol col : innerOuts) {
            projections.add(new com.legend.sql.SqlSelect.Projection(
                    SqlExpr.Column.of("cells", innerOuts, col.name()), col.name(), col));
        }
        // NULL propagates through CONCAT — one poisoned cell (NULL) nulls
        // the whole row canon, exactly the decline we want
        SqlExpr rowCanon = null;
        for (int i = 0; i < cellCanons.size(); i++) {
            SqlExpr cell = SqlExpr.Column.of("cells", innerOuts, CELL_CANON + i);
            rowCanon = rowCanon == null ? cell
                    : SqlExpr.Call.of(SqlFn.CONCAT,
                            SqlExpr.Call.of(SqlFn.CONCAT, rowCanon,
                                    new SqlExpr.StringLit(TDS_CELL_SEP)),
                            cell);
        }
        projections.add(new com.legend.sql.SqlSelect.Projection(
                Objects.requireNonNull(rowCanon, "grid canon over 0 columns"),
                ROW_CANON,
                new com.legend.sql.OutputCol(ROW_CANON,
                        SqlType.Scalar.VARCHAR, true)));
        return new TdsWrap(new com.legend.sql.SqlSelect(projections,
                false,
                new com.legend.sql.SqlSource.Subselect(cells, "cells", null),
                null, List.of(), null, null, List.of(), null, null,
                List.of()), null);
    }

    /** The plan a {@link #wrapTdsCanon} wrap was built over: its rows, with
     * none of the canon spelled. What only COUNTS rows (assertSize,
     * assertEmpty on a grid) reads this — formatting every cell to count
     * the rows put a whole grid's canon text in the statement for nothing
     * (2026-09-23). The wrap's shape is this class's own. */
    public static com.legend.sql.SqlQuery unwrapTdsCanon(com.legend.sql.SqlQuery wrapped) {
        if (wrapped instanceof com.legend.sql.SqlSelect outer
                && outer.from() instanceof com.legend.sql.SqlSource.Subselect c
                && "cells".equals(c.alias())
                && c.inner() instanceof com.legend.sql.SqlSelect cells
                && cells.from() instanceof com.legend.sql.SqlSource.Subselect side
                && "side".equals(side.alias())) {
            return side.inner();
        }
        throw new IllegalStateException("not a tds-canon wrap: " + wrapped.getClass().getSimpleName());
    }

    /** F13 — the IDENTITY canon of an instance whose layout carries the
     * synthetic {@code __id}: {@code {_type, _id}}, JSON-framed like the
     * keyed canon. Null when the layout has no identity field (Any/
     * variant wire trees, layoutless classes). */
    static @com.legend.Nullable SqlExpr identityCanon(SqlExpr v,
            String fqn, SqlType layout) {
        String idField = com.legend.compiler.element.ClassLayouts
                .SYNTHETIC_ID;
        if (!(layout instanceof SqlType.Struct st) || st.fields().stream()
                .noneMatch(f -> idField.equals(f.name()))) {
            return null;
        }
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.IS_NULL, v),
                new SqlExpr.NullLit())),
                new SqlExpr.JsonObject(List.of(
                        new SqlExpr.StringLit("_type"),
                        new SqlExpr.StringLit(fqn),
                        new SqlExpr.StringLit("_id"),
                        new SqlExpr.StructGet(v, idField))));
    }

    /** F13c — instance equality's ONE canon (the in-SQL eq/equal arm's
     * entry): keyed classes render their key tree ({@code instanceCanon},
     * the X5 relation), keyless classes their identity. Null =
     * unclaimable shape (the caller keeps its legacy behavior). */
    static @com.legend.Nullable SqlExpr instanceEqualityCanon(SqlExpr v,
            com.legend.compiler.element.@com.legend.Nullable EqualityKeys
                    keys,
            String fqn, SqlType layout) {
        return keys == null ? identityCanon(v, fqn, layout)
                : instanceCanon(v, keys, layout);
    }

    /**
     * X5 — the KEYED-INSTANCE canon: {@code Fqn(k1, k2, ...)} over the
     * struct column's key fields, in the model's key order. The
     * SPELLING of each leaf comes from the struct LAYOUT's field SQL
     * type (the layout was built from the concrete lowered values, so
     * it carries what the erased ClassType stamp cannot — the V11
     * doctrine: SQL types pick the recipe, runtime values gate the
     * rule); each leaf is KIND-TAGGED ('i:8' vs 'd:8') so cross-kind
     * key values can never byte-collide — the engine's same-primitive-
     * kind rule one level down. An empty [0..1] key value renders '[]'
     * (the engine compares key VALUE COLLECTIONS — empty equals
     * empty); a NULL instance cell stays NULL (EMPTY side semantics).
     * Null = unclaimed shape (to-many key, unknown field, unclaimable
     * leaf kind) — the caller declines, counted.
     */
    static @com.legend.Nullable SqlExpr instanceCanon(SqlExpr v,
            com.legend.compiler.element.EqualityKeys keys, SqlType layout) {
        // the BARE-ARRAY carrier (List<T>): the SQL value IS the one
        // to-many key's collection (PureSql — List travels as an array,
        // never a struct), so the key reads the value itself
        if (layout instanceof SqlType.Array at
                && keys.keys().size() == 1 && keys.keys().get(0).many()) {
            var k = keys.keys().get(0);
            SqlExpr leaf = taggedLeaf(SqlExpr.Column.derived(null, "__e"),
                    at.element(), k.nested());
            if (leaf == null) {
                return null;
            }
            SqlExpr arr = SqlExpr.Call.of(SqlFn.COALESCE,
                    SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, v,
                            new SqlExpr.Lambda(List.of("__e"), leaf)),
                    emptyArray());
            return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                    SqlExpr.Call.of(SqlFn.IS_NULL, v),
                    new SqlExpr.NullLit())),
                    new SqlExpr.JsonObject(List.of(
                            new SqlExpr.StringLit("_type"),
                            new SqlExpr.StringLit(keys.classFqn()),
                            new SqlExpr.StringLit(k.name()), arr)));
        }
        if (!(layout instanceof SqlType.Struct st)) {
            return null;
        }
        // JSON is the FRAMING, never the SPELLING (user ruling
        // 2026-08-22): every leaf value is OUR canonical string —
        // json_object contributes structure and escaping only, so
        // distinct key values can never collide ('a, b' vs 'a','b' —
        // the concat-framing ambiguity class is dead here). '_type'
        // carries the classifier FQN in the bytes themselves (the
        // engine's classifier-must-match rule rides the text, not just
        // the stamp gate). Nested keyed instances nest as JSON objects
        // (JSON-typed values embed structurally, no double-escaping);
        // to-many keys (List.values) are arrays of leaf canons via
        // list_transform — the engine compares key value COLLECTIONS
        // under the ordered list rule, which is exactly JSON array
        // equality over the element texts.
        List<SqlExpr> kv = new java.util.ArrayList<>();
        kv.add(new SqlExpr.StringLit("_type"));
        kv.add(new SqlExpr.StringLit(keys.classFqn()));
        for (var k : keys.keys()) {
            SqlType ft = st.fields().stream()
                    .filter(f -> f.name().equals(k.name()))
                    .map(SqlType.Struct.Field::type)
                    .findFirst().orElse(null);
            SqlExpr field = new SqlExpr.StructGet(v, k.name());
            SqlExpr c;
            if (k.many()) {
                SqlType elem = ft instanceof SqlType.Array at
                        ? at.element() : null;
                SqlExpr leaf = elem == null ? null
                        : taggedLeaf(SqlExpr.Column.derived(null, "__e"),
                                elem, k.nested());
                if (leaf == null) {
                    return null;
                }
                // NULL and empty are both the EMPTY key collection
                // (engine: empty equals empty) — normalize to []
                c = SqlExpr.Call.of(SqlFn.COALESCE,
                        SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, field,
                                new SqlExpr.Lambda(List.of("__e"), leaf)),
                        emptyArray());
            } else if (ft == null) {
                return null;
            } else {
                c = taggedLeaf(field, ft, k.nested());
                if (c == null) {
                    return null;
                }
                // an empty [0..1] key value is the EMPTY collection —
                // renderSide '[]' (engine: empty equals empty)
                c = SqlExpr.Call.of(SqlFn.COALESCE, c,
                        new SqlExpr.StringLit("[]"));
            }
            kv.add(new SqlExpr.StringLit(k.name()));
            kv.add(c);
        }
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.IS_NULL, v),
                new SqlExpr.NullLit())),
                new SqlExpr.JsonObject(kv));
    }

    /** F12 — the MAP canon: entry texts {@code [kLeaf, vLeaf]} built
     * per key (map_extract pairs each key with its value), SORTED (the
     * engine's order-insensitive mapEquals becomes byte-decidable),
     * JSON-framed with the carrier fqn. Leaf spellings are pure
     * literals (quoted strings end unambiguously, so the entry text
     * cannot collide across different key/value splits). Key and value
     * kinds come from the MAP layout's static types; an unclaimable
     * kind declines, counted. */
    private static @com.legend.Nullable SqlExpr mapCanon(SqlExpr v,
            SqlType layout, String fqn) {
        if (!(layout instanceof SqlType.Map mt)) {
            return null;
        }
        SqlExpr k = SqlExpr.Column.derived(null, "__k");
        SqlExpr kLeaf = taggedLeaf(k, mt.key(), null);
        SqlExpr vLeaf = taggedLeaf(SqlExpr.Call.of(SqlFn.LIST_GET,
                SqlExpr.Call.of(SqlFn.MAP_EXTRACT, v, k),
                new SqlExpr.IntLit(1)), mt.value(), null);
        if (kLeaf == null || vLeaf == null) {
            return null;
        }
        SqlExpr entry = SqlExpr.Call.of(SqlFn.CONCAT,
                SqlExpr.Call.of(SqlFn.CONCAT,
                        SqlExpr.Call.of(SqlFn.CONCAT,
                                new SqlExpr.StringLit("["), kLeaf),
                        new SqlExpr.StringLit(", ")),
                SqlExpr.Call.of(SqlFn.CONCAT, vLeaf,
                        new SqlExpr.StringLit("]")));
        SqlExpr entries = SqlExpr.Call.of(SqlFn.LIST_SORT,
                SqlExpr.Call.of(SqlFn.LIST_TRANSFORM,
                        SqlExpr.Call.of(SqlFn.MAP_KEYS, v),
                        new SqlExpr.Lambda(List.of("__k"), entry)));
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.IS_NULL, v),
                new SqlExpr.NullLit())),
                new SqlExpr.JsonObject(List.of(
                        new SqlExpr.StringLit("_type"),
                        new SqlExpr.StringLit(fqn),
                        new SqlExpr.StringLit("entries"), entries)));
    }

    /** One key LEAF: a nested keyed instance recurses (JSON-typed,
     * nests structurally); a scalar renders as PURE'S OWN LITERAL
     * SPELLING ({@link #literalCanon}). */
    private static @com.legend.Nullable SqlExpr taggedLeaf(SqlExpr field,
            SqlType ft,
            com.legend.compiler.element.@com.legend.Nullable EqualityKeys
                    nested) {
        // F10 proper: a constructed instance carries its own canon
        // (__canon, stamped at its construction site) — a nested struct
        // contributes it directly, a JSON-carried one through its object
        if (ft instanceof SqlType.Struct st && hasCanonField(st)) {
            return SqlExpr.StructGet.of(field,
                    com.legend.compiler.element.ClassLayouts.SYNTHETIC_CANON);
        }
        if (ft == SqlType.Scalar.JSON) {
            return jsonSlotCanon(field);
        }
        if (nested != null) {
            return instanceCanon(field, nested, ft);
        }
        Type kind = Type.kindOfSqlType(ft);
        if (kind == null) {
            return null;
        }
        SqlExpr lit = literalCanon(field, kind);
        // a NULL field canons NULL like a JSON-carried one (the caller's
        // coalesce spells both '[]'): DuckDB's concat swallows a NULL
        // operand, so an unguarded String spelling read '' — the NULL
        // parentName of pair($r.values->at(0), $r.values->at(1)) beside
        // the expected ^TDSNull() (bucket 9)
        return lit == null ? null : new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.IS_NULL, field), new SqlExpr.NullLit())), lit);
    }

    /** The EMPTY key collection (NULL and empty both normalize to it). */
    private static SqlExpr emptyArray() {
        return new SqlExpr.ArrayLit(List.of());
    }

    static boolean hasCanonField(SqlType.Struct st) {
        return st.fields().stream().anyMatch(f ->
                com.legend.compiler.element.ClassLayouts.SYNTHETIC_CANON.equals(f.name()));
    }

    /**
     * F10 proper — the canon a CONSTRUCTION site stamps into its
     * instance's {@code __canon} field, from the sibling fields: a keyed
     * class renders its key tree ({@code _type} + keys, the
     * {@link #instanceCanon} framing) with each key value's canon read
     * off the value itself — a nested constructed instance contributes
     * its OWN {@code __canon} (built before its parent, so recursive
     * polymorphic shapes stay finite), a JSON-carried one the
     * {@code __canon} in its object (an object without one is its
     * identity), a scalar its literal spelling; a keyless class is its
     * identity. An unclaimable leaf marks {@link #TREE_MARKER}: the
     * verdict declines the pair, never guesses.
     */
    public static SqlExpr constructionCanon(List<SqlExpr.StructLit.Field> fields,
            com.legend.compiler.element.@com.legend.Nullable EqualityKeys keys,
            String classFqn) {
        java.util.Map<String, SqlExpr.StructLit.Field> byName = new java.util.LinkedHashMap<>();
        for (SqlExpr.StructLit.Field f : fields) {
            byName.put(f.name(), f);
        }
        if (keys == null) {
            SqlExpr.StructLit.Field idF = byName.get(
                    com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID);
            return new SqlExpr.JsonObject(List.of(
                    new SqlExpr.StringLit("_type"), new SqlExpr.StringLit(classFqn),
                    new SqlExpr.StringLit("_id"),
                    idF == null ? new SqlExpr.NullLit() : idF.value()));
        }
        List<SqlExpr> kv = new java.util.ArrayList<>();
        kv.add(new SqlExpr.StringLit("_type"));
        kv.add(new SqlExpr.StringLit(keys.classFqn()));
        for (var k : keys.keys()) {
            SqlExpr.StructLit.Field f = byName.get(k.name());
            SqlExpr c;
            if (f == null) {
                c = new SqlExpr.StringLit(TREE_MARKER);
            } else {
                // the VALUE's own type first (the layout is value-built:
                // a JSON-declared Any slot holding a VARCHAR spells as the
                // string it is); the declared slot only for an untyped value
                SqlType ft = f.value().type() instanceof com.legend.sql.TypeFact.Typed tt
                        ? tt.type() : f.declared();
                if (k.many()) {
                    SqlType elem = ft instanceof SqlType.Array at ? at.element() : null;
                    c = elem == null ? new SqlExpr.StringLit(TREE_MARKER)
                            : SqlExpr.Call.of(SqlFn.COALESCE,
                                    SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, f.value(),
                                            new SqlExpr.Lambda(List.of("__e"),
                                                    valueCanon(SqlExpr.Column.derived(null, "__e"),
                                                            elem, k.nested()))),
                                    emptyArray());
                } else {
                    c = SqlExpr.Call.of(SqlFn.COALESCE,
                            valueCanon(f.value(), ft, k.nested()),
                            new SqlExpr.StringLit("[]"));
                }
            }
            kv.add(new SqlExpr.StringLit(k.name()));
            kv.add(c);
        }
        return new SqlExpr.JsonObject(kv);
    }

    /** One value's canon by its CARRIER (the construction-site rule). */
    private static SqlExpr valueCanon(SqlExpr v, @com.legend.Nullable SqlType t,
            com.legend.compiler.element.@com.legend.Nullable EqualityKeys nested) {
        if (t instanceof SqlType.Struct st) {
            if (hasCanonField(st)) {
                return SqlExpr.StructGet.of(v,
                        com.legend.compiler.element.ClassLayouts.SYNTHETIC_CANON);
            }
            SqlExpr c = nested == null ? null : instanceCanon(v, nested, st);
            return c == null ? new SqlExpr.StringLit(TREE_MARKER) : c;
        }
        if (t == SqlType.Scalar.JSON) {
            return jsonSlotCanon(v);
        }
        Type kind = t == null ? null : Type.kindOfSqlType(t);
        SqlExpr lit = kind == null ? null : literalCanon(v, kind);
        if (lit == null) {
            return new SqlExpr.StringLit(TREE_MARKER);
        }
        // a NULL slot canons NULL like the JSON-carried slot does (the
        // caller's coalesce spells both '[]'): DuckDB's concat swallows a
        // NULL operand, so an unguarded String spelling read '' — a
        // NULL parentName beside the expected ^TDSNull() (bucket 9)
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.IS_NULL, v), new SqlExpr.NullLit())), lit);
    }

    /** A JSON-carried slot's canon: NULL stays NULL; an OBJECT is the
     * {@code __canon} its constructor stamped (an object without one —
     * a producer outside the construction sites — is its identity,
     * {@code _type} + {@code _id}); a scalar takes the Any-cell spelling. */
    private static SqlExpr jsonSlotCanon(SqlExpr v) {
        SqlExpr canon = SqlExpr.Call.of(SqlFn.VARIANT_GET, v,
                new SqlExpr.StringLit(com.legend.compiler.element.ClassLayouts.SYNTHETIC_CANON));
        SqlExpr type = new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.VARIANT_GET, v,
                new SqlExpr.StringLit(com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE)),
                SqlType.Scalar.VARCHAR);
        SqlExpr id = new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.VARIANT_GET, v,
                new SqlExpr.StringLit(com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID)),
                SqlType.Scalar.VARCHAR);
        SqlExpr identity = new SqlExpr.JsonObject(List.of(
                new SqlExpr.StringLit("_type"), type,
                new SqlExpr.StringLit("_id"), id));
        return new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, v),
                        new SqlExpr.NullLit()),
                // the object arm is TEXT like every other arm: DuckDB types a
                // CASE by its JSON arm and PARSES the text arms as JSON
                // (a quoted 'ROOT' string canon raised Malformed JSON —
                // selfJoin::testSelfJoinPropertyMapping, bucket 9)
                new SqlExpr.Case.When(eqText(SqlExpr.Call.of(SqlFn.JSON_TYPE, v), "OBJECT"),
                        new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.COALESCE, canon, identity),
                                SqlType.Scalar.VARCHAR))),
                anyJsonCanon(v));
    }

    /** PURE'S OWN LITERAL SPELLING of a scalar (user ruling 2026-08-22
     * — no invented tag micro-format: the engine's grammar already
     * carries kind in text). Integer is bare ({@code 1}), Float always
     * has its point ({@code 1.0}), Decimal keeps its D suffix
     * ({@code 8.00D}), String is pure-quoted with pure escaping
     * ({@code 'a, b'}, {@code 'it\\'s'}), Boolean is bare, temporals
     * carry pure's {@code %} prefix — six disjoint spellings, the
     * engine's same-primitive-kind rule carried by engine syntax.
     * (X5's key-leaf rule, promoted to the shared literal channel —
     * F10 v1 compares Any-involving pairs in it.) */
    static @com.legend.Nullable SqlExpr literalCanon(SqlExpr v, Type kind) {
        return LiteralSpelling.literal(v, kind);
    }

    /** F10 v1 — the unclaimable-tree sentinel: an Any cell holding a
     * JSON array/object canons to this marker, and the VERDICT layer
     * DECLINES the pair on sight (a marker can never be compared — two
     * equal trees byte-matching it would fabricate equality). No
     * legitimate canon text contains U+0001. */
    public static final String TREE_MARKER = "\u0001tree";

    /** F10 v1 — the ANY-cell canon: pure-literal spelling dispatched
     * on the JSON carrier's RUNTIME type IN THE DATABASE (the carrier
     * keeps JSON-native kinds faithful: number-int/number-frac/string/
     * boolean; erased kinds — temporals and Decimals travel as their
     * JSON forms — canon as what the carrier holds, matching the host
     * referee's decode of the same wire; the kind-tagged carrier that
     * retires this blindness is F10 proper). Arrays/objects mark
     * {@link #TREE_MARKER} — decline, never guess. */
    static SqlExpr anyJsonCanon(SqlExpr v) {
        SqlExpr jt = SqlExpr.Call.of(SqlFn.JSON_TYPE, v);
        SqlExpr txt = new SqlExpr.Cast(
                SqlExpr.Call.of(SqlFn.VARIANT_GET, v,
                        new SqlExpr.StringLit("$")),
                SqlType.Scalar.VARCHAR);
        SqlExpr strLit = Objects.requireNonNull(
                literalCanon(txt, Type.Primitive.STRING));
        SqlExpr floatLit = Objects.requireNonNull(
                literalCanon(txt, Type.Primitive.FLOAT));
        return new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.IS_NULL, v),
                        new SqlExpr.NullLit()),
                // a JSON null cell IS the TDSNull slot of a row read through
                // the variant carrier (rows.get(col)); the expected side's
                // ^TDSNull() rewrites to the STRING sentinel, spelled quoted —
                // the same equivalence the host judge applies
                new SqlExpr.Case.When(eqText(jt, "NULL"),
                        new SqlExpr.StringLit("'TDSNull'")),
                new SqlExpr.Case.When(eqText(jt, "VARCHAR"), strLit),
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.OR,
                        eqText(jt, "BIGINT"), eqText(jt, "UBIGINT")), txt),
                new SqlExpr.Case.When(eqText(jt, "DOUBLE"), floatLit),
                new SqlExpr.Case.When(eqText(jt, "BOOLEAN"), txt)),
                new SqlExpr.StringLit(TREE_MARKER));
    }

    private static SqlExpr eqText(SqlExpr e, String s) {
        return SqlExpr.Call.of(SqlFn.EQUAL, e, new SqlExpr.StringLit(s));
    }
}
