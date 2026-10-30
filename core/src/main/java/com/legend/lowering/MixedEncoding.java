package com.legend.lowering;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;
import com.legend.values.PureDateLiteral;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The MIXED-ELEMENT encodings -- DATABASE-EXECUTED -- extracted from {@link Scalars} (CodeShapeGuardrail file split,
 * stamp C1 slice). A collection of elements of different kinds is a TAGGED LIST ({@link #taggedList}: parallel typed
 * channels by position, a rank for its order, docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a; track L, 2026-10-29);
 * elements under the Number or Date LUB also carry a COMPARABLE channel (every number a DOUBLE, every date a
 * TIMESTAMP) for a selection, whose winner is a tagged value ({@link MixedElems#tagged}). The spelling carrier (an
 * identity channel of print forms) was deleted with track L's L2. TENET: encodings chosen by STATIC type; every value
 * computation runs in the database (elements may be arbitrary expressions).
 */
final class MixedEncoding {

    private MixedEncoding() {
    }

    /**
     * Null when not per-element encodable or not mixed. Two channels per element: {@code vals}, its comparable (every
     * number a DOUBLE, every date a TIMESTAMP); and {@code candidates}, the element itself with its Pure kind and the
     * SQL slot that holds it exactly, which a SELECTION returns as a tagged value ({@link #tagged}: the winner's kind
     * survives as a type, never as a spelling; 2026-10-23). (A third channel of spellings went with track L's L2.)
     */
    record MixedElems(List<SqlExpr> vals, List<SqlExpr.Tagged.Candidate> candidates) {

        SqlExpr valList() {
            return new SqlExpr.ArrayLit(vals);
        }

        /** The selection: the candidate at the FIRST position of {@code winner} in the comparables (Pure's max and
         *  min keep the first of equal elements), carried as a tagged value with the winner on the comparable
         *  channel. */
        SqlExpr tagged(SqlExpr winner) {
            return taggedAt(SqlExpr.Call.of(SqlFn.LIST_POSITION, valList(), winner), winner);
        }

        /** The selection at {@code position} (1-based, the caller's recipe: mode's LAST position of the winner),
         *  carried as a tagged value with {@code winner} on the comparable channel. */
        SqlExpr taggedAt(SqlExpr position, SqlExpr winner) {
            return new SqlExpr.Tagged(position, winner, candidates);
        }
    }

    /** The written subsecond digits a kind {@code DateTime(n)} carries; null for every other kind. */
    static @com.legend.base.Nullable Integer writtenDigits(String kind) {
        return kind.startsWith("DateTime(")
                ? Integer.valueOf(kind.substring("DateTime(".length(), kind.length() - 1)) : null;
    }

    /** The enumeration a kind names (its path, track L, 2026-10-29); null for a primitive's kind. */
    static @com.legend.base.Nullable String enumerationKind(String kind) {
        return kind.contains("::") ? kind : null;
    }

    /** The enumeration a value of type {@code t} belongs to, as its kind: the enumeration's path; null for every
     *  other type, the abstract Enum declaration included (a mapping's toDomainValue, an EnumValueMapping's .enum: a
     *  value of no one enumeration, so of no kind, as the spelling carrier spelled none for it). */
    static @com.legend.base.Nullable String enumerationOf(Type t) {
        return t instanceof Type.EnumType et && !ABSTRACT_ENUM.equals(et.fqn()) ? et.fqn() : null;
    }

    static Type kindType(String kind) {
        if (writtenDigits(kind) != null) {
            return Type.Primitive.DATE_TIME;   // a DateTime at a written subsecond precision (PureDateLiteral.presentedAt)
        }
        String enumeration = enumerationKind(kind);
        if (enumeration != null) {
            // an enumeration's value: the kind is the enumeration's path, the slot holds the value's NAME (a
            // VARCHAR; the decoders read the name, the canon spells Enumeration.NAME)
            return new Type.EnumType(enumeration);
        }
        return switch (kind) {
            case "Integer" -> Type.Primitive.INTEGER;
            case "Float" -> Type.Primitive.FLOAT;
            case "Decimal" -> Type.Primitive.DECIMAL;
            case "StrictDate" -> Type.Primitive.STRICT_DATE;
            case "DateTime" -> Type.Primitive.DATE_TIME;
            case "Date" -> Type.Primitive.DATE;
            case "String" -> Type.Primitive.STRING;
            case "Boolean" -> Type.Primitive.BOOLEAN;
            default -> throw new IllegalStateException("a tagged slot of kind '" + kind + "': not one a mixed"
                    + " selection or list carries (Integer, Float, Decimal, StrictDate, DateTime, Date, String,"
                    + " Boolean)");
        };
    }

    /**
     * A mixed collection's elements as a tagged list (docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a's step B; the
     * result boundary program's track L, 2026-10-29): each element by its STATIC kind -- Integer BIGINT, Float
     * DOUBLE, Decimal its own DECIMAL(p,s), String VARCHAR, Boolean BOOLEAN, StrictDate DATE, DateTime TIMESTAMP (the
     * written subsecond precision in the kind), a partial Date its text (TEMPORAL_TEXT), a type's or an element's
     * value its NAME (VARCHAR, kind String: the wire convention for type and element values) -- into one channel per
     * distinct (kind, slot), the element at its position and NULL elsewhere; the tags each element's channel; the
     * comparable each element's rank ({@link #rankedComparable}: numbers, then dates, then Boolean, then String, as
     * Pure's compare orders mixed kinds; within a group by the group's own channel). Null when an element is of no
     * such kind (an instance, an enumeration, a variant, a computed Any): the caller keeps its lane.
     */
    static @com.legend.base.Nullable SqlExpr.TaggedList taggedList(TypedCollection c, List<SqlExpr> lowered) {
        int n = c.elements().size();
        if (n == 0 || lowered.size() != n) {
            return null;
        }
        List<String> kinds = new ArrayList<>(n);
        List<SqlType> slots = new ArrayList<>(n);
        List<SqlExpr> values = new ArrayList<>(n);
        List<SqlExpr> ranks = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            TypedSpec e = c.elements().get(i);
            SqlExpr x = unwrapVariant(lowered.get(i));
            String kind;
            SqlType slot;
            Type t = e.info().type();
            if (e instanceof com.legend.compiler.spec.typed.TypedTypeRef
                    || e instanceof com.legend.compiler.spec.typed.TypedPackageableRef) {
                kind = "String";
                slot = SqlType.Scalar.VARCHAR;
                t = Type.Primitive.STRING;
            } else if (t == Type.Primitive.INTEGER) {
                kind = "Integer";
                slot = SqlType.Scalar.BIGINT;
            } else if (t == Type.Primitive.FLOAT) {
                kind = "Float";
                slot = SqlType.Scalar.DOUBLE;
            } else if (t == Type.Primitive.DECIMAL || t instanceof Type.PrecisionDecimal) {
                kind = "Decimal";
                SqlType own = typeOf(x);
                if (!(own instanceof SqlType.Decimal)) {
                    throw new com.legend.error.NotImplementedException("a Decimal among mixed kinds whose SQL type is "
                            + own + ", not a DECIMAL(p,s) that holds it exactly: no slot for it");
                }
                slot = own;
            } else if (t == Type.Primitive.STRING) {
                kind = "String";
                slot = SqlType.Scalar.VARCHAR;
            } else if (t == Type.Primitive.BOOLEAN) {
                kind = "Boolean";
                slot = SqlType.Scalar.BOOLEAN;
            } else if (t == Type.Primitive.STRICT_DATE) {
                kind = "StrictDate";
                slot = SqlType.Scalar.DATE;
            } else if (t == Type.Primitive.DATE_TIME) {
                kind = e instanceof TypedCDate cd && cd.value() instanceof PureDateLiteral.DateWithSubsecond d
                        ? "DateTime(" + d.subsecond().length() + ")" : "DateTime";
                slot = SqlType.Scalar.TIMESTAMP;
            } else if (t == Type.Primitive.DATE) {
                if (partialComparable(e, x) == null) {
                    return null;
                }
                kind = "Date";
                slot = SqlType.Scalar.TEMPORAL_TEXT;
            } else if (enumerationOf(t) != null) {
                // an enumeration's value: the kind its enumeration's path, the slot its NAME (an enum value lowers
                // to its name; the canon spells Enumeration.NAME, so it never equals the name's string)
                kind = enumerationOf(t);
                slot = SqlType.Scalar.VARCHAR;
            } else {
                return null;
            }
            SqlExpr rank = rankedComparableOf(t, e, x);
            if (rank == null) {
                return null;
            }
            kinds.add(kind);
            slots.add(slot);
            values.add(x);
            ranks.add(rank);
        }
        // the comparable: where every element is of ONE comparison group (all numbers, all dates), the group's own
        // channel -- a DOUBLE, a TIMESTAMP -- as the selection's comparables are (a scalar every database orders;
        // a struct key is ordered as its text on SQLite and MariaDB, which have no struct); across groups the rank
        // struct, as the spelling carrier's sort ordered them
        Set<Integer> groups = new HashSet<>();
        for (SqlExpr r : ranks) {
            groups.add((int) ((SqlExpr.IntLit) ((SqlExpr.StructLit) r).fields().get(0).value()).value());
        }
        if (groups.size() == 1) {
            int g = groups.iterator().next();
            if (g == 0 || g == 1) {
                for (int i = 0; i < n; i++) {
                    ranks.set(i, ((SqlExpr.StructLit) ranks.get(i)).fields().get(g == 0 ? 1 : 2).value());
                }
            }
        }
        List<SqlType.Tagged.Slot> channelSlots = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            SqlType.Tagged.Slot s = new SqlType.Tagged.Slot(kinds.get(i), slots.get(i));
            if (!channelSlots.contains(s)) {
                channelSlots.add(s);
            }
        }
        List<SqlExpr> tags = new ArrayList<>(n);
        List<List<SqlExpr>> perChannel = new ArrayList<>();
        for (int k = 0; k < channelSlots.size(); k++) {
            perChannel.add(new ArrayList<>(n));
        }
        for (int i = 0; i < n; i++) {
            int k = channelSlots.indexOf(new SqlType.Tagged.Slot(kinds.get(i), slots.get(i)));
            tags.add(new SqlExpr.IntLit(k + 1));
            for (int j = 0; j < channelSlots.size(); j++) {
                perChannel.get(j).add(j == k ? values.get(i) : new SqlExpr.NullLit());
            }
        }
        List<SqlExpr.TaggedList.Channel> channels = new ArrayList<>(channelSlots.size());
        for (int k = 0; k < channelSlots.size(); k++) {
            SqlType.Tagged.Slot s = channelSlots.get(k);
            // the channel's array is TYPED as its slot (a NULL beside a value takes the value's type in most
            // databases, but a cast says it where the slot is the exact type, a DECIMAL(p,s) or a DATE)
            channels.add(new SqlExpr.TaggedList.Channel(s.kind(), s.type(),
                    new SqlExpr.Cast(new SqlExpr.ArrayLit(perChannel.get(k)), new SqlType.Array(s.type()))));
        }
        List<SqlExpr> positions = new ArrayList<>(n);
        for (int i = 1; i <= n; i++) {
            positions.add(new SqlExpr.IntLit(i));
        }
        SqlExpr.TaggedList tl = new SqlExpr.TaggedList(new SqlExpr.ArrayLit(positions), new SqlExpr.ArrayLit(tags),
                new SqlExpr.ArrayLit(ranks), channels);
        // an element that may be empty ([0..1]-stamped) contributes nothing when it is: the list's positions are
        // those whose element is present (Pure's collection holds no empties; the plain arm's CompactList)
        return compacts(c) ? tl.withPositions(TaggedLists.presentPositions(tl)) : tl;
    }

    /** A NUMBER-LUB literal mix ({@code [25.0, 1]}, {@code [342, 5.0, -2.0, 171, 1]}: two or more distinct numeric
     *  kinds, every element a literal) as a tagged list, read from its variant-carried lowering; null for any other
     *  shape (the caller keeps its lane). Elsewhere such a list keeps the variant carrier (arithmetic and the
     *  aggregates type on it); a root, a sort or a dedup reads it as its kinds. */
    static @com.legend.base.Nullable SqlExpr.TaggedList mixedNumericLiteral(TypedSpec spec, SqlExpr lowered) {
        if (!(spec instanceof TypedCollection c) || !(lowered instanceof SqlExpr.ArrayLit la)
                || c.elements().size() < 2 || la.elements().size() != c.elements().size()
                || c.info().type() != Type.Primitive.NUMBER) {
            return null;
        }
        Set<Type> kinds = new HashSet<>();
        for (int i = 0; i < c.elements().size(); i++) {
            Type t = c.elements().get(i).info().type();
            Type kind = t instanceof Type.PrecisionDecimal ? Type.Primitive.DECIMAL : t;
            if ((kind != Type.Primitive.INTEGER && kind != Type.Primitive.FLOAT && kind != Type.Primitive.DECIMAL)
                    || !isNumericLiteral(unwrapVariant(la.elements().get(i)))) {
                return null;
            }
            kinds.add(kind);
        }
        return kinds.size() < 2 ? null : taggedList(c, la.elements());
    }

    /** A lowered element that is a number written in the query: a literal, or one negated ({@code -2.0} is Pure's
     *  {@code minus(2.0)}, a call over a literal). */
    private static boolean isNumericLiteral(SqlExpr x) {
        if (x instanceof SqlExpr.IntLit || x instanceof SqlExpr.FloatLit || x instanceof SqlExpr.DecimalLit) {
            return true;
        }
        if (x instanceof SqlExpr.Cast c) {
            return isNumericLiteral(c.value());
        }
        // a negated literal (-2.0 is minus(2.0)); a binary minus over literals (5 - 2) is arithmetic, not a number
        // written in the query (the L1 audit, 2026-10-29)
        if (x instanceof SqlExpr.Call call && call.args().size() == 1
                && (call.fn() == SqlFn.NEGATE || call.fn() == SqlFn.MINUS)) {
            return isNumericLiteral(call.args().get(0));
        }
        return false;
    }

    /** An element's rank for a mixed list's order (the engine's compare over mixed kinds: numbers, then dates,
     *  then Boolean, then String; within a group by the group's own channel), built from the TYPED value: a struct
     *  {@code {g, n, d, s}} whose fields order lexicographically. Null for a kind outside the eight. */
    // a Decimal ranks with the numbers (register S123: compiled legend-pure sorts a BigDecimal after dates, Booleans and
    // Strings; the user's ruling)
    static @com.legend.base.Nullable SqlExpr rankedComparableOf(Type t, @com.legend.base.Nullable TypedSpec e,
            SqlExpr x) {
        int group;
        SqlExpr n = null;
        SqlExpr d = null;
        SqlExpr s = null;
        if (t == Type.Primitive.INTEGER || t == Type.Primitive.FLOAT || t == Type.Primitive.NUMBER
                || t == Type.Primitive.DECIMAL || t instanceof Type.PrecisionDecimal) {
            group = 0;
            n = new SqlExpr.Cast(x, SqlType.Scalar.DOUBLE);
        } else if (t == Type.Primitive.STRICT_DATE) {
            group = 1;
            d = new SqlExpr.Cast(x, SqlType.Scalar.TIMESTAMP);
        } else if (t == Type.Primitive.DATE_TIME) {
            group = 1;
            d = x;
        } else if (t == Type.Primitive.DATE) {
            group = 1;
            // a column's partial date (no written literal to read the precision from) has no instant to order by:
            // no rank (the caller keeps the variant lane; the L2 audit, 2026-10-29: a NULL rank sorted it silently)
            if (e == null) {
                return null;
            }
            d = partialComparable(e, x);
            if (d == null) {
                return null;
            }
        } else if (t == Type.Primitive.BOOLEAN) {
            group = 2;
            s = new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR);
        } else if (t == Type.Primitive.STRING) {
            group = 3;
            s = x;
        } else if (enumerationOf(t) != null) {
            // Pure's compare over an enumeration's value against a primitive: the enumeration's value AFTER every
            // primitive (legend-pure CompiledSupport.compareInt: a class outside PRIMITIVE_CLASS_COMPARISON_ORDER
            // is greater than any in it); two enumeration values, of one enumeration or of two, by their NAMES
            // (every enum value is one generated class, PureEnum, implements Comparable by getName():
            // EnumProcessor; the L1 audit corrected a path-first order, S122 withdrawn, 2026-10-29)
            group = 4;
            s = x;
        } else {
            return null;
        }
        return new SqlExpr.StructLit(List.of(
                new SqlExpr.StructLit.Field("g", new SqlExpr.IntLit(group)),
                new SqlExpr.StructLit.Field("n", n != null ? n : new SqlExpr.NullLit(), SqlType.Scalar.DOUBLE),
                new SqlExpr.StructLit.Field("d", d != null ? d : new SqlExpr.NullLit(), SqlType.Scalar.TIMESTAMP),
                new SqlExpr.StructLit.Field("s", s != null ? s : new SqlExpr.NullLit(), SqlType.Scalar.VARCHAR)));
    }

    /** The channel of a tagged list that holds a value of {@code kind} (a needle's static kind), else -1: a kind
     *  with no channel is a member of nothing in the list (a {@code DateTime(n)} is its own kind: Pure's date
     *  equality reads the subsecond text, the L1 audit's finding 2). */
    static int channelOf(SqlExpr.TaggedList tl, String kind) {
        for (int k = 0; k < tl.channels().size(); k++) {
            String ck = tl.channels().get(k).kind();
            if (ck.equals(kind)) {
                return k;
            }
        }
        return -1;
    }

    /** The tagged kind of a needle's static type: the name a tagged list's channel carries, or null for a type no
     *  channel holds (an instance, an enumeration, Any). */
    static @com.legend.base.Nullable String kindOf(TypedSpec e) {
        if (e instanceof com.legend.compiler.spec.typed.TypedTypeRef
                || e instanceof com.legend.compiler.spec.typed.TypedPackageableRef) {
            return "String";
        }
        Type t = e.info().type();
        if (t == Type.Primitive.INTEGER) {
            return "Integer";
        }
        if (t == Type.Primitive.FLOAT) {
            return "Float";
        }
        if (t == Type.Primitive.DECIMAL || t instanceof Type.PrecisionDecimal) {
            return "Decimal";
        }
        if (t == Type.Primitive.STRING) {
            return "String";
        }
        if (t == Type.Primitive.BOOLEAN) {
            return "Boolean";
        }
        if (t == Type.Primitive.STRICT_DATE) {
            return "StrictDate";
        }
        if (t == Type.Primitive.DATE_TIME) {
            // a written subsecond literal is its own kind, DateTime(n): Pure's date equality reads the subsecond
            // TEXT (AbstractPureDate.equals, the L1 audit, 2026-10-29), so .000 is not none
            return e instanceof TypedCDate cd && cd.value() instanceof PureDateLiteral.DateWithSubsecond d
                    ? "DateTime(" + d.subsecond().length() + ")" : "DateTime";
        }
        if (t == Type.Primitive.DATE) {
            // a PARTIAL date written in the query (%2014, %2014-02) is the Date kind; a value typed Date otherwise
            // (a variable, a column) holds a StrictDate, a DateTime or a partial, which only its runtime value
            // names: no static kind (the caller refuses by name, never answers false)
            return e instanceof TypedCDate cd && !cd.value().precision().atLeast(PureDateLiteral.Precision.HOUR)
                    && !(cd.value() instanceof PureDateLiteral.DateWithSubsecond) ? "Date" : null;
        }
        return enumerationOf(t);
    }

    /** The abstract Enum declaration ({@link #enumerationOf}). */
    private static final String ABSTRACT_ENUM = "meta::pure::metamodel::type::Enum";

    /** The channel a relation COLUMN of Pure type {@code t} takes in a tagged list (its kind and the slot holding it
     *  exactly; the row-major flatten, track L, 2026-10-29): an Integer a BIGINT, a Float a DOUBLE, a Decimal its own
     *  DECIMAL(p,s) ({@code own}, the column's wire type) -- another wire type holds it inexactly, refused by name --
     *  a String a VARCHAR, a Boolean a BOOLEAN, a StrictDate a DATE, a DateTime a TIMESTAMP, a partial Date its text
     *  (the temporal-text carrier), an enumeration's value its NAME. Null: a kind no channel holds (a class, Any). */
    static SqlType.Tagged.@com.legend.base.Nullable Slot columnChannel(Type t, @com.legend.base.Nullable SqlType own) {
        if (t == Type.Primitive.INTEGER) {
            return new SqlType.Tagged.Slot("Integer", SqlType.Scalar.BIGINT);
        }
        if (t == Type.Primitive.FLOAT) {
            return new SqlType.Tagged.Slot("Float", SqlType.Scalar.DOUBLE);
        }
        if (t == Type.Primitive.DECIMAL || t instanceof Type.PrecisionDecimal) {
            if (!(own instanceof SqlType.Decimal)) {
                throw new com.legend.error.NotImplementedException("a Decimal column among a row's mixed cells whose"
                        + " SQL type is " + own + ", not a DECIMAL(p,s) that holds it exactly: no slot for it");
            }
            return new SqlType.Tagged.Slot("Decimal", own);
        }
        if (t == Type.Primitive.STRING) {
            return new SqlType.Tagged.Slot("String", SqlType.Scalar.VARCHAR);
        }
        if (t == Type.Primitive.BOOLEAN) {
            return new SqlType.Tagged.Slot("Boolean", SqlType.Scalar.BOOLEAN);
        }
        if (t == Type.Primitive.STRICT_DATE) {
            return new SqlType.Tagged.Slot("StrictDate", SqlType.Scalar.DATE);
        }
        if (t == Type.Primitive.DATE_TIME) {
            // a relation's DateTime cell is read at NINE subsecond digits (legend-engine's value read, %09d; the
            // corpus's testDayOfMonth equates it with a literal written at nine), so the kind says so
            return new SqlType.Tagged.Slot("DateTime(9)", SqlType.Scalar.TIMESTAMP);
        }
        if (t == Type.Primitive.DATE) {
            return new SqlType.Tagged.Slot("Date", SqlType.Scalar.TEMPORAL_TEXT);
        }
        String enumeration = enumerationOf(t);
        if (enumeration != null) {
            return new SqlType.Tagged.Slot(enumeration, SqlType.Scalar.VARCHAR);
        }
        return null;
    }

    /** A column's cell in its channel's slot ({@link #columnChannel}): cast where the slot is a type the cell may
     *  not be in already (an INTEGER wire to BIGINT, a DATE wire to TIMESTAMP, a date's text), the cell itself
     *  where the slot is its own type. */
    static SqlExpr columnCell(SqlType.Tagged.Slot channel, SqlExpr cell) {
        SqlType own = typeOf(cell);
        if (channel.type().equals(own)) {
            return cell;
        }
        if (channel.type() == SqlType.Scalar.TEMPORAL_TEXT) {
            return new SqlExpr.Cast(cell, SqlType.Scalar.VARCHAR);
        }
        return new SqlExpr.Cast(cell, channel.type());
    }

    /** One expression per candidate of {@code t}, chosen by its tag: {@code CASE WHEN tag = k THEN arm(kind k,
     *  candidate k) ... END}, each arm built over the candidate's own value with its STATIC kind -- a print spells
     *  each candidate as itself (a Float literal's text the database's own plain text; a computed Float's the exact
     *  form, once), the judges' canon likewise. */
    static SqlExpr perCandidate(SqlExpr.Tagged t,
            java.util.function.BiFunction<Type, SqlExpr, SqlExpr> arm) {
        List<SqlExpr.Case.When> whens = new ArrayList<>();
        for (int k = 0; k < t.candidates().size(); k++) {
            SqlExpr.Tagged.Candidate c = t.candidates().get(k);
            whens.add(new SqlExpr.Case.When(
                    SqlExpr.Call.of(SqlFn.EQUAL, t.tag(), new SqlExpr.IntLit(k + 1)),
                    arm.apply(kindType(c.kind()), c.value())));
        }
        return new SqlExpr.Case(whens, null);
    }

    /** {@code args} with every tagged value read on its comparable channel: a comparison, an equality or a
     *  {@code compare} over a mixed selection's winner compares the number or the instant, as Pure does. */
    static List<SqlExpr> comparables(List<SqlExpr> args) {
        List<SqlExpr> out = new ArrayList<>(args.size());
        for (SqlExpr a : args) {
            out.add(a instanceof SqlExpr.Tagged t ? t.comparable() : a);
        }
        return out;
    }

    private static @com.legend.base.Nullable SqlType typeOf(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : null;
    }

    /** Identity consumers of the NUMBER-LUB literal carrier rebuild
     * element identity from the TYPED elements (encodeMixed), never from
     * the carrier (the numeric UNWRAP is {@link Numerics#numList}).
     * Non-carrier shapes pass through untouched. */
    static @com.legend.base.Nullable MixedElems mixedElems(TypedSpec arg,
                                 SqlExpr lowered) {
        if (!(arg instanceof TypedCollection c)
                || c.elements().size() < 2
                || !(lowered instanceof SqlExpr.ArrayLit la)
                || la.elements().size() != c.elements().size()) {
            return null;
        }
        Type lub = c.info().type();
        if (lub != Type.Primitive.NUMBER && lub != Type.Primitive.DATE) {
            return null;   // uniform-kind collections keep their native carrier
        }
        return encodeAll(c.elements(), la.elements());
    }

    /** The n-ary form: max(2D, 1.23) — each ARG one element. */
    static @com.legend.base.Nullable MixedElems mixedArgs(List<TypedSpec> args,
                                List<SqlExpr> lowered) {
        Set<Type> kinds = new HashSet<>();
        for (var a : args) {
            kinds.add(a.info().type());
        }
        return kinds.size() > 1 ? encodeAll(args, lowered) : null;
    }

    private static @com.legend.base.Nullable MixedElems encodeAll(
            List<TypedSpec> elems,
            List<SqlExpr> lowered) {
        List<SqlExpr> vals = new ArrayList<>();
        List<SqlExpr.Tagged.Candidate> candidates = new ArrayList<>();
        for (int i = 0; i < elems.size(); i++) {
            if (!encodeMixed(elems.get(i), lowered.get(i), vals, candidates)) {
                return null;
            }
        }
        return new MixedElems(vals, candidates);
    }

    /**
     * One element's (identity, comparable, candidate) SQL triple, dispatched on its STATIC type. All value work
     * happens in SQL. The candidate is the element itself under its kind and the slot that holds it exactly (an
     * Integer a BIGINT, a Float a DOUBLE, a Decimal its own DECIMAL(p,s), a StrictDate a DATE, a DateTime a
     * TIMESTAMP, a partial date its text); the root's egress casts it to the slot where its own type differs.
     */
    private static boolean encodeMixed(TypedSpec e,
                                       SqlExpr x,
                                       List<SqlExpr> vals,
                                       List<SqlExpr.Tagged.Candidate> candidates) {
        // a carrier-wrapped element unwraps: identity/comparable both
        // build from the RAW value (floatRepr over json cannot type)
        x = unwrapVariant(x);
        Type t = e.info().type();
        if (t == Type.Primitive.INTEGER) {
            vals.add(new SqlExpr.Cast(x, SqlType.Scalar.DOUBLE));
            candidates.add(new SqlExpr.Tagged.Candidate("Integer", SqlType.Scalar.BIGINT, x));
            return true;
        }
        if (t == Type.Primitive.FLOAT) {
            vals.add(x);
            candidates.add(new SqlExpr.Tagged.Candidate("Float", SqlType.Scalar.DOUBLE, x));
            return true;
        }
        if (t == Type.Primitive.DECIMAL || t instanceof Type.PrecisionDecimal) {
            vals.add(new SqlExpr.Cast(x, SqlType.Scalar.DOUBLE));
            SqlType slot = typeOf(x);
            if (!(slot instanceof SqlType.Decimal)) {
                throw new com.legend.error.NotImplementedException("a Decimal among mixed kinds whose SQL type is "
                        + slot + ", not a DECIMAL(p,s) that holds it exactly: no slot for it");
            }
            candidates.add(new SqlExpr.Tagged.Candidate("Decimal", slot, x));
            return true;
        }
        if (t == Type.Primitive.STRICT_DATE) {
            vals.add(new SqlExpr.Cast(x, SqlType.Scalar.TIMESTAMP));
            candidates.add(new SqlExpr.Tagged.Candidate("StrictDate", SqlType.Scalar.DATE, x));
            return true;
        }
        if (t == Type.Primitive.DATE_TIME) {
            vals.add(x);
            // the written precision is part of a DateTime value and no TIMESTAMP keeps it: the slot's kind carries
            // it as a fact the reader presents the instant at -- a subsecond literal's digit count, else the
            // second (as the spelling carrier spelled a computed or whole-second DateTime at ISO_LOCAL; the
            // Snowflake lane's five date winners read back at nine digits, 2026-10-23)
            String kind = e instanceof TypedCDate cd && cd.value() instanceof PureDateLiteral.DateWithSubsecond d
                    ? "DateTime(" + d.subsecond().length() + ")" : "DateTime";
            candidates.add(new SqlExpr.Tagged.Candidate(kind, SqlType.Scalar.TIMESTAMP, x));
            return true;
        }
        if (t == Type.Primitive.DATE) {
            // PARTIAL dates travel as STRINGS (master's pinned carrier): the
            // string IS the print form; the comparable composes via
            // make_timestamp from split components (strptime %Y rejects
            // 5-digit years; make_timestamp reaches year 294246).
            SqlExpr cmp = partialComparable(e, x);
            if (cmp == null) {
                return false;
            }
            vals.add(cmp);
            candidates.add(new SqlExpr.Tagged.Candidate("Date", SqlType.Scalar.TEMPORAL_TEXT, x));
            return true;
        }
        return false;
    }
    /** A date WITHOUT a time as its day number, {@code year * 10000 + month * 100 + day} in SQL (a partial date's
     *  missing parts 1, as {@link #partialComparable} pads them): a partial date's parts split from its text, a strict
     *  date's extracted; null for any other operand. Two such operands compare on it in any year (2026-10-20, the
     *  parity phase: {@code compare(%2001, %10999)} composed year 10999 as a timestamp, which MySQL, MariaDB and SQLite
     *  have none of -- a NULL compared, a wrong answer). */
    static @com.legend.base.Nullable SqlExpr dayNumberOrNull(TypedSpec e, SqlExpr x) {
        Type t = e.info().type();
        List<SqlExpr> parts;
        if (t == Type.Primitive.DATE) {
            PureDateLiteral.Precision prec = Scalars.datePrecision(e);
            if (prec.atLeast(PureDateLiteral.Precision.HOUR)) {
                return null;
            }
            parts = new ArrayList<>();
            for (int i = 1; i <= 3; i++) {
                boolean present = i == 1 || prec.atLeast(i == 2 ? PureDateLiteral.Precision.MONTH
                        : PureDateLiteral.Precision.DAY);
                parts.add(present ? new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.SPLIT_PART, x, new SqlExpr.StringLit("-"),
                        new SqlExpr.IntLit(i)), SqlType.Scalar.BIGINT) : new SqlExpr.IntLit(1));
            }
        } else if (t == Type.Primitive.STRICT_DATE) {
            parts = new ArrayList<>();
            for (com.legend.sql.DatePart p : List.of(com.legend.sql.DatePart.YEAR, com.legend.sql.DatePart.MONTH,
                    com.legend.sql.DatePart.DAY)) {
                parts.add(new SqlExpr.Cast(SqlExpr.Call.of(SqlFn.EXTRACT, new SqlExpr.DatePartLit(p), x),
                        SqlType.Scalar.BIGINT));
            }
        } else {
            return null;
        }
        return SqlExpr.Call.of(SqlFn.PLUS, SqlExpr.Call.of(SqlFn.PLUS,
                SqlExpr.Call.of(SqlFn.TIMES, parts.get(0), new SqlExpr.IntLit(10000)),
                SqlExpr.Call.of(SqlFn.TIMES, parts.get(1), new SqlExpr.IntLit(100))), parts.get(2));
    }

    /** A date operand's chronological comparable (strptime-padded partials); non-dates pass through. */
    static SqlExpr dateComparableOrSelf(TypedSpec e,
                                        SqlExpr x) {
        Type t = e.info().type();
        if (t == Type.Primitive.DATE) {
            SqlExpr cmp = partialComparable(e, x);
            if (cmp != null) {
                return cmp;
            }
        }
        if (t == Type.Primitive.STRICT_DATE) {
            return new SqlExpr.Cast(x, SqlType.Scalar.TIMESTAMP);
        }
        return x;
    }
    /**
     * A PARTIAL date string's chronological comparable, composed IN SQL:
     * {@code make_timestamp(split_part(x,'-',i)...)} per the STATIC
     * precision; null when the precision is not a known partial form.
     */
    private static @com.legend.base.Nullable SqlExpr partialComparable(TypedSpec e,
                                             SqlExpr x) {
        PureDateLiteral.Precision prec = Scalars.datePrecision(e);
        if (prec.atLeast(PureDateLiteral.Precision.HOUR)) {
            return null;
        }
        SqlExpr one = new SqlExpr.IntLit(1);
        SqlExpr zero = new SqlExpr.IntLit(0);
        SqlExpr year = new SqlExpr.Cast(
                SqlExpr.Call.of(SqlFn.SPLIT_PART, x, new SqlExpr.StringLit("-"), one),
                SqlType.Scalar.BIGINT);
        SqlExpr month = prec.atLeast(PureDateLiteral.Precision.MONTH) ? new SqlExpr.Cast(
                SqlExpr.Call.of(SqlFn.SPLIT_PART, x, new SqlExpr.StringLit("-"),
                        new SqlExpr.IntLit(2)),
                SqlType.Scalar.BIGINT) : one;
        SqlExpr day = prec.atLeast(PureDateLiteral.Precision.DAY) ? new SqlExpr.Cast(
                SqlExpr.Call.of(SqlFn.SPLIT_PART, x, new SqlExpr.StringLit("-"),
                        new SqlExpr.IntLit(3)),
                SqlType.Scalar.BIGINT) : one;
        return SqlExpr.Call.of(SqlFn.MAKE_TIMESTAMP, year, month, day, zero, zero, zero);
    }

    /** An Any-LUB {@code if} with DIFFERING branch kinds rides the
     * VARIANT carrier (the mixed-list discipline: a raw CASE cannot even
     * type — 'TDSNull' vs INT32, the TDS-getter witness); NULL stays the
     * bare empty carrier. Same-kind or non-Any ifs emit raw branches. */
    static SqlExpr lubCase(Type lub, TypedSpec thenB,
            @com.legend.base.Nullable TypedSpec elseB, SqlExpr cond,
            SqlExpr thenS, SqlExpr elseS) {
        boolean mixed = lub instanceof Type.ClassType ifCt
                && PlatformTypes.isAny(ifCt)
                && elseB != null
                && !thenB.info().type().equals(elseB.info().type());
        return new SqlExpr.Case(
                List.of(new SqlExpr.Case.When(cond,
                        mixed ? variantBranch(thenB, thenS) : thenS)),
                mixed && elseB != null ? variantBranch(elseB, elseS) : elseS);
    }

    /** One branch of a mixed-kind if on the variant carrier: a value is
     * TO_VARIANT-wrapped; a [1]-stamped NULL literal (the ^TDSNull()
     * instance — batch 69a) is the JSON null VALUE by the value law, so
     * the branch survives as an element; an optional NULL stays bare. */
    private static SqlExpr variantBranch(
            com.legend.compiler.spec.typed.TypedSpec b, SqlExpr s) {
        if (!(s instanceof SqlExpr.NullLit)) {
            return SqlExpr.Call.of(SqlFn.TO_VARIANT, s);
        }
        boolean oneStamped = b.info().multiplicity()
                instanceof com.legend.compiler.element.type.Multiplicity.Bounded mb
                && mb.lower() >= 1 && Integer.valueOf(1).equals(mb.upper());
        return oneStamped
                ? new SqlExpr.Cast(new SqlExpr.StringLit("null"),
                        com.legend.sql.SqlType.Scalar.JSON)
                : s;
    }

    /**
     * A primitive needle against class-typed elements (or vice versa) can
     * never be a member — the kinds are disjoint in pure's type system.
     * Any/mixed stays undecided (falls through to the SQL comparison).
     */
    static boolean kindMismatch(Type needle, Type elems) {
        boolean np = needle instanceof Type.Primitive || needle instanceof Type.PrecisionDecimal;
        boolean ep = elems instanceof Type.Primitive || elems instanceof Type.PrecisionDecimal;
        boolean nc = Scalars.isClassish(needle) && !PlatformTypes.isAny(needle);
        boolean ec = Scalars.isClassish(elems) && !PlatformTypes.isAny(elems);
        return (np && ec) || (nc && ep);
    }

    /** The VALUE LAW of the variant lane: a [1]-STAMPED element is a
     * VALUE and never vanishes — TDSNull is DATA on the grid convention
     * (engine tds.pure: {@code TDSRow.values : Any[*]} holds a
     * ^TDSNull() instance per null cell, built at the row read,
     * relationalMappingExecution buildExecutionResultInTDS). Its
     * surviving representation is the JSON null VALUE: statically for
     * the TDSNull literal (its scalar form IS the SQL NULL literal),
     * via COALESCE for a runtime [1] cell whose wire may still carry
     * NULL (declared-required columns under left joins — the
     * nullability lie). A ZERO-lower-bound element keeps the bare wrap:
     * an EMPTY decays by variant-decay semantics (pure collections hold
     * no empties). Static non-null literals keep the bare wrap so their
     * SQL stays byte-identical (COALESCE is a no-op on them). The
     * spelling {@code CAST('null' AS JSON)} is the dialect-probed
     * text-to-JSON idiom both backends navigate. {@code lowered} is the
     * element's already-lowered scalar form (the Lowerer owns scalar()).
     *
     * <p>SCOPE ({@code cellSlots}): the runtime COALESCE applies ONLY to
     * ROW-CELLS collections (TypedCollection.rowCells() — the
     * construction-declared fact off the Typer's rowCells() synthesis),
     * where a declared-[1] cell's WIRE can still carry NULL
     * (left joins under the nullability lie) and the slot is grid DATA. A
     * plain value collection's [1] element is stamp-guaranteed non-null,
     * and the bare TO_VARIANT shape is load-bearing downstream (consumers
     * structurally peel it — struct extraction, variant-column detection;
     * the blanket wrap broke both, gate-caught 2026-08-24). */
    static SqlExpr variantElement(
            com.legend.compiler.spec.typed.TypedSpec e, SqlExpr lowered,
            boolean cellSlots) {
        boolean oneStamped = e.info().multiplicity()
                instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                && b.lower() >= 1 && Integer.valueOf(1).equals(b.upper());
        SqlExpr jsonNull = new SqlExpr.Cast(new SqlExpr.StringLit("null"),
                com.legend.sql.SqlType.Scalar.JSON);
        if (oneStamped && lowered instanceof SqlExpr.NullLit) {
            return jsonNull;
        }
        SqlExpr wrapped = SqlExpr.Call.of(SqlFn.TO_VARIANT, lowered);
        if (cellSlots && oneStamped && !staticallyNonNull(lowered)) {
            return new SqlExpr.Call(SqlFn.COALESCE,
                    java.util.List.of(wrapped, jsonNull));
        }
        return wrapped;
    }

    /** An object as a reader sees it (a variant, a JSON document): its declared properties alone, each nested object
     *  the same, recursively -- its identity and its class ({@code __id}, {@code __type}) are the lowering's own,
     *  never data (docs/OBJECT_IDENTITY_2026_10_20.md §3.4). An object of a class with no properties keeps its class
     *  alone, its only data ({@code ClassLayouts.syntheticOnlyLayout}: an empty struct is no SQL value). A value that is
     *  no struct with synthetic fields is unchanged. */
    static SqlExpr declaredOnly(SqlExpr v) {
        if (!(v.type() instanceof com.legend.sql.TypeFact.Typed t)) {
            return v;
        }
        return declaredOnly(v, t.type());
    }

    private static SqlExpr declaredOnly(SqlExpr v, SqlType type) {
        return declaredOnly(v, type, true);
    }

    /** {@code mayBeAbsent}: whether {@code v} may be no object at all (a property's or a root's value); an element of
     *  a collection never is (Pure's collections hold no nulls), so it is rebuilt with no null guard, which a database
     *  without a null struct cannot type (ClickHouse: no supertype of Nothing and a Tuple, channel B's
     *  testExistsInSelect, 2026-10-20). */
    private static SqlExpr declaredOnly(SqlExpr v, SqlType type, boolean mayBeAbsent) {
        if (type instanceof SqlType.Array a && carriesSynthetic(a.element())) {
            return SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, v, new SqlExpr.Lambda(List.of("__d"),
                    declaredOnly(SqlExpr.Column.of(null, "__d", a.element(), true,
                            com.legend.sql.OutputCol.Origin.DERIVED), a.element(), false)));
        }
        if (!(type instanceof SqlType.Struct s) || !carriesSynthetic(s)) {
            return v;
        }
        List<SqlExpr.StructLit.Field> fields = new ArrayList<>();
        for (SqlType.Struct.Field f : kept(s)) {
            SqlExpr get = SqlExpr.StructGet.of(v, f.name());
            fields.add(new SqlExpr.StructLit.Field(f.name(), declaredOnly(get, f.type(), true), f.type()));
        }
        SqlExpr object = new SqlExpr.StructLit(fields);
        // an absent object stays absent, not a struct of NULLs
        return mayBeAbsent ? new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, v),
                new SqlExpr.NullLit())), object) : object;
    }

    /** {@code t} without its objects' synthetic fields, at any depth (the type {@link #declaredOnly} answers). */
    static SqlType declaredType(SqlType t) {
        if (t instanceof SqlType.Array a) {
            return new SqlType.Array(declaredType(a.element()));
        }
        if (t instanceof SqlType.Struct s && carriesSynthetic(s)) {
            return new SqlType.Struct(kept(s).stream()
                    .map(f -> new SqlType.Struct.Field(f.name(), declaredType(f.type()))).toList());
        }
        return t;
    }

    /** The fields of {@code s} a reader sees: the declared ones, or the class alone where there are none. */
    private static List<SqlType.Struct.Field> kept(SqlType.Struct s) {
        List<SqlType.Struct.Field> declared = s.fields().stream().filter(f -> !synthetic(f.name())).toList();
        return declared.isEmpty() ? s.fields().stream()
                .filter(f -> com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE.equals(f.name())).toList()
                : declared;
    }

    private static boolean carriesSynthetic(SqlType t) {
        if (t instanceof SqlType.Struct s) {
            return s.fields().stream().anyMatch(f -> synthetic(f.name()) || carriesSynthetic(f.type()));
        }
        return t instanceof SqlType.Array a && carriesSynthetic(a.element());
    }

    private static boolean synthetic(String name) {
        return com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID.equals(name)
                || com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE.equals(name)
                || com.legend.compiler.element.ClassLayouts.SYNTHETIC_CANON.equals(name);
    }

    /** An instance entering the JSON slot of a class with no layout of its own (instances of several classes meeting
     *  at an abstract ancestor): its struct carries its own class as {@code __type} (2026-10-20, docs/PLAN_PATH_PROGRAM_2026_10_17.md §9), the
     *  class a reader builds it as (the PCT's bridge, the result boundary program's phase 4); any other value
     *  unchanged. */
    static SqlExpr withClass(com.legend.compiler.spec.typed.TypedSpec e, SqlExpr lowered) {
        if (e.info().type() instanceof Type.ClassType own && lowered instanceof SqlExpr.StructLit s
                && s.fields().stream().noneMatch(f ->
                        com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE.equals(f.name()))) {
            java.util.List<SqlExpr.StructLit.Field> fields = new java.util.ArrayList<>(s.fields());
            fields.add(new SqlExpr.StructLit.Field(com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE,
                    new SqlExpr.StringLit(own.fqn())));
            return new SqlExpr.StructLit(fields);
        }
        return lowered;
    }

    private static boolean staticallyNonNull(SqlExpr e) {
        return e instanceof SqlExpr.StringLit || e instanceof SqlExpr.IntLit
                || e instanceof SqlExpr.FloatLit
                || e instanceof SqlExpr.DecimalLit
                || e instanceof SqlExpr.BoolLit || e instanceof SqlExpr.DateLit
                || e instanceof SqlExpr.TimestampLit;
    }

    /** The Pair STRUCT carrier {@code (first, second)}: a slot the Pair
     * declares Any rides the JSON carrier ({@link #variantSlot}). */
    static SqlExpr pairStruct(Type pairType, SqlExpr first, Type firstType,
            SqlExpr second, Type secondType) {
        List<Type> pairArgs = pairType instanceof Type.GenericType pg ? pg.arguments() : List.of();
        SqlExpr[] slots = {first, second};
        Type[] types = {firstType, secondType};
        for (int i = 0; i < 2; i++) {
            if (i < pairArgs.size() && pairArgs.get(i) instanceof Type.ClassType ct
                    && PlatformTypes.isAny(ct)) {
                slots[i] = variantSlot(slots[i], types[i]);
            }
        }
        return new SqlExpr.StructLit(List.of(
                new SqlExpr.StructLit.Field("first", slots[0]),
                new SqlExpr.StructLit.Field("second", slots[1])));
    }

    static SqlExpr variantSlot(SqlExpr x, Type staticType) {
        com.legend.sql.SqlType wire = x.type() instanceof com.legend.sql.TypeFact.Typed tf ? tf.type() : null;
        if (wire == com.legend.sql.SqlType.Scalar.JSON) {
            return x;
        }
        // a typed primitive, or a plain wire value (a VARCHAR cell read): the JSON value of what it holds (the
        // spelling carrier's text, read back by its spelling here until track L, 2026-10-29, is gone)
        return SqlExpr.Call.of(SqlFn.TO_VARIANT, x);
    }

    static SqlExpr unwrapVariant(SqlExpr x) {
        if (x instanceof SqlExpr.Call c && c.fn() == SqlFn.COALESCE
                && c.args().size() == 2
                && c.args().get(0) instanceof SqlExpr.Call inner
                && inner.fn() == SqlFn.TO_VARIANT
                && c.args().get(1) instanceof SqlExpr.Cast cast
                && cast.target() == com.legend.sql.SqlType.Scalar.JSON
                && cast.value() instanceof SqlExpr.StringLit s
                && s.value().equals("null")) {
            return inner.args().get(0);
        }
        if (x instanceof SqlExpr.Call c && c.fn() == SqlFn.TO_VARIANT) {
            return c.args().get(0);
        }
        return x;
    }

    /** True iff {@link #unwrapVariant} would peel — the carrier-presence
     * question the membership/aggregate arms ask. */
    static boolean variantWrapped(SqlExpr x) {
        return unwrapVariant(x) != x;
    }

    /** A constructed field's value in its layout SLOT's carrier (WORLD_MAP
     * §4): a struct-shaped value (a class with a layout — a single
     * {@code [$d]} or a many navigation) bound to a JSON slot (the declared
     * class is polymorphic/layoutless: {@code parameters :
     * RelationalOperationElement[*]}) takes the variant carrier per
     * element — the same {@code to_json} the literal-collection arm
     * spells, so every instance of the class shares ONE struct type
     * (DuckDB unifies the shape-CASE branches; the wire decodes per
     * element). Values already on the JSON carrier pass through. */
    static SqlExpr slotCarrier(SqlExpr v, boolean many, SqlType valueType,
            SqlType slot, java.util.function.Supplier<String> fresh) {
        if (slot != SqlType.Scalar.JSON || !(valueType instanceof SqlType.Struct)) {
            return v;
        }
        if (many) {
            String x = fresh.get();
            return SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, v,
                    new SqlExpr.Lambda(List.of(x),
                            SqlExpr.Call.of(SqlFn.TO_VARIANT, SqlExpr.Column.derived(null, x))));
        }
        return SqlExpr.Call.of(SqlFn.TO_VARIANT, v);
    }

    /** The identity layout's SYNTHETIC fields at a construction/copy site:
     * F13 {@code __id} (one deterministic id per site — a copy is a NEW
     * instance) and WORLD_MAP §4 {@code __type} (the constructed class, so
     * a polymorphic slot's value is judged by its own classifier). Null for
     * a model field. */
    static SqlExpr.StructLit.@com.legend.base.Nullable Field syntheticField(
            Type.Column c, @com.legend.base.Nullable String siteId, String classFqn) {
        return syntheticField(c, siteId == null ? null : new SqlExpr.StringLit(siteId), classFqn);
    }

    /** {@link #syntheticField(Type.Column, String, String)} with the object's id an expression: its site's id and the
     *  positions of the iterations around it (docs/OBJECT_IDENTITY_2026_10_20.md §3.2). */
    static SqlExpr.StructLit.@com.legend.base.Nullable Field syntheticField(
            Type.Column c, @com.legend.base.Nullable SqlExpr id, String classFqn) {
        if (com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID.equals(c.name())
                && id != null) {
            return new SqlExpr.StructLit.Field(c.name(), id, SqlType.Scalar.VARCHAR);
        }
        if (com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE.equals(c.name())) {
            return new SqlExpr.StructLit.Field(c.name(), new SqlExpr.StringLit(classFqn));
        }
        return null;
    }


    static String simpleName(String qn) {
        int cut = qn.lastIndexOf("::");
        return cut < 0 ? qn : qn.substring(cut + 2);
    }


    /** Pure LITERAL FLATTENING at construction (audit-of-R1: consumer-site
     * compaction was whack-a-mole): a VALUE-lane literal collection with an
     * element that CAN be empty ([0..1]/[0..*] stamped — an optional, a
     * conditional-membership residual) compacts ONCE so every consumer sees
     * the pure collection. */
    static boolean compacts(TypedCollection c) {
        return CollectionLanes.valueLane(c) && c.elements().stream().anyMatch(e ->
                e.info().multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                && b.lower() == 0);
    }
}
