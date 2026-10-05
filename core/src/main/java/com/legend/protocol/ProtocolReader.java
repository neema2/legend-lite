// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CByteArray;
import com.legend.protocol.spec.CDate;
import com.legend.protocol.spec.CDecimal;
import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CLatestDate;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.CTime;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.TypeAnnotation;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import com.legend.values.PureDateLiteral;
import com.legend.values.PureTimeLiteral;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Protocol JSON &rarr; the value-specification records the parser produces &mdash; the MIRROR of
 * {@link ProtocolEmitter}'s value-specification rules, so an upstream {@code pure/v1} request (a
 * {@code function} or {@code lambda} field) compiles exactly as the same query written in text
 * (docs/UPSTREAM_ENDPOINTS_DESIGN_2026_09_27.md, U1), and so every value specification inside a model
 * reads back to records that emit the same bytes ({@link ModelReader}, the protocol program's read leg).
 *
 * <p>Every wire quirk the emitter reproduces is undone here, rule for rule: an enum value arrives as a
 * {@code property} on a {@code packageableElementPtr}; a {@code receiver.name(args)} call as a
 * {@code property} with its arguments after the receiver; {@code ^X(k=v)} as a {@code func new} over a
 * {@code Class<X>} type instance; {@code #>{db.schema.T}#} as a {@code classInstance} of type {@code ">"}
 * with a path; the root package as a literal {@code null}. A {@code _type} (or {@code classInstance}
 * type) with no rule here is REFUSED, naming it &mdash; never skipped, never guessed; so is a field no
 * rule takes ({@link Wire}). Positions come from {@code sourceInformation} when the JSON carries it, and
 * are absent otherwise. Islands and literals with a shape of their own read in {@link SpecIslandReader}.
 */
public final class ProtocolReader {

    private ProtocolReader() {
    }

    /** A {@code {"_type":"lambda",...}} object, as text. */
    public static LambdaFunction lambda(String json) {
        return lambda(Json.parseObject(json));
    }

    /**
     * A {@code {"_type":"lambda",...}} object. Older protocol shapes are brought current first,
     * as upstream reads them ({@link ProtocolUpgrade}).
     */
    public static LambdaFunction lambda(Json.Obj o) {
        return lambdaNode(ProtocolUpgrade.upgrade(o));
    }

    /** A lambda node, already upgraded. */
    static LambdaFunction lambdaNode(Json.Node node) {
        Wire w = Wire.of(node, "lambda");
        String type = w.type();
        if (!"lambda".equals(type)) {
            throw Wire.refuse("expected a lambda, got _type '" + type + "'");
        }
        return readLambda(w);
    }

    /** One value specification node (already upgraded). */
    public static ValueSpecification valueSpec(Json.Node node) {
        if (node instanceof Json.Null) {
            // the ROOT PACKAGE spelled '::' is a literal null on the wire
            return new PackageableElementPtr("::");
        }
        Wire w = Wire.of(node, "value specification");
        String type = w.type();
        return w.done(Wire.rule(SPECS, type, "value specification").apply(w));
    }

    /** The reader rule for each value-specification {@code _type} on the wire. */
    private static final Map<String, Function<Wire, ValueSpecification>> SPECS = Map.ofEntries(
            Map.entry("lambda", ProtocolReader::readLambda),
            Map.entry("var", ProtocolReader::variableRef),
            Map.entry("func", ProtocolReader::func),
            Map.entry("property", ProtocolReader::property),
            Map.entry("collection", w -> new PureCollection(w.list("values", ProtocolReader::valueSpec),
                    collectionSpan(w))),
            Map.entry("string", ProtocolReader::string),
            Map.entry("boolean", w -> new CBoolean(w.bool("value"), w.span())),
            Map.entry("integer", w -> integer(w.take("value"), w.span())),
            Map.entry("float", ProtocolReader::floating),
            Map.entry("decimal", w -> new CDecimal(w.decimal("value"), null, w.span())),
            Map.entry("strictDate", w -> date(w, true)),
            Map.entry("dateTime", w -> date(w, false)),
            Map.entry("latestDate", w -> new CLatestDate(w.span())),
            Map.entry("strictTime", ProtocolReader::time),
            Map.entry("packageableElementPtr", w -> pointer(w, false)),
            Map.entry("unitType", w -> pointer(w, true)),
            Map.entry("enumValue", w -> new EnumValue(w.str("fullPath"), w.str("value"), null, w.span(), true)),
            Map.entry("byteArray", w -> new CByteArray(w.str("value"), w.span())),
            Map.entry("classInstance", SpecIslandReader::classInstance),
            Map.entry("genericTypeInstance", SpecIslandReader::typeInstance));

    // ---------------------------------------------------------------------
    // Lambdas and variables
    // ---------------------------------------------------------------------

    private static LambdaFunction readLambda(Wire w) {
        List<Variable> params = w.list("parameters", ProtocolReader::lambdaParameter);
        List<ValueSpecification> body = w.list("body", ProtocolReader::valueSpec);
        return w.done(new LambdaFunction(params, body, w.span()));
    }

    /**
     * A lambda parameter: an UNTYPED one is the bare {@code {"_type":"var","name":...}} (no span, no
     * multiplicity); a TYPED one carries its type, multiplicity and the span of its declaration.
     */
    private static Variable lambdaParameter(Json.Node node) {
        Wire w = Wire.of(node, "lambda parameter");
        expectVar(w);
        Json.Node gt = w.opt("genericType");
        if (gt == null) {
            return w.done(new Variable(w.str("name")));
        }
        return w.done(new Variable(w.str("name"), genericType(gt), multiplicity(w.take("multiplicity")),
                w.span()));
    }

    private static void expectVar(Wire w) {
        String type = w.type();
        if (!"var".equals(type)) {
            throw Wire.refuse("expected a var, got _type '" + type + "'");
        }
    }

    /** A variable REFERENCE ({@code $x}): name and span, never a type. */
    private static ValueSpecification variableRef(Wire w) {
        return new Variable(w.str("name"), null, null, w.span());
    }

    // ---------------------------------------------------------------------
    // Applications
    // ---------------------------------------------------------------------

    /**
     * A {@code func}. Two spellings come back to the record the grammar builds: {@code ^X(...)} (a
     * {@code new} over a {@code Class<X>} type instance, {@link SpecIslandReader#newInstance}), and every
     * other call, kept as written ({@code let}'s name string, the caret specials {@code pair}/{@code col}
     * the engine desugars to, a table reference spelled as a call).
     */
    private static ValueSpecification func(Wire w) {
        String function = w.str("function");
        List<Json.Node> raw = w.arr("parameters");
        SourceInfo pos = w.span();
        if (AppliedFunction.NEW.equals(function)) {
            ValueSpecification ni = SpecIslandReader.newInstance(raw, pos);
            if (ni != null) {
                return ni;
            }
        }
        List<ValueSpecification> params = new ArrayList<>(raw.size());
        for (Json.Node p : raw) {
            params.add(valueSpec(p));
        }
        return new AppliedFunction(function, params, List.of(), pos);
    }

    /**
     * A {@code property} node is one of three things on the wire (the emitter's rules): an ENUM value
     * (one parameter, a {@code packageableElementPtr}), a {@code receiver.name(args)} call (arguments
     * after the receiver), or a plain property access.
     */
    private static ValueSpecification property(Wire w) {
        List<ValueSpecification> params = w.list("parameters", ProtocolReader::valueSpec);
        String name = w.str("property");
        SourceInfo pos = w.span();
        if (params.isEmpty()) {
            throw Wire.refuse("a property node has no receiver: " + name);
        }
        if (params.size() == 1 && params.get(0) instanceof PackageableElementPtr ptr) {
            return new EnumValue(ptr.fullPath(), name, ptr.pos(), pos, false);
        }
        if (params.size() > 1) {
            return new AppliedFunction(name, params, List.of(), pos, true, false);
        }
        return new AppliedProperty(params.get(0), name, pos);
    }

    /** A collection's multiplicity is its size, written twice; anything else has no record. */
    private static @com.legend.base.Nullable SourceInfo collectionSpan(Wire w) {
        int size = w.arr("values").size();
        multiplicityOfSize(w.take("multiplicity"), size, "collection");
        return w.span();
    }

    /** {@code {"lowerBound":n,"upperBound":n}} where {@code n} is a collection's size. */
    static void multiplicityOfSize(Json.Node m, int size, String what) {
        Multiplicity read = multiplicity(m);
        if (!(read instanceof Multiplicity.Concrete c) || c.lowerBound() != size || c.upperBound() == null
                || c.upperBound() != size) {
            throw Wire.refuse(what + " multiplicity " + read + " is not its size " + size);
        }
    }

    // ---------------------------------------------------------------------
    // Literals
    // ---------------------------------------------------------------------

    private static ValueSpecification string(Wire w) {
        Boolean multiLine = w.optBool("multiLine");
        if (multiLine != null && !multiLine) {
            throw Wire.refuse("a string with multiLine:false -- the wire never spells it");
        }
        return new CString(w.str("value"), w.span(), multiLine != null);
    }

    static CInteger integer(Json.Node v, @com.legend.base.Nullable SourceInfo pos) {
        BigDecimal d = Wire.exact(v, "integer literal");
        BigInteger i;
        if (d.scale() > 0 && d.stripTrailingZeros().scale() > 0) {
            throw Wire.refuse("an integer literal whose value is not an integer: " + d);
        }
        i = d.toBigIntegerExact();
        return i.bitLength() <= 63 ? new CInteger(i.longValue(), pos) : new CInteger(i, pos);
    }

    /**
     * A float literal: the double the wire carries, and its exact digits only where they do not survive the
     * double (the parser's rule, NumberLiterals.floating) -- never for JSON the emitter wrote, whose float is a
     * double's own spelling.
     */
    private static ValueSpecification floating(Wire w) {
        double d = Wire.asDouble(w.take("value"), "float literal");
        BigDecimal exact = w.decimal("value");
        return new CFloat(d, exact.compareTo(BigDecimal.valueOf(d)) != 0 ? exact : null, w.span());
    }

    /**
     * A date literal. The wire carries the source spelling verbatim; a MONTH-precision value keeps a
     * leading {@code %} (the emitter's quirk), undone here. DAY precision is {@code strictDate}, every
     * other precision {@code dateTime}: a value on the other tag has no record that emits it.
     */
    private static ValueSpecification date(Wire w, boolean strict) {
        String written = w.str("value");
        String body = written.startsWith("%") ? written.substring(1) : written;
        PureDateLiteral value = PureDateLiteral.parse(body);
        boolean day = value.precision() == PureDateLiteral.Precision.DAY;
        boolean month = value.precision() == PureDateLiteral.Precision.MONTH;
        if (day != strict || month != written.startsWith("%")) {
            throw Wire.refuse("date literal '" + written + "' on the wrong tag for its precision");
        }
        return new CDate(value, body, w.span());
    }

    /** {@code %10:10:10} -- the value verbatim without the {@code %}; an out-of-range time keeps its text. */
    private static ValueSpecification time(Wire w) {
        String written = w.str("value");
        return new CTime(timeOrNull(written), written, w.span());
    }

    private static @com.legend.base.Nullable PureTimeLiteral timeOrNull(String written) {
        try {
            return PureTimeLiteral.parse(written);
        } catch (IllegalArgumentException outOfRange) {
            return null;
        }
    }

    /** A {@code packageableElementPtr}, or a {@code unitType} -- the same pointer, a {@code ~} in its path. */
    private static ValueSpecification pointer(Wire w, boolean unit) {
        String path = w.str("fullPath");
        if ((path.indexOf('~') >= 0) != unit) {
            throw Wire.refuse("pointer '" + path + "' on the wrong tag (a unit path has '~' and only a unit has)");
        }
        return new PackageableElementPtr(path, w.span());
    }

    // ---------------------------------------------------------------------
    // Types
    // ---------------------------------------------------------------------

    /**
     * The wire's {@code genericType}: a named type, a generic application (type arguments,
     * multiplicity arguments, type-variable values), or a relation type. The engine's backward-compat
     * {@code Result} -- written {@code Result<Any|1..*>} with a span-less {@code Any} -- is the bare
     * {@code Result} the grammar parsed.
     */
    static TypeExpression genericType(Json.Node node) {
        Wire gt = Wire.of(node, "genericType");
        List<Json.Node> multArgs = gt.arr("multiplicityArguments");
        Wire raw = gt.obj("rawType");
        List<Json.Node> typeArgs = gt.arr("typeArguments");
        List<ValueSpecification> tvv = new ArrayList<>();
        for (Json.Node v : gt.arr("typeVariableValues")) {
            tvv.add(valueSpec(v));
        }
        String rawType = raw.type();
        if ("relationType".equals(rawType)) {
            if (!multArgs.isEmpty() || !typeArgs.isEmpty() || !tvv.isEmpty()) {
                throw Wire.refuse("a relation type with type, multiplicity or value arguments");
            }
            TypeExpression rt = new TypeExpression.RelationType(raw.list("columns", ProtocolReader::column));
            raw.done(rt);
            return gt.done(rt);
        }
        if (!"packageableType".equals(rawType)) {
            throw Wire.refuse("no reader rule for a generic type whose rawType is '" + rawType
                    + "' -- add the rule, do not drop it");
        }
        String path = raw.str("fullPath");
        SourceInfo pos = raw.span();
        raw.done(path);
        if (isBareResult(path, multArgs, typeArgs, tvv)) {
            return gt.done(new TypeExpression.NameRef(path, pos));
        }
        List<TypeExpression> args = new ArrayList<>(typeArgs.size());
        for (Json.Node a : typeArgs) {
            args.add(genericType(a));
        }
        List<String> mults = new ArrayList<>(multArgs.size());
        for (Json.Node m : multArgs) {
            mults.add(multiplicityText(multiplicity(m)));
        }
        return gt.done(args.isEmpty() && mults.isEmpty() && tvv.isEmpty()
                ? new TypeExpression.NameRef(path, pos)
                : new TypeExpression.Generic(path, args, mults, tvv, pos));
    }

    /** {@code Result} with exactly the engine's synthesized {@code <Any|1..*>} (a span-less Any). */
    private static boolean isBareResult(String path, List<Json.Node> multArgs, List<Json.Node> typeArgs,
            List<ValueSpecification> tvv) {
        if (!"Result".equals(path) || multArgs.size() != 1 || typeArgs.size() != 1 || !tvv.isEmpty()) {
            return false;
        }
        Multiplicity m = multiplicity(multArgs.get(0));
        TypeExpression any = genericType(typeArgs.get(0));
        return m.equals(new Multiplicity.Concrete(1, null)) && any instanceof TypeExpression.NameRef n
                && n.name().equals("meta::pure::metamodel::type::Any") && n.pos() == null;
    }

    /** A relation-type column: always spelled with its multiplicity on the wire. */
    private static TypeExpression.Column column(Json.Node node) {
        Wire c = Wire.of(node, "relation column");
        return c.done(new TypeExpression.Column(c.str("name"), genericType(c.take("genericType")),
                multiplicity(c.take("multiplicity")), true, c.span()));
    }

    /** {@code {"lowerBound":n,"upperBound":m}}; an absent upper bound is {@code *}. */
    static Multiplicity multiplicity(Json.Node node) {
        Wire m = Wire.of(node, "multiplicity");
        return m.done(new Multiplicity.Concrete(m.integer("lowerBound"), m.optInt("upperBound")));
    }

    /** A multiplicity argument as the grammar spells it ({@code *}, {@code 1}, {@code 0..1}, {@code 1..*}). */
    static String multiplicityText(Multiplicity m) {
        Multiplicity.Concrete c = (Multiplicity.Concrete) m;
        if (c.upperBound() == null) {
            return c.lowerBound() == 0 ? "*" : c.lowerBound() + "..*";
        }
        return c.lowerBound() == c.upperBound() ? String.valueOf(c.lowerBound())
                : c.lowerBound() + ".." + c.upperBound();
    }

    /** A {@code @Type} annotation's record: a unit type keeps its name span on the annotation. */
    static TypeAnnotation.Named named(TypeExpression type, @com.legend.base.Nullable SourceInfo pos) {
        return new TypeAnnotation.Named(type, pos);
    }
}
