// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * The OLDER value-specification shapes legend-engine 4.145.0 still reads, each read as the record that means what
 * the engine makes of it -- part of {@link ProtocolReader} (one reader), apart because the emitter writes none of
 * them (docs/PROTOCOL_PROGRAM_2026_10_05.md, leg 2 step 2: the table of rules and the engine code each follows).
 * Three kinds:
 *
 * <ul>
 *   <li>what the engine's reader turns into today's object: the {@code class}, {@code enum},
 *       {@code mappingInstance}, {@code databaseInstance} and {@code primitiveType} pointers, the
 *       {@code hackedClass}/{@code hackedUnit} type annotations, a literal written as a {@code values} list, and the
 *       {@code classInstance} kinds written under their own {@code _type};</li>
 *   <li>what the engine keeps and compiles to the object a library function builds: a {@code qualifiedProperty} is
 *       a property access with arguments, an {@code aggregateValue} is {@code agg(...)}, a {@code tdsOlapRank} is
 *       {@code tds::func(...)}, and so on -- read as that call;</li>
 *   <li>what no Pure text means ({@code runtimeInstance}, {@code executionContextInstance},
 *       {@code alloySerializationConfig}, {@code whatever}, {@code unknownFunc}): refused by name.</li>
 * </ul>
 */
final class OlderSpecReader {

    private OlderSpecReader() {
    }

    private static final String PAIR = "meta::pure::functions::collection::pair";
    private static final String AGG = "meta::pure::functions::collection::agg";
    private static final String TDS_AGG = "meta::pure::tds::agg";
    private static final String TDS_COL = "meta::pure::tds::col";
    private static final String TDS_FUNC = "meta::pure::tds::func";
    private static final String NEW_UNIT = "newUnit";

    /** The {@code classInstance} kinds the engine also reads under their own {@code _type} ({@code ValueSpecification}'s
     *  subtypes for {@code ClassInstanceWrapper}). */
    private static final Set<String> WRAPPED = Set.of("path", "rootGraphFetchTree", "listInstance", "pair",
            "aggregateValue", "tdsAggregateValue", "tdsColumnInformation", "tdsSortInformation", "tdsOlapRank",
            "tdsOlapAggregation", "runtimeInstance", "executionContextInstance", "alloySerializationConfig");

    /** The literals the engine reads through {@code PrimitiveValueSpecification.customParsePrimitive}: each may be
     *  written as a {@code values} list. ({@code strictTime} and {@code byteArray} are read field by field, and the
     *  engine drops a list there.) */
    private static final Set<String> VALUES_LISTS = Set.of("integer", "float", "decimal", "boolean", "dateTime",
            "strictDate", "latestDate", "string");

    /** The value specifications whose {@code multiplicity} is their own (a variable's, a collection's size). */
    private static final Set<String> OWN_MULTIPLICITY = Set.of("var", "collection");

    private static final Multiplicity ONE = new Multiplicity.Concrete(1, 1);

    /** Today's rules and the older {@code _type}s' together, one table. */
    static Map<String, Function<Wire, ValueSpecification>> withOlder(
            Map<String, Function<Wire, ValueSpecification>> current) {
        Map<String, Function<Wire, ValueSpecification>> all = new LinkedHashMap<>(current);
        all.put("class", w -> ProtocolReader.pointer(w, false));
        all.put("enum", w -> ProtocolReader.pointer(w, false));
        all.put("mappingInstance", w -> ProtocolReader.pointer(w, false));
        all.put("databaseInstance", w -> ProtocolReader.pointer(w, false));
        all.put("primitiveType", w -> new PackageableElementPtr(oneOf(w, "name", "fullPath"), w.span()));
        all.put("hackedClass", w -> SpecIslandReader.annotation(w.str("fullPath"), w.span()));
        all.put("hackedUnit", w -> SpecIslandReader.annotation(oneOf(w, "unitType", "fullPath"), w.span()));
        all.put("qualifiedProperty", OlderSpecReader::qualifiedProperty);
        all.put("unitInstance", OlderSpecReader::unitInstance);
        all.put("whatever", w -> {
            throw noText("whatever", "the engine marks it 'should not be coming to the system'");
        });
        all.put("unknownFunc", w -> {
            throw noText("unknownFunc", "the engine marks it 'should not be coming to the system'");
        });
        for (String kind : WRAPPED) {
            all.put(kind, OlderSpecReader::wrapped);
        }
        return java.util.Collections.unmodifiableMap(all);
    }

    // ---------------------------------------------------------------------
    // What every value specification may carry
    // ---------------------------------------------------------------------

    /**
     * A {@code multiplicity} on a single value, which the engine's reader takes and never uses ({@code One}'s
     * write-only field; its own readers do not look): accepted when it is {@code [1]}, what the value already is;
     * anything else is refused, never dropped. A {@code classInstance} kind under its own {@code _type} is read by
     * a strict class, which refuses one; so does this reader, through that kind's rule.
     */
    static void singleValue(Wire w, @com.legend.base.Nullable String type) {
        if (type == null || OWN_MULTIPLICITY.contains(type) || WRAPPED.contains(type)) {
            return;
        }
        Json.Node m = w.opt("multiplicity");
        if (m != null) {
            Multiplicity read = ProtocolReader.multiplicity(m);
            if (!read.equals(ONE)) {
                throw Wire.refuse("a " + type + " with multiplicity " + read
                        + ": the engine ignores it, and the value is one");
            }
        }
    }

    // ---------------------------------------------------------------------
    // A literal written as a values list
    // ---------------------------------------------------------------------

    static boolean isValuesList(Wire w, String type) {
        if ((type.equals("strictTime") || type.equals("byteArray")) && w.has("values")) {
            throw Wire.refuse("a " + type + " written as a values list: the engine reads a " + type
                    + " field by field and drops the list");
        }
        return VALUES_LISTS.contains(type) && w.has("values");
    }

    /**
     * {@code {"_type":"integer","values":[...]}}, the engine's older literal ({@code customParsePrimitive}): none is
     * an empty collection (with no position, as the engine makes it), one is the literal, more a collection of
     * position-free literals. Its {@code multiplicity}, which the engine never reads, must be the list's size. Before
     * all of it, a string's "Fix Empty Set Bug" ({@code CString.CStringDeserializer}): an empty list under an upper
     * bound of one is the empty string, with no position.
     */
    static ValueSpecification valuesList(Wire w, String type) {
        List<Json.Node> values = w.arr("values");
        Json.Node m = w.opt("multiplicity");
        if ("string".equals(type) && values.isEmpty() && m instanceof Json.Obj mo
                && mo.fields().get("upperBound") instanceof Json.Num ub && ub.isInteger() && ub.longValue() == 1) {
            w.span();
            return new CString("");
        }
        if (w.has("value")) {
            throw Wire.refuse("a " + type + " with both 'value' and 'values': the engine reads 'values' alone");
        }
        if (m != null) {
            Multiplicity read = ProtocolReader.multiplicity(m);
            if (!read.equals(new Multiplicity.Concrete(values.size(), values.size()))) {
                throw Wire.refuse("a " + type + " list of " + values.size() + " with multiplicity " + read
                        + ": the engine ignores it, and the value is the list");
            }
        }
        if (values.size() == 1) {
            // the one literal, with the rest of the object (its position; a string's multiLine) as written
            LinkedHashMap<String, Json.Node> one = new LinkedHashMap<>();
            one.put("_type", Json.str(type));
            one.put("value", values.get(0));
            one.putAll(w.rest().fields());
            return ProtocolReader.valueSpec(new Json.Obj(one));
        }
        SourceInfo span = w.span();
        List<ValueSpecification> literals = new ArrayList<>(values.size());
        for (Json.Node v : values) {
            LinkedHashMap<String, Json.Node> one = new LinkedHashMap<>();
            one.put("_type", Json.str(type));
            one.put("value", v);
            literals.add(ProtocolReader.valueSpec(new Json.Obj(one)));
        }
        return new PureCollection(literals, values.isEmpty() ? null : span);
    }

    // ---------------------------------------------------------------------
    // Pointers and properties
    // ---------------------------------------------------------------------

    /**
     * Exactly one of two string fields, the engine's older and current names for one thing; with both, the engine
     * reads the first and drops the second, so both is refused.
     */
    static String oneOf(Wire w, String first, String second) {
        String a = w.optStr(first);
        String b = w.optStr(second);
        if (a != null) {
            if (b != null) {
                throw Wire.refuse(w.where() + " has both '" + first + "' and '" + second
                        + "': the engine reads '" + first + "' and drops the other");
            }
            return a;
        }
        if (b == null) {
            throw Wire.refuse(w.where() + " has no '" + second + "'");
        }
        return b;
    }

    /**
     * {@code qualifiedProperty}: the engine compiles it exactly as a property access with arguments
     * ({@code ValueSpecificationBuilder.visit(AppliedQualifiedProperty)}, {@code processProperty}); its
     * {@code class} (the receiver's class) is kept, as a property's is.
     */
    private static ValueSpecification qualifiedProperty(Wire w) {
        String ownerClass = w.optStr("class");
        List<ValueSpecification> params = w.list("parameters", ProtocolReader::valueSpec);
        return ProtocolReader.propertyAccess(w.str("qualifiedProperty"), params, w.span(), ownerClass);
    }

    /**
     * {@code unitInstance}, a quantity of a unit: what the grammar spells {@code 5 Mass~Kilogram} and parses to its
     * constructor {@code newUnit(Mass~Kilogram, 5)} (SpecParser's unit literal).
     */
    private static ValueSpecification unitInstance(Wire w) {
        String unit = w.str("unitType");
        if (unit.indexOf('~') < 0) {
            throw Wire.refuse("a unitInstance whose unitType '" + unit + "' names no unit (no '~')");
        }
        Json.Node value = w.take("unitValue");
        if (!(value instanceof Json.Num n)) {
            throw Wire.refuse("a unitInstance whose unitValue is not a number: " + Wire.abbreviate(value));
        }
        LinkedHashMap<String, Json.Node> number = new LinkedHashMap<>();
        number.put("_type", Json.str(n.isInteger() ? "integer" : "float"));
        number.put("value", value);
        return new AppliedFunction(NEW_UNIT, List.of(new PackageableElementPtr(unit),
                ProtocolReader.valueSpec(new Json.Obj(number))), List.of(), w.span());
    }

    // ---------------------------------------------------------------------
    // classInstance kinds
    // ---------------------------------------------------------------------

    /**
     * A {@code classInstance} kind written under its own {@code _type} ({@code ClassInstanceWrapper}). The engine
     * never sees that {@code _type} (Jackson takes it to pick the reader), so the kind is the first of its field
     * tests, in its order; the rest of the object is that kind's value, and its span the instance's.
     */
    private static ValueSpecification wrapped(Wire w) {
        Json.Obj value = w.rest();
        String kind = kindOf(value);
        Json.Node at = value.fields().get("sourceInformation");
        SourceInfo pos = at == null ? null : Wire.sourceInfo(at, kind);
        if ("rootGraphFetchTree".equals(kind)) {
            LinkedHashMap<String, Json.Node> typed = new LinkedHashMap<>(value.fields());
            typed.put("_type", Json.str("rootGraphFetchTree"));
            value = new Json.Obj(typed);
        }
        return SpecIslandReader.classInstanceValue(kind, value, pos);
    }

    /** {@code ClassInstanceWrapper.ClassInstanceWrapperDeserializer}'s field tests, first match wins. */
    private static String kindOf(Json.Obj o) {
        Map<String, Json.Node> f = o.fields();
        if (f.containsKey("path")) {
            return "path";
        }
        if (f.containsKey("class")) {
            return "rootGraphFetchTree";
        }
        if (f.containsKey("values") || f.isEmpty() || (f.size() == 1 && f.containsKey("sourceInformation"))) {
            return "listInstance";
        }
        if (f.containsKey("first")) {
            return "pair";
        }
        if (f.containsKey("mapFn")) {
            return f.containsKey("name") ? "tdsAggregateValue" : "aggregateValue";
        }
        if (f.containsKey("columnFn")) {
            return "tdsColumnInformation";
        }
        if (f.containsKey("direction")) {
            return "tdsSortInformation";
        }
        if (f.containsKey("function")) {
            return f.containsKey("columnName") ? "tdsOlapAggregation" : "tdsOlapRank";
        }
        if (f.containsKey("runtime")) {
            return "runtimeInstance";
        }
        if (f.containsKey("executionContext")) {
            return "executionContextInstance";
        }
        if (f.containsKey("typeKeyName")) {
            return "alloySerializationConfig";
        }
        throw Wire.refuse("an older classInstance none of whose fields names its kind (the engine: NOT SUPPORTED): "
                + Wire.abbreviate(o));
    }

    /** The value of each older {@code classInstance} kind, read as the call that builds the same object. */
    static final Map<String, java.util.function.BiFunction<Json.Node, SourceInfo, ValueSpecification>> KINDS =
            Map.ofEntries(
                    Map.entry("listInstance", OlderSpecReader::listInstance),
                    Map.entry("pair", OlderSpecReader::pair),
                    Map.entry("aggregateValue", (v, pos) -> aggregate(v, pos, false)),
                    Map.entry("tdsAggregateValue", (v, pos) -> aggregate(v, pos, true)),
                    Map.entry("tdsColumnInformation", OlderSpecReader::columnInformation),
                    Map.entry("tdsSortInformation", OlderSpecReader::sortInformation),
                    Map.entry("tdsOlapRank", (v, pos) -> olap(v, pos, false)),
                    Map.entry("tdsOlapAggregation", (v, pos) -> olap(v, pos, true)),
                    Map.entry("runtimeInstance", (v, pos) -> {
                        throw noText("runtimeInstance", "the engine's printers write nothing for it");
                    }),
                    Map.entry("executionContextInstance", (v, pos) -> {
                        throw noText("executionContextInstance", "the engine's printers write nothing for it");
                    }),
                    Map.entry("alloySerializationConfig", (v, pos) -> {
                        throw noText("alloySerializationConfig", "the engine's printers cannot write it");
                    }));

    /** {@code PureList}: {@code list([...])}, the {@code List} the library's {@code list} builds. */
    private static ValueSpecification listInstance(Json.Node value, @com.legend.base.Nullable SourceInfo pos) {
        Wire v = Wire.of(value, "listInstance");
        List<ValueSpecification> values = v.listOrEmpty("values", ProtocolReader::valueSpec);
        StoreReader.sameSpan(pos, v.span(), "listInstance");
        return v.done(AppliedFunction.list(new PureCollection(values), pos));
    }

    /** {@code Pair}: {@code pair(first, second)} (the engine's own {@code ^Pair(...)} converter's call). */
    private static ValueSpecification pair(Json.Node value, @com.legend.base.Nullable SourceInfo pos) {
        Wire v = Wire.of(value, "pair");
        ValueSpecification first = ProtocolReader.valueSpec(v.take("first"));
        ValueSpecification second = ProtocolReader.valueSpec(v.take("second"));
        StoreReader.sameSpan(pos, v.span(), "pair");
        return v.done(call(PAIR, pos, first, second));
    }

    /** {@code AggregateValue}: {@code agg(mapFn, aggregateFn)}; {@code TDSAggregateValue}: {@code tds::agg(name, ...)}. */
    private static ValueSpecification aggregate(Json.Node value, @com.legend.base.Nullable SourceInfo pos,
            boolean tds) {
        Wire v = Wire.of(value, tds ? "tdsAggregateValue" : "aggregateValue");
        String name = tds ? v.str("name") : null;
        LambdaFunction map = ProtocolReader.lambdaNode(v.take("mapFn"));
        LambdaFunction agg = ProtocolReader.lambdaNode(v.take("aggregateFn"));
        StoreReader.sameSpan(pos, v.span(), v.where());
        return v.done(name == null ? call(AGG, pos, map, agg) : call(TDS_AGG, pos, new CString(name), map, agg));
    }

    /** {@code TDSColumnInformation}: {@code tds::col(columnFn, name)}, the {@code BasicColumnSpecification} it builds. */
    private static ValueSpecification columnInformation(Json.Node value, @com.legend.base.Nullable SourceInfo pos) {
        Wire v = Wire.of(value, "tdsColumnInformation");
        String name = v.str("name");
        LambdaFunction fn = ProtocolReader.lambdaNode(v.take("columnFn"));
        StoreReader.sameSpan(pos, v.span(), "tdsColumnInformation");
        return v.done(call(TDS_COL, pos, fn, new CString(name)));
    }

    /** {@code TDSSortInformation}: {@code tds::asc(column)} or {@code tds::desc(column)}, by its direction. */
    private static ValueSpecification sortInformation(Json.Node value, @com.legend.base.Nullable SourceInfo pos) {
        Wire v = Wire.of(value, "tdsSortInformation");
        String column = v.str("column");
        String direction = v.str("direction");
        String function = switch (direction) {
            case "ASC" -> "meta::pure::tds::asc";
            case "DESC" -> "meta::pure::tds::desc";
            default -> throw Wire.refuse("a tdsSortInformation direction '" + direction
                    + "': SortDirection has ASC and DESC");
        };
        StoreReader.sameSpan(pos, v.span(), "tdsSortInformation");
        return v.done(call(function, pos, new CString(column)));
    }

    /** {@code TdsOlapRank}: {@code tds::func(function)}; {@code TdsOlapAggregation}: {@code tds::func(columnName, function)}. */
    private static ValueSpecification olap(Json.Node value, @com.legend.base.Nullable SourceInfo pos,
            boolean aggregation) {
        Wire v = Wire.of(value, aggregation ? "tdsOlapAggregation" : "tdsOlapRank");
        String column = aggregation ? v.str("columnName") : null;
        LambdaFunction fn = ProtocolReader.lambdaNode(v.take("function"));
        StoreReader.sameSpan(pos, v.span(), v.where());
        return v.done(column == null ? call(TDS_FUNC, pos, fn) : call(TDS_FUNC, pos, new CString(column), fn));
    }

    private static AppliedFunction call(String function, @com.legend.base.Nullable SourceInfo pos,
            ValueSpecification... params) {
        return new AppliedFunction(function, List.of(params), List.of(), pos);
    }

    private static IllegalArgumentException noText(String kind, String why) {
        return Wire.refuse("an older '" + kind + "' value specification: no Pure text means it (" + why
                + "), so lite has no record for it");
    }
}
