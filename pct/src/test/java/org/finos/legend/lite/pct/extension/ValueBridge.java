// Copyright 2026 Legend Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.lite.pct.extension;

import org.eclipse.collections.api.map.MutableMap;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.element.type.ExprType;

import org.finos.legend.pure.m3.navigation.Instance;
import org.finos.legend.pure.m3.navigation.M3Properties;
import org.finos.legend.pure.m3.navigation.ProcessorSupport;
import org.finos.legend.pure.m3.navigation.ValueSpecificationBootstrap;
import org.finos.legend.pure.m4.ModelRepository;
import org.finos.legend.pure.m4.coreinstance.CoreInstance;
import org.finos.legend.pure.m4.coreinstance.primitive.date.DateFunctions;
import org.finos.legend.pure.m4.coreinstance.primitive.date.PureDate;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * THE OUTBOUND BIJECTION (the permanent piece of the derived minimum,
 * ADAPTER_NECESSITY_CENSUS §5b): the platform's typed TypedSide
 * values become the reference interpreter's CoreInstances — one arm
 * per entry of the platform's CLOSED egress vocabulary, keyed on the
 * VALUE's class, with identity LOOKUPS where the interpreter's
 * equality is object identity (enums, types) and REAL typed instances
 * built from the PLATFORM's own generic returnType (List, Map, class
 * structs). Declared-type reads are ASSERTIONS (loud walls), never
 * choices. This file exists as long as Mode B does; nothing in it is
 * transport-contingent.
 */
final class ValueBridge {

    private final ModelRepository modelRepository;

    ValueBridge(ModelRepository modelRepository) {
        this.modelRepository = modelRepository;
    }

    /** A value's answer: its elements as the plan's typed rows decoded them (a row per element -- a List or a Map is
     *  one), built as the reference's instances by the query's own result type, its type and its multiplicity: a
     *  to-one value as itself, a collection's elements wrapped as one (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md,
     *  phase 4: the bridge reads typed rows, never a text). */
    CoreInstance values(List<?> cells0, com.legend.executionplan.ExecutionPlan.Column column, ExprType declared,
            ProcessorSupport ps) {
        Type type = declared.type();
        // a date the database holds as a timestamp has the precision its digits show (the fraction's trailing zeros
        // dropped), the closest to the precision Pure wrote (register S118); one carried as its own text keeps the
        // precision it was written at -- the plan's written-precision fact (ExecutionPlan.Precision, 2026-10-28,
        // where the slot's spelling was matched until then)
        boolean written = column.precision() == com.legend.executionplan.ExecutionPlan.Precision.WRITTEN;
        List<?> cells = written ? cells0 : cells0.stream().map(ValueBridge::trimmedDates).toList();
        if (!declared.multiplicity().requireBounded("a PCT value").isMany()) {
            if (cells.size() > 1) {
                throw new IllegalStateException("a to-one value (" + declared + ") answered " + cells.size()
                        + " elements");
            }
            Object value = cells.isEmpty() ? null : cells.get(0);
            if (emptyCell(value)) {
                return ValueSpecificationBootstrap.wrapValueSpecification(
                        org.eclipse.collections.api.factory.Lists.immutable.empty(), true, ps);
            }
            return ValueSpecificationBootstrap.wrapValueSpecification(toCoreInstance(value, type, ps), true, ps);
        }
        var coreInstances = new ArrayList<CoreInstance>();
        for (Object value : cells) {
            if (!emptyCell(value)) {
                coreInstances.add(toCoreInstance(value, type, ps));
            }
        }
        // CLASS-typed elements (Pair<T,U>): the interpreted cast validates
        // the WRAPPER's genericType — hand it the declared one explicitly
        if (classFqnOf(type) != null && !isMapReturn(type)) {
            CoreInstance gt = genericTypeOf(type, ps);
            if (gt != null) {
                return ValueSpecificationBootstrap.wrapValueSpecification_ResultGenericTypeIsKnown(
                        org.eclipse.collections.impl.factory.Lists.immutable.withAll(coreInstances),
                        gt, true, ps);
            }
        }
        return ValueSpecificationBootstrap.wrapValueSpecification(
                org.eclipse.collections.impl.factory.Lists.immutable.withAll(coreInstances), true, ps);
    }


    // ===== toCoreInstance: single Java → CoreInstance conversion =====

    /**
     * A JDBC STRUCT (java.util.Map) whose DECLARED type is a CLASS builds a
     * REAL pure instance (the DynamicNew construction pattern) — Pair has
     * equality keys, so reconstructed pairs compare and print like natives.
     * Recursive: nested pair structs rebuild through the generic arguments.
     */
    private CoreInstance structToInstance(java.util.Map<?, ?> struct, Type declared0,
                                          ProcessorSupport ps) {
        // an object's identity fields (docs/OBJECT_IDENTITY_2026_10_20.md §3.4) are no properties: its id and canon
        // are dropped, and its class field names the instance's own class where it is not the declared one (a
        // subclass's instance under its superclass's type)
        Object own = struct.get(com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE);
        Type declared = own instanceof String c && !c.equals(classFqnOf(declared0)) ? new Type.ClassType(c)
                : declared0;
        String fqn = classFqnOf(declared);
        CoreInstance classifier = fqn == null ? null : ps.package_getByUserPath(fqn);
        if (classifier == null) {
            throw new RuntimeException("cannot rebuild struct instance for type " + declared);
        }
        CoreInstance inst = modelRepository.newEphemeralAnonymousCoreInstance(null, classifier);
        // the INTERPRETED cast validates generics — stamp the classifier
        // generic type (the DynamicNew pattern) from the declared engine type
        CoreInstance cgt = genericTypeOf(declared, ps);
        if (cgt != null) {
            Instance.addValueToProperty(inst, M3Properties.classifierGenericType, cgt, ps);
        }
        for (var en : struct.entrySet()) {
            Object v = en.getValue();
            if (emptyCell(v)) {
                continue;   // an absent property, not a value
            }
            String prop = String.valueOf(en.getKey());
            if (prop.equals(com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID)
                    || prop.equals(com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE)
                    || prop.equals(com.legend.compiler.element.ClassLayouts.SYNTHETIC_CANON)) {
                continue;
            }
            // NESTED class-typed properties resolve their REAL type from
            // the interpreter metamodel — the declared parent type must
            // not leak onto children (CO_Firm.employees are CO_Person,
            // never CO_Firm)
            Type pt = v instanceof java.util.Map || v instanceof java.util.List
                    ? classPropertyTypeOf(declared, prop, ps)
                    : propertyTypeOf(declared, prop);
            if (v instanceof java.util.List<?> lv) {
                // multi-valued property: EVERY element reconstructs
                var cis = new java.util.ArrayList<CoreInstance>();
                for (Object e : lv) {
                    if (!emptyCell(e)) {
                        cis.add(toCoreInstance(e, pt, ps));
                    }
                }
                Instance.setValuesForProperty(inst, prop,
                        org.eclipse.collections.impl.factory.Lists.immutable
                                .withAll(cis), ps);
                continue;
            }
            CoreInstance ci = toCoreInstance(v, pt, ps);
            Instance.setValuesForProperty(inst, prop,
                    org.eclipse.collections.impl.factory.Lists.immutable.with(ci), ps);
        }
        return inst;
    }

    /** The property's return type from the interpreter metamodel when it
     * is a CLASS (nested struct reconstruction); {@code declared}
     * otherwise — scalar handling stays untouched. */
    private static Type classPropertyTypeOf(Type declared, String prop,
            ProcessorSupport ps) {
        Type byGenerics = propertyTypeOf(declared, prop);
        if (byGenerics != declared) {
            return byGenerics;   // Pair generics already carry it
        }
        String fqn = classFqnOf(declared);
        if (fqn == null) {
            return declared;
        }
        CoreInstance cls = ps.package_getByUserPath(fqn);
        CoreInstance p = cls == null ? null
                : ps.class_findPropertyUsingGeneralization(cls, prop);
        CoreInstance gt = p == null ? null
                : Instance.getValueForMetaPropertyToOneResolved(
                        p, M3Properties.genericType, ps);
        CoreInstance raw = gt == null ? null
                : Instance.getValueForMetaPropertyToOneResolved(
                        gt, M3Properties.rawType, ps);
        if (raw != null && ps.instance_instanceOf(raw,
                org.finos.legend.pure.m3.navigation.M3Paths.Class)) {
            String pf = org.finos.legend.pure.m3.navigation.PackageableElement
                    .PackageableElement.getUserPathForPackageableElement(raw);
            if (pf != null) {
                return new Type.ClassType(pf);
            }
        }
        return declared;
    }

    /** A pure GenericType CoreInstance mirroring the declared engine type (recursive). */
    private CoreInstance genericTypeOf(Type t, ProcessorSupport ps) {
        String raw = rawPathOf(t);
        if (raw == null) {
            return null;
        }
        CoreInstance rawType = ps.package_getByUserPath(raw);
        if (rawType == null) {
            return null;
        }
        CoreInstance gtClass = ps.package_getByUserPath("meta::pure::metamodel::type::generics::GenericType");
        CoreInstance gt = modelRepository.newEphemeralAnonymousCoreInstance(null, gtClass);
        Instance.addValueToProperty(gt, "rawType", rawType, ps);
        int declared = 0;
        if (t instanceof Type.GenericType g) {
            for (Type arg : g.arguments()) {
                CoreInstance argGt = genericTypeOf(arg, ps);
                if (argGt == null) {
                    return null;
                }
                Instance.addValueToProperty(gt, "typeArguments", argGt, ps);
                declared++;
            }
        }
        // the ENGINE BRIDGE erases generic args (raw Pair) — pad missing
        // arguments with Any so the harness's cast is a legal downcast
        int params = rawType.getValueForMetaPropertyToMany("typeParameters").size();
        for (int i = declared; i < params; i++) {
            CoreInstance anyGt = modelRepository.newEphemeralAnonymousCoreInstance(null, gtClass);
            Instance.addValueToProperty(anyGt, "rawType",
                    ps.package_getByUserPath("meta::pure::metamodel::type::Any"), ps);
            Instance.addValueToProperty(gt, "typeArguments", anyGt, ps);
        }
        return gt;
    }

    /** The M3 path of a type's raw classifier (primitives at their simple names). */
    private static String rawPathOf(Type t) {
        return switch (t) {
            case Type.GenericType g -> g.rawFqn();
            case Type.ClassType ct -> ct.fqn();
            case Type.Primitive p -> p.typeName();
            default -> null;
        };
    }

    private static String classFqnOf(Type t) {
        return switch (t) {
            case Type.GenericType g -> g.rawFqn();
            case Type.ClassType ct -> ct.fqn();
            default -> null;
        };
    }

    /** Pair's first/second resolve through the generic ARGUMENTS (nesting recurses). */
    private static Type propertyTypeOf(Type declared, String prop) {
        if (declared instanceof Type.GenericType g && g.arguments().size() == 2
                && "meta::pure::functions::collection::Pair".equals(classFqnOf(g))) {
            return "first".equals(prop) ? g.arguments().get(0)
                    : "second".equals(prop) ? g.arguments().get(1) : declared;
        }
        return declared;
    }

    /** The declared return is the Map<U,V> carrier (never a class STRUCT). */
    private static boolean isMapReturn(Type t) {
        return "meta::pure::functions::collection::Map".equals(classFqnOf(t));
    }

    private static boolean isListReturn(Type t) {
        return "meta::pure::functions::collection::List".equals(classFqnOf(t));
    }

    /** B2 — a REAL {@code ^List<E>} instance built from the PLATFORM's
     * own {@code List<E>} type: classifier + generic stamp + values.
     * The harness's declared cast validates the stamp downstream. */
    private CoreInstance listInstance(java.util.List<?> list, Type declared,
            ProcessorSupport ps) {
        CoreInstance listCls = ps.package_getByUserPath(
                "meta::pure::functions::collection::List");
        CoreInstance inst = modelRepository
                .newEphemeralAnonymousCoreInstance(null, listCls);
        CoreInstance gt = genericTypeOf(declared, ps);
        if (gt != null) {
            Instance.addValueToProperty(inst,
                    M3Properties.classifierGenericType, gt, ps);
        }
        Type elem = declared instanceof Type.GenericType g
                && g.arguments().size() == 1 ? g.arguments().get(0) : declared;
        var cis = new ArrayList<CoreInstance>();
        for (Object v : list) {
            if (!emptyCell(v)) {
                cis.add(toCoreInstance(v, elem, ps));
            }
        }
        Instance.setValuesForProperty(inst, "values",
                org.eclipse.collections.impl.factory.Lists.immutable
                        .withAll(cis), ps);
        return inst;
    }

    /** B2 — a REAL interpreter Map instance ({@code MapCoreInstance},
     * the same class the interpreted {@code newMap} native constructs),
     * stamped with the PLATFORM's own {@code Map<K,V>} generic type and
     * filled directly. The pure side passes it through untouched — no
     * marker class, no rebuild, no declared-type consult anywhere. */
    private CoreInstance mapInstance(java.util.Map<?, ?> map, Type declared,
            ProcessorSupport ps) {
        Type k = declared instanceof Type.GenericType g
                && g.arguments().size() == 2 ? g.arguments().get(0) : null;
        Type v = declared instanceof Type.GenericType g
                && g.arguments().size() == 2 ? g.arguments().get(1) : null;
        CoreInstance mapRawType = ps.package_getByUserPath(
                org.finos.legend.pure.m3.navigation.M3Paths.Map);
        var inst = new org.finos.legend.pure.runtime.java.interpreted
                .natives.MapCoreInstance(
                        org.eclipse.collections.impl.factory.Lists
                                .immutable.empty(),
                        "", null, mapRawType, -1, modelRepository, false, ps);
        CoreInstance gt = genericTypeOf(declared, ps);
        if (gt != null) {
            Instance.addValueToProperty(inst,
                    M3Properties.classifierGenericType, gt, ps);
        }
        var internal = inst.getMap();
        for (var en : map.entrySet()) {
            internal.put(
                    toCoreInstance(en.getKey(),
                            k != null ? k : declared, ps),
                    toCoreInstance(en.getValue(),
                            v != null ? v : declared, ps));
        }
        return inst;
    }


    /** {@code v} with every date in it at the precision its digits show (a timestamp's trailing zero fraction
     *  dropped). */
    private static Object trimmedDates(Object v) {
        return switch (v) {
            case null -> null;
            case com.legend.values.PureDateLiteral d -> d.trimmed();
            case List<?> l -> l.stream().map(ValueBridge::trimmedDates).toList();
            case Map<?, ?> m -> {
                Map<Object, Object> out = new java.util.LinkedHashMap<>();
                m.forEach((k, x) -> out.put(k, trimmedDates(x)));
                yield out;
            }
            default -> v;
        };
    }

    /** THE channel-scoped empty rule — ONE owner (documented-debts
     * 2026-08-18; the audit counted six scattered unconditional drops
     * that had only been re-commented). A NULL element is a pure EMPTY:
     * pure collections and properties hold no empties, so SQL NULL and
     * JSON-null decay both vanish here — the same convention as the
     * Executor's COLLECTION shaping. The COUNTED case is not at risk:
     * a top-level Collection result reaching this bridge has ALREADY
     * passed the Executor's declared-lower-bound wall, and the
     * per-element sites (list elements, struct properties) have no
     * declared bound to violate; a defect manufacturing spurious NULLs
     * surfaces in the reference asserts, which compare VALUES. */
    private static boolean emptyCell(Object v) {
        return v == null;
    }

    private CoreInstance toCoreInstance(Object value, Type type, ProcessorSupport ps) {
        // B2 (truthfulness burn): the PLATFORM's type carries the full
        // generic shape (Map<K,V> / List<E> with arguments) — Java builds
        // the typed instances HERE; the pure side only passes them through.
        // (JDBC also hands STRUCT values as java.util.Map — the PLATFORM
        // type gates, or ^Person(...) results would land here.)
        if (value instanceof java.util.Map<?, ?> map && isMapReturn(type)) {
            return mapInstance(map, type, ps);
        }
        if (value instanceof java.util.Map<?, ?> struct && classFqnOf(type) != null) {
            return structToInstance(struct, type, ps);
        }

        if (value instanceof Boolean b) {
            return modelRepository.newBooleanCoreInstance(b);
        }
        if (value instanceof Integer i) {
            return modelRepository.newIntegerCoreInstance(i);
        }
        if (value instanceof Long l) {
            return modelRepository.newIntegerCoreInstance(l);
        }
        if (value instanceof BigInteger bi) {
            return modelRepository.newIntegerCoreInstance(bi.toString());
        }
        // Numeric kinds ride the WIRE's own carriers (slice-4 census,
        // measured on both PCT lanes): a Decimal-contract value arrives
        // as BigDecimal (DECIMAL column), a Float as Double. The old
        // declared-kind consult arms on Double, the Float32 arm and the
        // Number catch-all all measured ZERO — deleted; an unlisted
        // numeric carrier now hits the terminal wall, loudly (F5.5).
        if (value instanceof BigDecimal bd) {
            if (type instanceof Type.Primitive p
                    && (p == Type.Primitive.DECIMAL || p == Type.Primitive.NUMBER)) {
                return modelRepository.newDecimalCoreInstance(bd);
            }
            if (type instanceof Type.PrecisionDecimal) {
                // scale is part of the VALUE surface: abs(-3.0D) prints 3.0D
                return modelRepository.newDecimalCoreInstance(bd);
            }
            // B8: a decimal-exact value under the FLOAT label IS the
            // reference's own Float shape — legend-pure's interpreted
            // Float is BigDecimal-backed by declaration
            // (FloatCoreInstance extends PrimitiveCoreInstance<BigDecimal>),
            // and newFloatCoreInstance takes the BigDecimal directly.
            if (type instanceof Type.Primitive p2 && p2 == Type.Primitive.FLOAT) {
                return modelRepository.newFloatCoreInstance(bd);
            }
            throw new UnsupportedOperationException(
                    "DECIMAL wire value under a non-Decimal contract: "
                    + type + " — no kind re-derivation");
        }
        if (value instanceof Double d) {
            if (!Double.isFinite(d)) {
                // legend-pure's Float is a decimal (FloatCoreInstance holds a BigDecimal): it has no NaN or infinity
                throw new IllegalStateException("the database answered " + d + " for a Float, which has no " + d
                        + " in Pure");
            }
            return modelRepository.newFloatCoreInstance(BigDecimal.valueOf(d));
        }
        // Dates — THE wire temporal (D-arc 2026-08-21): PureDateLiteral
        // carries written precision; engine's own parser reconstructs
        // the precision-exact PureDate class from the spelling. The
        // old java.sql/java.time arms are DELETED (cut-over-hard): a
        // raw driver temporal reaching this extension is a fetch-seam
        // leak and hits the terminal no-typed-conversion wall, loudly.
        if (value instanceof com.legend.values.PureDateLiteral pdl) {
            // J8a (slice-4 census): the declared-type NARROWING arm is
            // DELETED — its one witness (parseDate over a bare-date
            // literal, refined StrictDate but cast to TIMESTAMP) is
            // cured at the emission (Scalars parseDate rule casts to
            // DATE when the node's own stamp is StrictDate). The wire
            // delivers kind-faithful temporals; the adapter only boxes.
            PureDate pd = DateFunctions.parsePureDate(pdl.toEngineString());
            String topLevel = switch (pdl.precision()) {
                case YEAR, MONTH -> "Date";
                case DAY -> "StrictDate";
                default -> "DateTime";
            };
            return modelRepository.newCoreInstance(pd.toString(),
                    modelRepository.getTopLevel(topLevel), null);
        }
        // (LocalTime/StrictTime arm DELETED by census — zero traffic;
        // StrictTime has no SQL carrier (PureSql), so nothing crosses.
        // A LocalTime reaching here hits the terminal wall by name.)
        // Strings
        if (value instanceof String s) {
            // type(x) crosses the wire as the type's NAME; resolve it to the
            // canonical Type instance — assertIs compares IDENTITY, and
            // package lookup returns the one true instance.
            if ("meta::pure::metamodel::type::Type".equals(classFqnOf(type))) {
                CoreInstance typeInstance = ps.package_getByUserPath(s);
                if (typeInstance != null) {
                    return typeInstance;
                }
            }
            // Enum values cross the wire as their NAME; the declared type
            // carries the enumeration — resolve to the CANONICAL enum-value
            // instance (equality on enums is identity in interpreted pure).
            if (type instanceof Type.EnumType et) {
                CoreInstance enumeration = ps.package_getByUserPath(et.fqn());
                if (enumeration != null) {
                    for (CoreInstance v : Instance.getValueForMetaPropertyToManyResolved(
                            enumeration, M3Properties.values, ps)) {
                        if (s.equals(v.getName())) {
                            return v;
                        }
                    }
                }
            }
            // J8h (slice-4 census): the date-shaped-text reparse arm is
            // DELETED BY MEASUREMENT — zero firings on both PCT lanes;
            // temporals cross as PureDateLiteral (D-arc), never as bare
            // date text. Any temporal-typed String is now the F5.5 wall.
            if (type instanceof Type.Primitive tp && tp.isTemporal()) {
                throw new UnsupportedOperationException(
                        "temporal-typed value crossed as text: '"
                        + s + "' (" + tp + ") — the PureDateLiteral"
                        + " carrier owns temporals");
            }
            return modelRepository.newStringCoreInstance(s);
        }
        // Struct → class instance
        if (value instanceof Map<?, ?> map) {
            // F5.6: the old createClassInstance FABRICATED a Pair type
            // for any unknown struct; a probe throw across all 1,109 PCT
            // tests proved the branch unreachable — keep the wall LOUD
            throw new UnsupportedOperationException(
                    "struct result reached the PCT bridge (type=" + type
                    + ", keys=" + map.keySet() + ") — no fabrication;"
                    + " add a typed conversion");
        }
        // a List VALUE under a List<E> type builds the real instance
        // (B2 — nested list results recurse through the typed builder)
        if (value instanceof List<?> nested && isListReturn(type)) {
            return listInstance(nested, type, ps);
        }
        // List (struct arrays unwrapped by Row.java, e.g. zip → List<Pair>)
        if (value instanceof List<?> list) {
            Type elemType = type;
            var coreInstances = new ArrayList<CoreInstance>();
            for (Object elem : list) {
                if (!emptyCell(elem)) {
                    coreInstances.add(toCoreInstance(elem, elemType, ps));
                }
            }
            // Return as a single-element wrapping — the collection will be wrapped by caller
            if (coreInstances.size() == 1) return coreInstances.get(0);
            // F5.5: multi-element used to 'return first as fallback' and
            // empty fabricated a '[]' STRING — both are silent wrong
            // answers in scalar context
            throw new UnsupportedOperationException(
                    "list of " + coreInstances.size() + " elements in"
                    + " scalar conversion context (type=" + type + ")");
        }
        // J8j/J8k (slice-4 census): the UUID-under-String arm and the
        // DuckDBStruct arm (with its classInstance builder — the second
        // struct→instance owner, whose positional type-arg indexing was
        // a latent wrong-type channel) are DELETED BY MEASUREMENT —
        // zero firings on both PCT lanes; structs cross as
        // java.util.Map and build through structToInstance, the ONE
        // owner. F5.5: the terminal fallback is LOUD — an unconverted
        // kind is a missing typed conversion, not a String.
        throw new UnsupportedOperationException(
                "no typed conversion for " + value.getClass().getName()
                + " (type=" + type + ")");
    }



    // ===== TDS formatting =====

    /**
     * A relation's answer, the engine's JSON the server writes ({@code PureV1Writer}), read as legend-engine's own PCT
     * adapter reads it (pct_relational.pure: parseJSON, then fromJSON into its RelationalTDSResult with
     * nullReplacementInArray=TDSNull): the TDS builder's columns, each row's values as json-simple types them -- an
     * integral number an Integer, any other number a Float of its double (json-simple reads a Double, and the engine's
     * JsonParser makes the Float from its text), a string a String (a Variant's cell among them: the engine writes its JSON
     * as a string, measured 2026-10-28), a boolean a Boolean, a JSON null TDSNull. A structure's object or array
     * (json-simple's JSONObject or JSONArray) crosses as its compact JSON text, which toCSVString quotes.
     * The adapter's Pure writes the TDS text with getTDSResultFromProtocol's steps (pct_adapter.pure, engineTDS).
     */
    CoreInstance engineTDS(String engineJson, ProcessorSupport ps) {
        com.legend.json.Json.Obj answer = com.legend.json.Json.parseObject(engineJson);
        CoreInstance result = modelRepository.newCoreInstance("EngineTDSResult",
                pctClass("meta::legend::lite::pct::EngineTDSResult", ps), null);
        CoreInstance columnClass = pctClass("meta::legend::lite::pct::EngineTDSColumn", ps);
        CoreInstance rowClass = pctClass("meta::legend::lite::pct::EngineTDSRow", ps);
        com.legend.json.Json.Obj builder = (com.legend.json.Json.Obj) answer.get("builder");
        for (com.legend.json.Json.Node c : ((com.legend.json.Json.Arr) builder.get("columns")).items()) {
            com.legend.json.Json.Obj column = (com.legend.json.Json.Obj) c;
            CoreInstance col = modelRepository.newCoreInstance("EngineTDSColumn", columnClass, null);
            Instance.addValueToProperty(col, "name", modelRepository.newStringCoreInstance(
                    ((com.legend.json.Json.Str) column.get("name")).value()), ps);
            if (column.get("type") instanceof com.legend.json.Json.Str type) {
                Instance.addValueToProperty(col, "type", modelRepository.newStringCoreInstance(type.value()), ps);
            }
            Instance.addValueToProperty(result, "columns", col, ps);
        }
        com.legend.json.Json.Obj data = (com.legend.json.Json.Obj) answer.get("result");
        CoreInstance tdsNull = pctClass("meta::pure::tds::TDSNull", ps);
        for (com.legend.json.Json.Node r : ((com.legend.json.Json.Arr) data.get("rows")).items()) {
            CoreInstance row = modelRepository.newCoreInstance("EngineTDSRow", rowClass, null);
            for (com.legend.json.Json.Node v : ((com.legend.json.Json.Arr) ((com.legend.json.Json.Obj) r).get("values"))
                    .items()) {
                Instance.addValueToProperty(row, "values", engineValue(v, tdsNull), ps);
            }
            Instance.addValueToProperty(result, "rows", row, ps);
        }
        return ValueSpecificationBootstrap.wrapValueSpecification(result, true, ps);
    }

    /** One JSON value of the engine's row as json-simple and the engine's JsonParser make it. */
    private CoreInstance engineValue(com.legend.json.Json.Node v, CoreInstance tdsNull) {
        return switch (v) {
            case com.legend.json.Json.Str s -> modelRepository.newStringCoreInstance(s.value());
            case com.legend.json.Json.Bool b -> modelRepository.newBooleanCoreInstance(b.value());
            case com.legend.json.Json.Null n -> tdsNull;
            case com.legend.json.Json.Num n -> {
                String token = n.token() != null ? n.token() : com.legend.json.Json.toCompact(n);
                // json-simple's lexer: a number with a fraction or an exponent is a Double, any other a Long (one past
                // a long its parse refuses, as json-simple's does)
                yield token.contains(".") || token.contains("e") || token.contains("E")
                        ? modelRepository.newCoreInstance(Double.toString(Double.parseDouble(token)),
                                modelRepository.getTopLevel("Float"), null)
                        : modelRepository.newIntegerCoreInstance(Long.parseLong(token));
            }
            case com.legend.json.Json.Obj o -> modelRepository.newStringCoreInstance(com.legend.json.Json.toCompact(o));
            case com.legend.json.Json.Arr a -> modelRepository.newStringCoreInstance(com.legend.json.Json.toCompact(a));
        };
    }

    private static CoreInstance pctClass(String path, ProcessorSupport ps) {
        CoreInstance c = ps.package_getByUserPath(path);
        if (c == null) {
            throw new IllegalStateException(path + " is not in the PCT's Pure model");
        }
        return c;
    }

}
