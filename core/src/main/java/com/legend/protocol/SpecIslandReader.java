// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CDate;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CLatestDate;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.GqlIsland;
import com.legend.protocol.spec.GraphFetchLiteral;
import com.legend.protocol.spec.KeyExpression;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PathLiteral;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.SqlIsland;
import com.legend.protocol.spec.TdsLiteral;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import com.legend.values.PureDateLiteral;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * The value specifications with a wire shape of their own, read back -- part of {@link ProtocolReader}
 * (one reader), split by size: the {@code classInstance} islands ({@code #>{..}#}, graph fetch, column
 * specs, paths, SQL/GraphQL/TDS), the {@code genericTypeInstance} annotation and {@code ^X(...)}. Each
 * rule mirrors the {@link ProtocolEmitter} rule of the same name.
 */
final class SpecIslandReader {

    private SpecIslandReader() {
    }

    /** The {@code Class} type every {@code ^X(...)} instantiates on the wire. */
    private static final String CLASS = "meta::pure::metamodel::type::Class";

    /** The reader rule for each {@code classInstance} {@code type} on the wire. */
    private static final Map<String, BiFunction<Json.Node, SourceInfo, ValueSpecification>> CLASS_INSTANCES = Map.of(
            ">", SpecIslandReader::tableReference,
            "rootGraphFetchTree", SpecIslandReader::graphFetchTree,
            "colSpec", SpecIslandReader::colSpec,
            "colSpecArray", SpecIslandReader::colSpecArray,
            "path", (value, pos) -> pathLiteral(Wire.of(value, "path")),
            "SQL", (value, pos) -> {
                Wire w = Wire.of(value, "SQL island");
                return w.done(new SqlIsland(w.str("sql"), pos));
            },
            "GQL", (value, pos) -> new GqlIsland(GqlReader.document(value), pos),
            "TDS", (value, pos) -> {
                Wire w = Wire.of(value, "TDS island");
                String tds = w.str("tdsString");
                return w.done(TdsLiteral.of(tds, "#TDS{" + tds + "}#", pos));
            });

    static ValueSpecification classInstance(Wire w) {
        String type = w.str("type");
        SourceInfo pos = w.span();
        Json.Node value = w.take("value");
        return Wire.rule(CLASS_INSTANCES, type, "classInstance type").apply(value, pos);
    }

    /**
     * {@code #>{db.schema.T}#}: the database, then the rest of the path as the pos-less table-name
     * string the island parse synthesises (the emitter's discriminator); the store-only form has one
     * path element. Outer and inner spans are one span.
     */
    private static ValueSpecification tableReference(Json.Node value, @com.legend.base.Nullable SourceInfo pos) {
        Wire w = Wire.of(value, "table reference");
        List<String> path = w.strings("path");
        sameSpan(pos, w.span(), "table reference");
        if (path.isEmpty()) {
            throw Wire.refuse("a table reference with an empty path");
        }
        return w.done(AppliedFunction.tableReference(path.get(0), path.size() == 1 ? null
                : String.join(".", path.subList(1, path.size())), pos));
    }

    private static void sameSpan(@com.legend.base.Nullable SourceInfo outer,
            @com.legend.base.Nullable SourceInfo inner, String what) {
        if (!java.util.Objects.equals(outer, inner)) {
            throw Wire.refuse(what + ": the outer span " + outer + " is not the value's " + inner);
        }
    }

    // ---------------------------------------------------------------------
    // ^X(...)
    // ---------------------------------------------------------------------

    /**
     * {@code ^X<T|m>(k=v, ...)} on the wire: {@code func new} over [a span-less {@code Class<X>} type
     * instance (X's type and multiplicity arguments ride inside), a span-less empty string, a span-less
     * collection of {@code keyExpression}s]. Read back to the parser's {@code new(X, NewInstance)};
     * {@code null} when the parameters are not that shape (the call is then read as written).
     */
    static @com.legend.base.Nullable ValueSpecification newInstance(List<Json.Node> params,
            @com.legend.base.Nullable SourceInfo pos) {
        if (params.size() != 3 || !(params.get(0) instanceof Json.Obj p0)
                || !"genericTypeInstance".equals(p0.getStringOr("_type", null))) {
            return null;
        }
        Wire ti = Wire.of(p0, "new's class");
        ti.type();
        Wire gt = ti.obj("genericType");
        ti.done(gt);
        gt.emptyArray("multiplicityArguments");
        gt.emptyArray("typeVariableValues");
        Wire classRaw = gt.obj("rawType");
        classRaw.constant("_type", "packageableType");
        classRaw.constant("fullPath", CLASS);
        classRaw.done(gt);
        List<Json.Node> classArgs = gt.arr("typeArguments");
        gt.done(classArgs);
        if (classArgs.size() != 1) {
            throw Wire.refuse("new's Class<X> has " + classArgs.size() + " type arguments");
        }
        Wire inner = Wire.of(classArgs.get(0), "new's X");
        List<String> mults = new ArrayList<>();
        for (Json.Node m : inner.arr("multiplicityArguments")) {
            mults.add(ProtocolReader.multiplicityText(ProtocolReader.multiplicity(m)));
        }
        Wire raw = inner.obj("rawType");
        raw.constant("_type", "packageableType");
        String className = raw.done(raw.str("fullPath"));
        List<com.legend.protocol.TypeExpression> typeArgs = new ArrayList<>();
        for (Json.Node a : inner.arr("typeArguments")) {
            typeArgs.add(ProtocolReader.genericType(a));
        }
        inner.emptyArray("typeVariableValues");
        inner.done(className);
        emptyName(params.get(1), "");
        List<NewInstance.KeyBinding> keys = keyBindings(params.get(2), "new " + className);
        return new AppliedFunction(AppliedFunction.NEW, List.of(new PackageableElementPtr(className),
                new NewInstance(className, typeArgs, mults, keys)), List.of(), pos);
    }

    /** {@code new}'s span-less name argument: {@code ""} in ###Pure, {@code "dummy"} in model data. */
    static void emptyName(Json.Node node, String expected) {
        Wire s = Wire.of(node, "new's name");
        s.constant("_type", "string");
        s.constant("value", expected);
        s.done(expected);
    }

    /** {@code new}'s span-less collection of {@code keyExpression}s. */
    static List<NewInstance.KeyBinding> keyBindings(Json.Node node, String where) {
        Wire c = Wire.of(node, where + " key collection");
        c.constant("_type", "collection");
        List<NewInstance.KeyBinding> out = c.list("values", SpecIslandReader::keyBinding);
        ProtocolReader.multiplicityOfSize(c.take("multiplicity"), out.size(), where + " keys");
        return c.done(out);
    }

    /**
     * {@code {"_type":"keyExpression","add":false,"expression":v,"key":{"_type":"string","value":k}}};
     * {@code add} is always false on the engine wire, and an absent expression is {@code package=::}
     * (the root package, which the engine drops whole).
     */
    private static NewInstance.KeyBinding keyBinding(Json.Node node) {
        Wire k = Wire.of(node, "keyExpression");
        k.constant("_type", "keyExpression");
        k.constant("add", false);
        Json.Node expr = k.opt("expression");
        Wire key = k.obj("key");
        key.constant("_type", "string");
        String name = key.done(key.str("value"));
        ValueSpecification value = expr == null ? new PackageableElementPtr("::") : ProtocolReader.valueSpec(expr);
        return k.done(new NewInstance.KeyBinding(name, new KeyExpression(value, false, false)));
    }

    // ---------------------------------------------------------------------
    // @Type
    // ---------------------------------------------------------------------

    /**
     * {@code @Type}: a {@code genericTypeInstance} spanning {@code @..type}. A UNIT type ({@code ~} in its
     * name) loses its rawType span on the wire and the annotation's span is the name's.
     */
    static ValueSpecification typeInstance(Wire w) {
        com.legend.protocol.TypeExpression type = ProtocolReader.genericType(w.take("genericType"));
        SourceInfo pos = w.span();
        if (type instanceof com.legend.protocol.TypeExpression.NameRef n && n.name().indexOf('~') >= 0) {
            if (n.pos() != null) {
                throw Wire.refuse("a unit type annotation whose rawType carries a span: " + n.name());
            }
            return ProtocolReader.named(new com.legend.protocol.TypeExpression.NameRef(n.name(), pos), pos);
        }
        return ProtocolReader.named(type, pos);
    }

    // ---------------------------------------------------------------------
    // Column specs
    // ---------------------------------------------------------------------

    /** {@code ~name}: the outer and value spans are one span. */
    private static ValueSpecification colSpec(Json.Node value, @com.legend.base.Nullable SourceInfo pos) {
        ColSpec cs = colSpecValue(value);
        sameSpan(pos, cs.pos(), "colSpec " + cs.name());
        return cs;
    }

    private static ValueSpecification colSpecArray(Json.Node value, @com.legend.base.Nullable SourceInfo pos) {
        Wire w = Wire.of(value, "colSpecArray");
        return w.done(new ColSpecArray(w.list("colSpecs", SpecIslandReader::colSpecValue), pos));
    }

    /** A column spec's value: its lambdas or its declared type, name, span, annotations. */
    private static ColSpec colSpecValue(Json.Node node) {
        Wire v = Wire.of(node, "colSpec");
        Json.Node f1 = v.opt("function1");
        Json.Node f2 = v.opt("function2");
        Json.Node gt = v.opt("genericType");
        Json.Node m = v.opt("multiplicity");
        if (m != null && gt == null) {
            throw Wire.refuse("a column spec with a multiplicity and no type");
        }
        LambdaFunction l1 = f1 == null ? null : ProtocolReader.lambdaNode(f1);
        LambdaFunction l2 = f2 == null ? null : ProtocolReader.lambdaNode(f2);
        return v.done(new ColSpec(v.str("name"), l1, l2, null, List.of(), false, v.span(),
                gt == null ? null : ProtocolReader.genericType(gt),
                m == null ? null : ProtocolReader.multiplicity(m),
                v.listOrEmpty("stereotypes", DomainReader::stereotype),
                v.listOrEmpty("taggedValues", DomainReader::taggedValue)));
    }

    // ---------------------------------------------------------------------
    // Graph fetch
    // ---------------------------------------------------------------------

    /**
     * {@code #{Root {a, k {b}}}#}: the value's span is the class-name token (the outer one may be a
     * let's); each property node spans its name token. The wire cannot tell {@code prop()} from
     * {@code prop} (both carry no parameters), so a node is parenthesized exactly when it has arguments.
     */
    private static ValueSpecification graphFetchTree(Json.Node value, @com.legend.base.Nullable SourceInfo pos) {
        Wire root = Wire.of(value, "graph fetch tree");
        root.constant("_type", "rootGraphFetchTree");
        return root.done(new GraphFetchLiteral(root.str("class"), graphNodes(root.arr("subTrees")),
                graphSubTypes(root.arr("subTypeTrees")), root.span()));
    }

    private static List<GraphFetchLiteral.Node> graphNodes(List<Json.Node> trees) {
        List<GraphFetchLiteral.Node> out = new ArrayList<>();
        for (Json.Node t : trees) {
            Wire n = Wire.of(t, "graph fetch subtree");
            n.constant("_type", "propertyGraphFetchTree");
            // the engine's grammar refuses ->subType below the root; so does lite's
            n.emptyArray("subTypeTrees");
            List<ValueSpecification> args = n.list("parameters", SpecIslandReader::graphArg);
            out.add(n.done(new GraphFetchLiteral.Node(n.str("property"), n.span(), args, !args.isEmpty(),
                    n.optStr("alias"), n.optStr("subType"), graphNodes(n.arr("subTrees")))));
        }
        return out;
    }

    /** A level's {@code ->subType(@X){...}} entries. */
    private static List<GraphFetchLiteral.SubTypeNode> graphSubTypes(List<Json.Node> trees) {
        List<GraphFetchLiteral.SubTypeNode> out = new ArrayList<>();
        for (Json.Node t : trees) {
            Wire n = Wire.of(t, "graph fetch subtype tree");
            n.constant("_type", "subTypeGraphFetchTree");
            n.emptyArray("subTypeTrees");
            out.add(n.done(new GraphFetchLiteral.SubTypeNode(n.str("subTypeClass"), n.span(),
                    graphNodes(n.arr("subTrees")))));
        }
        return out;
    }

    /**
     * A graph node's call argument, back to the expression the grammar parses -- the mirror of the
     * emitter's {@code gftParam}: a date is always {@code dateTime} and keeps its {@code %}; an enum is
     * a real {@code enumValue} spanning the whole dotted path; a collection carries no span; a variable
     * spans its name without the dollar.
     */
    private static ValueSpecification graphArg(Json.Node node) {
        Wire w = Wire.of(node, "graph-fetch argument");
        String type = w.type();
        if ("dateTime".equals(type)) {
            String written = w.str("value");
            if (!written.startsWith("%")) {
                throw Wire.refuse("a graph-fetch date argument without its '%': " + written);
            }
            String body = written.substring(1);
            return w.done(new CDate(PureDateLiteral.parse(body), body, w.span()));
        }
        if ("enumValue".equals(type)) {
            String fullPath = w.str("fullPath");
            String value = w.str("value");
            SourceInfo at = w.span();
            if (at == null) {
                return w.done(new EnumValue(fullPath, value, null, null));
            }
            return w.done(new EnumValue(fullPath, value,
                    new SourceInfo(at.sourceId(), at.startLine(), at.startColumn(),
                            at.startLine(), at.startColumn() + fullPath.length() - 1),
                    new SourceInfo(at.sourceId(), at.endLine(),
                            at.endColumn() - value.length() + 1, at.endLine(), at.endColumn())));
        }
        if ("collection".equals(type)) {
            List<ValueSpecification> values = w.list("values", SpecIslandReader::graphArg);
            ProtocolReader.multiplicityOfSize(w.take("multiplicity"), values.size(), "graph-fetch collection");
            return w.done(new PureCollection(values));
        }
        if ("var".equals(type)) {
            String name = w.str("name");
            SourceInfo at = w.span();
            return w.done(new Variable(name, null, null, at == null ? null
                    : new SourceInfo(at.sourceId(), at.startLine(), at.startColumn() - 1, at.endLine(),
                            at.endColumn())));
        }
        if ("string".equals(type) || "integer".equals(type) || "boolean".equals(type)) {
            return ProtocolReader.valueSpec(w.json());
        }
        throw Wire.refuse("no reader rule for graph-fetch argument _type '" + type + "'");
    }

    // ---------------------------------------------------------------------
    // Paths
    // ---------------------------------------------------------------------

    /** A path VALUE object alone (no classInstance wrapper), as persistence's graphFetch slots embed it. */
    static ValueSpecification pathValue(Json.Node node) {
        return pathLiteral(Wire.of(node, "path"));
    }

    /**
     * {@code #/Root/prop#}: every span on the wire is SHIFTED RIGHT by the literal's length (the
     * engine's island re-parse): for a literal at column {@code s} of length {@code len}, the value's
     * span is {@code [s+len, s+2*len+2]}, a segment's {@code [s+len+a-2, s+len+b-1]} over its 0-based
     * characters {@code [a, b]}, an argument's {@code [s+len+a-1, s+len+b-1]}. Read back here into the
     * literal's own column, length and offsets, and the desugared lambda the grammar builds. Without
     * spans the literal has no position (and every offset is 0).
     */
    private static ValueSpecification pathLiteral(Wire v) {
        String alias = v.optStr("name");
        SourceInfo outer = v.span();
        String startType = v.str("startType");
        int len = 0;
        int base = 0;
        SourceInfo pos = null;
        if (outer != null) {
            if (outer.startLine() != outer.endLine()) {
                throw Wire.refuse("a multi-line path literal span " + outer);
            }
            len = outer.endColumn() - outer.startColumn() - 2;
            int s = outer.startColumn() - len;
            base = s + len;
            pos = new SourceInfo(outer.sourceId(), outer.startLine(), s, outer.startLine(), s + len - 1);
        }
        int at = base;
        List<PathLiteral.Segment> segments = v.list("path", n -> segment(n, at));
        ValueSpecification body = new Variable("_path");
        boolean dated = false;
        for (PathLiteral.Segment seg : segments) {
            if (seg.args().isEmpty()) {
                body = new com.legend.protocol.spec.AppliedProperty(body, seg.name());
            } else {
                dated = true;
                List<ValueSpecification> args = new ArrayList<>();
                args.add(body);
                for (PathLiteral.PathArg a : seg.args()) {
                    args.add(argValue(a));
                }
                body = new AppliedFunction(seg.name(), args);
            }
        }
        LambdaFunction fn = new LambdaFunction(List.of(new Variable("_path",
                new com.legend.protocol.TypeExpression.NameRef(startType), null)), List.of(body));
        return v.done(new PathLiteral(startType, segments, fn, alias, dated, pos, len));
    }

    private static PathLiteral.Segment segment(Json.Node node, int base) {
        Wire p = Wire.of(node, "path segment");
        p.constant("_type", "propertyPath");
        List<PathLiteral.PathArg> args = p.list("parameters", a -> pathArg(a, base));
        SourceInfo s = p.span();
        int innerStart = s == null ? 0 : s.startColumn() - base + 2;
        int innerEnd = s == null ? 0 : s.endColumn() - base + 1;
        return p.done(new PathLiteral.Segment(p.str("property"), innerStart, innerEnd, args, false));
    }

    private static PathLiteral.PathArg pathArg(Json.Node node, int base) {
        Wire a = Wire.of(node, "path argument");
        String type = a.type();
        if ("enumValue".equals(type)) {
            return a.done(new PathLiteral.PathArg.EnumArg(a.str("fullPath"), a.str("value")));
        }
        if ("collection".equals(type)) {
            List<PathLiteral.PathArg> elements = a.list("values", n -> pathArg(n, base));
            ProtocolReader.multiplicityOfSize(a.take("multiplicity"), elements.size(), "path collection");
            return a.done(new PathLiteral.PathArg.CollectionArg(elements));
        }
        SourceInfo s = a.span();
        int start = s == null ? 0 : s.startColumn() - base + 1;
        int end = s == null ? 0 : s.endColumn() - base + 1;
        PathLiteral.PathArg out;
        if ("latestDate".equals(type)) {
            out = new PathLiteral.PathArg.Latest(start, end);
        } else if ("dateTime".equals(type)) {
            out = new PathLiteral.PathArg.DateArg(a.str("value"), start, end);
        } else if ("integer".equals(type)) {
            out = new PathLiteral.PathArg.IntArg(a.lng("value"), start, end);
        } else if ("string".equals(type)) {
            out = new PathLiteral.PathArg.StrArg(a.str("value"), start, end);
        } else {
            throw Wire.refuse("no reader rule for a path argument of _type '" + type + "'");
        }
        return a.done(out);
    }

    /** A dated segment's argument as the expression the grammar parses it to. */
    private static ValueSpecification argValue(PathLiteral.PathArg a) {
        return switch (a) {
            case PathLiteral.PathArg.Latest l -> new CLatestDate();
            case PathLiteral.PathArg.DateArg d -> {
                String body = d.value().startsWith("%") ? d.value().substring(1) : d.value();
                yield new CDate(PureDateLiteral.parse(body), body, null);
            }
            case PathLiteral.PathArg.EnumArg e -> new EnumValue(e.fullPath(), e.value());
            case PathLiteral.PathArg.IntArg i -> new CInteger(i.value());
            case PathLiteral.PathArg.StrArg s -> new CString(s.value());
            case PathLiteral.PathArg.CollectionArg c -> {
                List<ValueSpecification> values = new ArrayList<>();
                for (PathLiteral.PathArg e : c.elements()) {
                    values.add(argValue(e));
                }
                yield new PureCollection(values);
            }
        };
    }
}
