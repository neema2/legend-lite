// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Protocol JSON to Pure text: the printer for lambdas and value specifications, as upstream's
 * {@code pure/v1/grammar/jsonToGrammar/lambda} prints them (its {@code DEPRECATED_PureGrammarComposerCore}
 * and {@code HelperValueSpecificationGrammarComposer}, ported rule for rule, the reference checkout
 * as spec; docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md, T4a).
 *
 * <p>It reads the WIRE, not lite's parsed tree: upstream composes its protocol objects, and the
 * parsed tree folds some wire shapes together (a method-call property, a table accessor), which
 * a printer must keep apart. Every {@code _type} and {@code classInstance} type upstream prints
 * has a rule here; anything else is REFUSED, naming it -- never skipped, never guessed. Byte
 * parity with upstream's printer is pinned by the parser-equivalence oracle over every lambda of
 * the reference corpus, in both styles.
 *
 * <p>Two styles: {@link Style#STANDARD} (one line) and {@link Style#PRETTY} (upstream's
 * indentation). Upstream's HTML style is not served.
 */
public final class PureComposer {

    /** How the text is laid out. */
    public enum Style { STANDARD, PRETTY }

    private static final String TAB = "  ";

    /** {@code HelperValueSpecificationGrammarComposer.SPECIAL_INFIX}. */
    private static final Map<String, String> SPECIAL_INFIX = Map.ofEntries(
            Map.entry("equal", "=="), Map.entry("lessThanEqual", "<="), Map.entry("lessThan", "<"),
            Map.entry("greaterThanEqual", ">="), Map.entry("greaterThan", ">"), Map.entry("plus", "+"),
            Map.entry("minus", "-"), Map.entry("times", "*"), Map.entry("divide", "/"),
            Map.entry("and", "&&"), Map.entry("or", "||"));

    private static final Set<String> CORE_FUNCTIONS_WITH_PREFIX_RENDERING = Set.of("if", "over");

    private static final Set<String> PRIMITIVE_VALUES = Set.of("string", "boolean", "integer", "float",
            "decimal", "dateTime", "strictDate", "strictTime", "latestDate");

    private final Style style;
    /** Upstream's {@code indentationString}: grows only when rendering pretty. */
    private final String indentation;
    /** A lambda's parameters print without their {@code $}. */
    private final boolean variableInSignature;

    private PureComposer(Style style, String indentation, boolean variableInSignature) {
        this.style = style;
        this.indentation = indentation;
        this.variableInSignature = variableInSignature;
    }

    /** A lambda, {@code {"_type":"lambda",...}}, as Pure text. */
    public static String lambda(Json.Obj lambda, Style style) {
        String type = lambda.getStringOr("_type", "");
        if (!"lambda".equals(type)) {
            throw refused("expected a lambda, got _type '" + type + "'");
        }
        return new PureComposer(style, "", false).visit(ProtocolUpgrade.upgrade(lambda));
    }

    /** Any value specification as Pure text. */
    public static String valueSpecification(Json.Node node, Style style) {
        return new PureComposer(style, "", false).visit(ProtocolUpgrade.upgrade(node));
    }

    /**
     * A value specification inside a model element ({@link ModelComposer}): upstream's printer built
     * from the element's composer context, whose indentation a lambda's closing brace, a collection's
     * closing bracket and a text block's lines are written at.
     */
    static String valueSpecification(Json.Node node, Style style, String indentation) {
        return new PureComposer(style, indentation, false).visit(ProtocolUpgrade.upgrade(node));
    }

    /** A function or derived property's parameter, as its signature spells it: no {@code $}. */
    static String signatureParameter(Json.Node variable) {
        return new PureComposer(Style.STANDARD, "", true).visit(ProtocolUpgrade.upgrade(variable));
    }

    /** {@code HelperValueSpecificationGrammarComposer.printGenericType}. */
    static String genericType(Json.Obj genericType) {
        return new PureComposer(Style.STANDARD, "", false).printGenericType(genericType);
    }

    // ---------------------------------------------------------------------
    // Builder state (upstream's Builder.newInstance(this).with...)
    // ---------------------------------------------------------------------

    private boolean pretty() {
        return style == Style.PRETTY;
    }

    private String returnChar() {
        return "\n";
    }

    /** {@code Builder.newInstance(this).withIndentation(count).build()}. */
    private PureComposer indented(int count) {
        return new PureComposer(style, pretty() ? indentation + " ".repeat(count) : indentation, variableInSignature);
    }

    /** {@code computeIndentationString(this, count)}. */
    private String indent(int count) {
        return indented(count).indentation;
    }

    private PureComposer signature() {
        return new PureComposer(style, indentation, true);
    }

    private static int tabs(int n) {
        return n * TAB.length();
    }

    // ---------------------------------------------------------------------
    // Dispatch
    // ---------------------------------------------------------------------

    private String visit(Json.Node node) {
        if (node instanceof Json.Null) {
            // the root package spelled '::' is a literal null on the wire (ProtocolReader)
            return "::";
        }
        Json.Obj o = obj(node, "value specification");
        String type = o.getStringOr("_type", null);
        if (type == null) {
            throw refused("a value specification has no _type");
        }
        if (PRIMITIVE_VALUES.contains(type) && !o.has("value") && o.has("values")) {
            return legacyValues(type, items(o, "values"));
        }
        return switch (type) {
            case "lambda" -> lambdaText(o);
            case "var" -> variable(o);
            case "func" -> appliedFunction(o);
            case "property" -> appliedProperty(o);
            case "qualifiedProperty" -> qualifiedProperty(o);
            case "collection" -> renderCollection(values(o, "values"), v -> possiblyAddParenthesis(v));
            case "integer" -> integer(o);
            case "decimal" -> decimal(o.get("value")).toString() + "D";
            case "string" -> string(o);
            case "boolean" -> String.valueOf(o.getBool("value"));
            case "float" -> String.valueOf(number(o.get("value")).doubleValue());
            case "dateTime", "strictDate", "strictTime" -> percent(o.getString("value"));
            case "latestDate" -> "%latest";
            case "byteArray" -> "toBytes(" + convertString(byteArrayText(o), true) + ")";
            case "packageableElementPtr", "class", "enum", "unitType", "mappingInstance", "primitiveType"
                    -> o.getString("fullPath");
            case "hackedClass", "hackedUnit" -> "@" + o.getString("fullPath");
            case "enumValue" -> o.getString("fullPath") + "." + convertIdentifier(o.getString("value"));
            case "genericTypeInstance" -> printGenericType(o.getObj("genericType"));
            case "keyExpression" -> keyExpression(o);
            case "unitInstance" -> unitInstance(o);
            case "classInstance" -> classInstance(o.getString("type"), o.getObj("value"));
            // the legacy wrapper spellings: a classInstance whose type IS the _type
            case "path", "rootGraphFetchTree", "listInstance", "aggregateValue", "tdsAggregateValue",
                 "tdsOlapRank", "tdsOlapAggregation" -> {
                Json.Obj wrapped = o.getObjOr("value", null);
                yield classInstance(type, wrapped != null ? wrapped : o);
            }
            default -> throw refused("no composer rule for value specification _type '" + type
                    + "' -- add the rule, do not drop it");
        };
    }

    // ---------------------------------------------------------------------
    // Lambda, variable
    // ---------------------------------------------------------------------

    private String lambdaText(Json.Obj lambda) {
        Json.Arr bodyArr = lambda.getArrOr("body", null);
        if (bodyArr == null) {
            return "";
        }
        List<Json.Node> body = bodyArr.items();
        List<Json.Node> params = items(lambda, "parameters");
        boolean addWrapper = body.size() > 1 || params.size() > 1;
        boolean addCR = body.size() > 1;
        List<String> ps = new ArrayList<>();
        for (Json.Node p : params) {
            ps.add(signature().visit(p));
        }
        PureComposer inner = addCR ? indented(tabs(1)) : this;
        List<String> bs = new ArrayList<>();
        for (Json.Node b : body) {
            bs.add(inner.visit(b));
        }
        return (addWrapper ? "{" : "")
                + String.join(", ", ps)
                + "|" + (addCR ? returnChar() + indent(tabs(1)) : "")
                + String.join(";" + returnChar() + indent(tabs(1)), bs)
                + (addCR ? ";" + returnChar() : "") + (addWrapper ? indentation + "}" : "");
    }

    private String variable(Json.Obj v) {
        Json.Obj gt = v.getObjOr("genericType", null);
        return (variableInSignature ? "" : "$") + convertIdentifier(v.getString("name"))
                + (gt != null ? ": " + printGenericType(gt) + "[" + multiplicity(v.getObjOr("multiplicity", null)) + "]" : "");
    }

    // ---------------------------------------------------------------------
    // Applied functions
    // ---------------------------------------------------------------------

    private String appliedFunction(Json.Obj af) {
        String fullName = af.getString("function");
        int index = fullName.lastIndexOf("::");
        String function = index == -1 ? fullName : fullName.substring(index + 2);
        List<Json.Node> parameters = items(af, "parameters");

        if ("getAll".equals(function)) {
            return visit(parameters.get(0)) + ".all(" + joinVisit(parameters.subList(1, parameters.size()), ", ") + ")";
        }
        if ("getAllVersions".equals(function)) {
            return visit(parameters.get(0)) + ".allVersions(" + joinVisit(parameters.subList(1, parameters.size()), ", ") + ")";
        }
        if ("letFunction".equals(function)) {
            return "let " + convertIdentifier(obj(parameters.get(0), "let name").getString("value")) + " = "
                    + visit(parameters.get(1));
        }
        if (Set.of("cast", "to", "toMany").contains(function)) {
            String castType = visit(parameters.get(1));
            if (isType(parameters.get(1), "genericTypeInstance")) {
                castType = "@" + castType;
            }
            return possiblyAddParenthesis(parameters.get(0)) + "->" + fullName + "(" + castType + ")";
        }
        if ("subType".equals(function)) {
            return visit(parameters.get(0)) + "->" + fullName + "(@" + visit(parameters.get(1)) + ")";
        }
        if ("new".equals(function)) {
            Json.Node param = parameters.get(parameters.size() - 1);
            List<Json.Node> vals = isType(param, "collection") ? items((Json.Obj) param, "values") : List.of(param);
            String type;
            if (isType(parameters.get(0), "genericTypeInstance")) {
                Json.Obj gt = ((Json.Obj) parameters.get(0)).getObj("genericType");
                type = printGenericType(obj(items(gt, "typeArguments").get(0), "type argument"));
            } else {
                type = visit(parameters.get(0));
            }
            return "^" + type + "(" + joinVisit(vals, " , ") + ")";
        }
        if ("not".equals(function)) {
            if (isFunction(parameters.get(0), "equal")) {
                List<Json.Node> eq = items((Json.Obj) parameters.get(0), "parameters");
                return possiblyAddParenthesis("not", eq.get(0)) + " != " + possiblyAddParenthesis("not", eq.get(1));
            }
            return "!" + possiblyAddParenthesis("not", parameters.get(0));
        }
        if ("divide".equals(function) && parameters.size() == 3) {
            return renderFunction(af);
        }
        if (isInfix(af)) {
            if (isType(parameters.get(0), "collection") && Set.of("plus", "minus", "times", "divide").contains(function)) {
                List<String> parts = new ArrayList<>();
                for (Json.Node v : items((Json.Obj) parameters.get(0), "values")) {
                    parts.add(possiblyAddParenthesis(function, v));
                }
                return String.join(" " + SPECIAL_INFIX.get(function) + " ", parts);
            }
            if ("minus".equals(function)) {
                return "-" + possiblyAddParenthesis("minus", parameters.get(0));
            }
            if (parameters.size() == 1) {
                return renderFunction(af);
            }
            boolean newLine = pretty() && !isPrimitiveValue(parameters.get(0)) && !isPrimitiveValue(parameters.get(1));
            return possiblyAddParenthesis(function, parameters.get(0))
                    + " " + SPECIAL_INFIX.get(function)
                    + (newLine ? returnChar() + indent(tabs(1)) : " ")
                    + possiblyAddParenthesis(function, parameters.get(1));
        }
        if (CORE_FUNCTIONS_WITH_PREFIX_RENDERING.contains(function)) {
            PureComposer inner = indented(tabs(1));
            List<String> ps = new ArrayList<>();
            for (Json.Node p : parameters) {
                ps.add(inner.visit(p));
            }
            return function + "("
                    + (pretty() ? returnChar() + indent(tabs(1)) : "")
                    + String.join("," + (pretty() ? returnChar() + indent(tabs(1)) : " "), ps)
                    + (pretty() ? returnChar() + indent(tabs(0)) : "") + ")";
        }
        return renderFunction(af);
    }

    /** {@code HelperValueSpecificationGrammarComposer.renderFunction}. */
    private String renderFunction(Json.Obj af) {
        List<Json.Node> parameters = items(af, "parameters");
        List<String> segments = new ArrayList<>();
        for (String s : pathSegments(af.getString("function"))) {
            segments.add(convertIdentifier(s));
        }
        String functionName = String.join("::", segments);
        if (parameters.isEmpty()) {
            return functionName + "()";
        }
        Json.Node first = parameters.get(0);
        List<Json.Node> others = parameters.subList(1, parameters.size());
        boolean firstIsInfix = first instanceof Json.Obj fo && isType(fo, "func")
                && !"minus".equals(fo.getString("function")) && isInfix(fo);
        if (isType(first, "lambda") || firstIsInfix) {
            PureComposer inner = indented(tabs(2));
            List<String> ps = new ArrayList<>();
            for (Json.Node p : parameters) {
                ps.add(inner.visit(p));
            }
            return functionName + "("
                    + (pretty() ? returnChar() + indent(tabs(2)) : "")
                    + String.join("," + (pretty() ? returnChar() + indent(tabs(2)) : " "), ps)
                    + (pretty() ? returnChar() + indent(tabs(1)) : "") + ")";
        }
        if (others.isEmpty()) {
            if (firstIsInfix) {
                return functionName + "(" + visit(first) + ")";
            } else if (isPrimitiveValue(first)) {
                return functionName + "(" + visit(first) + ")";
            }
            return mayWrapInParenthesis(first) + "->" + functionName + "()";
        }
        if (others.size() == 1 && isPrimitiveValue(others.get(0))) {
            return mayWrapInParenthesis(first) + "->" + functionName + "("
                    + indented(tabs(1)).visit(others.get(0)) + ")";
        }
        PureComposer inner = indented(tabs(1));
        List<String> os = new ArrayList<>();
        for (Json.Node p : others) {
            os.add(inner.visit(p));
        }
        return mayWrapInParenthesis(first) + "->" + functionName + "("
                + (pretty() ? returnChar() + indent(tabs(1)) : "")
                + String.join("," + (pretty() ? returnChar() + indent(tabs(1)) : " "), os)
                + (pretty() ? returnChar() + indentation : "") + ")";
    }

    private String mayWrapInParenthesis(Json.Node value) {
        boolean wrap = isFunction(value, "minus") || isFunction(value, "not");
        return (wrap ? "(" : "") + visit(value) + (wrap ? ")" : "");
    }

    private String possiblyAddParenthesis(String function, Json.Node param) {
        if (Set.of("and", "or", "plus", "minus", "times", "divide", "not").contains(function)) {
            return possiblyAddParenthesis(param);
        }
        return visit(param);
    }

    private String possiblyAddParenthesis(Json.Node param) {
        if (param instanceof Json.Obj p && isType(p, "func") && isInfix(p)) {
            List<Json.Node> ps = items(p, "parameters");
            boolean oneParamAndApplied = ps.size() == 1 && isType(ps.get(0), "func");
            boolean paramMany = ps.size() > 1
                    || (isType(ps.get(0), "collection") && items((Json.Obj) ps.get(0), "values").size() > 1);
            if (oneParamAndApplied || paramMany) {
                return "(" + visit(param) + ")";
            }
        }
        return visit(param);
    }

    private static boolean isInfix(Json.Obj af) {
        String function = af.getString("function");
        if (SPECIAL_INFIX.containsKey(function)) {
            return true;
        }
        List<Json.Node> ps = items(af, "parameters");
        return "not".equals(function) && !ps.isEmpty() && isFunction(ps.get(0), "equal");
    }

    private static boolean isPrimitiveValue(Json.Node v) {
        return v instanceof Json.Obj o && PRIMITIVE_VALUES.contains(o.getStringOr("_type", ""));
    }

    // ---------------------------------------------------------------------
    // Properties
    // ---------------------------------------------------------------------

    private String appliedProperty(Json.Obj ap) {
        List<Json.Node> parameters = items(ap, "parameters");
        StringBuilder b = new StringBuilder(visit(parameters.get(0)));
        b.append(".").append(convertIdentifier(ap.getString("property")));
        if (parameters.size() > 1) {
            b.append("(").append(joinVisit(parameters.subList(1, parameters.size()), ", ")).append(")");
        }
        return b.toString();
    }

    private String qualifiedProperty(Json.Obj qp) {
        List<Json.Node> parameters = items(qp, "parameters");
        return visit(parameters.get(0)) + "." + convertIdentifier(qp.getString("qualifiedProperty"))
                + (parameters.size() > 1 ? "(" + joinVisit(parameters.subList(1, parameters.size()), ", ") + ")" : "");
    }

    // ---------------------------------------------------------------------
    // Class instances (the embedded DSLs)
    // ---------------------------------------------------------------------

    private String classInstance(String type, Json.Obj value) {
        return switch (type) {
            case ">" -> "#>{" + String.join(".", value.getStringArray("path")) + "}#";
            case "colSpec" -> "~" + printColSpec(value);
            case "colSpecArray" -> printColSpecArray(value);
            case "path" -> path(value);
            case "rootGraphFetchTree" -> rootGraphFetchTree(value);
            case "keyExpression" -> keyExpression(value);
            case "primitiveType" -> value.getString("fullPath");
            case "listInstance" -> "list([" + joinVisit(items(value, "values"), ",") + "])";
            case "aggregateValue" -> "agg(" + visit(value.get("mapFn")) + ", " + visit(value.get("aggregateFn")) + ")";
            case "tdsOlapRank", "tdsOlapAggregation" -> "olapGroupBy(" + visit(value.get("function")) + ")";
            case "tdsAggregateValue" -> "agg(" + convertString(value.getString("name"), true) + ","
                    + visit(value.get("mapFn")) + ", " + visit(value.get("aggregateFn")) + ")";
            // the embedded-language islands extensions print verbatim (SQLExpressionGrammarComposerExtension,
            // TDSRelationAccessorGrammarComposerExtension)
            case "SQL" -> "#SQL{" + value.getString("sql") + "}#";
            case "TDS" -> "#TDS{" + value.getString("tdsString") + "}#";
            default -> throw refused("no composer rule for classInstance type '" + type
                    + "' -- add the rule, do not drop it");
        };
    }

    private String printColSpec(Json.Obj col) {
        Json.Obj gt = col.getObjOr("genericType", null);
        Json.Node f1 = col.getOr("function1", null);
        Json.Node f2 = col.getOr("function2", null);
        return convertIdentifier(col.getString("name"))
                + (gt != null ? ":" + printGenericType(gt) : "")
                + (f1 != null && !(f1 instanceof Json.Null) ? ":" + (pretty() ? " " : "") + visit(f1) : "")
                + (f2 != null && !(f2 instanceof Json.Null) ? ":" + visit(f2) : "");
    }

    private String printColSpecArray(Json.Obj array) {
        StringBuilder b = new StringBuilder("~[");
        if (pretty()) {
            b.append(returnChar()).append(indent(tabs(1))).append(" ");
        }
        List<String> specs = new ArrayList<>();
        for (Json.Node c : items(array, "colSpecs")) {
            specs.add(printColSpec(obj(c, "colSpec")));
        }
        b.append(String.join("," + (pretty() ? returnChar() + " " + indent(tabs(1)) : " "), specs));
        if (pretty()) {
            b.append(returnChar()).append(indent(0)).append(" ");
        }
        return b.append("]").toString();
    }

    private String keyExpression(Json.Obj ke) {
        return removeQuotes(visit(ke.get("key"))) + "=" + visit(ke.get("expression"));
    }

    private String unitInstance(Json.Obj ui) {
        return number(ui.get("unitValue")).toString() + " " + ui.getString("unitType");
    }

    private String path(Json.Obj path) {
        List<String> elements = new ArrayList<>();
        for (Json.Node e : items(path, "path")) {
            Json.Obj pe = obj(e, "path element");
            List<Json.Node> ps = items(pe, "parameters");
            elements.add(pe.getString("property") + (ps.size() > 1 ? "(" + joinVisit(ps, ", ") + ")" : ""));
        }
        String name = path.getStringOr("name", null);
        return "#/" + convertPath(path.getString("startType"))
                + (elements.isEmpty() ? "" : "/" + String.join("/", elements))
                + (name == null || name.isEmpty() ? "" : "!" + name) + "#";
    }

    private String rootGraphFetchTree(Json.Obj root) {
        PureComposer inner = indented(tabs(1));
        String subTrees = joinGraph(inner, items(root, "subTrees"));
        String subTypeTrees = joinGraph(inner, items(root, "subTypeTrees"));
        String cr = pretty() ? returnChar() : "";
        return "#{" + cr
                + indent(tabs(1)) + root.getString("class") + "{" + cr
                + subTrees + (!subTrees.isEmpty() && !subTypeTrees.isEmpty() ? (pretty() ? "," + returnChar() : ",") : "")
                + subTypeTrees + cr
                + indent(tabs(1)) + "}" + cr
                + indentation + "}#";
    }

    private String joinGraph(PureComposer inner, List<Json.Node> trees) {
        List<String> out = new ArrayList<>();
        for (Json.Node t : trees) {
            out.add(inner.graphFetchTree(obj(t, "graph fetch tree")));
        }
        return String.join("," + (pretty() ? returnChar() : ""), out);
    }

    private String graphFetchTree(Json.Obj tree) {
        String type = tree.getString("_type");
        return switch (type) {
            case "propertyGraphFetchTree" -> propertyGraphFetchTree(tree);
            case "subTypeGraphFetchTree" -> subTypeGraphFetchTree(tree);
            case "rootGraphFetchTree" -> rootGraphFetchTree(tree);
            default -> throw refused("no composer rule for graph fetch tree _type '" + type + "'");
        };
    }

    private String subTrees(List<Json.Node> trees) {
        if (trees.isEmpty()) {
            return "";
        }
        String cr = pretty() ? returnChar() : "";
        return "{" + cr + joinGraph(indented(tabs(1)), trees) + cr + indent(tabs(1)) + "}";
    }

    private String propertyGraphFetchTree(Json.Obj tree) {
        String alias = tree.getStringOr("alias", null);
        List<Json.Node> params = items(tree, "parameters");
        String subType = tree.getStringOr("subType", null);
        return indent(tabs(1))
                + (alias != null ? convertString(alias, false) + ":" : "")
                + tree.getString("property")
                + (params.isEmpty() ? "" : "(" + joinVisit(params, ", ") + ")")
                + (subType != null ? "->subType(@" + subType + ")" : "")
                + subTrees(items(tree, "subTrees"));
    }

    private String subTypeGraphFetchTree(Json.Obj tree) {
        return indent(tabs(1)) + "->subType(@" + tree.getString("subTypeClass") + ")"
                + subTrees(items(tree, "subTrees"));
    }

    // ---------------------------------------------------------------------
    // Types
    // ---------------------------------------------------------------------

    private String printGenericType(Json.Obj gt) {
        List<Json.Node> typeArgs = items(gt, "typeArguments");
        List<Json.Node> multArgs = items(gt, "multiplicityArguments");
        List<Json.Node> typeVariableValues = items(gt, "typeVariableValues");
        StringBuilder b = new StringBuilder(printType(gt.getObj("rawType")));
        if (!typeArgs.isEmpty() || !multArgs.isEmpty()) {
            List<String> ts = new ArrayList<>();
            for (Json.Node t : typeArgs) {
                ts.add(printGenericType(obj(t, "type argument")));
            }
            List<String> ms = new ArrayList<>();
            for (Json.Node m : multArgs) {
                ms.add(multiplicity(obj(m, "multiplicity argument")));
            }
            b.append("<").append(String.join(", ", ts))
                    .append(multArgs.isEmpty() ? "" : "|" + String.join(",", ms)).append(">");
        }
        if (!typeVariableValues.isEmpty()) {
            b.append("(").append(joinVisit(typeVariableValues, ",")).append(")");
        }
        return b.toString();
    }

    private String printType(Json.Obj type) {
        String t = type.getStringOr("_type", "");
        if ("packageableType".equals(t)) {
            return type.getString("fullPath");
        }
        if ("relationType".equals(t)) {
            List<String> cols = new ArrayList<>();
            for (Json.Node c : items(type, "columns")) {
                Json.Obj col = obj(c, "relation column");
                Json.Obj m = col.getObjOr("multiplicity", null);
                boolean zeroOne = m == null || (m.getIntOr("lowerBound", 0) == 0 && upper(m) == 1);
                cols.add(convertIdentifier(col.getString("name")) + ":" + printGenericType(col.getObj("genericType"))
                        + (zeroOne ? "" : "[" + multiplicity(m) + "]"));
            }
            return "(" + String.join(", ", cols) + ")";
        }
        throw refused("no composer rule for a type of _type '" + t + "'");
    }

    /** {@code HelperDomainGrammarComposer.renderMultiplicity}; absent means {@code [*]}. */
    static String multiplicity(@com.legend.base.Nullable Json.Obj m) {
        int lower = m == null ? 0 : m.getIntOr("lowerBound", 0);
        int upper = m == null ? Integer.MAX_VALUE : upper(m);
        if (lower == 0 && upper == Integer.MAX_VALUE) {
            return "*";
        }
        return lower == upper ? String.valueOf(lower)
                : lower + ".." + (upper == Integer.MAX_VALUE ? "*" : String.valueOf(upper));
    }

    private static int upper(Json.Obj m) {
        Json.Node u = m.getOr("upperBound", null);
        return u instanceof Json.Num n ? (int) n.longValue() : Integer.MAX_VALUE;
    }

    // ---------------------------------------------------------------------
    // Literals
    // ---------------------------------------------------------------------

    private static String integer(Json.Obj o) {
        Json.Num n = number(o.get("value"));
        return n.decimalValue() != null ? n.decimalValue().toBigIntegerExact().toString() : String.valueOf(n.longValue());
    }

    /**
     * A decimal as its EXACT digits: {@code 10.10} prints {@code 10.10D}, so a print parses back to the
     * same tree. Deliberately NOT upstream's: its reader ({@code CDecimal.CDecimalDeserializer}) takes
     * the value's Jackson TREE node, where a JSON fraction is a double, then
     * {@code new BigDecimal(node.asText())} -- so {@code 10.10} comes back {@code 10.1} and
     * {@code 12345678901234567.89} comes back {@code 12345678901234568}. Upstream's own test of that
     * lambda expects {@code new BigDecimal("10.10")} (mathLibraryTests.pure); the user chose exactness,
     * 2026-09-28. ComposerParityTest names the prints this changes.
     */
    private static BigDecimal decimal(Json.Node v) {
        if (v instanceof Json.Str s) {
            return new BigDecimal(s.value());
        }
        Json.Num n = number(v);
        if (n.isInteger()) {
            return BigDecimal.valueOf(n.longValue());
        }
        if (n.decimalValue() != null) {
            return n.decimalValue();
        }
        return new BigDecimal(Double.toString(n.doubleValue()));
    }

    /**
     * The older wire's primitive, {@code {"_type":"integer","values":[...]}}
     * ({@code PrimitiveValueSpecification.customParsePrimitive}): none is an empty collection, one
     * the literal, more a collection of them. Accepted for backwards compatibility.
     */
    private String legacyValues(String type, List<Json.Node> values) {
        List<Json.Node> literals = new ArrayList<>();
        for (Json.Node v : values) {
            java.util.LinkedHashMap<String, Json.Node> f = new java.util.LinkedHashMap<>();
            f.put("_type", Json.str(type));
            f.put("value", v);
            literals.add(new Json.Obj(f));
        }
        if (literals.size() == 1) {
            return visit(literals.get(0));
        }
        return renderCollection(literals, this::possiblyAddParenthesis);
    }

    private static Json.Num number(Json.Node v) {
        if (v instanceof Json.Num n) {
            return n;
        }
        throw refused("a numeric literal whose value is not a number: " + v);
    }

    /** {@code generateValidDateValueContainingPercent}. */
    private static String percent(String date) {
        return date.indexOf('%') != -1 ? date : "%" + date;
    }

    private String string(Json.Obj o) {
        String s = o.getString("value");
        return o.getBoolOr("multiLine", false) ? renderTextBlock(s, indentation) : convertString(s, true);
    }

    private static String byteArrayText(Json.Obj o) {
        // the wire carries the bytes base64-encoded (Jackson's byte[]); upstream prints them as UTF-8 text
        byte[] bytes = java.util.Base64.getDecoder().decode(o.getString("value"));
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    /** {@code PureGrammarComposerUtility.convertString(val, escape)} with single quotes. */
    static String convertString(String val, boolean escape) {
        String body = val;
        if (escape) {
            body = escapeJava(val).replace("'", "\\'").replace("\\\"", "\"");
        }
        return "'" + body + "'";
    }

    /**
     * commons-text {@code StringEscapeUtils.escapeJava}: {@code "} and {@code \} escaped, the five
     * named control characters by name, every other code point outside {@code [32, 0x7f]} as
     * {@code \}{@code uXXXX} (upper-case hex; a supplementary code point as its surrogate pair).
     */
    static String escapeJava(String s) {
        StringBuilder b = new StringBuilder(s.length() + 8);
        int i = 0;
        while (i < s.length()) {
            int cp = s.codePointAt(i);
            i += Character.charCount(cp);
            switch (cp) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\b' -> b.append("\\b");
                case '\n' -> b.append("\\n");
                case '\t' -> b.append("\\t");
                case '\f' -> b.append("\\f");
                case '\r' -> b.append("\\r");
                default -> {
                    if (cp < 32 || cp > 0x7f) {
                        for (char c : Character.toChars(cp)) {
                            b.append("\\u").append(hex4(c));
                        }
                    } else {
                        b.appendCodePoint(cp);
                    }
                }
            }
        }
        return b.toString();
    }

    private static String hex4(int c) {
        String h = Integer.toHexString(c).toUpperCase(Locale.ENGLISH);
        return "0".repeat(Math.max(0, 4 - h.length())) + h;
    }

    /** {@code HelperValueSpecificationGrammarComposer.renderTextBlock}. */
    private static String renderTextBlock(String value, String indent) {
        boolean closeOnOwnLine = value.endsWith("\n");
        boolean escapeLeadingWhitespace = !closeOnOwnLine;
        boolean escapeQuotes = value.contains("'''");
        List<String> lines = lines(closeOnOwnLine ? value.substring(0, value.length() - 1) : value);
        StringBuilder b = new StringBuilder("'''\n");
        for (int i = 0; i < lines.size(); i++) {
            b.append(indent).append(escapeTextBlockLine(lines.get(i), escapeLeadingWhitespace, escapeQuotes));
            if (i < lines.size() - 1) {
                b.append('\n');
            }
        }
        if (closeOnOwnLine) {
            b.append('\n').append(indent);
        }
        return b.append("'''").toString();
    }

    private static String escapeTextBlockLine(String line, boolean escapeLeadingWhitespace, boolean escapeQuotes) {
        int leading = 0;
        while (leading < line.length() && Character.isWhitespace(line.charAt(leading))) {
            leading++;
        }
        boolean allWhitespace = leading == line.length() && !line.isEmpty();
        int trailingFrom = allWhitespace ? line.length() - 1 : trailingWhitespaceStart(line, leading);
        StringBuilder b = new StringBuilder(line.length());
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (i >= trailingFrom || (i == 0 && (allWhitespace || (escapeLeadingWhitespace && leading > 0)))) {
                b.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
            } else if (c == '\\') {
                b.append("\\\\");
            } else if (c == '\r') {
                b.append("\\r");
            } else if (c == '\'' && escapeQuotes) {
                b.append("\\'");
            } else {
                b.append(c);
            }
        }
        return b.toString();
    }

    private static int trailingWhitespaceStart(String line, int leading) {
        int end = line.length();
        while (end > leading && Character.isWhitespace(line.charAt(end - 1))) {
            end--;
        }
        return end;
    }

    // ---------------------------------------------------------------------
    // Identifiers
    // ---------------------------------------------------------------------

    /** {@code PureGrammarComposerUtility.convertIdentifier}: bare when it is one, else quoted. */
    static String convertIdentifier(String val) {
        if (val == null || val.isEmpty()) {
            return "";
        }
        return isUnquotedIdentifier(val) ? val : convertString(val, true);
    }

    /** {@code [A-Za-z_][A-Za-z0-9_$~]*}, upstream's unquoted identifier, read char by char. */
    private static boolean isUnquotedIdentifier(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            boolean letter = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
            boolean rest = (c >= '0' && c <= '9') || c == '$' || c == '~';
            if (!(letter || (i > 0 && rest))) {
                return false;
            }
        }
        return !s.isEmpty();
    }

    /**
     * A path's {@code ::}-separated segments as Java's String split gives them (upstream's printer
     * splits so): no separator is the text itself; otherwise trailing empty segments are dropped.
     */
    static List<String> pathSegments(String s) {
        if (s.indexOf("::") < 0) {
            return List.of(s);
        }
        List<String> out = new ArrayList<>();
        int from = 0;
        int at;
        while ((at = s.indexOf("::", from)) >= 0) {
            out.add(s.substring(from, at));
            from = at + 2;
        }
        out.add(s.substring(from));
        while (!out.isEmpty() && out.get(out.size() - 1).isEmpty()) {
            out.remove(out.size() - 1);
        }
        return out;
    }

    /** The lines of {@code s}, split at each newline, empty ones kept. */
    static List<String> lines(String s) {
        List<String> out = new ArrayList<>();
        int from = 0;
        int at;
        while ((at = s.indexOf('\n', from)) >= 0) {
            out.add(s.substring(from, at));
            from = at + 1;
        }
        out.add(s.substring(from));
        return out;
    }

    static String convertPath(String val) {
        List<String> out = new ArrayList<>();
        for (String s : pathSegments(val)) {
            out.add(convertIdentifier(s));
        }
        return String.join("::", out);
    }

    /** {@code PureGrammarParserUtility.removeQuotes}: the first and last character, unconditionally. */
    private static String removeQuotes(String s) {
        return s.substring(1, s.length() - 1);
    }

    // ---------------------------------------------------------------------
    // Plumbing
    // ---------------------------------------------------------------------

    private String renderCollection(List<Json.Node> values, Function<Json.Node, String> render) {
        if (values.isEmpty()) {
            return "[]";
        }
        boolean newLine = pretty() && (values.size() != 1
                || (!isPrimitiveValue(values.get(0)) && !isType(values.get(0), "var")));
        PureComposer inner = indented(tabs(1));
        List<String> out = new ArrayList<>();
        for (Json.Node v : values) {
            out.add(render.apply(v));
        }
        return "[" + (newLine ? returnChar() + inner.indentation : "")
                + String.join("," + (pretty() ? returnChar() + inner.indentation : " "), out)
                + (newLine ? returnChar() + indentation : "") + "]";
    }

    private String joinVisit(List<Json.Node> nodes, String sep) {
        List<String> out = new ArrayList<>();
        for (Json.Node n : nodes) {
            out.add(visit(n));
        }
        return String.join(sep, out);
    }

    private static boolean isType(Json.Node n, String type) {
        return n instanceof Json.Obj o && type.equals(o.getStringOr("_type", ""));
    }

    private static boolean isFunction(Json.Node n, String name) {
        return n instanceof Json.Obj o && isType(o, "func") && name.equals(o.getStringOr("function", ""));
    }

    private static List<Json.Node> values(Json.Obj o, String key) {
        return items(o, key);
    }

    private static List<Json.Node> items(Json.Obj o, String key) {
        Json.Arr a = o.getArrOr(key, null);
        return a == null ? List.of() : a.items();
    }

    private static Json.Obj obj(Json.Node n, String what) {
        if (n instanceof Json.Obj o) {
            return o;
        }
        throw refused(what + " is not a JSON object: " + n);
    }

    private static IllegalArgumentException refused(String why) {
        return new IllegalArgumentException("lambda JSON: " + why);
    }
}
