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
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.GqlIsland;
import com.legend.protocol.spec.GraphFetchLiteral;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.NewInstanceCast;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PathLiteral;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.QuotedGrammarCall;
import com.legend.protocol.spec.QuotedTreeCall;
import com.legend.protocol.spec.SqlIsland;
import com.legend.protocol.spec.TdsLiteral;
import com.legend.protocol.spec.TypeAnnotation;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * The value-specification records to Pure text: the printer for lambdas and value specifications, as upstream's
 * {@code pure/v1/grammar/jsonToGrammar/lambda} prints them (its {@code DEPRECATED_PureGrammarComposerCore} and
 * {@code HelperValueSpecificationGrammarComposer}, ported rule for rule, the reference checkout as spec;
 * docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md, T4a).
 *
 * <p>Upstream composes its protocol objects, so each record prints as the object the emitter writes for it (the
 * protocol program's leg 2, step 3: the printer over the records, its rules unchanged): a method-call property
 * ({@code propertyCall}) as a property access, the {@code #>{..}#} island as the island, {@code let} as its call, an
 * enum value as the property access the wire spells it, {@code ^X(...)} as {@code new} -- or, for the three classes
 * the engine writes a call for, that call ({@link CaretSpecials}) -- and every other application as the call it is.
 * A record upstream cannot print is REFUSED, naming it -- never skipped, never guessed. The JSON entry points read the
 * JSON first ({@link ProtocolReader}): {@code compose(read(J))}. Byte parity with upstream's printer is pinned by the
 * parser-equivalence oracle over every lambda of the reference corpus, in both styles.
 *
 * <p>Two styles: {@link Style#STANDARD} (one line) and {@link Style#PRETTY} (upstream's indentation). Upstream's HTML
 * style is not served.
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

    /** The bare {@code Result} type the emitter writes with its synthesized argument (DomainParseTreeWalker). */
    private static final String BARE_RESULT = "Result<meta::pure::metamodel::type::Any|1..*>";

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

    // ---------------------------------------------------------------------
    // Entry points: the records, and the JSON read first
    // ---------------------------------------------------------------------

    /** A lambda as Pure text. */
    public static String lambda(LambdaFunction lambda, Style style) {
        return new PureComposer(style, "", false).visit(lambda);
    }

    /** A lambda, {@code {"_type":"lambda",...}}, as Pure text: read, then printed. */
    public static String lambda(Json.Obj lambda, Style style) {
        return lambda(ProtocolReader.lambda(lambda), style);
    }

    /**
     * JSON to text for a lambda (the conversion legend-engine's {@code jsonToGrammar/lambda} makes): its JSON read
     * ({@link ProtocolReader#lambda(String)}, at the lambda depth limit) and printed in {@code style}.
     */
    public static String lambda(String json, Style style) {
        return lambda(ProtocolReader.lambda(json), style);
    }

    /** Any value specification as Pure text. */
    public static String valueSpecification(ValueSpecification value, Style style) {
        return new PureComposer(style, "", false).visit(value);
    }

    /** Any value specification's JSON as Pure text: read, then printed. */
    public static String valueSpecification(Json.Node node, Style style) {
        return valueSpecification(read(node), style);
    }

    /**
     * A value specification inside a model element ({@link ModelComposer}): upstream's printer built from the
     * element's composer context, whose indentation a lambda's closing brace, a collection's closing bracket and a
     * text block's lines are written at.
     */
    static String valueSpecification(ValueSpecification value, Style style, String indentation) {
        return new PureComposer(style, indentation, false).visit(value);
    }

    /**
     * A statement sequence -- a function's or a derived property's body -- each statement printed as
     * {@link #valueSpecification(ValueSpecification, Style, String)} prints it, but the first, when more follow,
     * keeping the braces of a lambda its text ends with ({@link #lambdaText}).
     */
    static List<String> statements(List<ValueSpecification> body, Style style, String indentation) {
        PureComposer composer = new PureComposer(style, indentation, false);
        List<String> out = new ArrayList<>(body.size());
        for (int i = 0; i < body.size(); i++) {
            out.add(composer.visit(body.get(i), i == 0 && body.size() > 1));
        }
        return out;
    }

    /**
     * A legacy service test's parameter: there {@code list([...])} is the list instance the engine's grammar writes
     * ({@code ServiceParseTreeWalker}) and its printer prints as one -- {@code list([a,b])}, no space after a comma --
     * where anywhere else it is the ordinary call {@code list} (the emitter decides by the same position: TailEmitter).
     */
    static String legacyServiceParameter(ValueSpecification value, Style style) {
        if (value instanceof AppliedFunction af && calls(af, AppliedFunction.LIST) && !af.propertyCall()
                && af.parameters().size() == 1 && af.parameters().get(0) instanceof PureCollection c) {
            PureComposer composer = new PureComposer(style, "", false);
            return "list([" + composer.joinVisit(c.values(), ",") + "])";
        }
        return valueSpecification(value, style, "");
    }

    /** A function or derived property's parameter, as its signature spells it: no {@code $}. */
    static String signatureParameter(Variable variable) {
        return new PureComposer(Style.STANDARD, "", true).visit(variable);
    }

    /** {@code HelperValueSpecificationGrammarComposer.printGenericType}. */
    static String genericType(TypeExpression genericType) {
        return new PureComposer(Style.STANDARD, "", false).printGenericType(genericType);
    }

    private static ValueSpecification read(Json.Node node) {
        return ProtocolReader.valueSpec(ProtocolUpgrade.upgrade(node));
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
    // Dispatch: each record as the protocol object the emitter writes for it
    // ---------------------------------------------------------------------

    private String visit(ValueSpecification v) {
        return visit(v, false);
    }

    /** {@code v}'s text; {@code tail}: text follows it that a brace-less lambda ending it would read as its own -- the
     *  rest of a sequence's first statement, or an arrow, a dot or an operator after it ({@link #lambdaText}). */
    private String visit(ValueSpecification v, boolean tail) {
        return switch (v) {
            case LambdaFunction l -> lambdaText(l, tail);
            case Variable var -> variable(var);
            case AppliedFunction af -> appliedFunction(af, tail);
            case AppliedProperty ap -> visit(ap.receiver(), true) + "." + convertIdentifier(ap.property());
            // on the wire a property on the enumeration's pointer, or an enumValue node: the same text
            case EnumValue e -> e.fullPath() + "." + convertIdentifier(e.value());
            case PureCollection c -> renderCollection(c.values(), this::possiblyAddParenthesis);
            case CInteger c -> c.value().toString();
            case CDecimal c -> c.value().toString() + "D";
            case CString c -> c.multiLine() ? renderTextBlock(c.value(), indentation) : convertString(c.value(), true);
            case CBoolean c -> String.valueOf(c.value());
            case CFloat c -> Double.toString(c.value());
            case CDate d -> percent(written(d.written(), "date"));
            case CTime t -> percent(written(t.written(), "time"));
            case CLatestDate l -> "%latest";
            case CByteArray c -> "toBytes(" + convertString(byteArrayText(c.value()), true) + ")";
            case PackageableElementPtr ptr -> ptr.fullPath();
            case TypeAnnotation.Named named -> printGenericType(named.type());
            case TypeAnnotation.RelationShape rs -> relationShape(rs);
            case TypeAnnotation.MultiplicityRef m -> throw refused("a multiplicity annotation: upstream writes none");
            case TypeAnnotation.Wildcard w -> throw refused("a wildcard type annotation: upstream writes none");
            case NewInstance ni -> newInstance(ni);
            case ColSpec cs -> "~" + printColSpec(cs, tail);
            case ColSpecArray ca -> printColSpecArray(ca);
            case PathLiteral pl -> path(pl);
            case GraphFetchLiteral gf -> rootGraphFetchTree(gf);
            // the embedded-language islands extensions print verbatim (SQLExpressionGrammarComposerExtension,
            // TDSRelationAccessorGrammarComposerExtension)
            case SqlIsland si -> "#SQL{" + si.sql() + "}#";
            case TdsLiteral tl -> "#TDS{" + tl.tdsString() + "}#";
            case GqlIsland gi -> throw refused("no composer rule for classInstance type 'GQL' -- add the rule, do not"
                    + " drop it");
            // a quote/eval carrier's wire face is its original call
            case QuotedTreeCall q -> visit(q.original(), tail);
            case QuotedGrammarCall q -> visit(q.original(), tail);
            case NewInstanceCast nc -> throw refused("a new-instance cast: upstream writes none");
        };
    }

    // ---------------------------------------------------------------------
    // Lambda, variable
    // ---------------------------------------------------------------------

    /**
     * A lambda: braced when its body has more than one statement or it has more than one parameter, as upstream
     * prints it -- and, a deliberate difference (docs/SEMANTICS_REGISTER.md S38, "a lambda's braces where dropping them
     * changes the reading"), where text follows it that its body would read as its own ({@code tail}). A brace-less
     * lambda's body reads on as far as the grammar lets it: in a sequence's first statement that more statements
     * follow, the grammar makes the ';' optional, so the reader takes the ';' and the statements after it
     * (SpecParser.readBraceLessBlock; legend-engine's parser alike) -- upstream's print of
     * {@code let q = {|1}; $q->eval();} reads back as one statement; and a lambda followed by an arrow, a dot or an
     * operator ({@code {x|$x}->cast(@T)}, {@code {|1} == {|2}}) has it read into its body. Anywhere else the braces drop
     * as upstream drops them, and the print is upstream's byte for byte.
     */
    private String lambdaText(LambdaFunction lambda, boolean tail) {
        List<ValueSpecification> body = lambda.body();
        List<Variable> params = lambda.parameters();
        boolean upstreamWrapper = body.size() > 1 || params.size() > 1;
        // the kept braces close tight: the print is upstream's with the two braces added, in either style
        boolean addWrapper = upstreamWrapper || tail;
        boolean addCR = body.size() > 1;
        List<String> ps = new ArrayList<>();
        for (Variable p : params) {
            ps.add(signature().visit(p));
        }
        PureComposer inner = addCR ? indented(tabs(1)) : this;
        // a lone statement that is itself a brace-less lambda with no parameters would print right after this one's
        // '|' as '||', which lexes as the or operator: its braces stay too (S38; upstream's print does not parse)
        boolean opensWithPipe = !addCR && body.size() == 1 && body.get(0) instanceof LambdaFunction l
                && l.parameters().isEmpty();
        List<String> bs = new ArrayList<>();
        for (int i = 0; i < body.size(); i++) {
            bs.add(inner.visit(body.get(i), i == 0 && body.size() > 1 || opensWithPipe));
        }
        return (addWrapper ? "{" : "")
                + String.join(", ", ps)
                + "|" + (addCR ? returnChar() + indent(tabs(1)) : "")
                + String.join(";" + returnChar() + indent(tabs(1)), bs)
                + (addCR ? ";" + returnChar() : "") + (upstreamWrapper ? indentation + "}" : addWrapper ? "}" : "");
    }

    private String variable(Variable v) {
        return (variableInSignature ? "" : "$") + convertIdentifier(v.name())
                + (v.type() != null ? ": " + printGenericType(v.type()) + "[" + multiplicity(v.multiplicity()) + "]"
                : "");
    }

    // ---------------------------------------------------------------------
    // Applied functions
    // ---------------------------------------------------------------------

    /** {@code tail}: as {@link #visit(ValueSpecification, boolean)}'s -- passed on to the operand the call's text ends
     *  with, where it ends with one (a let's value, an operator's last operand), not one a bracket closes after; an
     *  operand that text follows (a receiver before an arrow or a dot, an operator's left operand) is a tail itself. */
    private String appliedFunction(AppliedFunction af, boolean tail) {
        if (af.propertyCall()) {
            // receiver.name(args): a property node, its arguments after the receiver
            List<ValueSpecification> ps = af.parameters();
            return visit(ps.get(0), true) + "." + convertIdentifier(af.function())
                    + (ps.size() > 1 ? "(" + joinVisit(ps.subList(1, ps.size()), ", ") + ")" : "");
        }
        if (af.island()) {
            // #>{db[.schema.table]}#: the database and the rest of the path, joined as the wire's path is
            String db = ((PackageableElementPtr) af.parameters().get(0)).fullPath();
            return "#>{" + db + (af.parameters().size() > 1 ? "." + ((CString) af.parameters().get(1)).value() : "")
                    + "}#";
        }
        if (calls(af, AppliedFunction.NEW) && !af.parameters().isEmpty()
                && af.parameters().get(af.parameters().size() - 1) instanceof NewInstance ni) {
            return newInstance(ni);
        }
        String fullName = af.function();
        int index = fullName.lastIndexOf("::");
        String function = index == -1 ? fullName : fullName.substring(index + 2);
        List<ValueSpecification> parameters = af.parameters();

        if ("getAll".equals(function)) {
            return visit(parameters.get(0)) + ".all(" + joinVisit(parameters.subList(1, parameters.size()), ", ") + ")";
        }
        if ("getAllVersions".equals(function)) {
            return visit(parameters.get(0)) + ".allVersions(" + joinVisit(parameters.subList(1, parameters.size()), ", ")
                    + ")";
        }
        if ("letFunction".equals(function)) {
            return "let " + convertIdentifier(((CString) parameters.get(0)).value()) + " = " + visit(parameters.get(1), tail);
        }
        if (Set.of("cast", "to", "toMany").contains(function)) {
            String castType = visit(parameters.get(1));
            if (parameters.get(1) instanceof TypeAnnotation) {
                castType = "@" + castType;
            }
            return possiblyAddParenthesis(parameters.get(0), true) + "->" + fullName + "(" + castType + ")";
        }
        if ("subType".equals(function)) {
            return visit(parameters.get(0), true) + "->" + fullName + "(@" + visit(parameters.get(1)) + ")";
        }
        if ("new".equals(function)) {
            // a new the reader kept as written (not today's ^X(...)): its last argument's values are the keys
            ValueSpecification last = parameters.get(parameters.size() - 1);
            List<ValueSpecification> vals = last instanceof PureCollection c ? c.values() : List.of(last);
            // the class: a Class<X> annotation's X, else the pointer as written
            String type = parameters.get(0) instanceof TypeAnnotation.Named n
                    && n.type() instanceof TypeExpression.Generic g && !g.arguments().isEmpty()
                    ? printGenericType(g.arguments().get(0)) : visit(parameters.get(0));
            return "^" + type + "(" + joinVisit(vals, " , ") + ")";
        }
        if ("not".equals(function)) {
            if (isFunction(parameters.get(0), "equal")) {
                List<ValueSpecification> eq = ((AppliedFunction) parameters.get(0)).parameters();
                return possiblyAddParenthesis("not", eq.get(0), true) + " != " + possiblyAddParenthesis("not", eq.get(1), tail);
            }
            return "!" + possiblyAddParenthesis("not", parameters.get(0), tail);
        }
        if ("divide".equals(function) && parameters.size() == 3) {
            return renderFunction(af);
        }
        if (isInfix(af)) {
            if (parameters.get(0) instanceof PureCollection run && Set.of("plus", "minus", "times", "divide").contains(function)) {
                List<String> parts = new ArrayList<>();
                List<ValueSpecification> values = run.values();
                for (int i = 0; i < values.size(); i++) {
                    parts.add(possiblyAddParenthesis(function, values.get(i), i < values.size() - 1 || tail));
                }
                return String.join(" " + SPECIAL_INFIX.get(function) + " ", parts);
            }
            if ("minus".equals(function)) {
                return "-" + possiblyAddParenthesis("minus", parameters.get(0), tail);
            }
            if (parameters.size() == 1) {
                return renderFunction(af);
            }
            boolean newLine = pretty() && !isPrimitiveValue(parameters.get(0)) && !isPrimitiveValue(parameters.get(1));
            return possiblyAddParenthesis(function, parameters.get(0), true)
                    + " " + SPECIAL_INFIX.get(function)
                    + (newLine ? returnChar() + indent(tabs(1)) : " ")
                    + possiblyAddParenthesis(function, parameters.get(1), tail);
        }
        if (CORE_FUNCTIONS_WITH_PREFIX_RENDERING.contains(function)) {
            PureComposer inner = indented(tabs(1));
            List<String> ps = new ArrayList<>();
            for (ValueSpecification p : parameters) {
                ps.add(inner.visit(p));
            }
            return function + "("
                    + (pretty() ? returnChar() + indent(tabs(1)) : "")
                    + String.join("," + (pretty() ? returnChar() + indent(tabs(1)) : " "), ps)
                    + (pretty() ? returnChar() + indent(tabs(0)) : "") + ")";
        }
        return renderFunction(af);
    }

    /** {@code ^X(...)}: the call the engine writes for the three special classes, else the keyed instance. */
    private String newInstance(NewInstance ni) {
        AppliedFunction special = CaretSpecials.call(ni);
        if (special != null) {
            return appliedFunction(special, false);
        }
        if (ni.className().isEmpty()) {
            throw refused("a new-instance on a variable receiver: upstream writes none");
        }
        List<String> keys = new ArrayList<>(ni.properties().size());
        for (NewInstance.KeyBinding k : ni.properties()) {
            keys.add(removeQuotes(convertString(k.key(), true)) + "=" + visit(k.expression().value()));
        }
        // the class as the emitter writes it -- by name, not through a type's spelling rules (a bare Result stays bare)
        String type = ni.typeArguments().isEmpty() && ni.typeMultiplicityArguments().isEmpty() ? ni.className()
                : printGenericType(new TypeExpression.Generic(ni.className(), ni.typeArguments(),
                        ni.typeMultiplicityArguments(), List.of(), null));
        return "^" + type + "(" + String.join(" , ", keys) + ")";
    }

    /** {@code HelperValueSpecificationGrammarComposer.renderFunction}. */
    private String renderFunction(AppliedFunction af) {
        List<ValueSpecification> parameters = af.parameters();
        List<String> segments = new ArrayList<>();
        for (String s : pathSegments(af.function())) {
            segments.add(convertIdentifier(s));
        }
        String functionName = String.join("::", segments);
        if (parameters.isEmpty()) {
            return functionName + "()";
        }
        ValueSpecification first = parameters.get(0);
        List<ValueSpecification> others = parameters.subList(1, parameters.size());
        boolean firstIsInfix = wireCall(first) instanceof AppliedFunction fo && !calls(fo, "minus")
                && isInfix(fo);
        if (first instanceof LambdaFunction || firstIsInfix) {
            PureComposer inner = indented(tabs(2));
            List<String> ps = new ArrayList<>();
            for (ValueSpecification p : parameters) {
                ps.add(inner.visit(p));
            }
            return functionName + "("
                    + (pretty() ? returnChar() + indent(tabs(2)) : "")
                    + String.join("," + (pretty() ? returnChar() + indent(tabs(2)) : " "), ps)
                    + (pretty() ? returnChar() + indent(tabs(1)) : "") + ")";
        }
        if (others.isEmpty()) {
            if (isPrimitiveValue(first)) {
                return functionName + "(" + visit(first) + ")";
            }
            return mayWrapInParenthesis(first) + "->" + functionName + "()";
        }
        if (others.size() == 1 && isPrimitiveValue(others.get(0))) {
            return mayWrapInParenthesis(first) + "->" + functionName + "(" + indented(tabs(1)).visit(others.get(0)) + ")";
        }
        PureComposer inner = indented(tabs(1));
        List<String> os = new ArrayList<>();
        for (ValueSpecification p : others) {
            os.add(inner.visit(p));
        }
        return mayWrapInParenthesis(first) + "->" + functionName + "("
                + (pretty() ? returnChar() + indent(tabs(1)) : "")
                + String.join("," + (pretty() ? returnChar() + indent(tabs(1)) : " "), os)
                + (pretty() ? returnChar() + indentation : "") + ")";
    }

    private String mayWrapInParenthesis(ValueSpecification value) {
        boolean wrap = isFunction(value, "minus") || isFunction(value, "not");
        return (wrap ? "(" : "") + visit(value, !wrap) + (wrap ? ")" : "");
    }

    private String possiblyAddParenthesis(String function, ValueSpecification param) {
        return possiblyAddParenthesis(function, param, false);
    }

    /** {@code tail}: as {@link #visit(ValueSpecification, boolean)}'s; inside parentheses nothing is at the tail. */
    private String possiblyAddParenthesis(String function, ValueSpecification param, boolean tail) {
        if (Set.of("and", "or", "plus", "minus", "times", "divide", "not").contains(function)) {
            return possiblyAddParenthesis(param, tail);
        }
        return visit(param, tail);
    }

    private String possiblyAddParenthesis(ValueSpecification param) {
        return possiblyAddParenthesis(param, false);
    }

    private String possiblyAddParenthesis(ValueSpecification param, boolean tail) {
        if (wireCall(param) instanceof AppliedFunction p && isInfix(p)) {
            List<ValueSpecification> ps = p.parameters();
            boolean oneParamAndApplied = ps.size() == 1 && wireCall(ps.get(0)) != null;
            boolean paramMany = ps.size() > 1 || (ps.get(0) instanceof PureCollection c && c.values().size() > 1);
            if (oneParamAndApplied || paramMany) {
                return "(" + visit(param) + ")";
            }
        }
        return visit(param, tail);
    }

    /**
     * The call {@code v} is on the wire -- a {@code func} node -- or {@code null}: an application that is no method-call
     * property and no island, a quote carrier's original call, {@code ^X(...)} as the {@code new} (or a special class's
     * call) the emitter writes.
     */
    private static @com.legend.base.Nullable AppliedFunction wireCall(ValueSpecification v) {
        return switch (v) {
            case AppliedFunction af when af.propertyCall() || af.island() -> null;
            case AppliedFunction af when calls(af, AppliedFunction.NEW) && !af.parameters().isEmpty()
                    && af.parameters().get(af.parameters().size() - 1) instanceof NewInstance ni -> newCall(ni);
            case AppliedFunction af -> af;
            case NewInstance ni -> newCall(ni);
            case QuotedTreeCall q -> wireCall(q.original());
            case QuotedGrammarCall q -> wireCall(q.original());
            default -> null;
        };
    }

    private static AppliedFunction newCall(NewInstance ni) {
        AppliedFunction special = CaretSpecials.call(ni);
        return special != null ? special : new AppliedFunction(AppliedFunction.NEW, List.of(ni));
    }

    private static boolean isInfix(AppliedFunction af) {
        String function = af.function();
        if (SPECIAL_INFIX.containsKey(function)) {
            return true;
        }
        List<ValueSpecification> ps = af.parameters();
        return "not".equals(function) && !ps.isEmpty() && isFunction(ps.get(0), "equal");
    }

    private static boolean isFunction(ValueSpecification v, String name) {
        return wireCall(v) instanceof AppliedFunction af && calls(af, name);
    }

    /**
     * Whether {@code af} is a call of {@code name} as written -- the printer's one name test: upstream's printer
     * dispatches on the wire's function names (there is no resolved declaration on the wire).
     */
    private static boolean calls(AppliedFunction af, String name) {
        return name.equals(af.function());
    }

    /** The literals upstream counts primitive ({@code PRIMITIVE_VALUES}: not a byte array, not an enum value). */
    private static boolean isPrimitiveValue(ValueSpecification v) {
        return v instanceof CString || v instanceof CBoolean || v instanceof CInteger || v instanceof CFloat
                || v instanceof CDecimal || v instanceof CDate || v instanceof CTime || v instanceof CLatestDate;
    }

    // ---------------------------------------------------------------------
    // Class instances (the embedded DSLs)
    // ---------------------------------------------------------------------

    private String printColSpec(ColSpec col) {
        return printColSpec(col, false);
    }

    /** {@code tail}: as {@link #visit(ValueSpecification, boolean)}'s -- a lone column spec's text ends with its last
     *  lambda ({@code ~a:x|$x.b}), which the grammar reads as any lambda ({@code oneColSpec: ... COLON anyLambda}). */
    private String printColSpec(ColSpec col, boolean tail) {
        return convertIdentifier(col.name())
                + (col.colType() != null ? ":" + printGenericType(col.colType()) : "")
                // a deliberate difference (docs/SEMANTICS_REGISTER.md S39): upstream's printColSpec leaves a declared
                // multiplicity out, and its print then reads back without it; lite writes it, as the grammar spells it
                + (col.colType() != null && col.colTypeMult() != null ? "[" + multiplicity(col.colTypeMult()) + "]" : "")
                + (col.function1() != null
                        ? ":" + (pretty() ? " " : "") + visit(col.function1(), tail && col.function2() == null) : "")
                + (col.function2() != null ? ":" + visit(col.function2(), tail) : "");
    }

    private String printColSpecArray(ColSpecArray array) {
        StringBuilder b = new StringBuilder("~[");
        if (pretty()) {
            b.append(returnChar()).append(indent(tabs(1))).append(" ");
        }
        List<String> specs = new ArrayList<>();
        for (ColSpec c : array.colSpecs()) {
            specs.add(printColSpec(c));
        }
        b.append(String.join("," + (pretty() ? returnChar() + " " + indent(tabs(1)) : " "), specs));
        if (pretty()) {
            b.append(returnChar()).append(indent(0)).append(" ");
        }
        return b.append("]").toString();
    }

    /** {@code #/Root/a/b#}: a segment prints its arguments only when it has more than one (upstream's printer). */
    private String path(PathLiteral path) {
        List<String> elements = new ArrayList<>();
        for (PathLiteral.Segment s : path.segments()) {
            List<String> args = new ArrayList<>();
            for (PathLiteral.PathArg a : s.args()) {
                args.add(pathArg(a));
            }
            elements.add(s.name() + (args.size() > 1 ? "(" + String.join(", ", args) + ")" : ""));
        }
        String name = path.alias();
        return "#/" + convertPath(path.startType())
                + (elements.isEmpty() ? "" : "/" + String.join("/", elements))
                + (name == null || name.isEmpty() ? "" : "!" + name) + "#";
    }

    /** A path argument as the literal the wire writes for it, for a path printed outside a value specification. */
    static String pathArgument(PathLiteral.PathArg a) {
        return new PureComposer(Style.STANDARD, "", false).pathArg(a);
    }

    /** A path argument as the literal the wire writes for it. */
    private String pathArg(PathLiteral.PathArg a) {
        return switch (a) {
            case PathLiteral.PathArg.Latest l -> "%latest";
            case PathLiteral.PathArg.DateArg d -> percent(d.value());
            case PathLiteral.PathArg.EnumArg e -> e.fullPath() + "." + convertIdentifier(e.value());
            case PathLiteral.PathArg.IntArg i -> String.valueOf(i.value());
            case PathLiteral.PathArg.StrArg s -> convertString(s.value(), true);
            case PathLiteral.PathArg.CollectionArg c -> {
                List<String> out = new ArrayList<>();
                for (PathLiteral.PathArg e : c.elements()) {
                    out.add(pathArg(e));
                }
                yield renderStrings(out, c.elements().size() == 1 && !(c.elements().get(0)
                        instanceof PathLiteral.PathArg.EnumArg) && !(c.elements().get(0)
                        instanceof PathLiteral.PathArg.CollectionArg));
            }
        };
    }

    private String rootGraphFetchTree(GraphFetchLiteral root) {
        PureComposer inner = indented(tabs(1));
        String subTrees = joinNodes(inner, root.subTrees());
        String subTypeTrees = joinSubTypes(inner, root.subTypeTrees());
        String cr = pretty() ? returnChar() : "";
        return "#{" + cr
                + indent(tabs(1)) + root.className() + "{" + cr
                + subTrees + (!subTrees.isEmpty() && !subTypeTrees.isEmpty() ? (pretty() ? "," + returnChar() : ",") : "")
                + subTypeTrees + cr
                + indent(tabs(1)) + "}" + cr
                + indentation + "}#";
    }

    private String joinNodes(PureComposer inner, List<GraphFetchLiteral.Node> nodes) {
        List<String> out = new ArrayList<>();
        for (GraphFetchLiteral.Node n : nodes) {
            out.add(inner.propertyGraphFetchTree(n));
        }
        return String.join("," + (pretty() ? returnChar() : ""), out);
    }

    private String joinSubTypes(PureComposer inner, List<GraphFetchLiteral.SubTypeNode> nodes) {
        List<String> out = new ArrayList<>();
        for (GraphFetchLiteral.SubTypeNode n : nodes) {
            out.add(inner.indent(tabs(1)) + "->subType(@" + n.subTypeClass() + ")" + inner.subTrees(n.subTrees()));
        }
        return String.join("," + (pretty() ? returnChar() : ""), out);
    }

    private String subTrees(List<GraphFetchLiteral.Node> trees) {
        if (trees.isEmpty()) {
            return "";
        }
        String cr = pretty() ? returnChar() : "";
        return "{" + cr + joinNodes(indented(tabs(1)), trees) + cr + indent(tabs(1)) + "}";
    }

    private String propertyGraphFetchTree(GraphFetchLiteral.Node tree) {
        return indent(tabs(1))
                + (tree.alias() != null ? convertString(tree.alias(), false) + ":" : "")
                + tree.property()
                + (tree.parameters().isEmpty() ? "" : "(" + joinVisit(tree.parameters(), ", ") + ")")
                + (tree.subType() != null ? "->subType(@" + tree.subType() + ")" : "")
                + subTrees(tree.subTrees());
    }

    // ---------------------------------------------------------------------
    // Types
    // ---------------------------------------------------------------------

    private String printGenericType(TypeExpression type) {
        return switch (type) {
            case TypeExpression.NameRef n -> "Result".equals(n.name()) ? BARE_RESULT : n.name();
            case TypeExpression.Generic g -> {
                if ("Result".equals(g.name()) && g.arguments().isEmpty() && g.multiplicityArguments().isEmpty()
                        && g.typeVariableValues().isEmpty()) {
                    yield BARE_RESULT;
                }
                StringBuilder b = new StringBuilder(g.name());
                if (!g.arguments().isEmpty() || !g.multiplicityArguments().isEmpty()) {
                    List<String> ts = new ArrayList<>();
                    for (TypeExpression t : g.arguments()) {
                        ts.add(printGenericType(t));
                    }
                    List<String> ms = new ArrayList<>();
                    for (String m : g.multiplicityArguments()) {
                        ms.add(multiplicity(ProtocolEmitter.parseMultArg(m, g.name())));
                    }
                    b.append("<").append(String.join(", ", ts))
                            .append(ms.isEmpty() ? "" : "|" + String.join(",", ms)).append(">");
                }
                if (!g.typeVariableValues().isEmpty()) {
                    b.append("(").append(joinVisit(g.typeVariableValues(), ",")).append(")");
                }
                yield b.toString();
            }
            case TypeExpression.RelationType rt -> {
                List<String> cols = new ArrayList<>();
                for (TypeExpression.Column c : rt.columns()) {
                    // an undeclared multiplicity is 0..1 on the wire, which prints none
                    boolean zeroOne = !c.multiplicityDeclared() || isZeroOne(c.multiplicity());
                    cols.add(convertIdentifier(c.name()) + ":" + printGenericType(c.type())
                            + (zeroOne ? "" : "[" + multiplicity(c.multiplicity()) + "]"));
                }
                yield "(" + String.join(", ", cols) + ")";
            }
            default -> throw refused("no composer rule for a type " + type);
        };
    }

    /** {@code @Relation<(a:T)>} or the bare {@code @(a:T)}: the relation type, wrapped in its spelled name. */
    private String relationShape(TypeAnnotation.RelationShape rs) {
        List<String> cols = new ArrayList<>();
        for (TypeAnnotation.RelationShape.Column c : rs.columns()) {
            if (!(c.type() instanceof TypeAnnotation.Named nt) || c.name() == null) {
                throw refused("a wildcard @Relation column: upstream writes none");
            }
            boolean zeroOne = c.multiplicity() == null || isZeroOne(c.multiplicity());
            cols.add(convertIdentifier(c.name()) + ":" + printGenericType(nt.type())
                    + (zeroOne ? "" : "[" + multiplicity(c.multiplicity()) + "]"));
        }
        String relation = "(" + String.join(", ", cols) + ")";
        return rs.spelledName() == null ? relation : rs.spelledName() + "<" + relation + ">";
    }

    private static boolean isZeroOne(Multiplicity m) {
        return m instanceof Multiplicity.Concrete c && c.lowerBound() == 0 && c.upperBound() != null
                && c.upperBound() == 1;
    }

    /** {@code HelperDomainGrammarComposer.renderMultiplicity}; absent means {@code [*]}. */
    static String multiplicity(@com.legend.base.Nullable Multiplicity m) {
        if (!(m instanceof Multiplicity.Concrete c)) {
            if (m == null) {
                return "*";
            }
            throw refused("no composer rule for a multiplicity " + m);
        }
        int lower = c.lowerBound();
        Integer upper = c.upperBound();
        if (lower == 0 && upper == null) {
            return "*";
        }
        return upper != null && lower == upper ? String.valueOf(lower)
                : lower + ".." + (upper == null ? "*" : String.valueOf(upper));
    }

    // ---------------------------------------------------------------------
    // Literals
    // ---------------------------------------------------------------------

    private static String written(@com.legend.base.Nullable String written, String what) {
        if (written == null) {
            throw refused("a " + what + " literal without its written form: synthesized, not printable");
        }
        return written;
    }

    /** {@code generateValidDateValueContainingPercent}. */
    private static String percent(String date) {
        return date.indexOf('%') != -1 ? date : "%" + date;
    }

    /** A date literal written outside a value specification (a milestoning infinity date): as {@link CDate} prints. */
    static String dateLiteral(String written) {
        return percent(written);
    }

    private static String byteArrayText(String base64) {
        // the wire carries the bytes base64-encoded (Jackson's byte[]); upstream prints them as UTF-8 text
        byte[] bytes = java.util.Base64.getDecoder().decode(base64);
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

    private String renderCollection(List<ValueSpecification> values, Function<ValueSpecification, String> render) {
        if (values.isEmpty()) {
            return "[]";
        }
        boolean newLine = pretty() && (values.size() != 1
                || (!isPrimitiveValue(values.get(0)) && !(values.get(0) instanceof Variable)));
        List<String> out = new ArrayList<>();
        for (ValueSpecification v : values) {
            out.add(render.apply(v));
        }
        return wrapCollection(out, newLine);
    }

    /** A collection of already printed items: {@code onlyPrimitive} -- one primitive item -- stays on its line. */
    private String renderStrings(List<String> items, boolean onlyPrimitive) {
        if (items.isEmpty()) {
            return "[]";
        }
        return wrapCollection(items, pretty() && (items.size() != 1 || !onlyPrimitive));
    }

    private String wrapCollection(List<String> out, boolean newLine) {
        PureComposer inner = indented(tabs(1));
        return "[" + (newLine ? returnChar() + inner.indentation : "")
                + String.join("," + (pretty() ? returnChar() + inner.indentation : " "), out)
                + (newLine ? returnChar() + indentation : "") + "]";
    }

    private String joinVisit(List<? extends ValueSpecification> nodes, String sep) {
        List<String> out = new ArrayList<>();
        for (ValueSpecification n : nodes) {
            out.add(visit(n));
        }
        return String.join(sep, out);
    }

    private static IllegalArgumentException refused(String why) {
        return new IllegalArgumentException("lambda JSON: " + why);
    }
}
