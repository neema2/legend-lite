package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.parser.element.Function;
import com.legend.sql.SqlExpr;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Scalar native dispatch, keyed by the RESOLVED overload's identity &mdash; the
 * {@link Pure} catalog constant Phase G chose ({@code TypedFunction.definition()}).
 * Dispatch never touches name strings; registration is catalog-driven: a
 * {@code family} maps EVERY overload of a Pure function to one semantic name
 * (all {@code lessThan} overloads mean {@code <}), and specific-overload
 * overrides land where the overload IS the decision &mdash; {@code plus} on
 * Strings is {@code ||}, on Numbers {@code +}; the type checker already chose.
 * An unregistered overload is a loud error naming the signature.
 */
final class Scalars {

    /** A rule receives the ALREADY-LOWERED argument expressions. */
    interface Rule extends BiFunction<TypedNativeCall, List<SqlExpr>, SqlExpr> {
    }

    private static final Map<Function, Rule> RULES = new IdentityHashMap<>();

    private Scalars() {
    }

    /** Register every catalog overload of {@code pureName} under one semantic name. */
    private static void family(String semanticName, String pureName) {
        List<? extends Function> overloads = Pure.nativeFunctionsAt(pureName);
        if (overloads.isEmpty()) {
            throw new IllegalStateException("no catalog overloads for '" + pureName + "'");
        }
        for (Function f : overloads) {
            RULES.put(f, (n, args) -> new SqlExpr.Call(semanticName, args));
        }
    }

    static {
        family("equal", "equal");
        family("notEqual", "notEqual");
        family("less", "lessThan");
        family("lessEqual", "lessThanEqual");
        family("greater", "greaterThan");
        family("greaterEqual", "greaterThanEqual");
        family("and", "and");
        family("or", "or");
        family("not", "not");
        family("plus", "plus");
        family("minus", "minus");
        family("times", "times");
        family("divide", "divide");
        family("mod", "mod");
        family("rem", "rem");
        family("abs", "abs");
        family("isNull", "isEmpty");
        family("isNotNull", "isNotEmpty");
        family("length", "length");
        family("upper", "toUpper");
        family("lower", "toLower");

        // Overload-specific overrides — the resolved signature IS the decision.
        RULES.put(Pure.PLUS__STRING_1__STRING_1, (n, args) -> new SqlExpr.Call("concat", args));
        RULES.put(Pure.IN__ANY_1__ANY_MANY, (n, args) -> {
            List<SqlExpr> flat = new ArrayList<>();
            flat.add(args.get(0));
            if (args.get(1) instanceof SqlExpr.ArrayLit arr) {
                flat.addAll(arr.elements());
            } else {
                flat.add(args.get(1));
            }
            return new SqlExpr.Call("in", flat);
        });
    }

    /** The lowering for {@code call}'s resolved overload; loud error when unregistered. */
    static SqlExpr lower(TypedNativeCall call, List<SqlExpr> loweredArgs) {
        Rule rule = RULES.get(call.callee().definition());
        if (rule == null) {
            throw new IllegalStateException("no scalar lowering registered for resolved overload '"
                    + call.callee().qualifiedName() + "' with " + call.callee().parameters().size()
                    + " parameter(s)");
        }
        return rule.apply(call, loweredArgs);
    }

    /** Literal cell of a TDS row → typed SQL literal, by the column's Pure type. */
    static SqlExpr tdsCell(String cell, Type type) {
        if (cell == null || cell.isEmpty()) {
            return new SqlExpr.NullLit();
        }
        if (type == Type.Primitive.INTEGER) {
            return new SqlExpr.IntLit(Long.parseLong(cell));
        }
        if (type == Type.Primitive.FLOAT || type == Type.Primitive.NUMBER
                || type == Type.Primitive.DECIMAL) {
            return new SqlExpr.DecimalLit(new java.math.BigDecimal(cell));
        }
        if (type == Type.Primitive.BOOLEAN) {
            return new SqlExpr.BoolLit(Boolean.parseBoolean(cell));
        }
        if (type == Type.Primitive.STRICT_DATE) {
            return new SqlExpr.DateLit(cell.startsWith("%") ? cell.substring(1) : cell);
        }
        if (type == Type.Primitive.DATE_TIME || type == Type.Primitive.DATE) {
            return new SqlExpr.TimestampLit(cell.startsWith("%") ? cell.substring(1) : cell);
        }
        return new SqlExpr.StringLit(cell);
    }
}
