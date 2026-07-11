package com.legend.compiler.element.type;

/**
 * EXACT identification of the platform's distinguished types — the ONE home
 * for these checks. Suffix matching ({@code endsWith("::List")}) was a bug
 * class, not a convenience: a user class that happens to share a simple name
 * (my::domain::List) must never be mistaken for the platform carrier.
 *
 * <p>The FQN constants mirror the {@code builtin/Pure} prelude declarations;
 * {@code PlatformTypesDriftTest} pins the two against each other so neither
 * can move alone. (Constants rather than {@code Pure.X.qualifiedName()}
 * references keep this package free of a dependency on the parser-level
 * prelude classes.)
 */
public final class PlatformTypes {

    private PlatformTypes() {
    }

    public static final String ANY = "meta::pure::metamodel::type::Any";
    public static final String NIL = "meta::pure::metamodel::type::Nil";
    public static final String VARIANT = "meta::pure::metamodel::variant::Variant";
    public static final String LIST = "meta::pure::functions::collection::List";
    public static final String PAIR = "meta::pure::functions::collection::Pair";
    public static final String FUNCTION = "meta::pure::metamodel::function::Function";

    /** The top type. */
    public static boolean isAny(Type t) {
        return t instanceof Type.ClassType c && c.fqn().equals(ANY);
    }

    /** The bottom type (the []-born element type). */
    public static boolean isNil(Type t) {
        return t instanceof Type.ClassType c && c.fqn().equals(NIL);
    }

    /** The semi-structured JSON carrier. */
    public static boolean isVariant(Type t) {
        return t instanceof Type.ClassType c && c.fqn().equals(VARIANT);
    }

    /** The {@code List<T>} collection carrier (parameterized form). */
    public static boolean isListCarrier(Type t) {
        return t instanceof Type.GenericType g && g.rawFqn().equals(LIST)
                && g.arguments().size() == 1;
    }

    /** The {@code Function<{…}>} value carrier (parameterized form). */
    public static boolean isFunctionCarrier(Type t) {
        return t instanceof Type.GenericType g && g.rawFqn().equals(FUNCTION);
    }
}
