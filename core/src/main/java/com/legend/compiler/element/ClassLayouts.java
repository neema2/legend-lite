package com.legend.compiler.element;

import com.legend.compiler.element.type.Type;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The CANONICAL value layout of a class — the field list an instance VALUE
 * carries when it lowers to a SQL struct. The layout is the MODEL's, never
 * the instance's: declared stored properties in declaration order, inherited
 * first (superclass walk), with a parameterized receiver's type arguments
 * substituted into generic property types. Two instances of one class always
 * share one layout; an instance that omits an optional property still has the
 * field (NULL-valued).
 *
 * <p>Classes with no stored properties (the body-less native carriers:
 * List, SortInfo, _Window, …) have NO layout — {@link Optional#empty()} —
 * so they keep hitting the loud lowering walls rather than becoming empty
 * structs.
 */
public final class ClassLayouts {

    private ClassLayouts() {
    }

    /** The layout of a class-typed VALUE, or empty when {@code t} is not a layoutable class. */
    public static Optional<List<Type.Column>> layoutOf(ModelContext ctx, Type t) {
        return switch (t) {
            case Type.ClassType ct when !ct.fqn().endsWith("::Variant") ->
                    ctx.findClass(ct.fqn()).flatMap(c -> layout(ctx, c, Map.of()));
            case Type.GenericType g -> ctx.findClass(g.rawFqn()).flatMap(c -> {
                if (c.typeParameters().size() != g.arguments().size()) {
                    return Optional.empty();   // malformed parameterization — let the caller stay loud
                }
                Map<String, Type> args = new LinkedHashMap<>();
                for (int i = 0; i < c.typeParameters().size(); i++) {
                    args.put(c.typeParameters().get(i), g.arguments().get(i));
                }
                return layout(ctx, c, args);
            });
            default -> Optional.empty();
        };
    }

    private static Optional<List<Type.Column>> layout(ModelContext ctx, TypedClass cls,
                                                      Map<String, Type> typeArgs) {
        List<Type.Column> fields = new ArrayList<>();
        LinkedHashSet<String> seen = new LinkedHashSet<>();
        collect(ctx, cls, typeArgs, fields, seen);
        return fields.isEmpty() ? Optional.empty() : Optional.of(fields);
    }

    /** Inherited stored properties first (super walk), then locally declared ones. */
    private static void collect(ModelContext ctx, TypedClass cls, Map<String, Type> typeArgs,
                                List<Type.Column> out, LinkedHashSet<String> seen) {
        for (String superFqn : cls.superClassFqns()) {
            ctx.findClass(superFqn).ifPresent(s -> collect(ctx, s, typeArgs, out, seen));
        }
        for (Property p : cls.properties()) {
            if (p instanceof Property.Stored stored && seen.add(stored.name())) {
                out.add(new Type.Column(stored.name(),
                        substitute(stored.type(), typeArgs), stored.multiplicity()));
            }
        }
    }

    /** Positional generic instantiation: replace type-parameter occurrences with the receiver's arguments. */
    private static Type substitute(Type t, Map<String, Type> typeArgs) {
        return switch (t) {
            case Type.TypeVar v -> typeArgs.getOrDefault(v.name(), t);
            case Type.GenericType g -> new Type.GenericType(g.rawFqn(),
                    g.arguments().stream().map(a -> substitute(a, typeArgs)).toList());
            default -> t;
        };
    }
}
