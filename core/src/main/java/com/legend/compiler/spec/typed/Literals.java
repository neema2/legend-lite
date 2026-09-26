// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.PlatformTypes;
import java.util.ArrayList;
import java.util.List;

/**
 * COMPILE-TIME FOLDING OF LITERAL-ONLY EXPRESSIONS (2026-09-22). Measured over the
 * DuckDB database lane: 167 of the 168 statements the harness sent as SIDES were
 * constants — string literals joined by {@code +} or {@code joinStrings}, and two bare
 * integers — sent to the database only so it would concatenate literals. That is the
 * compiler's job. This folder evaluates exactly the shapes that census found and
 * nothing more: a literal leaf, a collection of foldables, a variable bound by a let to a
 * foldable, {@code +} over strings, {@code joinStrings} over strings. No arithmetic, no
 * functions over data. Anything else returns null and is sent as before — the folder
 * never guesses, and the side census stays honest about what remains.
 */
public final class Literals {

    private Literals() {
    }

    /** The constant value of {@code e} — a String, a Number, or a List of those — or
     * null when {@code e} is not a literal-only expression over {@code lets}. */
    public static @com.legend.base.Nullable Object fold(TypedSpec e, List<TypedSpec> lets) {
        return switch (e) {
            case TypedCString s -> s.value();
            case TypedCInteger i -> i.value();
            case TypedCollection c -> {
                List<Object> out = new ArrayList<>(c.elements().size());
                for (TypedSpec el : c.elements()) {
                    Object v = fold(el, lets);
                    if (v == null) {
                        yield null;
                    }
                    if (v instanceof List<?> nested) {
                        out.addAll(nested);   // pure collections flatten
                    } else {
                        out.add(v);
                    }
                }
                yield out;
            }
            case TypedVariable v -> {
                TypedLet let = Lets.binding(lets, v.name());
                yield let == null ? null : fold(let.value(), lets);
            }
            case TypedNativeCall n when PlatformTypes.isPlus(n.callee().qualifiedName()) -> {
                List<String> parts = strings(n.args(), lets);
                yield parts == null ? null : String.join("", parts);
            }
            case TypedNativeCall n when PlatformTypes.STRING_JOIN_STRINGS.equals(n.callee().qualifiedName()) -> {
                List<TypedSpec> a = n.args();
                List<String> parts = strings(List.of(a.get(0)), lets);
                if (parts == null) {
                    yield null;
                }
                // joinStrings(strings) / (strings, separator) / (strings, prefix, separator, suffix)
                String prefix = "", sep = "", suffix = "";
                if (a.size() == 2) {
                    if (!(fold(a.get(1), lets) instanceof String s1)) {
                        yield null;
                    }
                    sep = s1;
                } else if (a.size() == 4) {
                    if (!(fold(a.get(1), lets) instanceof String p) || !(fold(a.get(2), lets) instanceof String s2)
                            || !(fold(a.get(3), lets) instanceof String x)) {
                        yield null;
                    }
                    prefix = p;
                    sep = s2;
                    suffix = x;
                } else if (a.size() != 1) {
                    yield null;
                }
                yield prefix + String.join(sep, parts) + suffix;
            }
            // replace(string, target, replacement) over literals — the corpus normalizes
            // golden SQL text this way (3 of the 5 residual sides after the first fold)
            case TypedNativeCall n when PlatformTypes.STRING_REPLACE.equals(n.callee().qualifiedName())
                    && n.args().size() == 3 -> {
                if (fold(n.args().get(0), lets) instanceof String s
                        && fold(n.args().get(1), lets) instanceof String t
                        && fold(n.args().get(2), lets) instanceof String r) {
                    yield s.replace(t, r);
                }
                yield null;
            }
            default -> null;
        };
    }

    /** Every argument folded and flattened to strings; null when any is not a string. */
    private static @com.legend.base.Nullable List<String> strings(List<TypedSpec> args, List<TypedSpec> lets) {
        List<String> out = new ArrayList<>();
        for (TypedSpec a : args) {
            Object v = fold(a, lets);
            if (v instanceof String s) {
                out.add(s);
            } else if (v instanceof List<?> l) {
                for (Object el : l) {
                    if (!(el instanceof String s)) {
                        return null;
                    }
                    out.add(s);
                }
            } else {
                return null;
            }
        }
        return out;
    }
}
