// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;

/**
 * A function element's printed name: its wire {@code name} carries the signature suffix the parser
 * appended ({@code f_String_1__Integer_1_}); upstream's printer cuts it back off
 * ({@code HelperValueSpecificationGrammarComposer.getFunctionName} and {@code getFunctionSignature}).
 */
final class FunctionNames {

    private FunctionNames() {
    }

    /** {@code getFunctionName}: the package, {@code ::}, the name less its signature. */
    static String functionName(Json.Obj function) {
        String name = nameWithoutSignature(function);
        String pkg = Composing.str(function, "package");
        return pkg == null || pkg.isEmpty() ? name : pkg + "::" + name;
    }

    /** {@code getFunctionNameWithNoPackage}. */
    static String nameWithoutSignature(Json.Obj function) {
        String name = function.getString("name");
        int at = name.indexOf(signature(function));
        return at > 0 ? name.substring(0, at) : name;
    }

    /** {@code getFunctionSignature}. */
    private static String signature(Json.Obj function) {
        List<Json.Obj> parameters = Composing.objs(function, "parameters");
        List<String> ps = new ArrayList<>();
        for (Json.Obj p : parameters) {
            Json.Obj gt = Composing.objOr(p, "genericType");
            if (gt != null) {
                ps.add(simpleName(packageablePath(gt)) + "_" + multiplicitySignature(p.getObj("multiplicity")));
            }
        }
        String s = String.join("__", ps) + "__" + simpleName(packageablePath(function.getObj("returnGenericType")))
                + "_" + multiplicitySignature(function.getObj("returnMultiplicity")) + "_";
        return parameters.isEmpty() ? s : "_" + s;
    }

    /** The raw type's path: upstream casts the raw type to a packageable type, so any other refuses. */
    private static String packageablePath(Json.Obj genericType) {
        Json.Obj raw = genericType.getObj("rawType");
        if (!"packageableType".equals(Composing.type(raw))) {
            throw Composing.refused("a function signature over a raw type of _type '" + Composing.type(raw)
                    + "' (upstream's printer cannot name it)");
        }
        return raw.getString("fullPath");
    }

    /** {@code getClassSignature}: the path's last segment. */
    private static String simpleName(String path) {
        List<String> segments = PureComposer.pathSegments(path);
        return segments.isEmpty() ? "" : segments.get(segments.size() - 1);
    }

    /** {@code getMultiplicitySignature}. */
    private static String multiplicitySignature(Json.Obj m) {
        int lower = m.getIntOr("lowerBound", 0);
        int upper = m.getOr("upperBound", null) instanceof Json.Num n ? (int) n.longValue() : Integer.MAX_VALUE;
        if (lower == upper) {
            return String.valueOf(lower);
        }
        if (lower == 0 && upper == Integer.MAX_VALUE) {
            return "MANY";
        }
        return "$" + lower + "_" + (upper == Integer.MAX_VALUE ? "MANY" : String.valueOf(upper)) + "$";
    }
}
