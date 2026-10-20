// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The names a schema import gives (docs/MODEL_IMPORT_STUDY_2026_10_20.md §4, register S95): a schema's or a
 * property's name kept when Pure can hold it, else upstream's camel case; an enumeration value's sanitized as
 * upstream's; a class written in place named after its owner and its property; a list's item in the singular.
 */
final class SchemaNames {

    /** The words Pure reads as literals, which no name may be. */
    static final Set<String> LITERAL_WORDS = Set.of("true", "false");

    private SchemaNames() {
    }

    /** A schema's or property's name as Pure holds it: as it is when it can be, else camel-cased (its name in the
     *  schema kept as a tag, {@link #annotations}). */
    static String pureName(String raw, boolean type) {
        if (isName(raw) && (!type || Character.isUpperCase(raw.charAt(0)) || !Character.isLetter(raw.charAt(0)))) {
            return raw;
        }
        return camelCase(raw, type);
    }

    /** The name of a class or enumeration written in place: its owner's, then its property's in upper camel case
     *  ({@code Customer}'s {@code tax_exempt} is {@code CustomerTaxExempt}, as openapi-generator names it). */
    static String inPlaceName(String owner, String property) {
        String part = camelCase(property, true);
        // the _ a name beginning with a digit is given is not needed after the owner's
        return owner + (!owner.isEmpty() && part.startsWith("_") && part.length() > 1 ? part.substring(1) : part);
    }

    /** A name Pure holds as it is: an ASCII letter or {@code _}, then letters, digits and {@code _}; no literal's
     *  word. */
    static boolean isName(String name) {
        if (name.isEmpty() || Character.isDigit(name.charAt(0)) || LITERAL_WORDS.contains(name)) {
            return false;
        }
        for (int i = 0; i < name.length(); i++) {
            if (!alnum(name.charAt(i)) && name.charAt(i) != '_') {
                return false;
            }
        }
        return true;
    }

    /** An ASCII letter or digit. */
    private static boolean alnum(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
    }

    /** A document's file name without its extension ({@code common.yaml} is {@code common}). */
    static String withoutExtension(String file) {
        int dot = file.lastIndexOf('.');
        if (dot <= 0 || dot == file.length() - 1) {
            return file;
        }
        for (int i = dot + 1; i < file.length(); i++) {
            char c = file.charAt(i);
            if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'))) {
                return file;
            }
        }
        return file.substring(0, dot);
    }

    /** The runs of ASCII letters and digits of a text: the words a name is made of. */
    private static List<String> words(String raw) {
        List<String> out = new ArrayList<>();
        int start = -1;
        for (int i = 0; i <= raw.length(); i++) {
            boolean in = i < raw.length() && alnum(raw.charAt(i));
            if (in && start < 0) {
                start = i;
            } else if (!in && start >= 0) {
                out.add(raw.substring(start, i));
                start = -1;
            }
        }
        return out;
    }

    /** Upstream's camel case ({@code makeCamelCase}) of the parts between characters a name cannot hold. */
    static String camelCase(String raw, boolean upperFirst) {
        StringBuilder out = new StringBuilder();
        for (String part : words(raw)) {
            String rest = part.substring(1);
            boolean first = out.length() == 0;
            out.append(upperFirst || !first ? part.substring(0, 1).toUpperCase(Locale.ROOT)
                    : part.substring(0, 1).toLowerCase(Locale.ROOT))
                    .append(rest.equals(rest.toUpperCase(Locale.ROOT)) ? rest.toLowerCase(Locale.ROOT) : rest);
        }
        String held = out.length() == 0 ? "_" : out.toString();
        held = Character.isDigit(held.charAt(0)) ? "_" + held : held;
        return LITERAL_WORDS.contains(held) ? held + "_" : held;
    }

    /** An enumeration value as Pure holds it: each character a name cannot hold made {@code _}. */
    static String valueName(String value) {
        StringBuilder made = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            made.append(alnum(c) || c == '_' ? c : '_');
        }
        String held = made.toString();
        if (held.isEmpty()) {
            return "_";
        }
        held = Character.isDigit(held.charAt(0)) ? "_" + held : held;
        return LITERAL_WORDS.contains(held) ? held + "_" : held;
    }

    /** One of a list property's items, named for a class or enumeration of its own: {@code lines} a line. */
    static String singular(String plural) {
        if (plural.endsWith("ies") && plural.length() > 3) {
            return plural.substring(0, plural.length() - 3) + "y";
        }
        if (plural.endsWith("sses") || plural.endsWith("xes") || plural.endsWith("ches") || plural.endsWith("shes")) {
            return plural.substring(0, plural.length() - 2);
        }
        // status, analysis, address: a singular already
        if (plural.endsWith("s") && !plural.endsWith("ss") && !plural.endsWith("us") && !plural.endsWith("is")
                && plural.length() > 1) {
            return plural.substring(0, plural.length() - 1);
        }
        return plural;
    }
}
