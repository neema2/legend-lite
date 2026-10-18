// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.graphql;

import com.legend.compiler.graphql.GqlSchema.EnumType;
import com.legend.compiler.graphql.GqlSchema.FieldDef;
import com.legend.compiler.graphql.GqlSchema.InputObjectType;
import com.legend.compiler.graphql.GqlSchema.InputValueDef;
import com.legend.compiler.graphql.GqlSchema.InterfaceType;
import com.legend.compiler.graphql.GqlSchema.ObjectType;
import com.legend.compiler.graphql.GqlSchema.ScalarType;
import com.legend.compiler.graphql.GqlSchema.Schema;
import com.legend.compiler.graphql.GqlSchema.TypeDef;

import java.util.List;

/**
 * The generated schema as SDL (docs/GRAPHQL_STUDY_2026_10_18.md §5): what {@code describe} returns and what the
 * goldens pin. Deterministic: types in the schema's order, fields in the model's declaration order, descriptions
 * as block strings. The text is the schema language of the September 2025 edition, nothing more.
 */
public final class SdlPrinter {

    private SdlPrinter() {
    }

    public static String print(Schema schema) {
        StringBuilder b = new StringBuilder();
        for (TypeDef t : schema.types()) {
            if (b.length() > 0) {
                b.append('\n');
            }
            switch (t) {
                case ObjectType o -> composite(b, "type", o.name(), o.description(), o.implementsList(), o.fields());
                case InterfaceType i ->
                        composite(b, "interface", i.name(), i.description(), i.implementsList(), i.fields());
                case InputObjectType in -> {
                    b.append("input ").append(in.name()).append(" {\n");
                    for (InputValueDef f : in.fields()) {
                        description(b, "  ", f.description());
                        b.append("  ").append(f.name()).append(": ").append(GqlSchema.spell(f.type())).append('\n');
                    }
                    b.append("}\n");
                }
                case EnumType e -> {
                    description(b, "", e.description());
                    b.append("enum ").append(e.name()).append(" {\n");
                    for (String v : e.values()) {
                        b.append("  ").append(v).append('\n');
                    }
                    b.append("}\n");
                }
                case ScalarType s -> {
                    description(b, "", s.description());
                    b.append("scalar ").append(s.name()).append('\n');
                }
            }
        }
        return b.toString();
    }

    private static void composite(StringBuilder b, String keyword, String name,
            @com.legend.base.Nullable String description, List<String> implementsList, List<FieldDef> fields) {
        description(b, "", description);
        b.append(keyword).append(' ').append(name);
        if (!implementsList.isEmpty()) {
            b.append(" implements ").append(String.join(" & ", implementsList));
        }
        b.append(" {\n");
        for (FieldDef f : fields) {
            description(b, "  ", f.description());
            b.append("  ").append(f.name());
            if (!f.args().isEmpty()) {
                b.append('(');
                for (int i = 0; i < f.args().size(); i++) {
                    InputValueDef a = f.args().get(i);
                    if (i > 0) {
                        b.append(", ");
                    }
                    b.append(a.name()).append(": ").append(GqlSchema.spell(a.type()));
                }
                b.append(')');
            }
            b.append(": ").append(GqlSchema.spell(f.type()));
            if (f.deprecationReason() != null) {
                b.append(" @deprecated(reason: ").append(quoted(f.deprecationReason())).append(')');
            }
            b.append('\n');
        }
        b.append("}\n");
    }

    /** A block string on its own lines; nothing when there is no description. */
    private static void description(StringBuilder b, String indent, @com.legend.base.Nullable String text) {
        if (text == null || text.isEmpty()) {
            return;
        }
        b.append(indent).append("\"\"\"\n");
        for (String line : text.split("\n", -1)) {
            b.append(indent).append(line.replace("\"\"\"", "\\\"\"\"")).append('\n');
        }
        b.append(indent).append("\"\"\"\n");
    }

    private static String quoted(String s) {
        StringBuilder b = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                default -> b.append(c);
            }
        }
        return b.append('"').toString();
    }
}
