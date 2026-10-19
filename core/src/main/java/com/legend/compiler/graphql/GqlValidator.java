// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.graphql;

import com.legend.protocol.graphql.GqlSchema;
import com.legend.protocol.graphql.GqlSchema.Composite;
import com.legend.protocol.graphql.GqlSchema.EnumType;
import com.legend.protocol.graphql.GqlSchema.InputObjectType;
import com.legend.protocol.graphql.GqlSchema.ScalarType;
import com.legend.protocol.graphql.GqlSchema.Schema;
import com.legend.protocol.graphql.GqlSchema.TypeDef;
import com.legend.protocol.spec.Gql;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * THE VALIDATOR (docs/GRAPHQL_STUDY_2026_10_18.md §6.3): a document against a generated schema. The
 * specification's validation rules first (one operation chosen, fragments defined once and used, variables
 * declared and used, fields on their types, arguments typed, spreads possible, leaves and composites selected as
 * their kind requires, response keys that merge), then lite's own: the exact-type fragment trap (G10), the group
 * rules (G7), the limits (G17). Every problem is reported, not the first; a problem carries the selection path.
 * Valid documents get their typed result shape (§8.4). Nothing here executes or lowers.
 */
public final class GqlValidator {

    private GqlValidator() {
    }

    /** The ceilings a data space declares (G17). */
    public record Limits(int maxDepth, int maxListFieldsPerLevel) {
        public static final Limits DEFAULT = new Limits(8, 25);
    }

    /** One problem: where in the document (the selection path, response keys dotted) and what. */
    public record Problem(String path, String message) {
        @Override
        public String toString() {
            return path.isEmpty() ? message : path + ": " + message;
        }
    }

    /** The verdict: every error, every warning, and the typed result shape of a valid document. */
    public record Validation(List<Problem> errors, List<Problem> warnings, @com.legend.base.Nullable String shape,
            @com.legend.base.Nullable Gql.Operation operation) {
        public Validation {
            errors = List.copyOf(errors);
            warnings = List.copyOf(warnings);
        }

        public boolean valid() {
            return errors.isEmpty();
        }
    }

    /** The mutable state of one validation, explicit (the code-shape guard). */
    static final class Frame {
        final Schema schema;
        final Limits limits;
        final List<Problem> errors = new ArrayList<>();
        final List<Problem> warnings = new ArrayList<>();
        final Map<String, Gql.Fragment> fragments = new LinkedHashMap<>();
        final Map<String, Gql.VariableDef> variables = new LinkedHashMap<>();
        final Set<String> variablesUsed = new LinkedHashSet<>();
        final Set<String> fragmentsUsed = new LinkedHashSet<>();

        Frame(Schema schema, Limits limits) {
            this.schema = schema;
            this.limits = limits;
        }

        void error(String path, String message) {
            errors.add(new Problem(path, message));
        }

        void warn(String path, String message) {
            warnings.add(new Problem(path, message));
        }
    }

    public static Validation validate(Gql.Document document, Schema schema, Limits limits,
            @com.legend.base.Nullable String operationName) {
        Frame f = new Frame(schema, limits);
        List<Gql.Operation> operations = new ArrayList<>();
        for (Gql.Definition d : document.definitions()) {
            switch (d) {
                case Gql.Operation op -> operations.add(op);
                case Gql.Fragment fr -> {
                    if (f.fragments.put(fr.name(), fr) != null) {
                        f.error("", "fragment " + fr.name() + " is defined twice");
                    }
                }
                default -> f.error("", "a type-system definition in a query document: a document sent to the endpoint"
                        + " holds operations and fragments only");
            }
        }
        Gql.Operation op = chooseOperation(f, operations, operationName);
        if (op == null) {
            return new Validation(f.errors, f.warnings, null, null);
        }
        checkFragments(f);
        declareVariables(f, op);
        String shape = new SelectionWalker(f).walkOperation(op);
        for (String v : f.variables.keySet()) {
            if (!f.variablesUsed.contains(v)) {
                f.error("", "variable $" + v + " is declared and never used");
            }
        }
        for (String name : f.fragments.keySet()) {
            if (!f.fragmentsUsed.contains(name)) {
                f.error("", "fragment " + name + " is defined and never spread");
            }
        }
        return new Validation(f.errors, f.warnings, f.errors.isEmpty() ? shape : null, op);
    }

    private static @com.legend.base.Nullable Gql.Operation chooseOperation(Frame f, List<Gql.Operation> operations,
            @com.legend.base.Nullable String operationName) {
        if (operations.isEmpty()) {
            f.error("", "no operation: a document is a query, written `{ ... }` or `query Name { ... }`");
            return null;
        }
        Gql.Operation chosen = null;
        if (operationName != null) {
            for (Gql.Operation op : operations) {
                if (operationName.equals(op.name())) {
                    chosen = op;
                }
            }
            if (chosen == null) {
                f.error("", "no operation named " + operationName);
                return null;
            }
        } else if (operations.size() == 1) {
            chosen = operations.get(0);
        } else {
            f.error("", "several operations and no operationName: name the one to run");
            return null;
        }
        if (operations.size() > 1) {
            for (Gql.Operation op : operations) {
                if (op.name() == null) {
                    f.error("", "an anonymous operation beside other operations");
                }
            }
        }
        String type = chosen.type();
        if (type != null && !"query".equals(type)) {
            f.error("", "a " + type + " operation: this endpoint reads; writes are not this line's"
                    + " (docs/GRAPHQL_STUDY_2026_10_18.md §2.3)");
            return null;
        }
        return chosen;
    }

    private static void checkFragments(Frame f) {
        for (Gql.Fragment fr : f.fragments.values()) {
            TypeDef t = f.schema.find(fr.typeCondition());
            if (!(t instanceof Composite)) {
                f.error("fragment " + fr.name(), "fragment " + fr.name() + " is on " + fr.typeCondition()
                        + ", which is " + (t == null ? "no type of the schema" : "not an object or interface type"));
            }
        }
    }

    private static void declareVariables(Frame f, Gql.Operation op) {
        for (Gql.VariableDef v : op.variables()) {
            String name = v.dollarName().startsWith("$") ? v.dollarName().substring(1) : v.dollarName();
            if (f.variables.put(name, v) != null) {
                f.error("", "variable $" + name + " is declared twice");
            }
            GqlSchema.TypeRef type = GqlSchema.TypeRef.of(v.type());
            String base = GqlSchema.baseName(type);
            TypeDef t = f.schema.find(base);
            boolean input = t instanceof InputObjectType || t instanceof EnumType || t instanceof ScalarType
                    || ValueCoercion.isBuiltinScalar(base);
            if (!input) {
                f.error("", "variable $" + name + " is typed " + GqlSchema.spell(type) + ", which is "
                        + (t == null ? "no type of the schema" : "not an input type"));
            } else if (v.defaultValue() != null) {
                ValueCoercion.coerce(f, v.defaultValue(), type, "$" + name + " default");
            }
        }
    }
}
