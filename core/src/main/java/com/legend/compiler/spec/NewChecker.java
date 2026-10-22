package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.Property;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.NewInstance;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Instance construction {@code ^Class(prop=value, …)} (engine {@code NewChecker}).
 * Each named property must exist on the class; its value is type-checked and
 * validated against the property's declared type. The result is {@code Class[1]}.
 */
final class NewChecker {

    private NewChecker() {
    }

    /**
     * Copy-with-update {@code ^$existing(prop=value, …)}: the class is the
     * VARIABLE's static type; each override validates against its declared
     * property exactly like construction.
     */
    static TypedSpec checkCopy(Typer t, com.legend.protocol.spec.ValueSpecification receiver,
                               NewInstance ni, Env env) {
        TypedSpec source = t.synth(receiver, env);
        Type st = source.info().type();
        String classFqn = switch (st) {
            case Type.ClassType ct -> ct.fqn();
            case Type.GenericType g -> g.rawFqn();
            default -> throw new TypeInferenceException(
                    "^$var(...) copies a CLASS instance; the variable is " + st.typeName());
        };
        Map<String, TypedSpec> overrides = new LinkedHashMap<>();
        ni.properties().forEach(kb -> {
            String name = kb.key();
            com.legend.protocol.spec.KeyExpression key = kb.expression();
            Property prop = pathProperty(t, classFqn, name);
            // F17: '+=' APPENDS to the receiver's property (real pure's
            // copy-add semantics) — desugared HERE to
            // concatenate(receiver.prop, value), so downstream the
            // override is the ordinary construction shape. The parsed
            // isAdd flag was previously dropped: '+=' silently behaved
            // as '=' (the fold family's wrong accumulators).
            com.legend.protocol.spec.ValueSpecification overrideExpr = key.value();
            if (key.isAdd()) {
                var propRead = new com.legend.protocol.spec.AppliedProperty(receiver, name);
                overrideExpr = Calls.platform("concatenate", java.util.List.of(propRead, key.value()));
            }
            TypedSpec value = t.synth(overrideExpr, env);
            t.kernel().unify(prop.type(), value.info().type(), new Bindings());
            // the SAME multiplicity subsumption as construction (real pure
            // validates copy through the instantiation validator too — the
            // audit found this half missing)
            if (prop.multiplicity() instanceof Multiplicity.Bounded declared
                    && value.info().multiplicity() instanceof Multiplicity.Bounded actual) {
                boolean lowOk = actual.lower() >= declared.lower();
                boolean highOk = declared.upper() == null
                        || (actual.upper() != null && actual.upper() <= declared.upper());
                if (!lowOk || !highOk) {
                    throw new TypeInferenceException("property '" + name + "' of '"
                            + classFqn + "' declares multiplicity " + declared
                            + " but the value has " + actual);
                }
            }
            overrides.put(name, value);
        });
        return new com.legend.compiler.spec.typed.TypedCopyInstance(source, classFqn, overrides,
                new ExprType(st, Multiplicity.Bounded.ONE), com.legend.compiler.spec.typed.Evaluation.of(ni));
    }

    /** Every STORED required property of the class hierarchy must be
     * supplied — real pure's NewValidator:132 compile rejection.
     * Association-injected properties are structurally absent from
     * {@code properties()} (resolved from the association index —
     * pure's own Association skip); a multiplicity admitting 0 is
     * optional; the AllVersions ctor key supplies its base; the
     * generated-source escape ({@link Env#lenientNew}) exempts
     * mapping-synth ctors exactly as pure exempts generated sources. */
    private static void requireAllRequired(Typer t, NewInstance ni) {
        java.util.Set<String> supplied = new java.util.HashSet<>();
        ni.properties().forEach(kb -> {
            String k = kb.key();
            supplied.add(k.endsWith("AllVersions")
                    ? k.substring(0, k.length() - "AllVersions".length())
                    : k);
        });
        java.util.ArrayDeque<String> walk = new java.util.ArrayDeque<>();
        java.util.Set<String> seenClasses = new java.util.HashSet<>();
        walk.add(ni.className());
        while (!walk.isEmpty()) {
            String cf = walk.poll();
            if (!seenClasses.add(cf)) {
                continue;
            }
            var tc = t.model().findClass(cf);
            if (tc.isEmpty()) {
                continue;
            }
            for (Property p : tc.get().properties()) {
                if (p instanceof Property.Stored s
                        && !supplied.contains(s.name())
                        && !s.hasDefault()
                        && s.multiplicity() instanceof Multiplicity.Bounded b
                        && b.lower() >= 1) {
                    throw new TypeInferenceException(
                            "Missing value(s) for required property '"
                            + s.name() + "' which has a multiplicity of "
                            + b.text() + " for type " + ni.className());
                }
            }
            walk.addAll(tc.get().superClassFqns());
        }
    }

    static TypedSpec check(Typer t, NewInstance ni, Env env) {
        TypedSpec json = JsonChecker.newInstance(t, ni, env);
        if (json != null) {
            return json;
        }
        if (t.model().findClass(ni.className()).isEmpty()) {
            throw new TypeInferenceException("unknown class '" + ni.className() + "' in ^" + ni.className() + "(…)");
        }
        Map<String, TypedSpec> properties = new LinkedHashMap<>();
        ni.properties().forEach(kb -> {
            String name = kb.key();
            com.legend.protocol.spec.KeyExpression key = kb.expression();
            if (key.isLocal()) {
                // mapping-LOCAL property (+id: Integer[1]: COL): owned by
                // the MAPPING, not the class (engine local-property
                // semantics) — no class property to validate against; the
                // value's own type stands (the normalizer emitted both)
                properties.put(name, t.synthRecordField(key.value(), env));
                return;
            }
            Property prop = t.model().findProperty(ni.className(), name)
                    // the SYNTHETIC milestoned sweep KEY (<base>AllVersions
                    // on a temporal-class end — real pure GENERATES it;
                    // same base-name rule as GraphFetchChecker): the ctor
                    // key validates against the BASE property
                    .or(() -> name.endsWith("AllVersions")
                            ? t.model().findProperty(ni.className(),
                                    name.substring(0, name.length()
                                            - "AllVersions".length()))
                            : java.util.Optional.empty())
                    .orElseThrow(() ->
                    new TypeInferenceException("class '" + ni.className() + "' has no property '" + name + "'"));
            TypedSpec value = t.synthRecordField(key.value(), env);
            try {
                t.kernel().unify(prop.type(), value.info().type(), new Bindings());   // value must conform
            } catch (TypeInferenceException e) {
                String v = String.valueOf(key.value());
                throw new TypeInferenceException("property '" + name + "' of '"
                        + ni.className() + "': " + e.getMessage()
                        + " (value: " + (v.length() > 400 ? v.substring(0, 400) : v)
                        + ")");
            }
            // Multiplicity conformance: FULL subsumption, exactly real
            // pure's NewValidator (Multiplicity.subsumes) — the declared
            // range must contain the value's range, so even [0..1] into a
            // [1] property is a static error and the writer must spell
            // ->toOne(). Synthesized mapping bodies conform by EMISSION:
            // MappingNormalizer wraps store reads bound to [1] properties
            // in toOne(...) rather than this checker weakening — the
            // hand-written surface stays pure-compatible. EXCEPTION:
            // navigate() values — statically T[*] by design; conformance
            // is the mapping resolver's question.
            if (!(value instanceof com.legend.compiler.spec.typed.TypedNavigate)
                    && prop.multiplicity() instanceof Multiplicity.Bounded declared
                    && value.info().multiplicity() instanceof Multiplicity.Bounded actual) {
                boolean lowOk = actual.lower() >= declared.lower();
                boolean highOk = declared.upper() == null
                        || (actual.upper() != null && actual.upper() <= declared.upper());
                if (!lowOk || !highOk) {
                    throw new TypeInferenceException("property '" + name + "' of '"
                            + ni.className() + "' declares multiplicity " + declared
                            + " but the value has " + actual);
                }
            }
            properties.put(name, value);
        });
        // MISSING-REQUIRED validation (Part-1 fix, 2026-08-26 — real
        // pure's NewValidator:132 rejects at COMPILE time; our lowering
        // was emitting a NULL slot that flowed to a silent null value
        // at access). STORED properties only, hierarchy-walked;
        // association-injected properties are structurally absent from
        // properties() (resolved from the association index at lookup
        // — pure's own Association skip); a multiplicity admitting 0
        // is optional. The AllVersions ctor key supplies its base.
        if (!env.lenientNew()) {
            requireAllRequired(t, ni);
        }
        // DECLARED DEFAULTS (real pure's constructor: an unspelled property
        // with a declared default value takes it — FunctionCall.distinct =
        // false): synthesized here so the instance literal SPELLS it and
        // every downstream reader (folds, lowering, the verdict) sees one shape
        java.util.ArrayDeque<String> dwalk = new java.util.ArrayDeque<>();
        java.util.Set<String> dseen = new java.util.HashSet<>();
        dwalk.add(ni.className());
        while (!dwalk.isEmpty()) {
            String cf = dwalk.poll();
            if (!dseen.add(cf)) {
                continue;
            }
            var tc = t.model().findClass(cf);
            if (tc.isEmpty()) {
                continue;
            }
            for (Property p : tc.get().properties()) {
                if (p instanceof Property.Stored s && s.defaultValue() != null
                        && !properties.containsKey(s.name())) {
                    properties.put(s.name(), t.synth(s.defaultValue(), env));
                }
            }
            dwalk.addAll(tc.get().superClassFqns());
        }
        // PARAMETERIZED platform classes: ^Pair(first=..., second=...) types
        // as Pair<t(first), t(second)> — its generic params ARE its property
        // types; a raw ClassType broke zip-over-pair-literals at the
        // lowering boundary.
        if (ni.className().equals(com.legend.compiler.element.type.PlatformTypes.PAIR)) {
            TypedSpec first = properties.get("first");
            TypedSpec second = properties.get("second");
            if (first != null && second != null) {
                return new TypedNewInstance(ni.className(), properties,
                        new ExprType(new Type.GenericType(ni.className(),
                                java.util.List.of(first.info().type(), second.info().type())),
                                Multiplicity.Bounded.ONE), com.legend.compiler.spec.typed.Evaluation.of(ni));
            }
        }
        // ^List(values=...) types as List<t(values)> — same principle: the
        // generic param IS the values collection's element type.
        if (ni.className().equals(com.legend.compiler.element.type.PlatformTypes.LIST)) {
            TypedSpec values = properties.get("values");
            // an EXPLICIT type argument (^List<PropertyPathNode>() — the
            // empty typed list the lineage scans seed) is the element type;
            // otherwise the values collection's
            Type elem = !ni.typeArguments().isEmpty()
                    ? t.namedType(ni.typeArguments().get(0))
                    : values != null ? values.info().type()
                    : new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.ANY);
            return new TypedNewInstance(ni.className(), properties,
                    new ExprType(new Type.GenericType(ni.className(), java.util.List.of(elem)),
                            Multiplicity.Bounded.ONE), com.legend.compiler.spec.typed.Evaluation.of(ni));
        }
        // EXPLICIT generic instantiation (^Result<SelectSQLQuery>(...)):
        // the spelled type arguments RIDE the instance type — dropping
        // them made the value a raw ClassType and failed conformance
        // against generic-typed properties (the post-processor hooks)
        if (!ni.typeArguments().isEmpty()) {
            java.util.List<Type> args = ni.typeArguments().stream()
                    .map(t::namedType).toList();
            return new TypedNewInstance(ni.className(), properties,
                    new ExprType(new Type.GenericType(ni.className(), args),
                            Multiplicity.Bounded.ONE), com.legend.compiler.spec.typed.Evaluation.of(ni));
        }
        return new TypedNewInstance(ni.className(), properties,
                new ExprType(new Type.ClassType(ni.className()), Multiplicity.Bounded.ONE), com.legend.compiler.spec.typed.Evaluation.of(ni));
    }

    /** The property a copy key names — a DOTTED key walks the navigation
     * path (real pure's deep copy: ^$p(address.name='x') sets the address's
     * name), each hop through the previous property's class. */
    private static Property pathProperty(Typer t, String classFqn, String name) {
        String cls = classFqn;
        Property prop = null;
        for (String seg : name.split("\\.")) {
            final String at = cls;
            prop = t.model().findProperty(at, seg).orElseThrow(() ->
                    new TypeInferenceException("class '" + at + "' has no property '" + seg + "'"));
            Type pt = prop.type();
            cls = pt instanceof Type.ClassType c ? c.fqn()
                    : pt instanceof Type.GenericType g ? g.rawFqn() : cls;
        }
        return java.util.Objects.requireNonNull(prop);
    }
}
