package com.legend.resolver;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.MappingResolutionException;
import com.legend.error.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The &beta;-substitution engine: rewrites a user lambda written over CLASS
 * instances into the same expression over the mapping pipeline's ROW,
 * replacing each {@code $p.prop} with the binding table's typed expression
 * (its own row variable renamed to this instantiation's fresh row var).
 *
 * <p>THE single path-extraction funnel: H3's DemandScan reuses
 * {@link #propertyOnUserVar} so demand analysis and rewriting cannot drift.
 *
 * <p>Discipline (plan risk #1): a replacement always carries the SAME
 * {@link ExprType} as the node it replaces &mdash; binding conformance is
 * G's guarantee (the body compiled through NewChecker's strict subsumption)
 * &mdash; so every enclosing node's info stays valid and no restamping pass
 * exists. Nodes outside the H2 expression vocabulary fail LOUD naming
 * themselves (corpus-driven expansion, never silent).
 */
final class Substitution {

    /** The instantiation being substituted into: fresh row var + bindings. */
    record Target(String userVar, String freshRowVar, String classFqn,
                  String mappingFqn, String sourceRowVar,
                  Map<String, TypedSpec> bindings, Type.RelationType rowType,
                  java.util.Set<String> strippedSlots,
                  Map<String, String> slotPrefixes,
                  Map<String, AssocSub> assocs,
                  java.util.Set<String> assocEnds) {}

    /** A demanded association head: how its leaf bindings substitute. */
    record AssocSub(String prefix, String targetRowVar,
                    Map<String, TypedSpec> targetBindings, String targetClassFqn,
                    java.util.Set<String> targetSlotAliases) {}

    private final Target target;

    Substitution(Target target) {
        this.target = Objects.requireNonNull(target, "target");
    }

    /**
     * Rewrite {@code lambda}'s body over the row: parameters
     * {@code [p]} become {@code [freshRowVar]} and the info is rebuilt as
     * {@code {row[1] -> <result>}}.
     */
    TypedLambda rewriteLambda(TypedLambda lambda) {
        if (lambda.parameters().size() != 1) {
            throw new NotImplementedException("object-space lambda with "
                    + lambda.parameters().size() + " parameters is not supported yet");
        }
        List<TypedSpec> body = new ArrayList<>(lambda.body().size());
        for (TypedSpec stmt : lambda.body()) {
            body.add(rewrite(stmt));
        }
        Type.FunctionType oldFn = (Type.FunctionType) lambda.info().type();
        Type.FunctionType newFn = new Type.FunctionType(
                List.of(new Type.Param(target.rowType(), Multiplicity.Bounded.ONE)),
                oldFn.result());
        return new TypedLambda(List.of(target.freshRowVar()), body,
                new ExprType(newFn, Multiplicity.Bounded.ONE));
    }

    /**
     * THE path funnel: if {@code n} is a property access whose receiver is
     * the user's lambda variable, its property name; else {@code null}.
     * (H3 extends this to multi-hop paths; DemandScan shares it.)
     */
    static String propertyOnUserVar(TypedSpec n, String userVar) {
        java.util.List<String> p = pathOf(n, userVar);
        return p != null && p.size() == 1 ? p.get(0) : null;
    }

    /**
     * THE path funnel: the full property chain when {@code n}'s receiver
     * chain bottoms at the user's lambda variable ({@code $p.employer.legal}
     * &rArr; {@code [employer, legal]}); {@code null} otherwise. DemandScan
     * and the rewrite share this single extractor.
     */
    static java.util.List<String> pathOf(TypedSpec n, String userVar) {
        // toOne() look-through: $p.employer->toOne().legal is the idiomatic
        // spelling after an optional navigation — the coercion is
        // multiplicity-only and transparent to the path (audit R3).
        if (n instanceof TypedNativeCall c && c.args().size() == 1
                && c.callee().qualifiedName().endsWith("toOne")) {
            return pathOf(c.args().get(0), userVar);
        }
        if (!(n instanceof TypedPropertyAccess pa)) {
            return null;
        }
        if (pa.source() instanceof TypedVariable v && v.name().equals(userVar)) {
            return java.util.List.of(pa.property());
        }
        java.util.List<String> inner = pathOf(pa.source(), userVar);
        if (inner == null) {
            return null;
        }
        java.util.List<String> out = new ArrayList<>(inner);
        out.add(pa.property());
        return out;
    }

    private TypedSpec rewrite(TypedSpec n) {
        java.util.List<String> path = pathOf(n, target.userVar());
        if (path != null && path.size() == 2) {
            return rewritePath(path.get(0), path.get(1), n);
        }
        if (path != null && path.size() > 2) {
            throw new NotImplementedException("multi-hop navigation "
                    + String.join(".", path) + " is not supported yet");
        }
        String prop = propertyOnUserVar(n, target.userVar());
        if (prop != null) {
            TypedSpec binding = target.bindings().get(prop);
            if (binding == null) {
                if (target.assocEnds().contains(prop)) {
                    throw new NotImplementedException("association property '$"
                            + target.userVar() + "." + prop + "' used other than"
                            + " as a navigation head (class-typed value /"
                            + " isEmpty / whole-instance) is not supported yet");
                }
                throw new MappingResolutionException("property '" + prop
                        + "' of class '" + target.classFqn()
                        + "' is not mapped in mapping '" + target.mappingFqn() + "'",
                        target.classFqn());
            }
            return renameRowVar(binding);
        }
        return switch (n) {
            case TypedVariable v when v.name().equals(target.userVar()) ->
                    throw new NotImplementedException(
                            "object-space use of the instance variable '$" + v.name()
                                    + "' other than property access is not supported yet");
            case TypedVariable v -> v;
            case TypedPropertyAccess pa -> new TypedPropertyAccess(
                    rewrite(pa.source()), pa.property(), pa.info());
            case TypedNativeCall c -> new TypedNativeCall(c.callee(),
                    rewriteAll(c.args()), c.info());
            case TypedCollection c -> new TypedCollection(rewriteAll(c.elements()), c.info());
            case TypedIf i -> new TypedIf(rewrite(i.condition()),
                    rewrite(i.thenBranch()), i.elseBranch().map(this::rewrite), i.info());
            case TypedLambda l -> {
                if (l.parameters().contains(target.freshRowVar())) {
                    throw new IllegalStateException("resolver bug: nested lambda"
                            + " binds the fresh row var '" + target.freshRowVar()
                            + "' — fresh-var selection must avoid user names");
                }
                yield l.parameters().contains(target.userVar())
                        ? l   // shadowing: substitution stops (standard capture rule)
                        : new TypedLambda(l.parameters(), rewriteAll(l.body()), l.info());
            }
            // Literals: nothing to substitute.
            case TypedCString ignored -> n;
            case TypedCInteger ignored -> n;
            case TypedCFloat ignored -> n;
            case TypedCDecimal ignored -> n;
            case TypedCBoolean ignored -> n;
            case TypedCDate ignored -> n;
            case TypedEnumValue ignored -> n;
            default -> throw new NotImplementedException(
                    "object-space expression node " + n.getClass().getSimpleName()
                            + " is not substitutable yet (H2 vocabulary)");
        };
    }

    /** {@code $p.head.leaf}: embedded ctor look-through, or association leaf. */
    private TypedSpec rewritePath(String head, String leaf, TypedSpec original) {
        TypedSpec headBinding = target.bindings().get(head);
        if (headBinding != null) {
            TypedSpec inner = headBinding;
            if (inner instanceof TypedNativeCall c && c.args().size() == 1
                    && c.callee().qualifiedName().endsWith("toOne")) {
                inner = c.args().get(0);
            }
            if (inner instanceof com.legend.compiler.spec.typed.TypedNewInstance ctor) {
                // EMBEDDED: the inner binding reads the PARENT row — a
                // parent-alias column, never a join (V1 §D.4 semantics).
                TypedSpec leafExpr = ctor.properties().get(leaf);
                if (leafExpr == null) {
                    throw new MappingResolutionException("property '" + leaf
                            + "' of embedded '" + head + "' on class '"
                            + target.classFqn() + "' is not mapped in mapping '"
                            + target.mappingFqn() + "'", target.classFqn());
                }
                return renameRowVar(leafExpr);
            }
            throw new NotImplementedException("navigation through class-typed"
                    + " slot property '" + head + "' is not supported yet");
        }
        AssocSub a = target.assocs().get(head);
        if (a == null) {
            throw new IllegalStateException("resolver bug: undemanded navigation"
                    + " '" + head + "." + leaf + "' — the demand scan and the"
                    + " rewrite disagreed");
        }
        TypedSpec leafBinding = a.targetBindings().get(leaf);
        if (leafBinding == null) {
            throw new MappingResolutionException("property '" + leaf
                    + "' of class '" + a.targetClassFqn()
                    + "' is not mapped in mapping '" + target.mappingFqn() + "'",
                    a.targetClassFqn());
        }
        TypedSpec leafInner = leafBinding;
        if (leafInner instanceof TypedNativeCall lc && lc.args().size() == 1
                && lc.callee().qualifiedName().endsWith("toOne")) {
            leafInner = lc.args().get(0);
        }
        if (leafInner instanceof com.legend.compiler.spec.typed.TypedNewInstance) {
            throw new NotImplementedException("class-typed property '" + leaf
                    + "' of association target '" + a.targetClassFqn()
                    + "' (embedded) is not supported yet");
        }
        // The target pipeline materialized with EMPTY demand — its own join
        // slots are STRIPPED. A leaf whose binding reads one would silently
        // prefix a nonexistent column: loud instead (nested navigation
        // through the target's joins is a later slice).
        if (Pipelines.referencesAliasOn(leafBinding, a.targetRowVar(),
                a.targetSlotAliases())) {
            throw new NotImplementedException("property '" + leaf + "' of class '"
                    + a.targetClassFqn() + "' is mapped through the target's own"
                    + " join slots; nested navigation joins are not supported yet");
        }
        return Pipelines.prefixColumns(leafBinding, a.targetRowVar(), a.prefix(),
                v -> new TypedVariable(target.freshRowVar(),
                        new ExprType(target.rowType(), Multiplicity.Bounded.ONE)));
    }

    private List<TypedSpec> rewriteAll(List<TypedSpec> ns) {
        List<TypedSpec> out = new ArrayList<>(ns.size());
        for (TypedSpec n : ns) {
            out.add(rewrite(n));
        }
        return out;
    }

    /**
     * Freshen a binding expression through THE unified row-read rewriter
     * ({@link Pipelines#rewriteRowReads}) — slot-condition rewriting and
     * binding rewriting share one implementation with a loud default, so
     * the demand scan and the substitution cannot drift. The row variable
     * maps to this instantiation's fresh var (stamped with the
     * MATERIALIZED row type); converted-slot sub-row reads become their
     * prefixed flat columns; stripped-slot reads and out-of-vocabulary
     * nodes are loud resolver bugs.
     */
    private TypedSpec renameRowVar(TypedSpec n) {
        return Pipelines.rewriteRowReads(n, target.sourceRowVar(),
                target.slotPrefixes(), target.strippedSlots(),
                v -> new TypedVariable(target.freshRowVar(),
                        new ExprType(target.rowType(), Multiplicity.Bounded.ONE)));
    }}
