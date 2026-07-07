package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.Property;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedGraphFetch;
import com.legend.compiler.spec.typed.TypedGraphTree;
import com.legend.compiler.spec.typed.TypedSerialize;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.ColSpec;
import com.legend.parser.spec.ColSpecArray;
import com.legend.parser.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * {@code graphFetch(#{Class{…}}#)} and {@code serialize(#{…}#)} (engine
 * {@code GraphFetchChecker} / {@code SerializeChecker}). The parser encodes the
 * tree as a {@link ColSpecArray} (each node: {@code function1} = the generated
 * accessor lambda, {@code function2} wraps the nested sub-tree); this checker
 * validates every property against its owner class <em>recursively</em>
 * (nesting requires a class-typed property) and reifies the tree.
 * {@code graphFetch} is a projection &mdash; the result is the SOURCE type
 * unchanged; {@code serialize} returns {@code String[1]} from its signature.
 */
final class GraphFetchChecker {

    private GraphFetchChecker() {
    }

    static TypedSpec graphFetch(Typer t, AppliedFunction af, Env env) {
        Checked c = checkTree(t, af, env, "graphFetch");
        return new TypedGraphFetch(c.source(), c.tree(), c.source().info());
    }

    static TypedSpec serialize(Typer t, AppliedFunction af, Env env) {
        Checked c = checkTree(t, af, env, "serialize");
        Optional<TypedSpec> config = af.parameters().size() > 2
                ? Optional.of(t.synth(af.parameters().get(2), env)) : Optional.empty();
        // String[1] — from the registered signature's return, never hardcoded.
        // ARITY-resolved (two serialize overloads are registered) via CoreFn,
        // not a magic string + blind get(0) (audit finding).
        var sigs = t.model().findFunction(CoreFn.SERIALIZE.parseName());
        int arity = af.parameters().size();
        var sig = sigs.stream().filter(f -> f.parameters().size() == arity).findFirst()
                .orElseThrow(() -> new TypeInferenceException(
                        "no registered 'serialize' overload accepts " + arity + " argument(s)"));
        return new TypedSerialize(c.source(), c.tree(), config,
                new ExprType(sig.returnType(), sig.returnMultiplicity()));
    }

    private record Checked(TypedSpec source, List<TypedGraphTree> tree) {
    }

    /**
     * The shared half: a class-collection source + a property tree validated
     * against it. (The tree's colspecs carry generated lambdas the generic
     * deferred path can't route &mdash; the signature's tree parameter is not a
     * {@code FuncColSpec} &mdash; so validation walks the class model directly.)
     */
    private static Checked checkTree(Typer t, AppliedFunction af, Env env, String fn) {
        if (af.parameters().size() < 2 || !(af.parameters().get(1) instanceof ColSpecArray tree)) {
            throw new TypeInferenceException(fn + " expects (classCollection, #{Class{…}}#)");
        }
        TypedSpec source = t.synth(af.parameters().get(0), env);
        if (!(source.info().type() instanceof Type.ClassType ct)) {
            throw new TypeInferenceException(fn + " requires a class-typed source, got "
                    + source.info().type().typeName());
        }
        return new Checked(source, validate(t, ct.fqn(), tree, fn));
    }

    /** Validate one tree level against its owner class; recurse into class-typed sub-trees. */
    private static List<TypedGraphTree> validate(Typer t, String classFqn, ColSpecArray tree, String fn) {
        List<TypedGraphTree> out = new ArrayList<>(tree.colSpecs().size());
        for (ColSpec cs : tree.colSpecs()) {
            Property prop = t.model().findProperty(classFqn, cs.name()).orElseThrow(() ->
                    new TypeInferenceException(fn + " tree: class " + classFqn
                            + " has no property '" + cs.name() + "'"));
            ColSpecArray nested = nestedTree(cs);
            if (nested == null) {
                out.add(new TypedGraphTree(cs.name(), List.of()));
                continue;
            }
            if (!(prop.type() instanceof Type.ClassType nestedClass)) {
                throw new TypeInferenceException(fn + " tree: property '" + cs.name()
                        + "' is not class-typed and cannot carry a sub-tree");
            }
            out.add(new TypedGraphTree(cs.name(), validate(t, nestedClass.fqn(), nested, fn)));
        }
        return out;
    }

    /** The nested sub-tree a colspec's {@code function2} wraps, or {@code null} for a leaf. */
    private static ColSpecArray nestedTree(ColSpec cs) {
        if (cs.function2() == null || cs.function2().body().isEmpty()) {
            return null;
        }
        ValueSpecification body = cs.function2().body().get(0);
        return body instanceof ColSpecArray arr ? arr : null;
    }
}
