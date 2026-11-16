// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.error.TypeInferenceException;

import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import java.util.ArrayList;
import java.util.List;

/** The ONE rule for a multi-statement lambda body (batch 54): lets inline,
 * a leading let substitutes forward, {@code fail(..); v} is the raise typed
 * at the value, {@code assert(c, m); v} is {@code if(c, |v, |fail(m))}. */
final class LambdaBodies {

    /** One assert of pure's family as a body GUARD (essential/tests/*.pure, legend-pure 5.99.0): each
     *  {@code assertX(args, [message])} is {@code assert(condition, message)} as pure composes it -- the condition from
     *  the leading {@code condArgs} arguments, and, where the call gives no message, pure's own default message, built
     *  from the same arguments by the same functions pure's body calls (C5-J's batch, 3a, the orchestrator's ruling of
     *  2026-11-16: until then the guard knew seven of the family and gave each the message "Assert failed", and a
     *  leading assertSize or assertContains was typed and dropped, a false green). Every member of the family lite
     *  registers; pure's assertNotSize, assertNotContains, assertNotEq and assertIsNot are not registered, so a call of
     *  one is refused at typing, by name. */
    private record Guard(int condArgs, java.util.function.Function<List<ValueSpecification>, ValueSpecification> condition,
            DefaultMessage defaultMessage) {
    }

    /** Pure's default message over an assert's arguments, typed where the message's form is decided by their types. */
    @FunctionalInterface
    private interface DefaultMessage {
        ValueSpecification of(List<ValueSpecification> args, Typer t, Env scope);
    }

    private static final String ASSERTS = "meta::pure::functions::asserts::";

    /** The assert family as body GUARDS, by FQN. */
    private static final java.util.Map<String, Guard> ASSERT_GUARDS = java.util.Map.ofEntries(
            // assert.pure: assert(condition) is assert($condition, 'Assert failed')
            java.util.Map.entry(ASSERTS + "assert", new Guard(1, a -> a.get(0), (a, t, s) -> text("Assert failed"))),
            // assertFalse(condition) is assert(!$condition)
            java.util.Map.entry(ASSERTS + "assertFalse", new Guard(1, a -> call("not", a.get(0)),
                    (a, t, s) -> text("Assert failed"))),
            // assertEquals.pure: one value each '\nexpected: %r\nactual:   %r', else their representations listed
            java.util.Map.entry(ASSERTS + "assertEquals", new Guard(2, a -> call("equal", a.get(0), a.get(1)),
                    LambdaBodies::equalsMessage)),
            // assertNotEquals.pure: one value each '%r should not equal %r', else the lists joined by ' should not equal '
            java.util.Map.entry(ASSERTS + "assertNotEquals", new Guard(2,
                    a -> call("not", call("equal", a.get(0), a.get(1))), LambdaBodies::notEqualsMessage)),
            // assertEmpty.pure: '%s is not empty' over ^List<Any>(values=$collection), a String as itself, every other
            // value its representation (the host's PureAsserts.notEmptyMessage writes it so)
            java.util.Map.entry(ASSERTS + "assertEmpty", new Guard(1, a -> call("isEmpty", a.get(0)),
                    LambdaBodies::notEmptyMessage)),
            // assertNotEmpty.pure: assertFalse($collection->isEmpty(), 'Expected non-empty collection')
            java.util.Map.entry(ASSERTS + "assertNotEmpty", new Guard(1, a -> call("not", call("isEmpty", a.get(0))),
                    (a, t, s) -> text("Expected non-empty collection"))),
            // assertSize.pure: assertEq($size, $collection->size(), 'expected size: %s, actual size: %s', ...)
            java.util.Map.entry(ASSERTS + "assertSize", new Guard(2,
                    a -> call("eq", a.get(1), call("size", a.get(0))),
                    (a, t, s) -> format("expected size: %s, actual size: %s", a.get(1), call("size", a.get(0))))),
            // assertContains.pure: '%s does not contain %r', the collection's representations listed
            java.util.Map.entry(ASSERTS + "assertContains", new Guard(2, a -> call("contains", a.get(0), a.get(1)),
                    (a, t, s) -> format("%s does not contain %r", representations(a.get(0), "[", "]"), a.get(1)))),
            // assertEq.pure / assertIs.pure: '\nexpected: %r\nactual:   %r'
            java.util.Map.entry(ASSERTS + "assertEq", new Guard(2, a -> call("eq", a.get(0), a.get(1)),
                    (a, t, s) -> format("\nexpected: %r\nactual:   %r", a.get(0), a.get(1)))),
            java.util.Map.entry(ASSERTS + "assertIs", new Guard(2, a -> call("is", a.get(0), a.get(1)),
                    (a, t, s) -> format("\nexpected: %r\nactual:   %r", a.get(0), a.get(1)))),
            // assertInstanceOf.pure: 'expected %r to be an instance of %s, actual: %s'
            java.util.Map.entry(ASSERTS + "assertInstanceOf", new Guard(2, a -> call("instanceOf", a.get(0), a.get(1)),
                    LambdaBodies::instanceOfMessage)),
            // assertSameElements.pure: assertEquals($expected->sort(), $actual->sort(), ...) with both sorted lists
            java.util.Map.entry(ASSERTS + "assertSameElements", new Guard(2,
                    a -> call("equal", call("sort", a.get(0)), call("sort", a.get(1))),
                    (a, t, s) -> call("plus", new com.legend.protocol.spec.PureCollection(List.of(
                            representations(call("sort", a.get(0)), "\nexpected: [", "]"),
                            representations(call("sort", a.get(1)), "\nactual:   [", "]")))))),
            // assertEqWithinTolerance.pure: abs($expected - $actual) <= abs($delta)
            java.util.Map.entry(ASSERTS + "assertEqWithinTolerance", new Guard(3,
                    a -> call("lessThanEqual", call("abs", call("minus", new com.legend.protocol.spec.PureCollection(
                            List.of(a.get(0), a.get(1))))), call("abs", a.get(2))),
                    (a, t, s) -> format("\nexpected: %r\nactual:   %r", a.get(0), a.get(1)))));

    /** A platform function by its bare name, as pure's assert bodies call it. */
    private static ValueSpecification call(String name, ValueSpecification... args) {
        return Calls.platform(name, List.of(args));
    }

    private static ValueSpecification text(String value) {
        return new com.legend.protocol.spec.CString(value);
    }

    /** {@code format(pattern, [args])}. */
    private static ValueSpecification format(String pattern, ValueSpecification... args) {
        return call("format", text(pattern), new com.legend.protocol.spec.PureCollection(List.of(args)));
    }

    /** {@code $values->map(v | $v->toRepresentation())->joinStrings(open, ', ', close)}. */
    private static ValueSpecification representations(ValueSpecification values, String open, String close) {
        com.legend.protocol.spec.Variable v = new com.legend.protocol.spec.Variable("assertElement");
        ValueSpecification each = new LambdaFunction(List.of(v), List.of(call("toRepresentation",
                new com.legend.protocol.spec.Variable("assertElement"))));
        return call("joinStrings", call("map", values, each), text(open), text(", "), text(close));
    }

    /** assertEmpty.pure's default message, {@code format('%s is not empty', [^List<Any>(values=$collection)])}: a
     *  List's {@code %s} writes a String as itself and every other value its representation (the host's
     *  PureAsserts.notEmptyMessage writes it so), decided here by the values' type: Strings as themselves, any other
     *  primitive by its representation, an empty collection's {@code []}; values of any other type -- more than one
     *  type, a class's, an enumeration's -- are refused by name. */
    private static ValueSpecification notEmptyMessage(List<ValueSpecification> a, Typer t, Env scope) {
        com.legend.compiler.element.type.ExprType values = t.synth(a.get(0), scope).info();
        if (values.multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                && b.upper() != null && b.upper() == 0) {
            return text("[] is not empty");
        }
        ValueSpecification listed;
        if (values.type() == com.legend.compiler.element.type.Type.Primitive.STRING) {
            listed = call("joinStrings", a.get(0), text("["), text(", "), text("]"));
        } else if (values.type() instanceof com.legend.compiler.element.type.Type.Primitive) {
            listed = representations(a.get(0), "[", "]");
        } else {
            throw new com.legend.error.NotImplementedException("assertEmpty with no message, leading a lambda, over"
                    + " values of type " + values.type().typeName() + ": pure's default message writes a String as"
                    + " itself and any other value its representation, which lite plans for values of one primitive"
                    + " type alone; give the assert a message");
        }
        return call("plus", new com.legend.protocol.spec.PureCollection(List.of(listed, text(" is not empty"))));
    }

    /** Whether two values are one value each, where their types decide it: TRUE where both are {@code [1]}, FALSE
     *  where either can never be one value, null where only the run decides (pure's assertEquals and assertNotEquals
     *  pick their message's form by it, {@code if(eq($expected->size(), 1) && eq($actual->size(), 1), ...)}: where
     *  the types decide the test, the other form is never planned -- a database with no list encoding writes the
     *  one-value form alone). */
    private static @com.legend.base.Nullable Boolean oneEach(List<ValueSpecification> a, Typer t, Env scope) {
        com.legend.compiler.element.type.Multiplicity m0 = t.synth(a.get(0), scope).info().multiplicity();
        com.legend.compiler.element.type.Multiplicity m1 = t.synth(a.get(1), scope).info().multiplicity();
        if (exactlyOne(m0) && exactlyOne(m1)) {
            return Boolean.TRUE;
        }
        return neverOne(m0) || neverOne(m1) ? Boolean.FALSE : null;
    }

    private static boolean exactlyOne(com.legend.compiler.element.type.Multiplicity m) {
        return m instanceof com.legend.compiler.element.type.Multiplicity.Bounded b && b.lower() == 1
                && b.upper() != null && b.upper() == 1;
    }

    private static boolean neverOne(com.legend.compiler.element.type.Multiplicity m) {
        return m instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                && (b.lower() > 1 || b.upper() != null && b.upper() < 1);
    }

    /** The form pure's message takes for one value each ({@code one}) or else ({@code many}), as {@link #oneEach}
     *  decides it, the run deciding it where the types do not. */
    private static ValueSpecification bySize(List<ValueSpecification> a, Typer t, Env scope, ValueSpecification one,
            ValueSpecification many) {
        Boolean decided = oneEach(a, t, scope);
        if (decided != null) {
            return decided ? one : many;
        }
        ValueSpecification bothOne = call("and", call("eq", call("size", a.get(0)),
                new com.legend.protocol.spec.CInteger(1L)), call("eq", call("size", a.get(1)),
                new com.legend.protocol.spec.CInteger(1L)));
        return call("if", bothOne, new LambdaFunction(List.of(), List.of(one)),
                new LambdaFunction(List.of(), List.of(many)));
    }

    /** assertEquals.pure's default message. */
    private static ValueSpecification equalsMessage(List<ValueSpecification> a, Typer t, Env scope) {
        return bySize(a, t, scope,
                format("\nexpected: %r\nactual:   %r", call("toOne", a.get(0)), call("toOne", a.get(1))),
                format("\nexpected: %s\nactual:   %s", representations(a.get(0), "[", "]"),
                        representations(a.get(1), "[", "]")));
    }

    /** assertNotEquals.pure's default message. */
    private static ValueSpecification notEqualsMessage(List<ValueSpecification> a, Typer t, Env scope) {
        return bySize(a, t, scope,
                format("%r should not equal %r", call("toOne", a.get(0)), call("toOne", a.get(1))),
                call("plus", new com.legend.protocol.spec.PureCollection(List.of(representations(a.get(0), "[", "]"),
                        text(" should not equal "), representations(a.get(1), "[", "]")))));
    }

    /** assertInstanceOf.pure's default message, {@code format('expected %r to be an instance of %s, actual: %s',
     *  [$instance, $type->elementToPath(), $instance->type()->toOne()->elementToPath()])}: the value's type as it runs
     *  is known when the plan is made for a value of a concrete primitive type alone (a primitive type named by its
     *  name, as pure's elementToPath names it, and as the judges word the same assert, DatabaseJudge.instanceOf); any
     *  other value's is refused by name. */
    private static ValueSpecification instanceOfMessage(List<ValueSpecification> a, Typer t, Env scope) {
        TypedSpec instance = t.synth(a.get(0), scope);
        TypedSpec type = t.synth(a.get(1), scope);
        String path = switch (type) {
            case com.legend.compiler.spec.typed.TypedTypeRef tr -> tr.target().typeName();
            case com.legend.compiler.spec.typed.TypedPackageableRef pr -> pr.fullPath();
            default -> null;
        };
        if (!(instance.info().type() instanceof com.legend.compiler.element.type.Type.Primitive p)
                || p == com.legend.compiler.element.type.Type.Primitive.NUMBER
                || p == com.legend.compiler.element.type.Type.Primitive.DATE || path == null) {
            throw new com.legend.error.NotImplementedException("assertInstanceOf with no message, leading a lambda:"
                    + " pure's default message names the value's type as it runs, which lite knows when the plan is"
                    + " made for a value of a concrete primitive type alone; give the assert a message");
        }
        String named = java.util.Arrays.stream(com.legend.compiler.element.type.Type.Primitive.values())
                .filter(q -> q.qualifiedName().equals(path)).findFirst()
                .map(com.legend.compiler.element.type.Type.Primitive::typeName).orElse(path);
        return format("expected %r to be an instance of %s, actual: %s", a.get(0), text(named), text(p.typeName()));
    }

    private LambdaBodies() {
    }

    /** A lambda BODY as one typed expression — the one rule for every
     * body-consuming checker (match arms, if branches): a single
     * expression as is; {@code [let*, final]} by source-level let-inlining
     * ({@link SourceSubst#inlineLets}); {@code [fail(..)+, value]} as the
     * raise typed at the unreachable value ({@link IfChecker#failThenValue}
     * — toPostgresModel's TableFunctionParamPlaceHolder arm); anything else
     * stays loud, never a silently dropped statement. */
    static TypedSpec synthBody(Typer t, LambdaFunction lam, Env scope) {
        if (lam.body().size() == 1) {
            return t.synth(lam.body().get(0), scope);
        }
        LambdaFunction folded = SourceSubst.inlineLets(lam);
        if (folded != null) {
            // the objects the splice evaluates apart are untracked (docs/OBJECT_IDENTITY_2026_10_20.md §4.4)
            java.util.Set<Object> untracked = SourceSubst.untrackedLetNodes(lam);
            TypedSpec body = t.synth(folded.body().get(0), scope);
            return untracked.isEmpty() ? body : com.legend.compiler.spec.typed.Evaluation.untrack(body, untracked);
        }
        // a LEADING let before a non-let statement (let c = …; assert(..); …):
        // substitute it forward and take the rest through the same rule
        com.legend.protocol.spec.CString leading = SourceSubst.letName(lam.body().get(0));
        if (leading != null && lam.body().size() > 1) {
            ValueSpecification bound = ((AppliedFunction) lam.body().get(0)).parameters().get(1);
            List<ValueSpecification> rest = new ArrayList<>();
            for (ValueSpecification s : lam.body().subList(1, lam.body().size())) {
                rest.add(SourceSubst.substitute(s, java.util.Map.of(leading.value(), bound)));
            }
            return synthBody(t, new LambdaFunction(lam.parameters(), rest), scope);
        }
        TypedSpec asFail = IfChecker.failThenValue(t, lam, scope);
        if (asFail != null) {
            return asFail;
        }
        TypedSpec guarded = assertThenValue(t, lam, scope);
        if (guarded != null) {
            return guarded;
        }
        // any other leading statement whose value is discarded: pure evaluates it, so a raise inside it fails the
        // body; lite plans no effect it cannot carry in one expression. It is KEPT, typed, for the lowering to refuse
        // by name (TypedDiscarded; C5-J's batch, 3a, the user's ruling of 2026-11-16, register S-batch-6, the refusal
        // at planning: "typing asks fit, not run", so a model holding one compiles and every prelude body types at
        // boot) -- until then it was typed and dropped, its raise lost, a false green
        ValueSpecification first = lam.body().get(0);
        String what = first instanceof AppliedFunction af ? resolvedName(t, af) : first.getClass().getSimpleName();
        TypedSpec statement = t.synth(first, scope);
        TypedSpec value = synthBody(t, new LambdaFunction(lam.parameters(), lam.body().subList(1, lam.body().size())),
                scope);
        return new com.legend.compiler.spec.typed.TypedDiscarded(statement, value, what, value.info());
    }

    /** The function a statement calls, by its full path where its name resolves to one, else as written. */
    private static String resolvedName(Typer t, AppliedFunction af) {
        List<TypedFunction> cands = t.functionCandidates(af);
        return !cands.isEmpty() && cands.stream().allMatch(f -> f.qualifiedName().equals(cands.get(0).qualifiedName()))
                ? cands.get(0).qualifiedName() : af.function();
    }

    /** {@code assert(cond, msg); rest} is {@code if(cond, |rest, |fail(msg))}
     * — the raise semantics exactly (the database raises when the condition
     * is false; a spelled condition folds the guard away in the unroll —
     * toPostgresModel's converter-registry self-check). Null = not this
     * shape (the first statement is not an assert). */
    private static @com.legend.base.Nullable TypedSpec assertThenValue(Typer t, LambdaFunction lam, Env scope) {
        ValueSpecification first = lam.body().get(0);
        if (!(first instanceof AppliedFunction af) || af.parameters().isEmpty()) {
            return null;
        }
        // RESOLVED dispatch: every candidate the name resolves to is the
        // asserts::assert native (never a spelling compare)
        List<TypedFunction> cands = t.functionCandidates(af);
        if (cands.isEmpty()) {
            return null;
        }
        String fqn = cands.get(0).qualifiedName();
        if (!cands.stream().allMatch(f -> f.qualifiedName().equals(fqn))
                || !ASSERT_GUARDS.containsKey(fqn)) {
            return null;
        }
        // the guard's CONDITION and how many leading arguments it consumes
        // (asserts.pure: each assertX(args, [message]) is assert(cond, message))
        Guard guard = ASSERT_GUARDS.get(fqn);
        int condArgs = guard.condArgs();
        if (af.parameters().size() < condArgs) {
            return null;
        }
        List<ValueSpecification> condIn = af.parameters().subList(0, condArgs);
        TypedSpec cond = t.synth(guard.condition().apply(condIn), scope);
        List<ValueSpecification> msgArgs = af.parameters().subList(condArgs, af.parameters().size());
        TypedSpec message;
        if (msgArgs.isEmpty()) {
            // pure's own default message, from the same arguments
            message = t.synth(guard.defaultMessage().of(condIn, t, scope), scope);
        } else if (msgArgs.get(0) instanceof LambdaFunction thunk
                && thunk.parameters().isEmpty()) {
            message = synthBody(t, thunk, scope);
        } else if (msgArgs.size() == 1) {
            message = t.synth(msgArgs.get(0), scope);
        } else {
            // assert(cond, format, args) — the formatted message
            message = t.synth(Calls.platform("format", msgArgs), scope);
        }
        TypedSpec rest = synthBody(t, new LambdaFunction(lam.parameters(),
                lam.body().subList(1, lam.body().size())), scope);
        var fail = t.model().findFunction("meta::pure::functions::asserts::fail").stream()
                .filter(f -> f.parameters().size() == 1).findFirst()
                .orElseThrow(() -> new TypeInferenceException("fail(message) is not registered"));
        TypedSpec raise = new TypedNativeCall(fail, List.of(message), rest.info());
        return new com.legend.compiler.spec.typed.TypedIf(cond, rest,
                java.util.Optional.of(raise), rest.info());
    }

}
