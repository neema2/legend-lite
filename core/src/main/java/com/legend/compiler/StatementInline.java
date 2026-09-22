// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.spec.SourceSubst;
import com.legend.model.FunctionDefinition;
import com.legend.model.ImportScope;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * STATEMENT-level &beta;-reduction of user function calls (the front-door
 * sibling of {@link com.legend.compiler.spec.UserCallInliner}): a call whose
 * callee is a PROGRAM cannot become one expression the SQL lowering runs, so
 * Pure's call semantics — the callee's statements evaluated in order under
 * the parameter bindings — are spelled into the caller's statement list:
 * <pre>
 *   helper($m, 3);                 &lt;helper's statements with $m and 3
 *   let r = helper2($m);      →     substituted, lets renamed&gt;
 *                                  ... let r = &lt;helper2's last statement&gt;;
 * </pre>
 * Rules: (1) only a statement-root call or a let-bound call expands; a
 * program call in ARGUMENT position is first hoisted into a let before the
 * statement, in evaluation order (Pure evaluates arguments left to right
 * before the call; lambda bodies are deferred code and are not entered);
 * (2) a callee is a PROGRAM when its own statements reach a statement-only
 * call ({@link PlatformTypes#isStatementOnly}: an execution, a store effect,
 * a test-data generator, the seed-SQL form), when its body is a statement
 * SEQUENCE (a non-let statement before the last), or when it is a thin
 * wrapper whose value is a call to a program (to a non-program or a cycle); a value
 * function stays with the expression inliner, and a platform-owned verdict
 * ({@link PlatformTypes#isVerdictFunction}) is never opened — the statement
 * channel adjudicates its call; (3) parameters substitute (&beta;, the
 * expression inliner's rule) and every let the callee introduces is renamed
 * to a fresh {@code _s&lt;N&gt;_&lt;name&gt;} through the body, so the caller's
 * single-assignment scope never collides; (4) spliced statements expand
 * recursively; a call cycle leaves the inner call standing (the expression
 * inliner's loud wall names it).
 *
 * <p>Names are the RESOLVER's (the query is name-resolved before this pass
 * runs, and module bodies at build): an exact FQN, or the candidate FQNs the
 * resolver leaves on a bare call for signature matching — the one candidate
 * with a definition at the call's arity is the callee; several is the
 * typer's business. A native registered under a name is the platform's:
 * the model's Pure overloads of it are never opened.
 */
public final class StatementInline {

    private StatementInline() {
    }

    public static List<ValueSpecification> rewrite(List<ValueSpecification> statements,
            ImportScope imports, ModelContext ctx) {
        return new StatementInline.Pass(ctx).expand(statements, new ArrayDeque<>());
    }

    private static final class Pass {
        private final ModelContext ctx;
        /** Every binder minted so far, in order: the fresh-name ledger (its
         * size is the next index; names never repeat within a rewrite). */
        private final List<String> minted = new ArrayList<>();
        /** Program-ness per signature, memoized within a rewrite. */
        private final Map<String, Boolean> programs = new LinkedHashMap<>();

        Pass(ModelContext ctx) {
            this.ctx = ctx;
        }

        List<ValueSpecification> expand(List<ValueSpecification> statements,
                Deque<String> stack) {
            List<ValueSpecification> out = new ArrayList<>(statements.size());
            for (ValueSpecification st0 : statements) {
                // hoisted argument programs are statements like any other:
                // they expand (recursively) before the statement that read them
                List<ValueSpecification> hoisted = new ArrayList<>();
                ValueSpecification st = hoistProgramArguments(st0, hoisted);
                if (!hoisted.isEmpty()) {
                    out.addAll(expand(hoisted, stack));
                }
                CString letName = SourceSubst.letName(st);
                ValueSpecification callSite = letName == null ? st
                        : ((AppliedFunction) st).parameters().get(1);
                FunctionDefinition callee = callSite instanceof AppliedFunction af
                        && SourceSubst.letName(af) == null ? programCallee(af) : null;
                // the cycle guard keys on the SIGNATURE: an overload
                // forwarding to its sibling (runTest/3 -> runTest/4) is a
                // call, not recursion
                if (callee == null || stack.contains(signature(callee))) {
                    out.add(st);
                    continue;
                }
                AppliedFunction call = (AppliedFunction) callSite;
                Map<String, ValueSpecification> env = new LinkedHashMap<>();
                for (int i = 0; i < callee.parameters().size(); i++) {
                    env.put(callee.parameters().get(i).name(), call.parameters().get(i));
                }
                List<ValueSpecification> body = new ArrayList<>(callee.body().size());
                for (ValueSpecification s : callee.body()) {
                    CString ln = SourceSubst.letName(s);
                    if (ln == null) {
                        body.add(SourceSubst.substitute(s, env));
                        continue;
                    }
                    AppliedFunction let = (AppliedFunction) s;
                    String renamed = freshName(ln.value());
                    ValueSpecification value = SourceSubst.substitute(
                            let.parameters().get(1), env);
                    body.add(let.withParameters(List.of(new CString(renamed, ln.pos()), value)));
                    env.put(ln.value(), new Variable(renamed, null, null, ln.pos()));
                }
                stack.push(signature(callee));
                List<ValueSpecification> spliced = expand(body, stack);
                stack.pop();
                if (letName == null) {
                    out.addAll(spliced);
                    continue;
                }
                // a let-bound call: the callee's value is its last statement
                // (a trailing let IS its value, real pure)
                ValueSpecification last = spliced.remove(spliced.size() - 1);
                out.addAll(spliced);
                CString lastLet = SourceSubst.letName(last);
                if (lastLet != null) {
                    out.add(last);
                    last = new Variable(lastLet.value(), null, null, lastLet.pos());
                }
                out.add(((AppliedFunction) st).withParameters(List.of(letName, last)));
            }
            return out;
        }

        /** A program call in ARGUMENT position ({@code execute(f, m,
         * initDatabase(), ext)}: DDL effects, then a runtime value) is
         * hoisted into a let before the statement, in evaluation order. The
         * statement's own root call and a let's own bound call are the
         * splice's, not the hoist's. */
        private ValueSpecification hoistProgramArguments(ValueSpecification st,
                List<ValueSpecification> out) {
            CString letName = SourceSubst.letName(st);
            ValueSpecification root = letName == null ? st
                    : ((AppliedFunction) st).parameters().get(1);
            ValueSpecification hoisted = root.mapChildren(c -> hoistIn(c, out));
            if (hoisted == root) {
                return st;
            }
            return letName == null ? hoisted
                    : ((AppliedFunction) st).withParameters(List.of(letName, hoisted));
        }

        private ValueSpecification hoistIn(ValueSpecification v, List<ValueSpecification> out) {
            if (v instanceof LambdaFunction) {
                return v;
            }
            ValueSpecification inner = v.mapChildren(c -> hoistIn(c, out));
            if (inner instanceof AppliedFunction af && SourceSubst.letName(af) == null
                    && programCallee(af) != null) {
                String name = freshName("hoisted");
                out.add(new AppliedFunction("letFunction",
                        List.of(new CString(name), inner)));
                return new Variable(name);
            }
            return inner;
        }

        private static String signature(FunctionDefinition fd) {
            return fd.qualifiedName() + "/" + fd.parameters().size();
        }

        private String freshName(String name) {
            minted.add(name);
            return "_s" + minted.size() + "_" + name;
        }

        /** The callee when {@code af} calls a user function that is a PROGRAM
         * and not a platform-owned verdict, else null. */
        private @com.legend.base.Nullable FunctionDefinition programCallee(AppliedFunction af) {
            FunctionDefinition fd = resolvedDefinition(af);
            return fd == null || fd.body().isEmpty()
                    || PlatformTypes.isVerdictFunction(fd.qualifiedName())
                    || !isProgram(fd) ? null : fd;
        }

        /** The user definition the RESOLVER assigned a call: its exact FQN,
         * or the candidate FQNs it left on a bare name — the one candidate
         * with a user definition at this arity. Null when the platform owns
         * the name (a native is registered under it), when no candidate has
         * the arity, or when several do. */
        private @com.legend.base.Nullable FunctionDefinition resolvedDefinition(AppliedFunction af) {
            List<String> names = af.function().contains("::")
                    ? List.of(af.function()) : af.candidateFqns();
            FunctionDefinition found = null;
            for (String fqn : names) {
                // a native registered under the name is the platform's — the
                // catalog's FQN index answers without compiling anything
                if (!com.legend.builtin.Pure.nativeFunctionsAt(fqn).isEmpty()) {
                    return null;
                }
                FunctionDefinition d = null;
                for (FunctionDefinition fd : ctx.findFunctionDefinitions(fqn)) {
                    if (fd.parameters().size() != af.parameters().size()) {
                        continue;
                    }
                    if (d != null) {
                        return null;
                    }
                    d = fd;
                }
                if (d == null) {
                    continue;
                }
                if (found != null) {
                    return null;
                }
                found = d;
            }
            return found;
        }

        private boolean isProgram(FunctionDefinition fd) {
            Boolean known = programs.get(signature(fd));
            if (known != null) {
                return known;
            }
            programs.put(signature(fd), false);   // in progress: a cycle scores false
            boolean program = isProgram0(fd);
            programs.put(signature(fd), program);
            return program;
        }

        private boolean isProgram0(FunctionDefinition fd) {
            if (fd.body().stream().anyMatch(this::reachesStatementOnly)) {
                return true;
            }
            // a statement SEQUENCE (a non-let statement before the last —
            // two asserts in a row) cannot become one expression
            for (int i = 0; i < fd.body().size() - 1; i++) {
                if (SourceSubst.letName(fd.body().get(i)) == null) {
                    return true;
                }
            }
            // a thin wrapper whose VALUE is a program call (runTest/3 ->
            // runTest/4) is that program (the chain ends at a non-program or a cycle)
            if (fd.body().isEmpty()) {
                return false;
            }
            ValueSpecification last = fd.body().get(fd.body().size() - 1);
            if (SourceSubst.letName(last) != null) {
                last = ((AppliedFunction) last).parameters().get(1);
            }
            if (last instanceof AppliedFunction tail
                    && SourceSubst.letName(tail) == null) {
                FunctionDefinition callee = resolvedDefinition(tail);
                // a tail call chain ends at a non-program or a cycle (the
                // memo scores an in-progress callee false — no depth cap)
                return callee != null && !PlatformTypes.isVerdictFunction(callee.qualifiedName())
                        && isProgram(callee);
            }
            return false;
        }

        private boolean reachesStatementOnly(ValueSpecification v) {
            if (v instanceof AppliedFunction af
                    && ResolvedNames.referents(af).stream().anyMatch(PlatformTypes::isStatementOnly)) {
                return true;
            }
            return v.children().stream().anyMatch(this::reachesStatementOnly);
        }
    }
}
