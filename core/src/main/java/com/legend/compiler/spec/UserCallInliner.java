package com.legend.compiler.spec;


import com.legend.platform.WalledBodies;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedEval;
import com.legend.compiler.spec.typed.FreeVars;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedMatch;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSelect;
import com.legend.compiler.spec.typed.TypedSerializeGraph;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedSubst;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.builtin.Pure;
import com.legend.compiler.element.type.ExprType;
import com.legend.error.NotImplementedException;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Phase G&frac12; &mdash; whole-program &beta;-reduction of user-defined
 * function calls (the monomorphization family: SQL has no call frame, so
 * every {@link TypedUserCall} must be spliced into its caller BEFORE the
 * store resolver and the lowerer see the tree). After this pass no
 * {@code TypedUserCall} exists, and H's demand analysis sees THROUGH
 * function boundaries by construction (a callee navigating
 * {@code $p.address.street} contributes join demand like inline code).
 *
 * <p>Rules:
 * <ul>
 *   <li><b>&beta;</b> &mdash; parameter occurrences are replaced by the
 *       (already-rewritten) argument expressions; every node keeps its
 *       G-computed {@link com.legend.compiler.element.type.ExprType}
 *       (arguments conform by overload resolution; narrower replacements
 *       are conservative &mdash; the no-restamp discipline).</li>
 *   <li><b>lets reduce</b> &mdash; a callee's intermediate
 *       {@code let x = e;} statements substitute forward (Pure lets are
 *       single-assignment), so a call becomes ONE expression. Query-level
 *       lets reduce the same way ({@link #inlineBody}); the store resolver
 *       reads the consumed ones through {@link #queryLets()}.</li>
 *   <li><b>&alpha;-hygiene</b> &mdash; is {@link TypedSubst}'s, the one
 *       substitution engine (D24): at every &beta; site the environment is
 *       substituted ONCE, up front ({@code TypedSubst.apply}; a binder is
 *       renamed only when a term substituted beneath it has its name free,
 *       to the deterministic {@code b_&lt;k&gt;}), and the walk reduces the
 *       substituted tree with no environment. A term a &beta; site bound
 *       was reduced before it was bound and is spliced verbatim:
 *       {@link #reducedTerms} keeps the walk out of it.</li>
 *   <li><b>recursion is loud</b> &mdash; a call cycle throws naming the
 *       path ({@code f/1 -> g/2 -> f/1}); SQL cannot express it.</li>
 *   <li><b>eval of a literal lambda</b> &mdash; after substitution a
 *       higher-order parameter becomes a lambda literal;
 *       {@code $f-&gt;eval($x)} then &beta;-reduces the same way. Eval of
 *       anything else passes through (its own wall downstream).</li>
 * </ul>
 */
public final class UserCallInliner {

    private final SpecCompiler specs;
    private final java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec, java.util.Set<String>, TypedSpec> hook;
    private final ArrayDeque<com.legend.model.FunctionId> stack = new ArrayDeque<>();
    /** Per activation: the size of its literal-structure arguments (the
     * literal unroll's descent measure; 0 = none). */
    private final ArrayDeque<Integer> literalSizes = new ArrayDeque<>();
    /** Per activation: the DECLARED classes of its arguments — the
     * class-lattice descent measure for a non-literal re-entry. */
    private final ArrayDeque<java.util.Set<String>> argClassSets = new ArrayDeque<>();
    /** The QUOTED-code frames being rewritten (a quoted lambda, a
     * deactivate() subject): the literal unroll never evaluates inside —
     * the lambda body IS the value (witness
     * tesIsToOneDataTypeFunctionExpressionSequence: {@code ['a','b']->isEmpty()}
     * must stay a function expression). */
    private final ArrayDeque<TypedSpec> quotedFrames = new ArrayDeque<>();
    private final ArrayDeque<String> names = new ArrayDeque<>();
    /** The typing-surface-native → engine-program hand-off (Phase 5 strict run). */
    static final boolean HAND_OFF_ON = false;
    /** Lambda binders in scope at the CURRENT walk position (name → nesting
     * count) — passed to the hook so a query-level splice never captures a
     * lambda-bound variable spelled like an exec-let ({@code let r =
     * execute(...)} vs {@code ->map(r|$r.values...)}). */
    private final Map<String, Integer> bound = new LinkedHashMap<>();
    /** Query-level lets consumed by {@link #inlineBody} — graph-tree args
     * keep their source spelling, so the resolver resolves variable dates
     * through these (engine inScopeVars). */
    private final Map<String, TypedSpec> queryLets = new LinkedHashMap<>();

    public Map<String, TypedSpec> queryLets() {
        return queryLets;
    }
    /** Inside a postprocessor-CONFIG property: user calls STAND
     * (extraction reads them structurally); variables still substitute. */
    private boolean configMode;
    /** The terms the OPEN &beta; sites bound (arguments, let values, match
     * inputs, unrolled elements), each with the number of open scopes
     * holding it: each was reduced before it was bound and the substitution
     * splices it verbatim, so the walk returns it as it stands — a standing
     * call inside it (a recursion wall, a subsumed program, a config
     * property's) is never re-attempted, and no node is rewritten twice.
     * Scoped to the site ({@link Scope#close()}): callee bodies are
     * memoised, so a node of one is met again at the next inline site,
     * where it reduces afresh in that site's context (a quoted frame, the
     * hook's binders). By identity; never iterated. */
    private final Map<TypedSpec, Integer> reducedTerms = new java.util.IdentityHashMap<>();

    public UserCallInliner(SpecCompiler specs) {
        this(specs, null);
    }

    /**
     * {@code hook}: an OPTIONAL per-node pre-rewrite (the statement
     * executor's result-frame splice rides this walker instead of
     * duplicating the vocabulary switch). Fired before the standard
     * rewrite at every node; returning a DIFFERENT node replaces it and
     * the rewrite recurses into the replacement — the hook must return
     * the argument itself (same reference) when it does not apply.
     */
    public UserCallInliner(SpecCompiler specs,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec, java.util.Set<String>, TypedSpec> hook) {
        this(specs, hook, false);
    }

    private UserCallInliner(SpecCompiler specs,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec, java.util.Set<String>, TypedSpec> hook,
            boolean verdictSource) {
        this.specs = Objects.requireNonNull(specs, "specs");
        this.hook = hook;
        this.verdictSource = verdictSource;
    }

    /** Inline every user call in a query body (statements = lets + result). */
    /** The quantified verdict's SOURCE collection reduced with the literal
     * arms ON (a user-authored statement keeps its shape everywhere else —
     * engine parity): {@code DatabaseType->enumValues()->filter(e | $e->in(
     * [...]))} is its literal elements for the unroll; the unroll compares,
     * never computes — the elements were already spelled. */
    private final boolean verdictSource;

    /** An inliner whose literal arms are ON at the root — for the
     * quantified verdict's SOURCE only (see {@link #reduceVerdictSource}). */
    public static UserCallInliner forVerdictSource(SpecCompiler specs,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> hook) {
        return new UserCallInliner(specs, hook, true);
    }

    public TypedSpec reduceVerdictSource(TypedSpec source, List<TypedSpec> letPrefix) {
        List<TypedSpec> seq = new ArrayList<>(letPrefix);
        seq.add(source);
        List<TypedSpec> reduced = inlineBody(seq);
        return reduced.get(reduced.size() - 1);
    }

    public List<TypedSpec> inlineBody(List<TypedSpec> body) {
        // QUERY-level lets β-reduce exactly like callee lets — binders die
        // in the one substitution pass. A relation-typed let ($t = #TDS…#)
        // splices its pipeline into every use; downstream phases never see
        // a let. A TRAILING let IS its value (real pure: the let statement
        // yields it). KNOWN TRADE (audit): a let used twice EVALUATES twice
        // in SQL — for a non-deterministic row set (limit with no total
        // order) the two splices may disagree where real pure's
        // single-evaluation binding could not; CTE sharing is the future fix.
        try (Scope scope = new Scope()) {
            for (int i = 0; i < body.size() - 1; i++) {
                if (!(body.get(i) instanceof TypedLet let)) {
                    throw new NotImplementedException(
                            "only let statements may precede the query expression");
                }
                scope.put(let.name(), reduce(let.value(), scope));
            }
            // graph-tree args are NOT β-reduced (source spelling is the
            // serialize key) — the resolver reads consumed lets through this
            // (engine inScopeVars)
            queryLets.putAll(scope.terms);
            TypedSpec last = body.get(body.size() - 1);
            return List.of(last instanceof TypedLet let
                    ? reduce(let.value(), scope)
                    : reduce(last, scope));
        }
    }

    /** The environment of one &beta; site: each name's REDUCED term and
     * the term's free variables, read once when the term is bound (the
     * substitution reads them at every statement beneath). Open while the
     * site reduces (try-with-resources): its terms are in
     * {@link #reducedTerms} until it closes. */
    private final class Scope implements AutoCloseable {
        final Map<String, TypedSpec> terms = new LinkedHashMap<>();
        final Map<String, java.util.Set<String>> free = new LinkedHashMap<>();
        private final List<TypedSpec> own = new ArrayList<>();

        Scope() {
        }

        /** An inner site: the outer terms are read, not re-held. */
        Scope(Scope outer) {
            terms.putAll(outer.terms);
            free.putAll(outer.free);
        }

        /** Bind {@code name} to a reduced term for the statements beneath. */
        void put(String name, TypedSpec term) {
            put(name, term, FreeVars.of(term));
        }

        /** The same, the term's free variables already known. */
        void put(String name, TypedSpec term, java.util.Set<String> termFree) {
            terms.put(name, term);
            free.put(name, termFree);
            reducedTerms.merge(term, 1, Integer::sum);
            own.add(term);
        }

        @Override
        public void close() {
            for (TypedSpec t : own) {
                reducedTerms.compute(t, (k, c) -> c == null || c <= 1 ? null : c - 1);
            }
        }
    }

    /** {@code body} with the scope substituted, then reduced. */
    private TypedSpec reduce(TypedSpec body, Scope scope) {
        return rewrite(TypedSubst.apply(body, scope.terms, scope.free));
    }

    // =====================================================================
    // The call frame
    // =====================================================================

    /** A store-element IDENTITY call (StoreElementIdentity — the one
     * owner of the shape) is never opened: the resolver roots it as the
     * element's row, the structural readers consume the spelling. */
    private static boolean isStoreElementIdentity(String fqn, List<TypedSpec> args) {
        return com.legend.compiler.spec.typed.StoreElementIdentity.isIdentityCall(fqn, args);
    }

    /** A native that is a TYPING SURFACE only — no body, no evaluation —
     * over a program the model spells under the SAME name (the engine's
     * {@code relationalExtensions()}: registered so the extension argument
     * types everywhere, never evaluated; the corpus loads its Pure body):
     * consumed STRUCTURALLY (a field read over it), the program is compiler
     * input and inlines like any user call, STRICTLY — every field, every
     * let (Phase 5 batch 147, USER: strict first; each engine program the
     * chain meets is a ledger row). Anything else returns {@code src}. */
    private TypedSpec spelledProgramOr(TypedSpec src) {
        // SWITCHED OFF for batch 147 (landed as mechanism + ledger): with the
        // hand-off on, every test that passes a field of the extension record
        // through platform Pure depends on the WHOLE record compiling, and the
        // chain stops at ledger row 18 (function references as values — batch
        // 148's design leg). Re-enabled when the chain compiles end to end.
        if (!HAND_OFF_ON || !(src instanceof TypedNativeCall nc)) {
            return src;
        }
        List<com.legend.compiler.element.TypedFunction> programs =
                specs.ctx().findFunction(nc.callee().qualifiedName()).stream()
                        .filter(f -> !f.isNative() && f.body().isPresent()
                                && f.parameters().size() == nc.args().size())
                        .toList();
        if (programs.size() != 1) {
            return src;
        }
        return inlineCall(new TypedUserCall(programs.get(0), nc.args(), nc.info(), nc.pos()));
    }

    /** The UNROLL BUDGET: expansions one compile may perform before the
     * program is declared too large to unroll. The inliner expands every
     * live arm of every nested match; a program-sized tree walk (the
     * engine's post-processors rewriting a SQL AST — 20 element kinds per
     * match, walkers calling walkers) is exponential in that scheme and
     * never a query. Sibling of the recursion-cycle guard: a loud wall
     * naming the path, never a hang (batch 149, testDb2ColumnRename: ten
     * minutes at 100% CPU once its post-processor typed). */
    static final int UNROLL_BUDGET = 20_000;

    /** The budget's spend, one per inliner (the compile artifact's lifetime). */
    private static final class UnrollBudget {
        private int spent;

        boolean exceeded() {
            return ++spent > UNROLL_BUDGET;
        }
    }

    private final UnrollBudget budget = new UnrollBudget();

    /** ENGINE MACHINERY the platform never unrolls (SYSTEM_PRELUDE_DESIGN §3,
     * last row: "code that needs the engine's internals to produce a value
     * is a wall, named and counted"). USER 2026-09-08: the engine's SQL
     * post-processing is a COMPILER PASS here (post-processors-are-compiler-
     * passes); its Pure implementation — the SQL printer and the
     * PostProcessor registry properties — is walled whole until a design
     * session decides how post-processors work on this platform. Exact
     * FQNs; a reaching program fails at once, naming the wall. */
    // (the five original entries and every walled prelude body now live in
    // WalledBodies.REASONS — ONE list with reasons, batch 173)

    private TypedSpec inlineCall(TypedUserCall call) {
        // THE PICK (untangle step 4a): the implementation table says what runs
        // this declaration. A rule or a form: the call stands as a NATIVE call
        // with its arguments rewritten (the typer mints these; one reaches here
        // only through a program hand-off). A refusal: the wall, by its reason.
        // A subsumed program: an opaque typed value. A body: inlined below.
        com.legend.platform.Implementation row = call.callee().definition() == null ? null
                : specs.ctx().implementations().of(
                        com.legend.model.FunctionId.of(call.callee().definition()));
        if (row instanceof com.legend.platform.Implementation.Intrinsic
                || row instanceof com.legend.platform.Implementation.Form) {
            List<TypedSpec> pargs = new ArrayList<>(call.args().size());
            for (TypedSpec a : call.args()) {
                pargs.add(rewrite(a));
            }
            return NormalizeFolds.foldReflection(
                    new TypedNativeCall(call.callee(), pargs, call.info(), null));
        }
        if (row instanceof com.legend.platform.Implementation.Refused refused
                && refused.reason() != com.legend.platform.Implementation.Reason.MOOT) {
            com.legend.builtin.DecisionProbe.pick(call.callee().definition(), "WALLED-BODY");
            // a version without a row is not a wall: say which it is
            throw new com.legend.error.WalledBodyException(
                    (refused.reason() == com.legend.platform.Implementation.Reason.NO_ROW ? "no row for '" : "walled body '")
                    + call.callee().qualifiedName() + "': " + refused.why());
        }
        if (budget.exceeded()) {
            List<String> path = new ArrayList<>(names);
            java.util.Collections.reverse(path);
            throw new NotImplementedException("unroll budget exceeded (" + UNROLL_BUDGET
                    + " expansions): the program is a tree walk, not a query — "
                    + call.callee().qualifiedName() + " via " + String.join(" -> ", path));
        }
        List<TypedSpec> args = new ArrayList<>(call.args().size());
        for (TypedSpec a : call.args()) {
            args.add(rewrite(a));
        }
        if (configMode || isStoreElementIdentity(call.callee().qualifiedName(), args)) {
            return new TypedUserCall(call.callee(), args, call.info(), call.pos());
        }
        // a SUBSUMED ENGINE PROGRAM (Refused(MOOT) in the table): the body is
        // never spliced — the call stays a typed opaque value, typed by
        // upstream's own declaration; its value is dead by governance test
        if (row instanceof com.legend.platform.Implementation.Refused) {
            com.legend.builtin.DecisionProbe.pick(call.callee().definition(), "SUBSUMED");
            return new TypedUserCall(call.callee(), args, call.info(), call.pos());
        }
        com.legend.builtin.DecisionProbe.pick(call.callee().definition(), "BODY");
        // signatureKey identifies the OVERLOAD — name/arity conflated two
        // same-arity overloads into a false recursion (audit).
        com.legend.model.FunctionId key = call.callee().id();
        String shown = call.callee().qualifiedName() + "/"
                + call.callee().parameters().size();
        int literalSize = args.stream().filter(LiteralUnroll::literalStructure)
                .mapToInt(LiteralUnroll::size).sum();
        if (stack.contains(key)) {
            // TIER 1 RECURSION (LiteralUnroll): a recursive call DESCENDS
            // into a literal argument — strictly smaller than the enclosing
            // activation's, so the unroll is well-founded and bottoms out
            // on the literal's leaves; any other cycle stays the loud wall.
            // The measure is LEXICOGRAPHIC: (literal size, store-argument
            // classes). It descends when the literal size strictly shrinks,
            // or stays equal (a constant literal such as the conversion
            // state rides every level) while a STORE-valued argument of a
            // class no enclosing activation holds enters — ^Alias(
            // relationalElement = getTable(..)) re-enters on the Table row;
            // the class lattice is finite, so the unroll is well-founded.
            // A store value of a class already held would unroll a
            // row-backed tree (tier 2) and stands.
            int enclosing = enclosingLiteralSize(key);
            boolean literalDescent = literalSize < enclosing;
            boolean classDescent = literalSize == enclosing
                    && !argClasses(args).isEmpty()
                    && java.util.Collections.disjoint(argClasses(args), enclosingArgClasses(key));
            if (!literalDescent && !classDescent) {
                // the ENCLOSING activation stands whole (a half-inlined
                // recursive helper would hand the SQL channel a different
                // program than the host channel runs — the pk-inference
                // helpers' composition recursion); dead arms never reach
                // here since the static re-dispatch prunes them
                List<String> path = new ArrayList<>(names);
                java.util.Collections.reverse(path);
                throw new NotImplementedException("recursion cycle involving " + shown
                        + " (" + String.join(" -> ", path) + " -> " + shown
                        + ") — recursive functions cannot lower to SQL"
                        + (literalSize == 0 ? "" : " (the call does not descend into its literal argument)"));
            }
        }
        stack.push(key);
        literalSizes.push(literalSize);
        argClassSets.push(argClasses(args));
        names.push(shown);
        try {
            List<TypedSpec> body;
            try {
                body = specs.compile(call.callee()).body();
            } catch (TypeInferenceException e) {
                // the INLINE PATH names which caller demanded this body — a
                // wall in a dead arm's callee reads as the arm, not as a
                // random library function
                List<String> path = new ArrayList<>(names);
                java.util.Collections.reverse(path);
                throw new TypeInferenceException(e.getMessage()
                        + " [inlined via " + String.join(" -> ", path) + "]", e);
            }
            // A relation param accepts a SUPERSET schema (covariant call);
            // the spliced body then carries the caller's extra columns in
            // SQL while the call site is typed by the DECLARED return —
            // conform by EMISSION with a select down to the declared columns.
            boolean widened = false;
            TypedSpec reduced;
            try (Scope callEnv = new Scope()) {
                for (int i = 0; i < call.callee().parameters().size(); i++) {
                    callEnv.put(call.callee().parameters().get(i).name(), args.get(i));
                    if (rowType(call.callee().parameters().get(i).type()) instanceof
                                com.legend.compiler.element.type.Type.RelationType dp
                            && rowType(args.get(i).info().type()) instanceof
                                com.legend.compiler.element.type.Type.RelationType ap
                            && ap.columns().size() > dp.columns().size()) {
                        widened = true;
                    }
                }
                reduced = instantiate(deepFoldInlined(
                        reduceStatements(body, callEnv)), call, args);
            }
            if (widened && com.legend.compiler.element.type.Type
                    .relationSchema(call.info().type())
                    instanceof com.legend.compiler.element.type.Type.RelationType rt) {
                reduced = new com.legend.compiler.spec.typed.TypedSelect(reduced,
                        rt.columns().stream()
                                .map(com.legend.compiler.element.type.Type.Column::name)
                                .toList(),
                        call.info());
            }
            // a lifted VIEW (E.5) inlines like every function AND keeps its
            // name: the engine's ViewSelectSQLQuery — the relation is named
            // by the view where it stands (TypedViewRelation)
            if (com.legend.compiler.spec.typed.TypedViewRelation
                    .liftedViewName(call.callee()) != null) {
                reduced = com.legend.compiler.spec.typed.TypedViewRelation
                        .of(call.callee(), reduced);
            }
            return reduced;
        } catch (NotImplementedException e) {
            // The body cannot β-reduce (a recursion cycle unwinding one
            // level, a non-let intermediate statement) — the CALL STANDS
            // with rewritten args. Channels that can run calls consume it
            // (host call frames; the plan seam reads postprocessor config
            // structurally); SQL lowering keeps its loud TypedUserCall
            // frontier wall.
            return new TypedUserCall(call.callee(), args, call.info(), call.pos());
        } finally {
            stack.pop();
            literalSizes.pop();
            argClassSets.pop();
            names.pop();
        }
    }

    /**
     * GENERIC INSTANTIATION at the inlining seam — monomorphization at the
     * application (Phase 5 batch 147, row 15), applied to the WHOLE body:
     * a callee typed over its own type parameters leaves every node of its
     * body stamped with them ({@code $this.second : V} inside
     * {@code Pair<U,V>.toString()}); the application binds them by unifying
     * the declared parameter types against the argument types
     * ({@code V := Any} for a {@code Pair<String, Any>} receiver) and every
     * type-variable-stamped node resolves under those bindings, so the
     * lowering never dispatches on a type variable (a stale {@code V} fell
     * to toString's plain-cast arm and printed {@code <a, "b">} — batch 152,
     * the module's Pair.toString body running for the first time). A stamp
     * the bindings cannot resolve stays as the body's own (the lowering's
     * boundary refuses it loudly). The root additionally takes the CALL
     * SITE's type, the typer's own instantiation ({@link #instantiateRoot}).
     */
    private TypedSpec instantiate(TypedSpec reduced, TypedUserCall call,
            List<TypedSpec> args) {
        TypedSpec out = reduced;
        com.legend.compiler.element.TypedFunction callee = call.callee();
        if (!callee.typeParameters().isEmpty()) {
            InferenceKernel kernel = new InferenceKernel(specs.ctx());
            Bindings b = new Bindings();
            for (int i = 0; i < callee.parameters().size() && i < args.size(); i++) {
                try {
                    kernel.unify(callee.parameters().get(i).type(),
                            args.get(i).info().type(), b);
                } catch (TypeInferenceException e) {
                    // admitted by another of the resolver's rules (supertype
                    // instantiation, relation widening): that binding stays
                    // open and its stamps stay the body's own
                }
            }
            if (!(call.info().type() instanceof com.legend.compiler.element.type.Type.TypeVar)) {
                try {
                    kernel.unify(callee.returnType(), call.info().type(), b);
                } catch (TypeInferenceException e) {
                    // same
                }
            }
            out = redispatch(resolveStamps(out, kernel, b));
        }
        return instantiateRoot(out, call.info());
    }

    /**
     * The derived-shadow rule at INSTANTIATION time: the typer routes a
     * receiver's own qualified property before an Any-first native
     * ({@code Typer.derivedShadow}) when it types a call — but inside a
     * generic body the receiver was a type variable ({@code $this.second :
     * V}), so {@code ->toString()} bound to the native. Once the
     * application makes the receiver concrete (a nested {@code Pair}), the
     * same rule applies: the native call becomes the class's own body,
     * inlined in turn (testFormatPair's {@code <dog, <cat, mouse>>}).
     */
    private TypedSpec redispatch(TypedSpec n) {
        TypedSpec walked = n.mapChildren(this::redispatch);
        if (!(walked instanceof TypedNativeCall c) || c.args().isEmpty()
                || c.callee().parameters().isEmpty()
                || !com.legend.compiler.element.type.PlatformTypes.isAny(
                        c.callee().parameters().get(0).type())) {
            return walked;
        }
        TypedSpec recv = c.args().get(0);
        String classFqn = recv.info().type() instanceof com.legend.compiler.element.type.Type.ClassType ct
                ? ct.fqn()
                : recv.info().type() instanceof com.legend.compiler.element.type.Type.GenericType g
                        ? g.rawFqn() : null;
        if (classFqn == null
                || recv.info().multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded rb
                        && rb.isMany()) {
            return walked;
        }
        String fn = c.callee().qualifiedName();
        int cut = fn.lastIndexOf("::");
        String simple = cut < 0 ? fn : fn.substring(cut + 2);
        if (!(specs.ctx().findProperty(classFqn, simple).orElse(null)
                        instanceof com.legend.compiler.element.Property.Derived d)
                || d.parameters().size() != c.args().size() - 1) {
            return walked;
        }
        for (com.legend.compiler.element.TypedFunction body
                : specs.ctx().findFunction(d.bodyFunctionFqn())) {
            if (body.parameters().size() == c.args().size()) {
                return inlineCall(new TypedUserCall(body, c.args(), c.info(), c.pos()));
            }
        }
        return walked;
    }

    private static final Bindings NONE = new Bindings();

    private static TypedSpec resolveStamps(TypedSpec n, InferenceKernel kernel, Bindings b) {
        TypedSpec walked = n.mapChildren(k -> resolveStamps(k, kernel, b));
        com.legend.compiler.element.type.Type t = walked.info().type();
        // a stamp with no type variable is the body's own truth; one the
        // application left partly open (a variable it never bound) stays too
        if (!kernel.hasFreeTypeVars(t, NONE) || kernel.hasFreeTypeVars(t, b)) {
            return walked;
        }
        com.legend.compiler.element.type.Type r = kernel.resolve(t, b);
        return r.equals(t) ? walked
                : walked.withInfo(new ExprType(r, walked.info().multiplicity()));
    }

    /** The ROOT's instantiation: a type-variable-stamped root takes the
     * CALL SITE's info (the typer bound it from the arguments); a concrete
     * one is the body's own truth. */
    private static TypedSpec instantiateRoot(TypedSpec reduced, ExprType callInfo) {
        if (!(reduced.info().type() instanceof com.legend.compiler.element.type.Type.TypeVar)
                || callInfo.type() instanceof com.legend.compiler.element.type.Type.TypeVar) {
            return reduced;
        }
        ExprType ni = new ExprType(callInfo.type(), reduced.info().multiplicity());
        return switch (reduced) {
            case TypedNativeCall c -> new TypedNativeCall(c.callee(), c.args(), ni, c.pos());
            case com.legend.compiler.spec.typed.TypedCollection tc -> tc.withInfo(ni);
            case TypedUserCall uc -> new TypedUserCall(uc.callee(), uc.args(), ni, uc.pos());
            default -> reduced;
        };
    }

    /** The declared classes of the NON-literal arguments (store values;
     * a literal or non-class argument contributes nothing). */
    private static java.util.Set<String> argClasses(List<TypedSpec> args) {
        java.util.Set<String> out = new java.util.LinkedHashSet<>();
        for (TypedSpec a : args) {
            if (!LiteralUnroll.literalStructure(a)
                    && a.info().type() instanceof com.legend.compiler.element.type.Type.ClassType ct) {
                out.add(ct.fqn());
            }
        }
        return out;
    }

    /** The union of the argument classes of every enclosing activation of {@code key}. */
    private java.util.Set<String> enclosingArgClasses(com.legend.model.FunctionId key) {
        java.util.Set<String> out = new java.util.LinkedHashSet<>();
        java.util.Iterator<com.legend.model.FunctionId> k = stack.iterator();
        java.util.Iterator<java.util.Set<String>> c = argClassSets.iterator();
        while (k.hasNext()) {
            com.legend.model.FunctionId at = k.next();
            java.util.Set<String> classes = c.next();
            if (at.equals(key)) {
                out.addAll(classes);
            }
        }
        return out;
    }

    /** The arms a value of static type {@code t} can reach: the arm's class
     * is a super- or subtype of {@code t}, or some model class descends from
     * both (multiple inheritance). Any / non-class inputs keep every arm. */
    private List<com.legend.compiler.spec.typed.TypedMatchRuntime.Arm> liveArms(
            com.legend.compiler.spec.typed.TypedMatchRuntime mr, TypedSpec input) {
        com.legend.compiler.element.type.Type t = input.info().type();
        List<com.legend.compiler.spec.typed.TypedMatchRuntime.Arm> live = new ArrayList<>();
        if (t instanceof com.legend.compiler.element.type.Type.Primitive p) {
            // a PRIMITIVE value's class is its static type (no subclassing):
            // the arms at or above it in the primitive lattice
            for (var a : mr.arms()) {
                if (a.typeFqn().equals(com.legend.compiler.element.type.PlatformTypes.ANY)
                        || specs.ctx().isSubtype(p.qualifiedName(), a.typeFqn())) {
                    live.add(a);
                }
            }
            return live.isEmpty() ? mr.arms() : live;
        }
        if (!(t instanceof com.legend.compiler.element.type.Type.ClassType ct)
                || ct.fqn().equals(com.legend.compiler.element.type.PlatformTypes.ANY)) {
            return mr.arms();
        }
        var ctx = specs.ctx();
        // A SYSTEM-STORE ROW dispatches over the relation's KINDS: the
        // classes the system mapping binds beneath the declared class
        // (Table's rows are Table or View — never ViewSelectSQLQuery, a
        // class only programs construct). The store's schema is the fact;
        // the compiler reads it, the database never has to pick among
        // arms no row can take.
        java.util.Set<String> rows = systemRowClasses(input, ct.fqn());
        if (!rows.isEmpty()) {
            for (var a : mr.arms()) {
                String armType = a.typeFqn();
                if (armType.equals(com.legend.compiler.element.type.PlatformTypes.ANY)
                        || rows.stream().anyMatch(r -> declaredSubtype(ctx, r, armType, new java.util.HashSet<>()))) {
                    live.add(a);
                }
            }
            return live.isEmpty() ? mr.arms() : live;
        }
        for (var a : mr.arms()) {
            String armType = a.typeFqn();
            boolean related = armType.equals(com.legend.compiler.element.type.PlatformTypes.ANY)
                    || ctx.isSubtype(ct.fqn(), armType) || ctx.isSubtype(armType, ct.fqn());
            if (!related) {
                // the multiple-inheritance scan reads DECLARATIONS only: a
                // compiled subtype check over every model class would
                // compile every class — including poisoned ones (a corpus
                // protocol class naming an unloaded type), which is not this
                // decision's business. The declared-ancestor index is built
                // ONCE per inliner: this scan runs per arm per rewrite, and
                // walking every class's generalizations each time turned the
                // post-processor bodies' nested matches into a 10-minute hang
                // (batch 149, testDb2ColumnRename).
                for (java.util.Set<String> anc : declaredAncestors(ctx).values()) {
                    if (anc.contains(armType) && anc.contains(ct.fqn())) {
                        related = true;
                        break;
                    }
                }
            }
            if (related) {
                live.add(a);
            }
        }
        return live.isEmpty() ? mr.arms() : live;
    }

    /** The classes a SYSTEM-STORE read of declared class {@code declared}
     * can yield: the system mapping's bound classes at or beneath it.
     * Empty when {@code input} is not a navigation rooted at an element
     * reference (a metamodel row), or no bound class lies beneath. */
    private java.util.Set<String> systemRowClasses(TypedSpec input, String declared) {
        TypedSpec at = input;
        boolean navigated = false;
        while (true) {
            switch (at) {
                case TypedNativeCall c when !c.args().isEmpty() -> at = c.args().get(0);
                case com.legend.compiler.spec.typed.TypedFilter f -> at = f.source();
                case TypedMap m -> at = m.source();
                case com.legend.compiler.spec.typed.TypedPropertyAccess pa -> {
                    at = pa.source();
                    navigated = true;
                }
                case com.legend.compiler.spec.typed.TypedPackageableRef pr -> {
                    if (!navigated) {
                        return java.util.Set.of();
                    }
                    var ctx = specs.ctx();
                    var md = ctx.findMapping(com.legend.builtin.SystemMetamodel.MAPPING_FQN).orElse(null);
                    if (md == null) {
                        return java.util.Set.of();
                    }
                    java.util.Set<String> out = new java.util.LinkedHashSet<>();
                    for (var cb : md.classBindings()) {
                        if (declaredSubtype(ctx, cb.classFqn(), declared, new java.util.HashSet<>())) {
                            out.add(cb.classFqn());
                        }
                    }
                    return out;
                }
                default -> {
                    return java.util.Set.of();
                }
            }
        }
    }

    /** {@code cls <: sup} by the DECLARED generalizations alone (the
     * ModelContext rule — no class is compiled). */
    private static boolean declaredSubtype(com.legend.compiler.element.ModelContext ctx,
            String cls, String sup, java.util.Set<String> visited) {
        return ctx.isDeclaredSubtype(cls, sup);
    }

    /** Every model element's DECLARED ancestors (itself included), built
     * once per inliner — the model does not change while it inlines. */
    private java.util.Map<String, java.util.Set<String>> declaredAncestors(
            com.legend.compiler.element.ModelContext ctx) {
        if (declaredAncestors.isEmpty()) {
            java.util.Map<String, java.util.Set<String>> out = declaredAncestors;
            for (String cls : ctx.elementFqns()) {
                java.util.Set<String> anc = new java.util.HashSet<>();
                java.util.ArrayDeque<String> work = new java.util.ArrayDeque<>();
                work.add(cls);
                while (!work.isEmpty()) {
                    String c = work.poll();
                    if (!anc.add(c)) {
                        continue;
                    }
                    var cd = ctx.findClassDefinition(c);
                    if (cd.isEmpty()) {
                        continue;
                    }
                    for (com.legend.protocol.TypeExpression s : cd.get().superClasses()) {
                        String name = s instanceof com.legend.protocol.TypeExpression.NameRef nr ? nr.name()
                                : s instanceof com.legend.protocol.TypeExpression.Generic g ? g.name() : null;
                        if (name != null) {
                            work.add(name);
                        }
                    }
                }
                out.put(cls, anc);
            }
        }
        return declaredAncestors;
    }

    private final java.util.Map<String, java.util.Set<String>> declaredAncestors = new java.util.HashMap<>();

    /** The literal-argument size of the innermost enclosing activation of
     * {@code key} (the stacks are pushed together). */
    private int enclosingLiteralSize(com.legend.model.FunctionId key) {
        java.util.Iterator<com.legend.model.FunctionId> k = stack.iterator();
        java.util.Iterator<Integer> s = literalSizes.iterator();
        while (k.hasNext()) {
            com.legend.model.FunctionId at = k.next();
            int size = s.next();
            if (at.equals(key)) {
                return size;
            }
        }
        throw new IllegalStateException("no enclosing activation of " + key);
    }

    /** DIRECT self-recursion in the callee's (resolved) definition body —
     * the reason {@link #inlineCall} let the call stand, recovered at the
     * resolver's TypedUserCall wall so its message names the cycle.
     * Indirect cycles keep the generic did-not-&beta;-reduce line (naming
     * them needs the whole call graph). Lives HERE, not at the wall: the
     * resolver never touches the untyped value-spec AST (invariant 6c). */
    public static boolean selfRecursive(
            com.legend.compiler.element.TypedFunction callee) {
        if (!(callee.definition()
                instanceof com.legend.model.FunctionDefinition fd)) {
            return false;
        }
        java.util.ArrayDeque<com.legend.protocol.spec.ValueSpecification> work =
                new java.util.ArrayDeque<>(fd.body());
        while (!work.isEmpty()) {
            var vs = work.poll();
            if (vs instanceof com.legend.protocol.spec.AppliedFunction af
                    && af.function().equals(callee.qualifiedName())) {
                return true;
            }
            work.addAll(vs.children());
        }
        return false;
    }

    /** The row type of a relation-valued type: bare RelationType, or Relation<(...)>. */
    private static com.legend.compiler.element.type.Type rowType(
            com.legend.compiler.element.type.Type t) {
        if (t instanceof com.legend.compiler.element.type.Type.GenericType g
                && g.rawFqn().equals("meta::pure::metamodel::relation::Relation")
                && g.arguments().size() == 1) {
            return g.arguments().get(0);
        }
        return t;
    }

    /**
     * A statement list under an environment: intermediate lets substitute
     * FORWARD (their values see the bindings so far); the last statement is
     * the value. One expression comes out.
     */
    private TypedSpec reduceStatements(List<TypedSpec> body, Scope env) {
        try (Scope scope = new Scope(env)) {
            for (int i = 0; i < body.size() - 1; i++) {
                if (!(body.get(i) instanceof TypedLet let)) {
                    // a non-let intermediate whose value FOLDS to a literal
                    // structure is dead (a self-check whose asserts folded away
                    // — toPostgresModel's converter registry); anything else
                    // may raise and stays loud
                    TypedSpec reduced = reduce(body.get(i), scope);
                    if (LiteralUnroll.literalStructure(reduced)) {
                        continue;
                    }
                    TypedSpec residue = reduced;
                    while (residue instanceof com.legend.compiler.spec.typed.TypedCollection tc
                            && tc.elements().stream().anyMatch(e -> !LiteralUnroll.literalStructure(e))) {
                        residue = tc.elements().stream()
                                .filter(e -> !LiteralUnroll.literalStructure(e)).findFirst().orElse(tc);
                    }
                    throw new NotImplementedException("a non-let intermediate statement ("
                            + body.get(i).getClass().getSimpleName() + ", reduced to "
                            + residue.getClass().getSimpleName()
                            + (residue instanceof TypedNativeCall rc ? " " + rc.callee().qualifiedName() : "")
                            + (residue instanceof TypedMap rm ? " over " + rm.source().getClass().getSimpleName()
                                    + (rm.source() instanceof TypedNativeCall sc ? " " + sc.callee().qualifiedName()
                                            + " of " + sc.args().get(0).getClass().getSimpleName() : "") : "")
                            + ") in an inlined function body is not supported");
                }
                scope.put(let.name(), reduce(let.value(), scope));
            }
            // A TRAILING let IS its value (real pure: the let statement yields
            // it) — `{ let r = $x + 100 }` returns the sum, and no let node
            // survives into H/I.
            TypedSpec last = body.get(body.size() - 1);
            return last instanceof TypedLet let
                    ? reduce(let.value(), scope)
                    : reduce(last, scope);
        }
    }

    /** &beta;-reduce {@code eval(<literal lambda>, args)}. */
    private TypedSpec reduceEval(TypedEval ev, List<TypedSpec> args, TypedLambda lam) {
        if (lam.parameters().size() != args.size()) {
            throw new IllegalStateException("eval arity mismatch after inlining: "
                    + lam.parameters().size() + " parameter(s), " + args.size()
                    + " argument(s) — G should have rejected this");
        }
        try (Scope env = new Scope()) {
            for (int i = 0; i < args.size(); i++) {
                env.put(lam.parameters().get(i), args.get(i));
            }
            return reduceStatements(lam.body(), env);
        }
    }

    // =====================================================================
    // The rewriter — exhaustive over the sealed vocabulary (javac-enforced)
    // =====================================================================

    /**
     * QUOTED code and the TIER 1 literal unroll — the arms that must act
     * BEFORE a binder's body is rewritten: a quoted lambda / deactivate
     * subject substitutes variables but never folds; inside an inlined
     * body (never at the query's own level — a user-authored if/map keeps
     * its SQL shape: engine parity, witnesses testIfIncludingQualifiers
     * and the keyless-ctor-under-lambda decline), a literal condition
     * takes ONLY its branch, a literal collection applies the map/filter
     * lambda per element, a literal match input picks its arm — so a
     * recursive call inside descends on the literal instead of standing
     * on an unbound parameter. Empty when the node is none of these.
     */
    private Optional<TypedSpec> literalArms(TypedSpec n) {
        if (n instanceof TypedLambda l && l.quoted()) {
            quotedFrames.push(n);
            try {
                return Optional.of(lambda(l));
            } finally {
                quotedFrames.pop();
            }
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedDeactivate d) {
            quotedFrames.push(n);
            try {
                return Optional.of(d.mapChildren(this::rewrite));
            } finally {
                quotedFrames.pop();
            }
        }
        if (!quotedFrames.isEmpty() || (stack.isEmpty() && !verdictSource)) {
            return Optional.empty();
        }
        return switch (n) {
            case com.legend.compiler.spec.typed.TypedIf i -> {
                TypedSpec cond = rewrite(i.condition());
                if (cond instanceof TypedCBoolean lit) {
                    yield Optional.of(lit.value() ? rewrite(i.thenBranch())
                            : i.elseBranch().map(this::rewrite).orElseGet(() ->
                                    new com.legend.compiler.spec.typed.TypedCollection(
                                            List.of(), i.info())));
                }
                TypedSpec then = rewrite(i.thenBranch());
                Optional<TypedSpec> els = i.elseBranch().map(this::rewrite);
                yield Optional.of(cond == i.condition() && then == i.thenBranch()
                        && els.equals(i.elseBranch()) ? i
                        : new com.legend.compiler.spec.typed.TypedIf(cond, then, els, i.info()));
            }
            case TypedMap m -> {
                // a [*]-returning typing-surface native read through an
                // AUTO-MAP ($exts.routerExtensions()) hands off to its program
                TypedSpec src = spelledProgramOr(rewrite(m.source()));
                // a SPELLED collection (its elements may be any expression —
                // lambdas, standing calls: β-substitution is exact for pure
                // values) applies the mapper per element
                if (LiteralUnroll.spelledList(src) && m.mapper().parameters().size() == 1) {
                    // pure's map CONCATENATES the per-element results: a
                    // result that is neither a spelled collection nor exactly
                    // one value (a [*] read of an unspelled element) cannot
                    // be spliced into one spelled list — the map stands
                    List<TypedSpec> out = new ArrayList<>();
                    boolean spliceable = true;
                    for (TypedSpec e : LiteralUnroll.elements(src)) {
                        TypedSpec r;
                        try (Scope inner = new Scope()) {
                            inner.put(m.mapper().parameters().get(0), e);
                            r = reduceStatements(m.mapper().body(), inner);
                        }
                        if (!(r instanceof com.legend.compiler.spec.typed.TypedCollection)
                                && !(r.info().multiplicity() instanceof
                                        com.legend.compiler.element.type.Multiplicity.Bounded rb
                                        && rb.lower() == 1 && Integer.valueOf(1).equals(rb.upper()))) {
                            spliceable = false;
                            break;
                        }
                        out.addAll(LiteralUnroll.elements(r));
                    }
                    if (spliceable) {
                        yield Optional.of(new com.legend.compiler.spec.typed.TypedCollection(out, m.info()));
                    }
                }
                TypedLambda mapper = lambda(m.mapper());
                yield Optional.of(src == m.source() && mapper == m.mapper() ? m
                        : m.withChildren(List.of(src, mapper)));
            }
            case com.legend.compiler.spec.typed.TypedFilter f -> {
                TypedSpec src = rewrite(f.source());
                if (LiteralUnroll.spelledList(src) && f.predicate().parameters().size() == 1) {
                    // CONDITIONAL MEMBERSHIP (WORLD_MAP §4): a predicate that
                    // stays a SQL boolean after the element is substituted
                    // (a computed value inside it) keeps its element under
                    // that condition — if(cond, |e, |[]) — and the database
                    // decides; the list shape is still the compiler's
                    List<TypedSpec> out = new ArrayList<>();
                    for (TypedSpec e : LiteralUnroll.elements(src)) {
                        TypedSpec pred;
                        try (Scope inner = new Scope()) {
                            inner.put(f.predicate().parameters().get(0), e);
                            pred = reduceStatements(f.predicate().body(), inner);
                        }
                        if (pred instanceof TypedCBoolean keep) {
                            if (keep.value()) {
                                out.add(e);
                            }
                        } else {
                            ExprType guarded = new ExprType(e.info().type(),
                                    com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_ONE);
                            out.add(new com.legend.compiler.spec.typed.TypedIf(pred, e,
                                    Optional.of(new com.legend.compiler.spec.typed.TypedCollection(
                                            List.of(), guarded)), guarded));
                        }
                    }
                    // the filtered list's ELEMENT type is the source's: an
                    // inlined generic's `T[*]` stamp would otherwise ride an
                    // empty fold result to the lowering (firstNotNull<T>)
                    ExprType fi = f.info().type() instanceof com.legend.compiler.element.type.Type.TypeVar
                            ? new ExprType(src.info().type(), f.info().multiplicity()) : f.info();
                    yield Optional.of(new com.legend.compiler.spec.typed.TypedCollection(out, fi));
                }
                TypedLambda pred = lambda(f.predicate());
                yield Optional.of(src == f.source() && pred == f.predicate() ? f
                        : f.withChildren(List.of(src, pred)));
            }
            // fold over a SPELLED list unrolls (WORLD_MAP §4 list shape): the
            // accumulator is reduced element by element at compile time —
            // the database never sees a FoldCall whose accumulator is a
            // constructed instance (toPostgresModel's and/or chains)
            case com.legend.compiler.spec.typed.TypedFold fd
                    when fd.reducer().parameters().size() == 2 -> {
                TypedSpec src = rewrite(fd.source());
                if (!LiteralUnroll.spelledList(src)) {
                    yield Optional.empty();
                }
                TypedSpec acc = rewrite(fd.init());
                // the accumulator grows around its previous value: its free
                // variables are read step by step, never the whole again
                java.util.Set<String> accFree = FreeVars.of(acc);
                for (TypedSpec e : LiteralUnroll.elements(src)) {
                    TypedSpec soFar = acc;
                    java.util.Set<String> soFarFree = accFree;
                    try (Scope inner = new Scope()) {
                        inner.put(fd.reducer().parameters().get(0), e);
                        inner.put(fd.reducer().parameters().get(1), soFar, soFarFree);
                        acc = reduceStatements(fd.reducer().body(), inner);
                    }
                    accFree = FreeVars.of(acc, soFar, soFarFree);
                }
                yield Optional.of(acc);
            }
            // the COLLECTION groupBy over a spelled collection whose key
            // lambda folds per element: newMap(pair(key, ^List(values)) …)
            // — the map's SHAPE is the compiler's (WORLD_MAP §4)
            case TypedNativeCall gb when (com.legend.builtin.Pure.AT_COLLECTION_GROUP_BY.contains(gb.callee().id()) || com.legend.builtin.Pure.AT_TDS_GROUP_BY.contains(gb.callee().id()) || com.legend.builtin.Pure.AT_RELATION_GROUP_BY.contains(gb.callee().id())) && gb.args().size() == 2
                    && gb.args().get(1) instanceof TypedLambda keyFn
                    && keyFn.parameters().size() == 1 -> {
                TypedSpec src = rewrite(gb.args().get(0));
                // a SPELLED collection (elements may be any expression — the
                // registry's pairs carry lambdas); only the KEYS must fold
                if (!LiteralUnroll.spelledList(src)) {
                    TypedLambda kf = lambda(keyFn);
                    yield Optional.of(src == gb.args().get(0) && kf == keyFn ? gb
                            : gb.withChildren(List.of(src, kf)));
                }
                Map<Object, List<TypedSpec>> groups = new LinkedHashMap<>();
                Map<Object, TypedSpec> keyNodes = new LinkedHashMap<>();
                for (TypedSpec e : LiteralUnroll.elements(src)) {
                    TypedSpec key;
                    try (Scope inner = new Scope()) {
                        inner.put(keyFn.parameters().get(0), e);
                        key = reduceStatements(keyFn.body(), inner);
                    }
                    Optional<Object> k = LiteralUnroll.scalarValue(key);
                    if (k.isEmpty()) {
                        yield Optional.of(gb.withChildren(List.of(src, lambda(keyFn))));
                    }
                    groups.computeIfAbsent(k.get(), x -> new ArrayList<>()).add(e);
                    keyNodes.putIfAbsent(k.get(), key);
                }
                if (!(gb.info().type() instanceof com.legend.compiler.element.type.Type.GenericType mapT)
                        || mapT.arguments().size() != 2) {
                    yield Optional.of(gb.withChildren(List.of(src, lambda(keyFn))));
                }
                var pairFn = specs.ctx().findFunction("meta::pure::functions::collection::pair").get(0);
                var newMapFn = specs.ctx().findFunction("meta::pure::functions::collection::newMap").stream()
                        .filter(f -> f.parameters().size() == 1).findFirst().orElseThrow();
                ExprType listInfo = new ExprType(mapT.arguments().get(1),
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
                ExprType pairInfo = new ExprType(new com.legend.compiler.element.type.Type.GenericType(
                        com.legend.compiler.element.type.PlatformTypes.PAIR, mapT.arguments(),
                        mapT.multArguments()), com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
                List<TypedSpec> pairs = new ArrayList<>();
                for (var g : groups.entrySet()) {
                    ExprType valuesInfo = new ExprType(g.getValue().get(0).info().type(),
                            com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_MANY);
                    TypedSpec list = new com.legend.compiler.spec.typed.TypedNewInstance(
                            com.legend.compiler.element.type.PlatformTypes.LIST,
                            Map.of("values", new com.legend.compiler.spec.typed.TypedCollection(
                                    g.getValue(), valuesInfo)), listInfo);
                    pairs.add(new TypedNativeCall(pairFn, List.of(keyNodes.get(g.getKey()), list), pairInfo));
                }
                yield Optional.of(new TypedNativeCall(newMapFn, List.of(
                        new com.legend.compiler.spec.typed.TypedCollection(pairs, new ExprType(
                                pairInfo.type(), com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_MANY))),
                        gb.info()));
            }
            case com.legend.compiler.spec.typed.TypedMatchRuntime mr -> {
                TypedSpec input = rewrite(mr.input());
                Optional<TypedSpec> extra = mr.extra().map(this::rewrite);
                // a DYNAMIC arm prefix (extension-contributed arms) must fold
                // to [] before the spelled arms may dispatch
                Optional<TypedSpec> dyn = mr.dynamicArms().map(this::rewrite);
                boolean dynEmpty = dyn.isEmpty()
                        || dyn.get() instanceof com.legend.compiler.spec.typed.TypedCollection dc
                                && LiteralUnroll.elements(dc).isEmpty();
                Optional<com.legend.compiler.spec.typed.TypedMatchRuntime.Arm> arm =
                        dynEmpty ? LiteralUnroll.arm(mr, input, specs.ctx()) : Optional.empty();
                if (arm.isPresent()) {
                    yield Optional.of(dispatchArm(arm.get().param(), mr.extraParam(),
                            arm.get().body(), input, extra));
                }
                // STATIC RE-DISPATCH on the input's declared type: an arm whose
                // class no model class shares with the input's static type can
                // never match at runtime — it is dead and is NOT rewritten (a
                // dead arm's callees may not even type: the join-tree arms
                // reach engine sqlQueryToString helpers). One survivor
                // dispatches like a literal; several keep the runtime match.
                List<com.legend.compiler.spec.typed.TypedMatchRuntime.Arm> live = dynEmpty
                        ? liveArms(mr, input) : mr.arms();
                if (dynEmpty && live.size() == 1 && mr.arms().size() > 1
                        && input.info().multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded ib
                        && ib.lower() == 1 && Integer.valueOf(1).equals(ib.upper())) {
                    yield Optional.of(dispatchArm(live.get(0).param(), mr.extraParam(),
                            live.get(0).body(), input, extra));
                }
                List<TypedSpec> kids = new ArrayList<>();
                kids.add(input);
                extra.ifPresent(kids::add);
                dyn.ifPresent(kids::add);
                for (com.legend.compiler.spec.typed.TypedMatchRuntime.Arm a : mr.arms()) {
                    kids.add(live.contains(a) ? rewrite(a.body()) : a.body());
                }
                yield Optional.of(sameRefs(kids, mr.children()) ? mr : mr.withChildren(kids));
            }
            default -> Optional.empty();
        };
    }

    /** Deep literal-if prune over an INLINED body (see
     * NormalizeFolds.foldInlined — engine parity keeps user-authored
     * query ifs; inlined platform plumbing folds). */
    private static TypedSpec deepFoldInlined(TypedSpec n) {
        java.util.List<TypedSpec> kids = n.children();
        if (!kids.isEmpty()) {
            java.util.List<TypedSpec> out = new java.util.ArrayList<>(kids.size());
            boolean changed = false;
            for (TypedSpec k : kids) {
                TypedSpec f = deepFoldInlined(k);
                changed |= f != k;
                out.add(f);
            }
            if (changed) {
                n = n.withChildren(out);
            }
        }
        return NormalizeFolds.foldInlined(n);
    }

    private TypedSpec rewrite(TypedSpec n) {
        if (reducedTerms.containsKey(n)) {
            return n;
        }
        if (hook != null) {
            TypedSpec h = hook.apply(n, bound.keySet());
            if (h != n) {
                return rewrite(h);
            }
        }
        // literal-structure folds (tier 1): exact, or the node itself —
        // never inside quoted code
        TypedSpec r = rewriteSwitch(n);
        return quotedFrames.isEmpty() ? LiteralUnroll.fold(r, specs.ctx()) : r;
    }

    private TypedSpec rewriteSwitch(TypedSpec n) {
        Optional<TypedSpec> quotedOrUnrolled = literalArms(n);
        if (quotedOrUnrolled.isPresent()) {
            return quotedOrUnrolled.get();
        }
        return switch (n) {
            case TypedUserCall uc -> inlineCall(uc);

            // pair(a, b).first / .second — a STRUCTURAL read of a pair the
            // substitution made visible (a helper returning
            // pair($plan, $plan->planToString(...)), read through a query
            // let): the component itself; no pair value is ever built
            case com.legend.compiler.spec.typed.TypedPropertyAccess pa -> {
                TypedSpec src = spelledProgramOr(rewrite(pa.source()));
                if (src instanceof com.legend.compiler.spec.typed.TypedNativeCall pc
                        && pc.args().size() == 2
                        && pc.callee().definition() != null
                        && pc.callee().id().equals(
                                com.legend.model.FunctionId.of(com.legend.builtin.Pure.PAIR__U_1__V_1))
                        && (pa.property().equals("first")
                                || pa.property().equals("second"))) {
                    yield pc.args().get(pa.property().equals("first") ? 0 : 1);
                }
                yield src == pa.source() ? pa
                        : new com.legend.compiler.spec.typed.TypedPropertyAccess(
                                src, pa.property(), pa.info());
            }

            case TypedEval ev -> {
                TypedSpec fn = rewrite(ev.fn());
                List<TypedSpec> args = list(ev.args());
                yield fn instanceof TypedLambda lam
                        ? reduceEval(ev, args, lam)
                        : fn == ev.fn() && sameRefs(args, ev.args())
                                ? ev
                                : new TypedEval(fn, args, ev.info());
            }

            // Postprocessor CONFIG is consumed STRUCTURALLY at the plan
            // seam (mapper extraction reads schema()/getTable() call
            // shapes) — inside the property, VARIABLES still substitute
            // (the frame's bindings must reach the extraction) but USER
            // CALLS STAND, so the corpus's recursive getSchema/getTable
            // helpers never hit the recursion wall (the execute()-runtime
            // orchestration-position rule, one property deeper).
            // The same rule for every CLOSURE a spelled record holds (Phase 5
            // batch 147): a lambda stored as a field value is a VALUE — strict
            // Pure evaluates the record's fields, not the lambda's body; the
            // body compiles when the closure is APPLIED (the extension record's
            // connectionEquality arms dispatch through eval; its execution
            // hooks are never applied and must not wall the record)
            case com.legend.compiler.spec.typed.TypedNewInstance ni
                    when ni.properties().entrySet().stream().anyMatch(pe ->
                            com.legend.compiler.element.type.PlatformTypes
                                    .isPostProcessorConfigProperty(pe.getKey())
                                    || pe.getValue() instanceof TypedLambda)
                    && !configMode -> {
                var props = new LinkedHashMap<String, TypedSpec>();
                for (var pe : ni.properties().entrySet()) {
                    if (com.legend.compiler.element.type.PlatformTypes
                            .isPostProcessorConfigProperty(pe.getKey())
                            || pe.getValue() instanceof TypedLambda) {
                        configMode = true;
                        try {
                            props.put(pe.getKey(),
                                    rewrite(pe.getValue()));
                        } finally {
                            configMode = false;
                        }
                    } else {
                        props.put(pe.getKey(), rewrite(pe.getValue()));
                    }
                }
                yield new com.legend.compiler.spec.typed.TypedNewInstance(
                        ni.classFqn(), props, ni.info());
            }
            case com.legend.compiler.spec.typed.TypedCopyInstance cpi
                    when cpi.overrides().entrySet().stream().anyMatch(pe ->
                            com.legend.compiler.element.type.PlatformTypes
                                    .isPostProcessorConfigProperty(pe.getKey())
                                    || pe.getValue() instanceof TypedLambda)
                    && !configMode -> {
                var ovs = new LinkedHashMap<String, TypedSpec>();
                for (var pe : cpi.overrides().entrySet()) {
                    if (com.legend.compiler.element.type.PlatformTypes
                            .isPostProcessorConfigProperty(pe.getKey())
                            || pe.getValue() instanceof TypedLambda) {
                        configMode = true;
                        try {
                            ovs.put(pe.getKey(),
                                    rewrite(pe.getValue()));
                        } finally {
                            configMode = false;
                        }
                    } else {
                        ovs.put(pe.getKey(), rewrite(pe.getValue()));
                    }
                }
                yield new com.legend.compiler.spec.typed.TypedCopyInstance(
                        rewrite(cpi.source()), cpi.classFqn(), ovs,
                        cpi.info());
            }

            // BINDERS — already α-hygienic (the substitution renamed what a
            // spliced term would capture); the walk records them for the
            // hook and reduces beneath.
            case TypedLambda l -> lambda(l);
            // match is STATICALLY DISPATCHED (the checker picked the branch)
            // — the node IS a β-redex: substitute the input (and the extra
            // argument) into the chosen body and the match disappears; the
            // lowerer never needs a match arm.
            case TypedMatch m -> {
                TypedSpec input = rewrite(m.input());
                Optional<TypedSpec> extra = m.extra().map(this::rewrite);
                yield dispatchArm(m.param(), m.extraParam(), m.body(), input, extra);
            }
            case TypedLet let -> {
                // Reached only for lets inside lambda bodies (callee and
                // query-level lets reduce in reduceStatements / inlineBody):
                // the let stays, its value reduces.
                TypedSpec lv = rewrite(let.value());
                yield lv == let.value() ? let
                        : new TypedLet(let.name(), lv, let.info());
            }

            case TypedNativeCall c -> {
                // execute()'s RUNTIME argument is ORCHESTRATION position
                // (engine: the router evaluates connections outside the
                // planner) — user calls inside it (the corpus's
                // createDbAndGetConnection) stay UNINLINED; buildFrame
                // runs their effects once and treats the value as an
                // opaque handle. Inlining them hits the non-let
                // intermediate-statement wall on their effect bodies.
                int orchestration = com.legend.builtin.NativeFn.Handle
                        .orchestrationArgument(c.callee().id(), c.args().size());
                if (orchestration >= 0) {
                    List<TypedSpec> keepRt = new ArrayList<>(c.args().size());
                    for (int i = 0; i < c.args().size(); i++) {
                        keepRt.add(i == orchestration ? c.args().get(i)
                                : rewrite(c.args().get(i)));
                    }
                    yield sameRefs(keepRt, c.args()) ? c
                            : c.withChildren(keepRt);
                }
                // LAZY if (tier 1 unroll): a condition that folds to a
                // literal boolean evaluates ONLY the taken branch — a
                // partial evaluator never rewrites the untaken branch (its
                // recursion would not descend; its walls are not ours)
                List<TypedSpec> args = list(c.args());
                // HIGHER-ORDER map: a literal lambda stands where the checker
                // saw a function-valued expression ($f->map($func) —
                // MapChecker emits the plain call for anything but a literal,
                // so one here ARRIVED: by substitution (a bound term) or by
                // reduction (a call returning a lambda, a pair's component).
                // ONLY an exactly-[1] source β-reduces (map(v[1], f) ≡
                // f(v), pure semantics). A [0..1] source must NOT (audit
                // 22 self-catch): map over EMPTY is EMPTY, but a lambda
                // body NON-STRICT in its param (a constant, if with a
                // constant branch, isEmpty itself) would manufacture a
                // value after β-reduction — silent wrong value. [0..1]
                // and to-many sources rebuild the TypedMap construct node
                // the checker would have emitted.
                if ("meta::pure::functions::collection::map"
                        .equals(c.callee().qualifiedName())
                        && args.size() == 2
                        && args.get(1) instanceof TypedLambda lam
                        && lam.parameters().size() == 1) {
                    // audit 22a H1: the guard reads the POST-substitution
                    // multiplicity — a [1]-DECLARED param fed an
                    // effectively-[0..1] actual (the engine-convention
                    // acceptance) must NOT β-reduce either; the declared
                    // mult lied about emptiness.
                    if (args.get(0).info().multiplicity()
                            instanceof com.legend.compiler.element.type
                                    .Multiplicity.Bounded mb
                            && mb.lower() == 1 && mb.upper() != null
                            && mb.upper() == 1) {
                        try (Scope inner = new Scope()) {
                            inner.put(lam.parameters().get(0), args.get(0));
                            yield reduceStatements(lam.body(), inner);
                        }
                    }
                    yield new TypedMap(args.get(0), lam, c.info());
                }
                // untouched subtrees keep identity (F13 leans on it: the
                // instance-identity site key is the NODE — a gratuitous
                // rebuild would re-mint a let-bound instance per side)
                TypedSpec rebuilt = sameRefs(args, c.args()) ? c : c.withChildren(args);
                // the STRING ENTRY (executeLegendQuery): the hook saw the
                // call at entry, substituted; when a reduction above changed
                // an argument (a call opened into the query lambda), the
                // frame splice sees the lambda only NOW — re-offer it
                if (hook != null && rebuilt != c
                        && (com.legend.builtin.NativeFn.Handle.of(c.callee().id()).orElse(null) == com.legend.builtin.NativeFn.Handle.EXECUTE_LEGEND_QUERY)) {
                    TypedSpec h = hook.apply(rebuilt, bound.keySet());
                    if (h != rebuilt) {
                        yield rewrite(h);
                    }
                }
                yield rebuilt;
            }
            // Resolver OUTPUT vocabulary — never present pre-H; fails loud
            // here on a pipeline reordering rather than silently rebuilding.
            case TypedSerializeGraph sg -> throw new IllegalStateException(
                    "TypedSerializeGraph reached the inliner — it runs BEFORE the store resolver");
            // EVERY other variant is a pure structural rebuild: rewrite the
            // children (a lambda child re-enters through the TypedLambda arm,
            // which records its binders for the hook) and reassemble through
            // the variant's own withChildren inverse — field preservation is
            // the VARIANT's contract, not this walker's. The hand-written arms
            // this replaces dropped TypedAggCol.orderKey and skipped MapReduce
            // strategy lambdas (remediation T2.1). Untouched subtrees keep
            // identity.
            default -> n.mapChildren(this::rewrite);
        };
    }

    // =====================================================================
    // Binders and carriers
    // =====================================================================

    /**
     * A lambda: its binders are recorded for the hook's shadow guard
     * (parameters, then each body let from its statement on) and its
     * statements reduce; names stay as the substitution left them.
     */
    private TypedLambda lambda(TypedLambda l) {
        // the names to release at exit: the parameters now, each body
        // let as its statement passes
        List<String> guard = new ArrayList<>(l.parameters());
        l.parameters().forEach(p -> bound.merge(p, 1, Integer::sum));
        try {
            List<TypedSpec> body = new ArrayList<>(l.body().size());
            for (TypedSpec stmt : l.body()) {
                body.add(rewrite(stmt));
                if (stmt instanceof TypedLet let) {
                    bound.merge(let.name(), 1, Integer::sum);
                    guard.add(let.name());
                }
            }
            return sameRefs(body, l.body()) ? l
                    : new TypedLambda(l.parameters(), body, l.info(), l.quoted());
        } finally {
            guard.forEach(g -> bound.compute(g,
                    (k, c) -> c == null || c <= 1 ? null : c - 1));
        }
    }

    /** A dispatched match arm is a β-redex: the input (and the extra
     * argument) substitute into the chosen body. */
    private TypedSpec dispatchArm(String param, Optional<String> extraParam, TypedSpec body,
            TypedSpec input, Optional<TypedSpec> extra) {
        try (Scope inner = new Scope()) {
            inner.put(param, input);
            if (extraParam.isPresent()) {
                inner.put(extraParam.get(), extra.orElse(input));
            }
            return reduce(body, inner);
        }
    }

    /** Element-wise REFERENCE equality — the identity-preservation
     * check ("untouched subtrees keep identity", the walker contract
     * F13's site-identity keys lean on). */
    private static boolean sameRefs(List<TypedSpec> a, List<TypedSpec> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (a.get(i) != b.get(i)) {
                return false;
            }
        }
        return true;
    }

    private List<TypedSpec> list(List<TypedSpec> ns) {
        List<TypedSpec> out = new ArrayList<>(ns.size());
        for (TypedSpec n : ns) {
            out.add(rewrite(n));
        }
        return out;
    }

    // =====================================================================
    // Narrow β-binds for the executor's staging loops (Invariant 7: the
    // SUBSTITUTION is compiler work; the executor supplies the runtime
    // value and the moment). Owned here beside the full engine so a
    // second partial β-implementation never grows elsewhere again.
    // =====================================================================

    /** Bind an effectful map's parameter: {@code TypedVariable(param)}
     * reads at the node root or in native-call arguments replace with
     * the STRING literal (the corpus shape:
     * {@code executeInDb($sql, $connection)}); a read anywhere deeper is
     * LOUD — a wall, never silently unbound. Deliberately NARROWER than
     * {@link #inlineBody}: the wall documents the untested positions. */
    public static TypedSpec bindStringParam(TypedSpec node, String param,
            String value) {
        var lit = new com.legend.compiler.spec.typed.TypedCString(value,
                com.legend.compiler.element.type.ExprType.one(
                        com.legend.compiler.element.type.Type.Primitive.STRING));
        if (node instanceof TypedVariable tv && tv.name().equals(param)) {
            return lit;
        }
        if (node instanceof TypedNativeCall nc) {
            List<TypedSpec> args = new ArrayList<>();
            for (TypedSpec a : nc.args()) {
                args.add(a instanceof TypedVariable v2
                        && v2.name().equals(param) ? lit : a);
            }
            return new TypedNativeCall(nc.callee(), args, nc.info(), nc.pos());
        }
        if (referencesVar(node, param)) {
            throw new IllegalStateException("effectful map body reads the"
                    + " parameter '" + param + "' in an unsupported position");
        }
        return node;
    }

    /** Whether {@code node} (transitively) reads the variable. */
    public static boolean referencesVar(TypedSpec node, String name) {
        if (node instanceof TypedVariable tv && tv.name().equals(name)) {
            return true;
        }
        for (TypedSpec c : node.children()) {
            if (referencesVar(c, name)) {
                return true;
            }
        }
        return false;
    }
}
