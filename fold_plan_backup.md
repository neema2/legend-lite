# Fold Refactor: FoldSpec Architecture

## Principles

1. **No null guards** — `expressionType` always non-null, `TypeInfo` always stamped by checker
2. **No fallbacks** — FoldSpec always resolved; PlanGenerator does `switch(spec)` with no default
3. **No `substituteVariable`** — no AST rewriting anywhere; SQL-level unwrap in PlanGenerator
4. **No hardcoded operators** — `combineOp` extracted dynamically from body AST
5. **Everything signature-driven** — `resolveOverload` → `unify` → `resolve` → type bindings → classify

## Strategy Classification (4 variants)

| Strategy | Condition | SQL |
|---|---|---|
| **Concatenation** | body = `add(acc, elem)` | `listConcat(init, source)` |
| **SameType** | T == V | `listReduce(source, λ, init)` |
| **MapReduce** | T ≠ V, body decomposable | `listReduce(listTransform(source, transform), reducer, init)` |
| **CollectionBuild** | T ≠ V, V = List\<T\> | wrap + unwrap at SQL level + `listReduce` |

Classification order: Concatenation → (T == V → SameType) → (decompose → MapReduce) → CollectionBuild.

**How PlanGenerator knows to wrap:** FoldSpec.CollectionBuild is stamped by FoldChecker when T ≠ V **and** V is a list type **and** the body can't be decomposed into MapReduce. PlanGenerator reads the marker and handles the SQL-level wrapping (wrap source elements in `[elem]`) and unwrapping (emit `list_extract(elem, 1)` for every elem ref in the compiled SQL body).

---

## Step 1: Add FoldSpec to TypeInfo

#### [MODIFY] [TypeInfo.java](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/TypeInfo.java)

### FoldSpec sealed interface (add after `VariantAccess`, ~line 108)

```java
/**
 * Pre-resolved fold lowering strategy.
 * Computed by FoldChecker, consumed by PlanGenerator via exhaustive switch.
 * Each variant carries exactly the data PlanGenerator needs — zero AST inspection.
 */
public sealed interface FoldSpec {
    /** Body is add(acc, elem) → SQL: listConcat(init, source). */
    record Concatenation() implements FoldSpec {}

    /** T == V → SQL: listReduce(source, lambda, init). */
    record SameType() implements FoldSpec {}

    /**
     * T ≠ V with decomposable body → SQL: listReduce(listTransform(...), reducer, init).
     * @param elementTransform  f(elem) subtree (acc stripped from body)
     * @param combineOp         Binary op name from body (e.g. "plus", "times")
     */
    record MapReduce(ValueSpecification elementTransform, String combineOp) implements FoldSpec {}

    /**
     * T ≠ V, V = List<T>, non-decomposable body → wrap + SQL-level unwrap + listReduce.
     * Marker only — no payload. PlanGenerator handles wrapping/unwrapping.
     */
    record CollectionBuild() implements FoldSpec {}
}
```

### Record field (add after `resolvedFunc`)

```java
public record TypeInfo(
        // ... existing fields ...
        NativeFunctionDef resolvedFunc,
        FoldSpec foldSpec) {      // ← NEW
```

### Builder additions

```java
private FoldSpec foldSpec;                                           // field

private Builder(TypeInfo src) {
    // ... existing ...
    this.foldSpec = src.foldSpec();                                  // copy
}

public Builder foldSpec(FoldSpec v) { this.foldSpec = v; return this; }  // setter

public TypeInfo build() {
    // ... existing validation ...
    return new TypeInfo(/* existing args */, resolvedFunc,
            foldSpec);                                               // ← add
}
```

---

## Step 2: FoldChecker — classify + stamp FoldSpec

#### [MODIFY] [FoldChecker.java](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/checkers/FoldChecker.java)

Existing signature-driven `check()` is correct — add classification at the end and clean up null guards. The resolved T and V come straight from `unify()` bindings.

```java
package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.*;

/**
 * Signature-driven type checker + strategy classifier for {@code fold()}.
 *
 * <p>Signature: {@code fold<T,V>(source:T[*], lambda:{T[1],V[1]->V[1]}[1], init:V[1]):V[1]}
 *
 * <p>Two responsibilities:
 * <ol>
 *   <li>Type-check via resolveOverload/unify/resolve (standard checker flow)</li>
 *   <li>Classify fold → stamp {@link TypeInfo.FoldSpec} (PlanGenerator reads it)</li>
 * </ol>
 */
public class FoldChecker extends AbstractChecker {

    public FoldChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 3)
            throw new PureCompileException("fold() requires 3 parameters: source, lambda, init");

        // 1. Resolve overload
        NativeFunctionDef def = resolveOverload("fold", params, source);

        // 2. Compile init to bind V
        env.compileExpr(params.get(2), ctx);

        // 3. Unify: T from source, V from init
        Map<String, GenericType> bindings = unify(def, Arrays.asList(
                source.expressionType(),   // param[0]: T[*]
                null,                       // param[1]: lambda — skip
                env.typeInfoFor(params.get(2)).expressionType()  // param[2]: V[1]
        ));

        // 4. Compile lambda via signature-driven param binding
        if (!(params.get(1) instanceof LambdaFunction lambda))
            throw new PureCompileException("fold() argument 2 must be a lambda");

        PType.FunctionType ft = extractFunctionType(def.params().get(1));

        TypeChecker.CompilationContext lambdaCtx = ctx;
        for (int p = 0; p < lambda.parameters().size() && p < ft.paramTypes().size(); p++) {
            String paramName = lambda.parameters().get(p).name();
            GenericType resolvedParamType = resolve(ft.paramTypes().get(p).type(), bindings,
                    "fold() lambda param '" + paramName + "'");
            lambdaCtx = bindLambdaParam(lambdaCtx, paramName, resolvedParamType, source);
        }
        compileLambdaBody(lambda, lambdaCtx);

        // 5. Classify strategy from resolved T, V
        GenericType resolvedT = bindings.getOrDefault("T", GenericType.Primitive.ANY);
        GenericType resolvedV = bindings.getOrDefault("V", GenericType.Primitive.ANY);
        TypeInfo.FoldSpec spec = classifyFold(lambda, resolvedT, resolvedV, params.get(2));

        // 6. Output type + FoldSpec
        ExpressionType outputType = resolveOutput(def, bindings, "fold()");
        return TypeInfo.builder()
                .expressionType(outputType)
                .foldSpec(spec)
                .build();
    }

    // ==================== Strategy Classification ====================

    /**
     * Classifies fold into one of 4 strategies.
     * Order: Concatenation → SameType → MapReduce → CollectionBuild.
     * Uses resolved T, V from unify() — no null checks, no fallbacks.
     */
    private TypeInfo.FoldSpec classifyFold(LambdaFunction lambda,
                                           GenericType resolvedT, GenericType resolvedV,
                                           ValueSpecification initNode) {
        // 1. Concatenation: body is add(acc, elem)
        if (isFoldAddPattern(lambda))
            return new TypeInfo.FoldSpec.Concatenation();

        // 2. SameType: T == V
        if (resolvedT.typeName().equals(resolvedV.typeName()))
            return new TypeInfo.FoldSpec.SameType();

        // 3. T ≠ V: try decomposing body → MapReduce
        String accParam = lambda.parameters().size() >= 2
                ? lambda.parameters().get(1).name() : "y";
        ValueSpecification transform = extractElementTransform(
                lambda.body().get(0), accParam);
        if (transform != null) {
            String op = topLevelOp(lambda.body().get(0));
            return new TypeInfo.FoldSpec.MapReduce(transform, op);
        }

        // 4. T ≠ V, not decomposable → CollectionBuild (marker)
        return new TypeInfo.FoldSpec.CollectionBuild();
    }

    // ==================== Helpers ====================

    /**
     * True if body is the identity-add pattern: {e, a | $a->add($e)}.
     */
    private static boolean isFoldAddPattern(LambdaFunction lf) {
        if (lf.parameters().size() < 2 || lf.body().isEmpty()) return false;
        String elemParam = lf.parameters().get(0).name();
        String accParam = lf.parameters().get(1).name();
        if (lf.body().get(0) instanceof AppliedFunction bodyAf
                && TypeInfo.simpleName(bodyAf.function()).equals("add")
                && bodyAf.parameters().size() == 2) {
            var addSource = bodyAf.parameters().get(0);
            var addElem = bodyAf.parameters().get(1);
            return addSource instanceof Variable accVar && accVar.name().equals(accParam)
                    && addElem instanceof Variable elemVar && elemVar.name().equals(elemParam);
        }
        return false;
    }

    /**
     * Extracts element-only transform by stripping the accumulator from
     * the left spine of ANY binary op chain (not just plus).
     *
     * <p>Example: {@code plus(plus(acc, '; '), p.name)} → {@code plus('; ', p.name)}
     * <p>Example: {@code times(acc, length(x))} → {@code length(x)}
     *
     * @return element-only subtree, or null if not decomposable
     */
    private static ValueSpecification extractElementTransform(
            ValueSpecification body, String accParam) {
        if (!(body instanceof AppliedFunction af)) return null;
        if (af.parameters().size() != 2) return null;
        String op = TypeInfo.simpleName(af.function());

        ValueSpecification left = af.parameters().get(0);
        ValueSpecification right = af.parameters().get(1);

        // Base case: left is acc variable → return right
        if (left instanceof Variable v && v.name().equals(accParam))
            return right;

        // Recursive: left is same op chain containing acc
        if (left instanceof AppliedFunction leftAf
                && TypeInfo.simpleName(leftAf.function()).equals(op)) {
            ValueSpecification stripped = extractElementTransform(left, accParam);
            if (stripped != null)
                return new AppliedFunction(af.function(), List.of(stripped, right));
        }

        return null;
    }

    /**
     * Returns the top-level binary operator name from the body.
     * Used as combineOp for MapReduce reducer.
     */
    private static String topLevelOp(ValueSpecification body) {
        if (body instanceof AppliedFunction af)
            return TypeInfo.simpleName(af.function());
        return "plus"; // safe default — only reached for binary ops
    }
}
```

---

## Step 3: PlanGenerator — exhaustive switch on FoldSpec

#### [MODIFY] [PlanGenerator.java](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/plan/PlanGenerator.java)

Replace lines 3655-3770 with clean exhaustive switch. Zero AST helpers, zero undefined methods.

```java
case "fold" -> {
    // fold(source, {elem,acc|body}, init)
    if (params.size() >= 3
            && params.get(1) instanceof LambdaFunction(
            List<Variable> parameters, List<ValueSpecification> body
    )) {
        String elemParam = parameters.get(0).name();
        String accParam = parameters.get(1).name();

        SqlExpr source = c.apply(params.get(0));
        if (!firstArgIsList)
            source = new SqlExpr.FunctionCall("wrapList", List.of(source));
        SqlExpr init = c.apply(params.get(2));

        TypeInfo.FoldSpec spec = unit.types().get(af).foldSpec();

        yield switch (spec) {
            // Path 1: fold+add → listConcat(init, source)
            case TypeInfo.FoldSpec.Concatenation() ->
                new SqlExpr.FunctionCall("listConcat", List.of(init, source));

            // Path 2: T == V → listReduce(source, lambda, init)
            case TypeInfo.FoldSpec.SameType() -> {
                SqlExpr lambdaBody = c.apply(body.getLast());
                SqlExpr lambda = new SqlExpr.LambdaExpr(
                        List.of(accParam, elemParam), lambdaBody);
                yield new SqlExpr.FunctionCall("listReduce",
                        List.of(source, lambda, init));
            }

            // Path 3: T ≠ V, decomposable → listTransform + listReduce
            case TypeInfo.FoldSpec.MapReduce(var transform, var combineOp) -> {
                // listTransform(source, elem -> transform)
                SqlExpr transformBody = c.apply(transform);
                SqlExpr transformLambda = new SqlExpr.LambdaExpr(
                        List.of(elemParam), transformBody);
                SqlExpr mapped = new SqlExpr.FunctionCall("listTransform",
                        List.of(source, transformLambda));
                // listReduce(mapped, (acc, __x) -> combineOp(acc, __x), init)
                // Compile combineOp through the standard scalar path
                String freshX = "__x";
                var syntheticOp = new AppliedFunction(combineOp,
                        List.of(new Variable(accParam), new Variable(freshX)));
                SqlExpr reducerBody = c.apply(syntheticOp);
                SqlExpr reducerLambda = new SqlExpr.LambdaExpr(
                        List.of(accParam, freshX), reducerBody);
                yield new SqlExpr.FunctionCall("listReduce",
                        List.of(mapped, reducerLambda, init));
            }

            // Path 4: V = List<T>, non-decomposable → wrap + unwrap + listReduce
            case TypeInfo.FoldSpec.CollectionBuild() -> {
                // Wrap each source element: listTransform(source, elem -> [elem])
                SqlExpr wrapBody = new SqlExpr.FunctionCall("wrapList",
                        List.of(new SqlExpr.ColumnRef(elemParam)));
                SqlExpr wrapLambda = new SqlExpr.LambdaExpr(
                        List.of(elemParam), wrapBody);
                SqlExpr wrappedSource = new SqlExpr.FunctionCall("listTransform",
                        List.of(source, wrapLambda));
                // Compile body as-is, then SQL-level unwrap:
                // replace ColumnRef(elemParam) → listExtract(ColumnRef(elemParam), 1)
                SqlExpr lambdaBody = c.apply(body.getLast());
                SqlExpr unwrapped = unwrapElemRefs(lambdaBody, elemParam);
                SqlExpr lambda = new SqlExpr.LambdaExpr(
                        List.of(accParam, elemParam), unwrapped);
                yield new SqlExpr.FunctionCall("listReduce",
                        List.of(wrappedSource, lambda, init));
            }
        };
    }
    throw new PureCompileException("fold: requires 3 parameters with lambda");
}
```

### `unwrapElemRefs` helper (add to PlanGenerator utility section)

Small SQL-level post-processor — replaces `ColumnRef(elemParam)` with `listExtract(ColumnRef(elemParam), 1)` in a compiled SqlExpr tree. This is NOT AST rewriting — it operates on our own SqlExpr nodes after compilation.

```java
/**
 * SQL-level post-processor for CollectionBuild fold.
 * Replaces ColumnRef(elemParam) → listExtract(ColumnRef(elemParam), 1)
 * in the compiled SQL tree so wrapped elements get unwrapped in the body.
 */
private static SqlExpr unwrapElemRefs(SqlExpr expr, String elemParam) {
    if (expr instanceof SqlExpr.ColumnRef cr && cr.name().equals(elemParam)) {
        return new SqlExpr.FunctionCall("listExtract",
                List.of(expr, new SqlExpr.NumericLiteral(1)));
    }
    if (expr instanceof SqlExpr.FunctionCall fc) {
        var newArgs = fc.args().stream()
                .map(a -> unwrapElemRefs(a, elemParam))
                .toList();
        return new SqlExpr.FunctionCall(fc.name(), newArgs);
    }
    if (expr instanceof SqlExpr.Binary b) {
        return new SqlExpr.Binary(
                unwrapElemRefs(b.left(), elemParam), b.op(),
                unwrapElemRefs(b.right(), elemParam));
    }
    if (expr instanceof SqlExpr.LambdaExpr le) {
        // Don't unwrap inside nested lambdas that shadow the variable
        if (le.params().contains(elemParam)) return expr;
        return new SqlExpr.LambdaExpr(le.params(),
                unwrapElemRefs(le.body(), elemParam));
    }
    // Cast, Subquery, literals, etc. — no ColumnRef to unwrap
    return expr;
}
```

---

## What Gets Deleted

From PlanGenerator lines 3655-3770:
- ❌ `isFoldAddPattern()` call → replaced by `FoldSpec.Concatenation`
- ❌ `isFoldListAccumulator()` call → replaced by `FoldSpec.CollectionBuild`
- ❌ `extractFoldElementTransform()` call → moved to `FoldChecker.extractElementTransform`
- ❌ `substituteVariable()` call → replaced by SQL-level `unwrapElemRefs`
- ❌ `typeName()` calls → replaced by resolved T/V from `unify()` bindings
- ❌ All TypeInfo lookups for T/V → classification pre-resolved in FoldChecker

---

## Key Design Decisions

1. **MapReduce `combineOp` is dynamic** — `extractElementTransform` works for ANY binary op, and PlanGenerator compiles the reducer via `c.apply(syntheticOp)` so `plus` → `+` or `||`, `times` → `*`, etc. are all handled by the existing scalar function compilation. No hardcoded `"+"`.

2. **CollectionBuild unwrap is SQL-level** — `unwrapElemRefs` walks the compiled `SqlExpr` tree (our own nodes), NOT the Pure AST. It's 20 lines, covers the 4 SqlExpr node types that can contain ColumnRefs, and respects lambda shadowing.

3. **T/V come from `unify()` bindings** — `bindings.get("T")` and `bindings.get("V")` give the resolved types directly. No deriving from source/init TypeInfo with null checks. The fold signature `fold<T,V>(T[*], {T[1],V[1]->V[1]}, V[1]):V[1]` guarantees both are bound.

---

## Verification

```bash
# Fold-specific tests
mvn test -pl engine -Dtest="TypeInferenceIntegrationTest#testFold*"

# Variant fold
mvn test -pl engine -Dtest="VariantIntegrationTest#testFoldOnJsonArray"

# Full regression
mvn test -pl engine
```

| Strategy | Key Tests |
|---|---|
| Concatenation | `testFoldCollectionAccumulator`, `testFoldWithEmptyAccumulator`, `testFoldWithSingleValue` |
| SameType | `testFoldIntegerSum*`, `testFoldStringConcat*`, `testFoldIntegerWithExtraArithmetic` |
| MapReduce | `testFoldOnStructListWithStringAccumulator`, `testFoldMixedAccumulatorTypes`, `testFold_FromVariant` |
| CollectionBuild | `testFoldCollectionAccumulator_WithIfSizeTail` |
