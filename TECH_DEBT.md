# Legend-Lite Tech Debt

Track architectural improvements to address after PCT milestone.

---

## 1. SQL-Agnostic IR Layer

**Problem**: `PureCompiler` produces SQL-specific expressions (`SqlFunctionCall`, etc.) instead of abstract IR.

**Current**:
```
PureCompiler → SqlFunctionCall.of("list_concat", a, b) → SQLGenerator → "list_concat(a, b)"
```

**Target**:
```
PureCompiler → IR.ListConcat(a, b) → SQLGenerator → "list_concat(a, b)"  // DuckDB
                                   → SparkGenerator → "concat(a, b)"     // Spark
```

**Files**: `PureCompiler.java` (compileMethodCall, compileToSqlExpression)

---

## 2. Unify Function/Method Semantics

**Problem**: `MethodCall` and `FunctionCall` are handled separately, but method call is syntactic sugar.

**Current**:
```java
// In PureCompiler - duplicated logic in two places:
case MethodCall m -> compileMethodCall(m, context);
case FunctionCall f -> compileFunctionCall(f, context);
```

**Target**:
Normalize in parser or early compiler stage:
```
[1,2,3]->map(x|x+1)  ≡  map([1,2,3], x|x+1)
```
Then single unified dispatch for all function applications.

---

## Priority
Address after PCT pass rate reaches acceptable threshold (~80%+).
