---
description: NO FALLBACKS — Engineering Principles for Compiler Work
---

# NO FALLBACKS EVER

## The Rule

**DO NOT ADD FALLBACKS. EVER.**

When fixing a bug, fix the root cause. Do not add a fallback that papers over the symptom.

## Why

Fallbacks create hidden bugs. They mask the real problem and make the codebase harder to reason about.
When a future developer encounters unexpected behavior, they cannot tell whether the fallback
was intentional design or a band-aid over something broken.

## Specific Anti-Patterns to Avoid

1. **DO NOT** add `catch (Exception ignored) {}` to swallow errors
2. **DO NOT** add "if type is null, default to STRING" 
3. **DO NOT** add pattern-matching extractors that guess at AST shapes
4. **DO NOT** return a "safe default" when the real answer is unknown
5. **DO NOT** add a new code path to handle a shape that the existing code should already handle

## What To Do Instead

1. **Write an integration test** that reproduces the exact failure
2. **Find the root cause** — trace through the code to find where the real bug is
3. **Fix the root cause** — make the code handle the case correctly
4. **If the code lacks infrastructure** to handle the case, build the infrastructure
5. **If compileExpr should handle it, make compileExpr handle it** — don't extract/guess around it

## In the Compiler Specifically

- If `compileExpr` should be able to compile an expression, make it compile the expression.
  Do not extract partial information from the AST and guess the rest.
- If `toOne()` should propagate type, fix `toOne()`. Do not add a fallback in the caller.
- If type inference fails, the fix is in the type inference path, not in defaulting to STRING.
- Every `catch (PureCompileException ignored)` is a bug waiting to happen.
