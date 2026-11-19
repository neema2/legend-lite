# D3: a Type value's `toString` fails at the database — diagnosis (2026-11-13)

Diagnosis only; no product code. The orchestrator asked for it on 2026-11-13. It is a found item outside O2, filed with
DE3 (the product toString ruling, still with the user). The author is E.

## The failure, measured

`|1->type()->toString()` on the product door fails AT THE DATABASE on every in-process database, never by name:

| database | the error |
|---|---|
| DuckDB | `Binder Error: No function matches ... 'struct_extract(STRING_LITERAL, STRING_LITERAL)'` |
| SQLite | `malformed JSON` |
| H2 | `Invalid value "CHARACTER VARYING(7)" for parameter "JSON \| ROW"` |

The same happens for `[1, 2.0]->at(0)->type()->toString()`, `[1, 'a']->at(1)->type()->toString()` and
`[1, 2.0]->map(x|$x->type()->toString())`. (SQLite refuses the last one by name: TYPEOF is SQLite's storage class.)
`type()` alone answers correctly: `1->type()` is `"Integer"`, and so is a mixed element's.

## The trace

1. **Typer.** `meta::pure::functions::meta::type(any:Any[*]):Type[1]` (legend-pure `essential/meta/type/type.pure:44`).
   The call's static type is the CLASS `meta::pure::metamodel::type::Type`: a `Type.ClassType`, not a `GenericType`.
2. **Lowering of `type()`** (`Scalars`, the `Pure.AT_META_TYPE` rule). A static type answers its name as a
   `SqlExpr.StringLit`: `'Integer'` for a primitive, the path for a class or enumeration. Any other type answers
   `SqlFn.TYPEOF` of the value.
3. **Lowering of `toString`** (`Scalars`, the `AT_STRING_TO_STRING` rule, about lines 2820-2865). It dispatches on the
   ARGUMENT's static type:
   - The **type arm** ("a TYPE value prints its NAME", `testClassToString`) matches only
     `t instanceof Type.GenericType mg && PlatformTypes.isClassMetaclass(mg)`, i.e. `Class<T>`. A literal there prints
     its simple name; a type known only at run time is refused by name.
   - `Type[1]` is a `ClassType`, so it misses the type arm.
   - It falls to the **instance arm**:
     `t instanceof Type.ClassType && schemaView == null && InstanceEquality.userClass(t)`.
     `userClass` excludes Any, Nil, Variant and the List, Pair and Map carriers, but not the metamodel's type
     classifiers. So `Type` is taken for a user class.
   - The instance arm prints `'@_' || trackedId(x)`, which reads the struct field `__id` off `x`, and `x` is the string
     literal `'Integer'`.
4. **Render.** Each dialect writes that field read faithfully over a string, and the DATABASE rejects it. Nothing in
   lowering or rendering refuses it by name.

The root cause is one arm's guard. `PlatformTypes.isTypeClassifier` already names the five metamodel type classifiers
(`Class`, `Type`, `PrimitiveType`, `DataType`, `Enumeration`) as "a value of one of these is a TYPE value". The
`toString` rule tests only one of them, and only in its generic form.

## What upstream answers

- **legend-pure (compiled):** `'Integer'`. `type()` returns the metamodel's `PrimitiveType` element. `toString`
  (`CompiledSupport.pureToString(Object, ExecutionSupport)`) gives a packageable element's name: a compiled concrete
  element's `toString(es)` is `getName()` (`AbstractCompiledLazyConcreteElement`; read 2026-10-28,
  docs/result-boundary-homework/README.md §7). The same rule makes `testClassToString` assert
  `'STR_Person' == STR_Person->toString()` (`essential/string/toString/toString.pure:168`).
- **legend-engine (4.145.0, asked through tools/engine-runner, 2026-11-13):** refuses at planning for all four forms,
  `1->type()` included: `"meta::pure::functions::meta::genericType_Any_MANY__GenericType_1_ is not supported yet!"`
  (`core_external_language_java/generation/expressionGeneration.pure:307`).

## The smallest correct fix

Widen the type arm's guard to every type classifier, in both forms:

```java
if (t instanceof Type.GenericType mg && PlatformTypes.isClassMetaclass(mg)
        || t instanceof Type.ClassType ct && PlatformTypes.isTypeClassifier(ct.fqn())) {
    // a literal prints its simple name; a type known only at run time is refused by name (unchanged)
}
```

- `1->type()->toString()` and any statically known type: `'Integer'` (legend-pure's answer, the simple name).
- A type known only at run time (a mixed element's `type()`, which lowers to a CASE over the tags or to `TYPEOF`) is
  refused BY NAME, as the arm already does: "toString of a type known only at run time ... has no form yet". That is a
  database failure turned into a named refusal, and the run-time form stays open.
- `InstanceEquality.userClass` should also exclude the type classifiers. A `Type` value is never an instance struct,
  so its equality and print must never read `__id`. That is the same one-line guard, and it closes the class of bug,
  not just this instance.
- A test pins both: the static answer `'Integer'` (and a class's simple name), and the named refusal for a run-time
  type.

## Does it belong under DE3?

**No, in E's view.** DE3 rules on the product's print FORMS: which spelling a value's text takes, Pure's or the
database's. D3 is a dispatch defect, a type value read as an instance. The fix changes no spelling. It answers
legend-pure's name where the database failed, and refuses by name where it cannot answer. It moves no ruled answer,
so it needs no register row. It can land on its own once the orchestrator allows it. If DE3 later rules a different
print form for a type's name, it would change this arm's literal along with every other print.
