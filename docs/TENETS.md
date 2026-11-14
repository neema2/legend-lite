# Legend Lite — Tenets

> **North star for "lazy vs eager?" decisions.** Read this before reopening any
> debate about when work should happen. It exists to stop the loop.

## The tenet: Total Knowledge, Demand-Driven Work

> **Be eager and total about Knowledge. Be lazy and demand-driven about Work.**

The flip-flopping between "demand-driven everything" (the `engine` module's
overreach — it didn't even parse function bodies) and "eager everything" comes
from treating laziness as a single global slider. It is not. It is two
independent questions:

- **Knowledge** — cheap facts about *what exists and what shape it has*: tokens
  and AST (parsing), names → FQN, **existence + kind + signature** (the
  manifest), structural shape. Cheap, bounded, and what makes everything else
  *correct*.
- **Work** — expensive, query-specific *production*: type-checking a body into
  HIR, normalizing a mapping's semantics, lowering to MIR, generating SQL,
  executing.

Eager Knowledge makes the whole program visible and every downstream decision
sound. Lazy Work means you only pay for what a query actually touches.

## Why this is still SAFE

- **Using a thing triggers its validation.** You cannot execute an
  un-type-checked body — *needing* it compiles and validates it. An un-compiled
  body is **inert, not unsafe**. Demand-driven Work can never run something
  unvalidated.
- **Structural safety is global and eager.** Because Knowledge is total, every
  reference / kind / existence error is catchable up front, cheaply, for the
  whole program.

Safety is therefore two-tier and both tiers hold:

- **Reference / structural safety** → eager + total (from complete Knowledge).
- **Deep body safety** → demand-driven, but *guaranteed for anything you
  touch*, because touching = compiling.

The only dangerous laziness is **lazy Knowledge** (validating against an
incomplete world). That was the `engine` module's one real mistake:
it filed *parsing* under Work. Lazy Work breaks nothing.

## The decision table

| Phase | Side | Policy |
|---|---|---|
| Parse (text → AST) | Knowledge | **Eager, total** |
| Name-resolve (imports → FQN) | Knowledge | **Eager, total** |
| Manifest (existence + kind + signature) | Knowledge | **Eager, total** — cheap; gives structural safety + flat lookup |
| Element structure (properties, supertypes) | Knowledge | Known eagerly in-project; **materialized on-demand cross-project** via FQN + `ModelContext.findClass` — never force a transitive load |
| Body type-check (functions, derived props, constraints, mapping transforms) | **Work** | **Lazy** — compiled when something needs it |
| Lower → MIR → SQL → execute | **Work** | **Lazy** — purely query-driven |

Nuance on the Knowledge side: when materialization is *bulky* (full
cross-project structure), defer the **materialization** but keep the cheap
**index** (existence/kind/signature) eager. That is the Tier-1 manifest /
Tier-2 structure split — not a special case, just "index is cheap-eager, bulk
is lazy-materialized, and the representation never forces a transitive load."

## The litmus test (the loop-breaker)

For any future "lazy or eager?":

1. **Is it Knowledge or Work?** Knowledge → eager. Work → lazy.
2. **If I make it lazy, does a compile-time guarantee become a runtime
   surprise?** If yes and it's cheap → eager. If it only matters for what's
   actually used and it's expensive → lazy is correct.

## How the existing architecture already encodes this

- **Parse everything** (core's step-1 decision) = eager Knowledge.
- **The kind manifest** (`FQN → kind`, eager over the declared dependency
  closure) = eager Knowledge; enables structural safety + flat lookup.
- **F never triggers G** (AGENTS.md: only `TypedFunction` holds a parsed,
  un-type-checked body) = lazy Work. The Element Compiler (F = Knowledge) and
  the Expression Compiler (G = Work, on demand) split *is* this tenet.

We are at the balanced center — eager Knowledge, lazy Work — not at either
extreme.

## The second tenet: Java parses and binds; Pure decides

*Ruled by the user, 2026-11-03 ("Okay let's go", the timeseries v1 roadmap; the user's words: "java for parsing, not
for logic", with zero exceptions -- "maybe we need new pure constructs that's okay").*

A language lite carries (`#TS`, and every island like it) is read by Java and **means** what its Pure library says.

- **Java parses**: text to a tree, the grammar's refusals (a token out of place).
- **Java binds**: a name to what it names (a source, a column, a library function), and the facts read straight off
  the syntax or the model -- a `BY`, a subscript, a column's declared type, which read is the same read. A fact
  reaches the library as **data** (an argument, a type), never as a choice Java made for it.
- **Pure decides**: what a word computes, how lines meet, which rows an answer has, what is refused and with which
  fix. A rule is a library function, an overload the type checker picks, or a value the database computes.

Java may not branch on a derived fact to pick a meaning, compute a value, or refuse a meaning. Where Pure cannot yet
say a rule, the answer is a new Pure construct (the array `read`, overloads by a relation column's type), not a Java
exception: an exception that remains is listed with its reason and guarded (`TsTranslatorGuardrailTest`), and the
list only shrinks.

The litmus test: **if the library changed its mind, would Java have to change too?** If yes, the rule is in Java, and
it moves.
