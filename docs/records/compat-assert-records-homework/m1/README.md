# M1: verdict statements over typed values (item 3, Q-A)

Measured 2026-11-21 by the compatibility program's owner session, for Q-A (ruled: typed values, a core tenet).

Four ladder rungs (`core/src/test/resources/ladder/`, at `9fa42018`) rewritten by hand to decide over typed values,
with no canonical text. Each carries `__ix`, `__verdict` and the two values as the database's text, for the report
only.

| Rung | Assert | current | lean | typed | typed / current |
|---|---|---|---|---|---|
| r03_stringConstant | `assertEquals('a', 'a')` | 1,782 | 1,146 | 357 | 20% |
| r06_columnVsList | `assertSameElements(['a','b','c'], $r.values.name)` | 2,721 | 1,850 | 581 | 21% |
| r07_projectRows | `assertSize($r.values.rows, 3)` | 1,303 | 1,631 | 339 | 26% |
| r11_floatLeniency | `assertSameElements([0.5, 1.5, 2.5], $r.values.amount)` | 38,209 | 11,163 | 595 | 1.6% |

Sizes are characters. Each statement ran on DuckDB 2.0.1 over the ladder's table. With the ladder's three rows
every verdict is true; with a fourth row (`4, 'c', 3.5`) the three that read the table are false. The report
columns show the difference (`[a, b, c]` against `[a, b, c, c]`).

How each member decides:
- a scalar: `e IS NOT DISTINCT FROM a`;
- a count: `a = n`;
- a multiset: one signed `UNION ALL` grouped by value, true when no group's sum is non-zero.

The multiset form is the portable one: H2 2.5.252 has neither `EXCEPT ALL` nor `FULL OUTER JOIN`. `h2.sql` runs both
multiset members on H2: the right verdicts, with the report through `LISTAGG`.

What the typed form drops:
- the canon cells and their `'tree'` and null-canon checks;
- the float leniency, because a `DOUBLE` compares as a `DOUBLE`.

What it does not cover yet, for S4:
- a class instance or a tree value: there is no typed equality, so that member compares lite's own canonical text or
  refuses by name (design §8, Q-A);
- the fused `UNION ALL` across asserts: the report columns are `VARCHAR` in every member, so the members fuse.
