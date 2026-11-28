# legend-engine's answer for `1 == 1.0` in memory: M2M and XStore (asked 2026-11-25)

For the independent review's must-fix 7 (`independent-review-2026-11-25.md`; the START HERE's §8.1 and §8.4) and
S127's conflict with the design's §3 ("an M2M transform's `1 == 1.0` is `false`"). Asked by B2 at B3's request.
Recorded, not built on.

**How:** legend-engine 4.145.0's own planner and executor (`//tools/engine-runner:testable`'s jars, the recipe in
`docs/result-boundary-homework/README.md` §7, the probe `docs/result-boundary-homework/probes/EqualityProbe.java.txt`
unchanged), over the model below: a JSON model store per source class, a `data:` URL each.

**What it answers:**

| Query | legend-engine executes |
|---|---|
| `\|1 == 1.0` (host-side, a platform node, generated Java) | `true` (S127's answer, again) |
| M2M, `propertyCompare: $src.a == 1.0` (`a` Integer, 1) | `true` |
| M2M, `literalCompare: 1 == 1.0` | `true` |
| M2M, `floatSide: $src.f == 1` (`f` Float, 1.0) | `true` |
| XStore, `firm[p, f]: $this.firmId == $that.id` (`firmId` Integer 1, `id` Float 1.0), from a Person | the association joins: `{"name":"P","firm":{"name":"F"}}` |
| XStore, `employees[f, p]: $this.id == $that.firmId`, from a Firm | joins: `{"name":"F","employees":[{"name":"P"}]}` |
| legend-pure's compiled `CompiledSupport.equal(1, 1.0)`, the same classpath | `false` |

The M2M answers came as `{"builder":{"_type":"json"},"values":{"propertyCompare":true,"literalCompare":true,"floatSide":true}}`.

**So:** legend-engine's in-memory executor (M2M and in-memory XStore) answers a cross-kind number equality `true`, as
its host-side generated Java does (S127); legend-pure's own `equal` answers `false`. The design's "`engine` answers
those Pure's way, as legend-engine really does: an M2M transform's `1 == 1.0` is `false`" does not hold for these
shapes. Not asked: a chained M2M whose result is a TDS (`chain.pure:104-116` pushes it into the store query), a
relational XStore side, a non-number crossing (an enumeration against a String).

**The model:**

```
###Pure
Class test::Src
{
  a: Integer[1];
  f: Float[1];
}

Class test::Tgt
{
  propertyCompare: Boolean[1];
  literalCompare: Boolean[1];
  floatSide: Boolean[1];
}

Class test::SFirm
{
  id: Float[1];
  name: String[1];
}

Class test::SPerson
{
  name: String[1];
  firmId: Integer[1];
}

Class test::Firm
{
  id: Float[1];
  name: String[1];
}

Class test::Person
{
  name: String[1];
  firmId: Integer[1];
}

Association test::Firm_Person
{
  firm: test::Firm[0..1];
  employees: test::Person[*];
}

###Mapping
Mapping test::M
(
  *test::Tgt: Pure
  {
    ~src test::Src
    propertyCompare: $src.a == 1.0,
    literalCompare: 1 == 1.0,
    floatSide: $src.f == 1
  }
)

Mapping test::XM
(
  test::Firm[f]: Pure
  {
    ~src test::SFirm
    id: $src.id,
    name: $src.name
  }
  test::Person[p]: Pure
  {
    ~src test::SPerson
    name: $src.name,
    firmId: $src.firmId
  }
  test::Firm_Person: XStore
  {
    firm[p, f]: $this.firmId == $that.id,
    employees[f, p]: $this.id == $that.firmId
  }
)

###Runtime
Runtime test::RT
{
  mappings:
  [
    test::M
  ];
  connections:
  [
    ModelStore:
    [
      c1: #{
        JsonModelConnection
        {
          class: test::Src;
          url: 'data:application/json,{"a":1,"f":1.0}';
        }
      }#
    ]
  ];
}

Runtime test::XRT
{
  mappings:
  [
    test::XM
  ];
  connections:
  [
    ModelStore:
    [
      c2: #{
        JsonModelConnection
        {
          class: test::SFirm;
          url: 'data:application/json,{"id":1.0,"name":"F"}';
        }
      }#,
      c3: #{
        JsonModelConnection
        {
          class: test::SPerson;
          url: 'data:application/json,{"name":"P","firmId":1}';
        }
      }#
    ]
  ];
}
```
