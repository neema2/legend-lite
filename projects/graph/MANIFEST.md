# graph

Layer 0, no dependencies. Exports one profile and nothing else: the vocabulary a model uses to say that a
class is an edge of a property graph (docs/CYPHER_STUDY_2026_10_15.md §5.6). The profile is an ordinary Pure
element that compiles in legend-engine and in Studio; only legend-lite gives it meaning, in the Cypher island
(`compiler/element/GraphEdges`, read by the profile's full name, `PlatformTypes.GRAPH_PROFILE`).

Prefixes: none (one element).

| element | kind | note |
| --- | --- | --- |
| graph::graph | profile | stereotypes `Edge` (a class is a relationship with properties), `from` and `to` (the class-typed `[1]` properties holding its two ends, own, inherited or association-injected), `type` (a derived property whose body spells the relationship type as one enumeration value), `Open` (on a DataSpace, 2026-10-17: the graph a Cypher island's `USE` names is open, so a name the graph lacks reads as Neo4j reads it, no rows or null); no tags |

## Notes for downstream projects

- An edge class carries `<<graph::graph.Edge>>` and has exactly one property carrying `<<graph::graph.from>>`
  and one carrying `<<graph::graph.to>>`, each `[1]` and typed by a class: the two ends. In the association form the
  ends are association ends, which also gives the end classes their inverse navigation:
  `Association demo::ActorActing { <<graph::graph.from>> actor: demo::Actor[1]; actings: demo::Acting[*]; }`.
  In the compact form they are the class's own properties.
- The relationship type is the class's simple name, unless a derived property carrying `<<graph::graph.type>>`
  spells one enumeration value: `<<graph::graph.type>> type() { demo::RelationshipType.ACTED_IN }: demo::RelationshipType[1];`.
- Every other stored property of the class, inherited ones included, is the relationship's property.
- An edge class is never a node label; a pattern reaches it by its type, `(a:Actor)-[r:ACTED_IN]->(m:Movie)`.
