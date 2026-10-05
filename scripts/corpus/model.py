"""
Reads the stress corpus into a resolvable model.

The corpus is 46 files, 200 classes, 182 associations and 182 joins. Nothing here is
authored by hand against that: every fact the seeder and the oracle need — which table
backs a class, which column backs a property, which join a navigation crosses, and
whether that navigation is to-one or to-many — is read out of the .pure sources.

That matters for correctness of the *oracle*, not convenience. A hand-written map would
encode what I believe the mapping says; this encodes what it says. When the two disagree
the resolver raises instead of quietly producing a wrong expectation.

Grammar actually present in this corpus (asserted, not assumed — see check()):
  Table  NAME (COL TYPE [PRIMARY KEY], ...)          no schema qualifier, one Database
  Join   NAME(A.X = B.Y)                             single equality, no 'and', no {target}
  Class  pkg::Name { prop: Type[mult]; ... }
  Assoc  pkg::Name { endA: T[m]; endB: U[n]; }
  <cls>: Relational { ~mainTable [db] T   prop: [db] T.COL, ... }
  <assoc>: Relational { AssociationMapping ( end: [db] @Join, ... ) }

Anything outside that raises. The corpus is a fixture we control, so an unhandled form is
a signal to extend this deliberately rather than a reason to make the parser lenient.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import rhs

# The repository root. Under Bazel the generator runs as an action whose inputs are DECLARED,
# so the root is given (CORPUS_ROOT, the action's execroot) rather than found by resolving
# __file__ -- which follows the runfiles symlink back into the real checkout and would let the
# generator read files nobody declared.
ROOT = Path(os.environ["CORPUS_ROOT"]) if os.environ.get("CORPUS_ROOT") \
    else Path(__file__).resolve().parents[2]
STRESS = ROOT / "core/src/test/resources/stress"
PROJECTS = ROOT / "projects"
# The hand-written queries 92-services.pure is generated FROM. They used to be read back out
# of 92 itself, which made the file both the generator's input and its output.
QUERIES = ROOT / "scripts/corpus/queries.pure"

# THE LAYOUT, ONE LIST (Bazel workplan P2-02): core/stress.bzl, committed as JSON in core's test resources and
# diff-tested. GENERATED: the files build.py WRITES; DENSE_GENERATED: the files the dense generators write (inputs
# to build.py, never to the generator that writes them). Both are outputs, never inputs: every reader of "the
# corpus" below reads stress_sources(), so the generator's answer cannot depend on what it wrote last time.
# A generator action is given the layout as core/stress.bzl makes it (STRESS_LAYOUT, the execpath of
# //core:stress_layout), so one `bazel run //core:update_stress_corpus` regenerates from the new list; everything
# else reads the committed copy.
_LAYOUT = json.loads(Path(os.environ["STRESS_LAYOUT"]).read_text(encoding="utf-8") if os.environ.get("STRESS_LAYOUT")
                     else (ROOT / "core/src/test/resources/com/legend/integration/stress-layout.json")
                     .read_text(encoding="utf-8"))
GENERATED = frozenset(f for f, gen in _LAYOUT["generated"].items() if gen == "stress")
DENSE_GENERATED = frozenset(f for f, gen in _LAYOUT["generated"].items() if gen == "dense")

# Under Bazel the actions pass every declared input file (--inputs; take_inputs below), and the generators read
# those and nothing else (Bazel workplan P2-03): an unsandboxed run, whose CORPUS_ROOT is the whole execroot,
# cannot pick up a file nobody declared. Outside Bazel (None) the directories are globbed as before.
DECLARED: set[Path] | None = None


def take_inputs(argv: list[str]) -> None:
    """Sets DECLARED from the paths after --inputs in argv (up to the next --flag)."""
    global DECLARED
    if "--inputs" not in argv:
        return
    rest = argv[argv.index("--inputs") + 1:]
    DECLARED = {Path(p) for p in rest[:next((i for i, a in enumerate(rest) if a.startswith("--")), len(rest))]}


def _pure_files(directory: Path) -> list[Path]:
    """The .pure files directly in `directory`: the declared ones under Bazel, else the directory's."""
    if DECLARED is None:
        return list(directory.glob("*.pure"))
    return [p for p in DECLARED if p.parent == directory and p.suffix == ".pure"]


# Set by a generator to leave more files out of "the corpus" -- its own outputs.
EXCLUDE: set[str] = set()


# Where DENSE_GENERATED files are read from when not the checkout: under Bazel, build.py reads
# the dense generator's OUTPUTS (dense_build.py --out), so one update writes all ten files.
DENSE_DIR: Path | None = None


def stress_sources() -> list[Path]:
    """The hand-written (and other-generator) stress files, in file-name order: the generator's
    inputs. Order is by NAME, never by directory, because a headerless file inherits the
    section of the file before it."""
    files = {p.name: p for p in _pure_files(STRESS)
             if p.name not in GENERATED and p.name not in EXCLUDE}
    if DENSE_DIR is not None:
        for name in DENSE_GENERATED - EXCLUDE:
            files[name] = DENSE_DIR / name
    return [files[n] for n in sorted(files)]

# Projects from the dependency graph that the EXECUTABLE corpus depends on.
#
# The graph in projects/ is compile-only by design: no data, no runtimes, no services. That
# leaves a real gap, because three of this session's findings (F50, F51, F53) compile
# perfectly and fail at execution -- a cross-project reference that lowers to wrong SQL would
# be invisible to a compile check.
#
# So a SLICE of the graph is pulled into the executable corpus: its tables are seeded, its
# mapping is included in stress::AllMapping, and corpus classes navigate into it. Kept to a
# named list rather than the whole graph because every project added here is 200 more
# services to run, and the point is to prove the boundary executes rather than to re-test
# each project's content.
#
# Each one is here for a construct that can only fail at EXECUTION, which is the whole
# argument for linking anything at all:
#
#   core-tenor    a RANGE join across the boundary. A key equality that lowers wrongly
#                 returns nothing and is obvious; a range returns the wrong band.
#   core-fx       FUNCTIONS called across the boundary from a corpus derived property.
#   core-ratings  MILESTONING across the boundary -- `all(%date)` on a class that lives in
#                 another project, which projects/ could only ever compile.
# Dependencies BEFORE dependents: fee-core needs core-types and core-tenor to have been
# parsed. core-types exports no store and no mapping at all -- it is enums and functions --
# so it is here purely to satisfy fee-core, which is what a transitive dependency looks like.
LINKED_PROJECTS = list(_LAYOUT["linked_projects"])  # core/stress.bzl


# Section order within a project, not alphabetical. A .pure file with no `###` header
# inherits whatever section the PREVIOUS file left open, and the corpus's own first file
# (01-products.pure) has no header -- it relied on ###Pure being the default because nothing
# had ever come before it.
#
# Sorted alphabetically the project emits mapping, model, STORE, so the corpus's first file
# inherited ###Relational and its classes were handed to the relational parser. The whole
# corpus then failed with a bare `Unexpected token` naming no file and no line, which is
# exactly the failure model.check's header guard exists to prevent -- reaching the corpus
# from outside it for the first time.
#
# Mapping last, so what a following headerless file inherits is at least the section the
# corpus's own concatenation already ends on.
_SECTION_ORDER = {"model.pure": 0, "store.pure": 1, "mapping.pure": 2}


def store_closure(c, root: str) -> set:
    """`root` and every database it includes, transitively."""
    seen, stack = {root}, [root]
    while stack:
        for n in c.store_includes.get(stack.pop(), ()):
            if n not in seen:
                seen.add(n)
                stack.append(n)
    return seen


def linked_files() -> list[Path]:
    out = []
    for n in LINKED_PROJECTS:
        fs = sorted(_pure_files(PROJECTS / n))
        out += sorted(fs, key=lambda f: (_SECTION_ORDER.get(f.name, 99), f.name))
    return out


def mapping_closure(c: "Corpus", name: str) -> set:
    """Every Mapping reachable from `name` by `include`, transitively.

    Transitive because the corpus reaches core-tenor two ways: directly, and through
    fee_core::Mapping which includes it. A one-level check would call the second one a
    sibling and miss F55 on exactly the edge that found it.
    """
    seen, stack = set(), [name]
    while stack:
        for n in c.mapping_includes.get(stack.pop(), ()):
            if n not in seen:
                seen.add(n)
                stack.append(n)
    return seen


# ---------------------------------------------------------------- data model

@dataclass
class Column:
    name: str
    type: str
    pk: bool = False
    # Whether the DDL forbids a NULL here. Previously untracked, which is why the corpus's
    # adversarial NULL property (A2) only ever covered FOREIGN KEYS: nothing else could be
    # nulled without risking a NOT NULL violation in the generated DDL. That left every
    # transform over a plain column -- a dynafunction, most obviously -- with no null case
    # to meet.
    not_null: bool = False

    @property
    def kind(self) -> str:
        """Coarse class used by the seeder and by JSON rendering."""
        t = self.type.upper()
        # A parameterised type carries its precision -- DECIMAL(18,4), CHAR(12). The exact
        # membership tests below were written when VARCHAR was the only such type and
        # matched it by prefix, so every other parameterised type fell through to the raise.
        base = t.split("(")[0]
        if base in ("VARCHAR", "CHAR", "NVARCHAR", "TEXT"):
            return "string"
        if base in ("INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT"):
            return "int"
        if base in ("DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC"):
            return "float"
        # BIT is the boolean type in Legend's relational grammar. BOOLEAN is NOT in
        # RelationalDataType and legend-engine rejects it at parse time with
        # "Unsupported column data type 'BOOLEAN'" — legend-lite accepts it, which is how
        # this corpus shipped 194 unparseable columns.
        if base in ("BIT", "BOOLEAN"):
            return "bool"
        # Declared by the grammar and carried by the surface tables. They have no seeded
        # value and no rendering: the tables holding them have COMPOSITE keys, which the
        # seeder skips, so a kind is needed to READ the schema and never to produce a row.
        # Returning a plausible-looking kind instead would invite exactly that.
        if base in ("BINARY", "VARBINARY", "ARRAY", "OTHER", "SEMISTRUCTURED", "JSON"):
            return "unseedable"
        if base == "DATE":
            return "date"
        if base in ("TIMESTAMP", "DATETIME"):
            return "timestamp"
        raise ValueError(f"unhandled SQL type {self.type!r}")


@dataclass
class ViewCol:
    name: str
    source: str            # source column on the base table
    agg: str | None        # None for a grouping column; 'count' | 'sum' | 'max' | 'min'


@dataclass
class View:
    """A Legend View — an inlined GROUP BY, not a database view. No DDL is created for it;
    the engine folds the aggregation into the query it generates. The oracle therefore has
    to compute the same grouping itself, which views.py does."""
    name: str
    base: str
    group_by: list[str] = field(default_factory=list)
    columns: dict[str, ViewCol] = field(default_factory=dict)


@dataclass
class Milestoning:
    """Business-temporal (SCD2) or processing-temporal columns on a table."""
    kind: str          # 'business' | 'processing'
    frm: str
    thru: str
    # Required for %latest. Absent, dated queries still work and only %latest fails —
    # at plan generation, not at compile.
    infinity: str | None = None
    # THRU_IS_INCLUSIVE / OUT_IS_INCLUSIVE. Changes which rows a dated query selects at the
    # boundary, so it is read rather than ignored.
    inclusive: bool = False
    # A SNAPSHOT spec carries one as-of column instead of a from/thru pair; `frm` and `thru`
    # hold the same column and this flag is what distinguishes it from a degenerate range.
    snapshot: bool = False


@dataclass
class Table:
    name: str
    # Which Database declared it. Table names are scoped PER DATABASE in Legend; this
    # reader keys them globally, so two stores declaring the same name would silently
    # collide and one mapping would bind to the other's table. check() rejects that
    # rather than letting it happen quietly.
    database: str = ""
    # The Schema block the table is declared inside, or "default" when there is none.
    # Untracked until now, so a schema-qualified table could not be MAPPED (the mapping
    # names `[db]schema.TABLE`) nor SEEDED (a ###Data element keys rows by `schema.TABLE`),
    # and a Schema could only ever be declared. Both forms were verified against the engine
    # before this was added -- see scripts/corpus/repro/ -- rather than assumed to work.
    schema: str = "default"
    columns: dict[str, Column] = field(default_factory=dict)
    # A list, not one: a BITEMPORAL table declares both business and processing
    # milestoning in the same block, and the two are combined at query time.
    milestoning: list[Milestoning] = field(default_factory=list)

    def milestone(self, kind: str) -> Milestoning | None:
        for m in self.milestoning:
            if m.kind == kind:
                return m
        return None

    @property
    def pk(self) -> list[str]:
        return [c.name for c in self.columns.values() if c.pk]


@dataclass
class Join:
    """A.X = B.Y. Direction is resolved at use, not here — the same join is
    traversed both ways by the two ends of an association.

    `condition` carries the FULL parsed condition when the join is more than one equality --
    multi-column, non-equality, `or`, a dynafunction on either side. The four simple fields
    stay populated for the single-equality case, which is 192 of this corpus's 198 joins and
    which the indexed walk depends on; a general condition cannot be indexed, so it is
    evaluated per candidate row instead.

    Before this existed the reader dropped every join it could not fit into the four fields,
    without a word. Five of the six generated dense joins were in that state: unusable by any
    mapping, unexecutable by any service, and still counted as present by the feature meter.
    """
    name: str
    left_table: str
    left_col: str
    right_table: str
    right_col: str
    condition: object | None = None
    tables: tuple = ()

    @property
    def self_join(self) -> bool:
        return self.left_table == self.right_table

    def other(self, frm: str) -> tuple[str, str, str]:
        """(target_table, from_col, to_col) when entered from table `frm`.

        For a SELF-join both ends are the same table, so `frm` cannot disambiguate
        direction. The left-hand side is taken as the owning end — TRADER.MANAGER_ID
        points AT TRADER.TRADER_ID — which makes `manager` the navigable direction here.
        Reaching `reports` needs the reverse and is not modelled; the oracle refuses it
        rather than silently returning managers.
        """
        if self.self_join:
            return self.left_table, self.left_col, self.right_col
        if frm == self.left_table:
            return self.right_table, self.left_col, self.right_col
        if frm == self.right_table:
            return self.left_table, self.right_col, self.left_col
        raise KeyError(f"join {self.name} does not touch {frm}")


@dataclass
class Prop:
    name: str
    type: str          # 'String' | 'Float' | ... or a class FQN
    lower: int
    upper: int | None  # None == *


@dataclass
class Derived:
    """A derived property: `netCost() { $this.commission + $this.fees } : Float[0..1];`

    Stored as source text; oracle.py evaluates it. Deliberately not pre-parsed here — the
    expression grammar the oracle supports is narrow and enforced at evaluation time, so
    an expression outside it fails loudly at build rather than silently here.
    """
    name: str
    expr: str
    type: str
    lower: int
    upper: int | None
    # (name, type) per parameter; a QUALIFIED property if non-empty. The type is what lets a
    # generator choose an argument that type-checks rather than one that happens to work.
    params: list[tuple[str, str]] = field(default_factory=list)


@dataclass
class Func:
    """A standalone `function pkg::name(p: T[m], ...): R[m] { body }`.

    Stored as source text like a derived property; oracle.py evaluates it with the same
    expression evaluator. The FIRST parameter is the receiver — these are called
    extension-style, `$x->pkg::name()` — and the rest are supplied at the call site.
    """
    fqn: str
    params: list[tuple[str, str]]      # (name, type)
    ret: str
    body: str


@dataclass
class Klass:
    fqn: str
    props: dict[str, Prop] = field(default_factory=dict)
    derived: dict[str, Derived] = field(default_factory=dict)
    stereotypes: list[str] = field(default_factory=list)
    supertype: str | None = None

    @property
    def temporal(self) -> str | None:
        for s in self.stereotypes:
            if s.startswith("temporal."):
                return s.split(".", 1)[1]
        return None


@dataclass
class AssocEnd:
    """One navigable end. `owner` holds the property, `target` is what you land on."""
    owner: str
    name: str
    target: str
    to_many: bool
    join: str | None = None   # filled from the AssociationMapping
    # WHICH association declared this end. Recorded rather than inferred from the
    # association's NAME, which is what binding used to rely on: it split the short
    # name on "_" and required both class names to appear. An association not named
    # `A_B` therefore bound to nothing -- its ends kept `join=None` and every
    # navigation through it silently failed to resolve. Two of this corpus's
    # associations were in that state.
    assoc: str = ""


@dataclass
class Corpus:
    tables: dict[str, Table] = field(default_factory=dict)
    joins: dict[str, Join] = field(default_factory=dict)
    classes: dict[str, Klass] = field(default_factory=dict)
    enums: dict[str, list[str]] = field(default_factory=dict)
    functions: dict[str, Func] = field(default_factory=dict)
    # class fqn -> table name
    main_table: dict[str, str] = field(default_factory=dict)
    # (class fqn, property) -> (dynafunction name, [column, ...]) for property mappings
    # that TRANSFORM rather than copy. Kept beside `columns` rather than inside it because
    # six modules read `columns` expecting a bare column name; a transform there would
    # change meaning silently everywhere.
    dyna: dict[tuple[str, str], tuple[str, list[str]]] = field(default_factory=dict)

    # class -> (table, column) whose JSON payload backs it, via a Binding transformer. Its
    # properties are keys in that JSON, not columns.
    json_backed: dict[str, tuple[str, str]] = field(default_factory=dict)

    # mapping set id -> the class it maps. Needed to resolve `extends [setId]`, which names
    # a SET rather than a class.
    setid_class: dict[str, str] = field(default_factory=dict)

    # (parent class, property) -> embedded class. The hop stays on the SAME row: an
    # embedded property has no join and no association, so navigation must not look for one.
    embedded: dict[tuple[str, str], str] = field(default_factory=dict)

    # class fqn -> the Mapping that declared its class mapping (first one wins).
    declared_in: dict[str, str] = field(default_factory=dict)

    # (class, property) -> (binding path, table, column) for a Binding transformer. Recorded
    # so the property is not silently fabricated from the binding's NAME, and so a generator
    # can see it exists without the oracle having to deserialize the payload.
    bindings: dict[tuple[str, str], tuple[str, str, str]] = field(default_factory=dict)

    # (class, property) -> ([join names in order], target table, target column) for a
    # property mapped through a JOIN CHAIN rather than a column of the main table.
    chains: dict[tuple[str, str], tuple[list[str], str, str]] = field(default_factory=dict)

    # (class, property) the reader saw but cannot model -- join chains, Binding transformers.
    # Visible so a service using one fails with a reason rather than a mystery.
    unparsed: list[tuple[str, str]] = field(default_factory=list)

    # class fqn -> {property -> column}
    columns: dict[str, dict[str, str]] = field(default_factory=dict)
    # (class fqn, property) -> AssocEnd
    ends: dict[tuple[str, str], AssocEnd] = field(default_factory=dict)
    # EnumerationMapping name -> {source code -> enum value}. Many-to-one is legal and
    # used: 'B' and the legacy 'BOT' both mean BUY.
    enum_maps: dict[str, dict[str, str]] = field(default_factory=dict)
    # (class fqn, property) -> EnumerationMapping name
    enum_props: dict[tuple[str, str], str] = field(default_factory=dict)
    # class fqn -> the member TABLES of an Operation union mapping, in declared order
    unions: dict[str, list[str]] = field(default_factory=dict)
    views: dict[str, View] = field(default_factory=dict)
    # store filter name -> (table, column, op, literal)
    filters: dict[str, tuple[str, str, str, object]] = field(default_factory=dict)
    # class fqn -> the store filter its mapping applies
    class_filter: dict[str, str] = field(default_factory=dict)
    # Filters declared as MultiGrainFilter. Referenced from a mapping exactly like a Filter
    # -- the distinction is consumed by the planner, for join elision -- so the reader kept
    # no record of which was which and the feature could not be told apart from an ordinary
    # Filter even in principle.
    multigrain: set = field(default_factory=set)

    # cls -> ([join, ...], filter name). The predicate applies to the row the chain LANDS
    # on, so a row whose chain breaks is excluded -- unlike a to-one projection, where a
    # broken chain yields NULL and the row survives.
    class_filter_chain: dict[str, tuple[list[str], str]] = field(default_factory=dict)

    # (class, property) pairs declared in a MAPPING with `+` rather than on the class.
    local_props: set = field(default_factory=set)

    # database -> the databases it `include`s, directly. Not recorded until a corpus store
    # included a PROJECT store: an included store's tables live in the same physical database
    # at execution, so anything deciding "which tables does this store own" has to follow the
    # closure, or the included tables are never created and every query over them fails with
    # `Table with name X does not exist` -- which reads as a missing table rather than as a
    # table nobody seeded.
    store_includes: dict = field(default_factory=dict)

    # Mapping fqn -> the Mappings it `include`s DIRECTLY. Needed because "these two sets are
    # in different Mappings" says nothing on its own -- the corpus's ~150 domain mappings are
    # all siblings under stress::AllMapping and graph-fetch across them is fine. What is not
    # fine is a set reaching into a mapping its OWN mapping includes (F55), and only these
    # edges distinguish the two.
    mapping_includes: dict = field(default_factory=dict)

    # Classes whose set implementation carries ~distinct. The reader skipped the directive
    # as noise for a long time and the oracle never deduped, which was invisible while the
    # only ~distinct set in the corpus read a table whose rows were already unique.
    distinct_sets: set = field(default_factory=set)

    # -------------------------------------------------------- resolution

    def resolve(self, root: str, path: list[str]):
        """Walk a projection path from `root`.

        Returns (table, column, hops) where hops is the ordered list of
        (join_name, from_table, from_col, to_table, to_col) crossed to reach it.
        Raises on anything that does not resolve — the point of the exercise.
        """
        cls, table, hops = root, self.main_table.get(root), []
        if table is None:
            raise KeyError(f"class {root} has no ~mainTable")
        for i, step in enumerate(path):
            last = i == len(path) - 1
            end = self.ends.get((cls, step))
            if end is not None:
                if end.join is None:
                    raise KeyError(f"{cls}.{step} has no AssociationMapping")
                j = self.joins[end.join]
                tgt, fc, tc = j.other(table)
                hops.append((end.join, table, fc, tgt, tc))
                cls, table = end.target, tgt
                if last:
                    raise KeyError(f"path ends on association {cls}.{step}")
                continue
            child = self.embedded.get((cls, step))
            if child is not None:
                # An EMBEDDED hop stays on the same row: no join, no association, the
                # sub-object's columns live on the parent's table. So the class advances and
                # `table` and `hops` deliberately do not.
                if last:
                    raise KeyError(f"path ends on embedded property {cls}.{step}")
                cls = child
                continue
            src = self.json_backed.get(cls)
            if src is not None and step in self.classes.get(cls, Klass(cls)).props:
                # A Binding-backed class's properties are JSON KEYS. There is no column per
                # key -- they all come out of the one serialized column -- so that column is
                # what resolves, and the VALUE is deserialized by the oracle. Reported this
                # way so callers that only need the table and kind (rendering, for one) work
                # without knowing about bindings at all.
                return src[0], src[1], hops
            col = self.columns.get(cls, {}).get(step)
            if col is None:
                raise KeyError(f"{cls}.{step} is neither a mapped property nor an association")
            if not last:
                raise KeyError(f"{cls}.{step} is a column but the path continues")
            return table, col, hops
        raise KeyError("empty path")

    def owner_hops(self, root: str, path: list[str]):
        """(hops, table) reaching the row that CARRIES `path` -- every step consumed as a
        navigation, none required to end on a column.

        resolve() refuses a path ending on an embedded property, because such a property has
        no column of its own. That is right for a projection and wrong for asking "which row
        do this property's columns live on", which is what a dynafunction inside an embedded
        block needs: the block advances the class but contributes no hop, since the
        sub-object's columns are on the parent's row.
        """
        cls, table, hops = root, self.main_table.get(root), []
        if table is None:
            raise KeyError(f"class {root} has no ~mainTable")
        for step in path:
            end = self.ends.get((cls, step))
            if end is not None:
                if end.join is None:
                    raise KeyError(f"{cls}.{step} has no AssociationMapping")
                j = self.joins[end.join]
                tgt, fc, tc = j.other(table)
                hops.append((end.join, table, fc, tgt, tc))
                cls, table = end.target, tgt
                continue
            child = self.embedded.get((cls, step))
            if child is not None:
                cls = child                      # same row: deliberately no hop
                continue
            raise KeyError(f"{cls}.{step} is not a navigation")
        return hops, table

    def resolve_assoc(self, root: str, path: list[str]):
        """Walk a path that ends ON an association rather than on a column.

        Returns (hops, target_class). Used for aggregate projections such as
        `$x.trades->count()`, where the value is a property of the SET of landed rows
        rather than of any one row.
        """
        cls, table, hops = root, self.main_table.get(root), []
        if table is None:
            raise KeyError(f"class {root} has no ~mainTable")
        for step in path:
            end = self.ends.get((cls, step))
            if end is None:
                raise KeyError(f"{cls}.{step} is not an association")
            if end.join is None:
                raise KeyError(f"{cls}.{step} has no AssociationMapping")
            tgt, fc, tc = self.joins[end.join].other(table)
            hops.append((end.join, table, fc, tgt, tc))
            cls, table = end.target, tgt
        return hops, cls

    def resolve_derived(self, root: str, path: list[str]):
        """If the path ends on a derived property, return (hops, owning class, Derived).
        Otherwise None, so callers can fall through to the column resolver."""
        cls = root
        for step in path[:-1]:
            end = self.ends.get((cls, step))
            if end is None:
                return None
            cls = end.target
        # Walk the inheritance chain. A subclass has its supertype's derived properties --
        # `extends [base]` in the mapping inherits the property MAPPINGS, and the class
        # inherits the property itself -- but the reader records `derived` per class, so a
        # subclass of a class with a derived property had none of its own and the resolver
        # reported the property as neither a column nor an association. Columns were already
        # propagated to subclasses; derived properties were not, and nothing had noticed
        # because no subclass in the corpus carried one until the OTC taxonomy did.
        d, owner = None, cls
        while owner:
            d = self.classes.get(owner, Klass(owner)).derived.get(path[-1])
            if d is not None:
                break
            owner = self.classes.get(owner, Klass(owner)).supertype
        if d is None:
            return None
        hops = self.resolve_assoc(root, path[:-1])[0] if len(path) > 1 else []
        return hops, cls, d

    def owner_of(self, root: str, path: list[str]) -> str:
        """The class that declares the LAST step of a path — needed to look up whether
        that property carries an EnumerationMapping."""
        cls = root
        for step in path[:-1]:
            end = self.ends.get((cls, step))
            # An embedded hop advances the class without an association, so it has to be
            # consulted here too -- otherwise this raises on the very paths resolve() can
            # walk, and the two disagree about what the model contains.
            cls = end.target if end is not None else self.embedded[(cls, step)]
        return cls

    def to_many_on(self, root: str, path: list[str]) -> bool:
        """True if any hop along the path is to-many — i.e. the projection fans out."""
        cls = root
        for step in path[:-1]:
            end = self.ends.get((cls, step))
            if end is None:
                # An EMBEDDED hop cannot fan out: the sub-object lives on the same row, so
                # it contributes exactly one. Third navigation helper to need this -- resolve,
                # owner_of and now to_many_on -- which is the cost of adding a hop KIND
                # rather than a new association.
                cls = self.embedded[(cls, step)]
                continue
            if end.to_many:
                return True
            cls = end.target
        return False


# ---------------------------------------------------------------- parsing

_TABLE = re.compile(r"^\s*Table\s+(\w+)\s*\((.*)\)\s*$")
# `{target}` names the far side of a SELF-join, where both ends are the same table.
# Writing the table name twice would be a tautology matching every row against itself.
# `[db]` qualifiers on either side, for a CROSS-DATABASE join. Without admitting them the
# join was not captured at all -- and a chain referencing it failed with "join is not
# declared", which reads as a typo in the chain rather than as a join the reader could not
# see. Optional, so single-database joins are unaffected.
# Either side may also be SCHEMA-QUALIFIED -- `analytics.COMBO_SUMMARY.ROOT_ID`. Without
# that the simple pattern misses and the join falls through to the general-condition form,
# which sets no left_col/right_col; the seeder's foreign-key filler skips general conditions
# by design, so the FK column was filled by the generic value generator instead and pointed
# at nothing. The join still worked and the DATA silently did not join, which is the worst
# of the three possible outcomes.
_JOIN = re.compile(
    r"^\s*Join\s+(\w+)\s*\(\s*(?:\[[\w:]+\]\s*)?(?:\w+\.)?(\w+)\.(\w+)\s*=\s*"
    r"(?:\[[\w:]+\]\s*)?(?:\w+\.)?(\{target\}|\w+)\.(\w+)\s*\)\s*$")
# A Filter's column may carry a store qualifier -- `Filter F([db]T.COL is not null)` -- and
# its predicate may be a NULL TEST rather than a comparison. Neither was accepted before,
# so three of this corpus's seven filters matched nothing and were silently absent from
# c.filters. That is invisible rather than wrong only because all three happen to exclude
# no rows; a filter that discriminated would have made every expectation for its class
# wrong, with nothing pointing at the filter.
# A Join whose condition is anything more than a single equality. Matched loosely and then
# PARSED, because the condition can nest and a regex that tried to span it would match the
# wrong extent rather than fail.
_JOIN_HEAD = re.compile(r"^\s*Join\s+(\w+)\s*\((.*)\)\s*$")
# `[doc] NAME TYPE[(n[,m])] [PRIMARY KEY|NOT NULL]`, where NAME may be quoted.
_COLDEF = re.compile(r'^(\w+|"[^"]+")\s+(\w+(?:\(\d+(?:\s*,\s*\d+)?\))?)'
                     r'(?:\s+(?:PRIMARY KEY|NOT NULL))?$', re.I)
_FILTER_DECL = re.compile(
    # `(?:\w+\.)?` is a SCHEMA qualifier -- the fifth pattern in this reader to need one,
    # after _MAIN, _COLMAP, _ENUMCOLMAP and _BINDINGMAP. A filter over a schema-qualified
    # table matched nothing and was silently absent from c.filters, which check() then
    # reported as "uses undeclared store filter" against the CLASS rather than the filter.
    r"^\s*(?:MultiGrain)?Filter\s+(\w+)\s*\(\s*(?:\[[\w:]+\]\s*)?(?:\w+\.)?(\w+)\.(\w+)\s*"
    r"(?:(=|<>|<=|>=|<|>)\s*(.+?)|(is\s+not\s+null|is\s+null))\s*\)\s*$")
_CLS_FILTER = re.compile(r"^\s*~filter\s*\[[\w:]+\]\s*(\w+)\s*$")
# `~filter [db]@J1 > @J2 | [db]NAME` -- a filter reached through a JOIN CHAIN. The predicate
# applies to the row the chain LANDS on, so a row whose chain breaks is excluded.
# A join sequence may carry a JOIN KIND in parentheses -- `(INNER)` / `(OUTER)` -- before
# the leading pointer and before any later hop. The pattern admitted neither, so a filter
# using one raised rather than being read; the kind changes which rows survive the hop, so
# reading past it silently would be worse than the raise.
_CLS_FILTER_CHAIN = re.compile(
    r"^\s*~filter\s*(?:\[[\w:]+\]\s*)?(?:\(\s*\w+\s*\)\s*)?"
    r"((?:@\w+\s*(?:>\s*(?:\[[\w:]+\]\s*)?(?:\(\s*\w+\s*\)\s*)?)?)+)"
    r"\|\s*(?:\[[\w:]+\]\s*)?(\w+)\s*$")
# Stereotypes carry the temporal marker: `Class <<temporal.businesstemporal>> pkg::Name`
# The trailing `\s*$` used to be unconditional, which meant `Class X extends Y` did not
# match at all -- the class was SILENTLY SKIPPED and simply did not exist in the model. The
# corpus had no inheritance, so nothing ever noticed. Anything relying on a subclass would
# have failed with "unknown class" pointing at the USE rather than at the declaration the
# reader had quietly dropped.
# A Class header may carry, in this order: a leading documentation string, stereotypes,
# tagged values, and MULTIPLE supertypes. The pattern admitted only stereotypes and one
# supertype, so `Class <<s>> {p.t='v'} my::C extends A, B` matched NOTHING and the class did
# not exist -- the same silent skip that `extends` itself once caused. A class the reader
# cannot see has no properties, no mapping and no service, and nothing reports why.
_CLASS = re.compile(
    r"^\s*(?:'[^']*'\s*)?Class\s+(?:<<([^>]*)>>\s*)?(?:\{[^}]*\}\s*)?"
    r"([\w:]+)(?:\s+extends\s+([\w:]+(?:\s*,\s*[\w:]+)*))?\s*$")
_ASSOC = re.compile(r"^\s*Association\s+([\w:]+)\s*$")
_ENUM = re.compile(r"^\s*Enum\s+([\w:]+)\s*$")
_FUNC = re.compile(r"^\s*function\s+([\w:]+)\s*\(([^)]*)\)\s*:\s*([\w:]+)\s*\[([^\]]+)\]\s*$")
_FUNC_PARAM = re.compile(r"(\w+)\s*:\s*([\w:]+)\s*\[[^\]]+\]")
# A property may carry a documentation string, stereotypes, tagged values and an
# AGGREGATION KIND before its name, and a DEFAULT VALUE after its multiplicity. None was
# admitted, so `(composite) obligations: T[*];` and `active: Boolean[1] = true;` were both
# dropped in silence -- present in the model, absent from the reader, and therefore absent
# from every expectation computed over that class.
_PROP = re.compile(
    r"^\s*(?:'[^']*'\s*)?(?:<<[^>]*>>\s*)?(?:\{[^}]*\}\s*)?"
    r"(?:\(\s*(?:composite|shared|none)\s*\)\s*)?"
    # A type may carry TYPE PARAMETERS (`Relation<Any>`), a UNIT (`Money~USD`) or type
    # VARIABLE VALUES (`Varchar(200)`). Each was added after a property carrying it was
    # silently dropped -- or, once the guard existed, after it raised.
    r"(\w+)\s*:\s*"
    # An INLINE RELATION TYPE is a parenthesised column list -- `(name: String, id:
    # Integer[1])` -- which is a type, not a call, and shares no shape with the others.
    r"(\([^)]*\)|[\w:]+(?:~\w+)?(?:<[^>]*>)?(?:\(\s*\d+(?:\s*,\s*\d+)?\s*\))?)"
    r"\s*\[([^\]]+)\]\s*(?:=\s*[^;]+)?;\s*$")
# `name() { expr } : T[m];` and the qualified form `name(p: T[1], ...) { expr } : T[m];`
_DERIVED = re.compile(
    r"^\s*(\w+)\s*\(([^)]*)\)\s*\{(.+)\}\s*:\s*([\w:]+)\s*\[([^\]]+)\]\s*;\s*$")
# The TYPE is captured, not discarded. It used to be thrown away, so a qualified property's
# parameters were a list of names -- and a generator picking an argument for one had nothing
# to go on and used 0.5 for everything. That passed a Float to `tickerOn(vendor: String[1])`,
# which the oracle hit as `float + str` deep inside expression evaluation, naming neither the
# property nor the generator.
_PARAM = re.compile(r"(\w+)\s*:\s*([\w:]+)\s*\[[^\]]+\]")
# A reference may be SCHEMA-QUALIFIED: `[db]schema.TABLE.COL`. Tables are keyed globally
# by name here, so the schema qualifier is matched and discarded -- but it has to be
# MATCHED, or the pattern reads `analytics` as the table and `COMBO_SUMMARY` as the
# column and the mapping silently records a property against a table that does not exist.
_MAIN = re.compile(r'^\s*~mainTable\s*\[[\w:]+\]\s*(?:\w+\.)?(\w+|"[^"]+")\s*$')
# `Class: Relational`, `Class[id]: Relational`, and the root-marked `*Class: ...` form.
# `extends [parentId]` sits between the set id and the colon. Without admitting it, a
# subclass's class mapping did not match -- so `cur` stayed on the PREVIOUS class and every
# property of the subclass was recorded against its superclass. The symptom was
# "Can't find property 'coupon' in class 'hier::Instrument'" from the ENGINE, pointing at a
# generated service, three files away from the regex that caused it.
#
# Third construct to hit this exact blind spot: _CLASS could not see `Class X extends Y`,
# density's _BLOCK could not see a mapping using `extends [id]`, and now this. Each pattern
# was written before inheritance existed anywhere in the corpus.
_CLSMAP = re.compile(
    r"^\s*\*?([\w:]+)(?:\[(\w+)\])?(?:\s+extends\s*\[(\w+)\])?\s*:\s*Relational\s*\{?\s*$")
# An Operation class mapping may carry a SET ID like any other. Without it the
# header did not match, `cur_op` was never set, and the operation body fell to the
# property parser.
_OPMAP = re.compile(r"^\s*\*?([\w:]+)(?:\[\w+\])?\s*:\s*Operation\s*\{?\s*$")
# Three Operation forms share one shape: union, special_union and inheritance. The pattern
# named only the first, so the other two were not recorded as unions -- and, worse, their
# body then fell through to the property parser, which read `meta::pure::router::...` as a
# property called `meta`. A FABRICATED property, the same failure as `ProfileBinding`.
# `merge_...([idA, idB], {lambda})` -- the set ids are a BRACKETED list and are followed by
# a validation lambda, so the argument list cannot be read as a bare comma-separated set.
_MERGE = re.compile(r"merge_OperationSetImplementation_1__SetImplementation_MANY_"
                    r"\s*\(\s*(\[[^\]]*\])")
_UNION = re.compile(r"(?:special_|)(?:union|inheritance)"
                    r"_OperationSetImplementation_1__SetImplementation_MANY_"
                    r"\s*\(([^)]*)\)")
_COLMAP = re.compile(r'(\w+)\s*:\s*\[[\w:]+\]\s*(?:\w+\.)?(\w+|"[^"]+")\.(\w+|"[^"]+")')
# `+name: Type[mult]: [db]TABLE.COL` -- a LOCAL property, declared in the mapping rather than
# on the class. Three of them were in the corpus and the reader recorded none: not as a
# column, not as a chain, not even in `unparsed`. So nothing could project one, and nothing
# said so -- the construct scored as present on a text-matching meter while being invisible
# to every generator and to the oracle.
#
# Only the COLUMN form. `+localTag: String[0..1]: \'from-m2m\'` binds a constant rather than a
# column and has no column to record; it is left alone rather than guessed at.
_LOCALPROP = re.compile(
    r'\+\s*(\w+)\s*:\s*\w+\s*\[[^\]]+\]\s*:\s*\[[\w:]+\]\s*(?:\w+\.)?(\w+)\.(\w+)')
# `prop: concat([db]T.A, [db]T.B)` -- a DYNAFUNCTION property mapping. Like _ENUMCOLMAP this
# must be stripped BEFORE _COLMAP runs, or _COLMAP matches the first column inside the
# parentheses and records the property as a plain column mapping -- silently turning a
# transform into a copy, which the oracle would then agree with for the wrong reason.
# `prop: fn(` -- the ANCHOR for a dynafunction property mapping. Only the head is matched
# by pattern; the extent of the call is found by balancing parentheses and the contents are
# parsed by rhs.py, because the argument list can nest and can mix chains with columns.
_CALL_START = re.compile(r"(\w+)\s*:\s*(\w+)\s*\(")
# A property mapping whose right-hand side this reader has no rule for: a join chain
# (`prop: @A > @B | T.col`), a Binding transformer, an embedded block opener.
# `prop: [db]@J1 > @J2 | [db]T.COL` -- a JOIN CHAIN property mapping. The value comes from a
# table reached by following joins, with no association involved, so `resolve` (which walks
# associations) cannot find it. Must run BEFORE _COLMAP, which would otherwise match the
# trailing `T.COL` and record the property as a plain column of the MAIN table -- turning a
# navigation into a copy, silently and wrongly.
_CHAINMAP = re.compile(
    r"(\w+)\s*:\s*((?:\[[\w:]+\]\s*)?@[\w@\s>\[\]:.]*?)\|\s*(?:\[[\w:]+\]\s*)?(\w+)\.(\w+)")
_CHAINJOIN = re.compile(r"@(\w+)")

# `prop[targetSetId]: [db]@Join` -- a CLASS-TYPED property mapped over a join, with no
# trailing column because the property is a navigation rather than a value.
#
# This is the second of Legend's two valid ways to model an edge; the other is an Association
# with mapped ends. This reader modelled only the Association form, so a project using this
# one had its navigations recorded NOWHERE -- not in ends, not in chains, not in columns --
# and a query across such an edge died in `to_many_on` with a KeyError naming the property.
#
# Both styles are used across the project graph, and a project cannot tell which one a
# dependency chose from its MANIFEST. So the reader has to read both.
_JOINPROP = re.compile(
    r"(\w+)\s*(?:\[\s*\w+\s*\])?\s*:\s*(?:\[[\w:]+\]\s*)?@(\w+)\s*(?:,|$)", re.M)

# `prop: Binding path::B : [db]T.COL` -- a BINDING TRANSFORMER, reading a complex-typed
# property out of one column through an external-format binding.
#
# Must run BEFORE _COLMAP, and the consequence of not doing so was not a missed property but
# a FABRICATED one: _COLMAP matched the tail `ProfileBinding : [db]T.COL` and recorded a
# property called `ProfileBinding` on the class. A generated service then projected it and
# the engine answered "Can't find property 'ProfileBinding' in class 'hier::Issuer'" -- a
# property that never existed anywhere except in the reader's head.
# An EMBEDDED block opener: a bare property name followed by `(`. Its contents belong to the
# embedded CLASS, not to the parent, and this reader does not model that -- so they are
# routed to `unparsed` rather than flattened onto the parent, which is what it did before:
# `contact ( email: ..., phone: ... )` put `email` and `phone` on hier::Issuer, and a
# generated service projecting them was answered "Can't find property 'email' in class
# 'hier::Issuer'".
#
# `scope(` and `AssociationMapping(` share the shape and are excluded by name.
# The name and the `(` are usually on SEPARATE lines, so both forms are matched: the
# one-liner and a bare identifier whose next non-blank line is `(`.
# `scope([db]TABLE)` and the db-pointer-only `scope([db])`. Inside the first, property
# mappings are written with BARE column names; inside the second they carry `TABLE.COL`.
# Neither form matches _COLMAP, which requires a `[db]` prefix, so 60 generated class
# mappings were parsed as having no properties at all -- invisible rather than wrong,
# because each of those classes is also mapped flatly elsewhere.
_SCOPE_TABLE = re.compile(r"^\s*scope\s*\(\s*\[[\w:]+\]\s*(?:\w+\.)?(\w+)\s*\)\s*$")
_SCOPE_DB = re.compile(r"^\s*scope\s*\(\s*\[[\w:]+\]\s*\)\s*$")
_SCOPE_BARE = re.compile(r"(?:^|,)\s*(\w+)\s*:\s*(\w+)\s*(?:,|$)")
_SCOPE_QUAL = re.compile(r"(?:^|,)\s*(\w+)\s*:\s*(\w+)\.(\w+)\s*(?:,|$)")

_EMBED_OPEN = re.compile(r"^\s*(?!scope\b|AssociationMapping\b)([a-z]\w*)\s*\($")
_EMBED_NAME = re.compile(r"^\s*(?!scope\b|AssociationMapping\b)([a-z]\w*)\s*,?\s*$")

# The `(?:\w+\.)?` is a SCHEMA qualifier. _COLMAP and _MAIN had it and these two did not,
# so a schema-qualified table could be mapped column by column and not through an enum
# transformer or a binding -- the schema was captured as the table name and the check
# reported "maps to regrpt, not mainTable EXPOSURE_LINE", which names neither the schema nor
# the pattern that lost it.
_BINDINGMAP = re.compile(
    r"(\w+)\s*:\s*Binding\s+([\w:]+)\s*:\s*(?:\[[\w:]+\]\s*)?(?:\w+\.)?(\w+)\.(\w+)")

_LEFTOVER_PROP = re.compile(r"(?:^|,)\s*(\w+)\s*:\s*\S", re.M)
# `prop: EnumerationMapping <Name>: [db] TABLE.COL` — must be stripped BEFORE _COLMAP
# runs, or _COLMAP matches the tail and records the mapping NAME as the property.
_ENUMCOLMAP = re.compile(
    r"(\w+)\s*:\s*EnumerationMapping\s+(\w+)\s*:\s*\[[\w:]+\]\s*(?:\w+\.)?(\w+)\.(\w+)")
_ENUMMAP_HEAD = re.compile(r"^\s*([\w:]+)\s*:\s*EnumerationMapping\s+(\w+)\s*$")
# An enum value maps from a BRACKETED list of source values or from a SINGLE one, and a
# source value may be a string, an integer or a reference to another enum. Only the
# bracketed form was admitted, so `ALPHA: 1` raised -- correctly, since the guard is there,
# but the form is perfectly ordinary and the corpus simply had never written it.
_ENUMMAP_ROW = re.compile(
    r"^\s*(\w+)\s*:\s*(?:\[([^\]]*)\]|([^,\n]+?))\s*,?\s*$")
# An association end may name its SOURCE and TARGET set ids -- `end[srcId, tgtId]:` --
# which the pattern did not admit, so such an end bound to no join at all. The end
# still existed (it comes from the Association declaration), so the symptom was not a
# missing property but a navigation that could never resolve: `join=None`. The one
# association in the corpus written that way was unreachable for exactly this reason.
_ENDMAP = re.compile(r"(\w+)(?:\[\s*\w+\s*,\s*\w+\s*\])?\s*:\s*\[[\w:]+\]\s*@(\w+)")


def _strip(line: str) -> str:
    i = line.find("//")
    return (line[:i] if i >= 0 else line).rstrip()


def _mult(s: str) -> tuple[int, int | None]:
    s = s.strip()
    if s == "*":
        return 0, None
    if ".." in s:
        lo, hi = s.split("..", 1)
        return int(lo), (None if hi.strip() == "*" else int(hi))
    return int(s), int(s)


_MILESTONING_BLOCK = re.compile(r"milestoning\s*\((.*?)\)\s*\)\s*$", re.S)
# The FROM/THRU form, with its two optional trailing parameters in either order:
# INFINITY_DATE and the inclusivity flag (THRU_IS_INCLUSIVE / OUT_IS_INCLUSIVE). The pattern
# admitted INFINITY_DATE alone and in one position, so a table declaring inclusivity -- which
# CHANGES which rows a dated query selects -- failed to parse rather than being read.
_MILESTONING_ONE = re.compile(
    r"(business|processing)\s*\("
    r"\s*(?:BUS_FROM|PROCESSING_IN)\s*=\s*(\w+)\s*,"
    r"\s*(?:BUS_THRU|PROCESSING_OUT)\s*=\s*(\w+)\s*"
    r"(?:,\s*(?:INFINITY_DATE\s*=\s*%(?P<inf>[\d:.T+-]+)"
    r"|(?:THRU_IS_INCLUSIVE|OUT_IS_INCLUSIVE)\s*=\s*(?P<inc>true|false))\s*)*\)", re.S)
# The SNAPSHOT form: a single as-of column instead of a from/thru pair. Not modelled before,
# so a snapshot-milestoned table could not be declared at all.
_MILESTONING_SNAP = re.compile(
    r"(business|processing)\s*\("
    r"\s*(?:BUS_SNAPSHOT_DATE|PROCESSING_SNAPSHOT_DATE)\s*=\s*(\w+)\s*\)", re.S)


def _parse_milestoning(body: str):
    """Returns (specs, remaining body). A block may hold one spec or two — business AND
    processing on the same table is BITEMPORAL, and the two predicates AND together."""
    i = body.find("milestoning")
    if i < 0:
        return [], body
    j, depth = body.index("(", i) + 1, 1
    while depth and j < len(body):
        if body[j] == "(":
            depth += 1
        elif body[j] == ")":
            depth -= 1
        j += 1
    inner = body[body.index("(", i) + 1:j - 1]
    specs = [Milestoning(m.group(1), m.group(2), m.group(3), m.group("inf"),
                         inclusive=m.group("inc") == "true")
             for m in _MILESTONING_ONE.finditer(inner)]
    # A snapshot spec has ONE column, so `frm` and `thru` are the same and there is no
    # infinity date -- the shape is recorded rather than flattened, so a consumer can tell
    # the two apart instead of seeing a degenerate range.
    specs += [Milestoning(m.group(1), m.group(2), m.group(2), None, snapshot=True)
              for m in _MILESTONING_SNAP.finditer(inner)]
    if not specs:
        raise ValueError(f"unparseable milestoning block: {inner!r}")
    return specs, body[:i] + body[j:]


def _table_bodies(text: str) -> list[tuple[str, str, int]]:
    """(name, body) for every Table, matching parentheses so a declaration may span lines.

    The single-line reader this replaces could not see the canonical milestoned form,
    where `milestoning ( business(BUS_FROM = ..., BUS_THRU = ...) )` sits on its own lines
    ahead of the columns.
    """
    out = []
    # A table name may be QUOTED, which admits spaces and reserved words. `\w+` skipped such
    # a declaration entirely -- the table did not exist, and neither did any mapping over it.
    for m in re.finditer(r'\bTable\s+(\w+|"[^"]+")\s*\(', text):
        i, depth = m.end(), 1
        while depth and i < len(text):
            if text[i] == "(":
                depth += 1
            elif text[i] == ")":
                depth -= 1
            i += 1
        out.append((m.group(1).strip('"'), text[m.end():i - 1], m.start()))
    return out


_VIEW_GROUPBY = re.compile(r"~groupBy\s*\((.*?)\)", re.S)
_VIEW_AGG = re.compile(r"^(\w+)\s*:\s*(count|sum|max|min)\s*\(\s*(\w+)\.(\w+)\s*\)$")
_VIEW_PLAIN = re.compile(r"^(\w+)\s*:\s*(\w+)\.(\w+)(?:\s+PRIMARY\s+KEY)?$", re.I)


def _view_bodies(text: str) -> list[tuple[str, str]]:
    out = []
    for m in re.finditer(r"\bView\s+(\w+)\s*\(", text):
        i, depth = m.end(), 1
        while depth and i < len(text):
            if text[i] == "(":
                depth += 1
            elif text[i] == ")":
                depth -= 1
            i += 1
        out.append((m.group(1), text[m.end():i - 1]))
    return out


# A View may carry ~filter and ~distinct as well as ~groupBy, in that fixed order. The
# reader ignores the first two rather than modelling them: ~distinct changes which rows a
# view yields and ~filter changes which rows it sees, so a view using either is NOT
# oracle-computable from the base table alone. Stripping them keeps the reader honest about
# what it does understand -- the alternative was a parse failure that made the whole corpus
# unreadable the moment a view used a directive the base model never had.
_VIEW_DIRECTIVE = re.compile(r"~(filter\s+[\w:\[\]@|.]+|distinct)\s*", re.M)


def _parse_view(name: str, body: str, c: Corpus) -> None:
    body = _VIEW_DIRECTIVE.sub("", body)
    gb = _VIEW_GROUPBY.search(body)
    group_cols, base = [], None
    if gb:
        for ref in gb.group(1).split(","):
            ref = ref.strip()
            if not ref:
                continue
            tbl, col = ref.rsplit(".", 1)
            base = tbl.split("]")[-1]
            group_cols.append(col)
        body = _VIEW_GROUPBY.sub("", body, count=1)

    v = View(name, base or "", group_cols)
    for spec in _split_cols(body):
        spec = " ".join(spec.split())
        if not spec:
            continue
        m = _VIEW_AGG.match(spec)
        if m:
            v.base = v.base or m.group(3)
            v.columns[m.group(1)] = ViewCol(m.group(1), m.group(4), m.group(2))
            continue
        m = _VIEW_PLAIN.match(spec)
        if m:
            v.base = v.base or m.group(2)
            v.columns[m.group(1)] = ViewCol(m.group(1), m.group(3), None)
            continue
        raise ValueError(f"View {name}: unhandled column {spec!r}")
    c.views[name] = v


_DATABASE = re.compile(r"^\s*Database\s+([\w:]+)\s*$", re.M)



def _owning_schema(text: str, at: int) -> str:
    """The Schema block enclosing the declaration at `at`, or "default".

    Found by scanning backwards for the nearest `Schema x (` and checking the parentheses
    between it and here still leave us inside it -- a Schema's tables are nested, so a
    plain "nearest preceding Schema" would also claim every table declared AFTER the block
    closed.
    """
    best = "default"
    for m in re.finditer(r"^\s*Schema\s+(\w+)\s*$", text[:at], re.M):
        depth = text.count("(", m.end(), at) - text.count(")", m.end(), at)
        if depth > 0:
            best = m.group(1)
    return best

def _owning_database(text: str, at: int) -> str:
    """The Database whose declaration most recently precedes this offset."""
    last = ""
    for m in _DATABASE.finditer(text):
        if m.start() > at:
            break
        last = m.group(1)
    return last


def _fold_joins(text: str) -> str:
    """Join a `Join` declaration whose condition is spread over several lines.

    The corpus writes every join on one line, and a comment in fee-core records that a
    wrapped condition "is not parsed" -- which was true of THIS READER rather than of the
    grammar. core-account wraps all four of its two-column joins and compiles perfectly:

        Join Ca_AccountBranch(CA_ACCOUNT.INSTITUTION_ID = CA_BRANCH.INSTITUTION_ID
           and CA_ACCOUNT.BRANCH_CODE = CA_BRANCH.BRANCH_CODE)

    Folded on PARENTHESIS BALANCE rather than on a line count, because a condition may
    contain calls of its own. The third multi-line form a linked project has brought that
    the corpus itself never writes -- after the governed class header and the wrapped
    qualified property.
    """
    out, pending, depth = [], None, 0
    for raw in text.splitlines():
        if pending is None:
            if re.match(r"^\s*Join\s+\w+\s*\(", raw) and (
                    raw.count("(") != raw.count(")")):
                pending, depth = raw, raw.count("(") - raw.count(")")
                continue
            out.append(raw)
            continue
        pending = pending.rstrip() + " " + raw.strip()
        depth += raw.count("(") - raw.count(")")
        if depth <= 0:
            out.append(pending)
            pending = None
    if pending is not None:
        out.append(pending)
    return "\n".join(out)


def _parse_store(text: str, c: Corpus) -> None:
    text = _fold_joins("\n".join(_strip(l) for l in text.splitlines()))
    m = re.search(r"^Database\s+([\w:]+)", text, re.M)
    if m:
        c.store_includes.setdefault(m.group(1), []).extend(
            n for n in re.findall(r"^\s*include\s+([\w:]+)\s*$", text, re.M))
    for name, body in _view_bodies(text):
        _parse_view(name, body, c)
    for name, body, at in _table_bodies(text):
        if name in c.tables and c.tables[name].database != _owning_database(text, at):
            # STORE SUBSTITUTION requires two databases declaring the SAME table -- that is
            # the whole mechanism: `include M [dbA -> dbB]` rewrites the store pointer and
            # every column reference has to still resolve. So a duplicate name is only a
            # hazard when the two tables are DIFFERENT; when the column sets are identical,
            # binding to "the other one" cannot change any answer.
            #
            # The check stays otherwise, because the hazard it guards is real (F7: names are
            # scoped per Database in Legend and global here, so two genuinely different
            # tables sharing a name would silently cross-bind).
            existing = {col.name for col in c.tables[name].columns.values()}
            incoming = set(re.findall(r"^\s*(\w+)\s+[A-Z]", body, re.M))
            if not existing or existing != incoming:
                raise ValueError(
                    f"table {name} is declared by both {c.tables[name].database} and "
                    f"{_owning_database(text, at)} with DIFFERENT columns. Names are scoped "
                    f"per Database in Legend but global here, so one mapping would bind to "
                    f"the other's table. (Identical shapes are allowed -- that is store "
                    f"substitution.)")
            continue
        t = Table(name, database=_owning_database(text, at),
                  schema=_owning_schema(text, at))
        t.milestoning, body = _parse_milestoning(body)
        for spec in _split_cols(body):
            spec = " ".join(spec.split())
            if not spec:
                continue
            # A column may be QUOTED (spaces, reserved words) and its TYPE may carry a
            # precision. Splitting on whitespace took `"first name" VARCHAR(100)` as name
            # `"first` and type `name"`, so a quoted column silently became a wrong one --
            # worse than being skipped, because the table still existed with a fabricated
            # column.
            m_col = _COLDEF.match(spec)
            if m_col is None:
                raise ValueError(
                    f"table {t.name}: column form not modelled by this reader -- {spec!r}")
            name, ctype = m_col.group(1).strip('"'), m_col.group(2)
            u = spec.upper()
            t.columns[name] = Column(name, ctype, u.endswith("PRIMARY KEY"),
                                     "NOT NULL" in u)
        c.tables[t.name] = t

    for raw in text.splitlines():
        line = _strip(raw)
        m = _FILTER_DECL.match(line)
        if m:
            if m.group(6):                      # `is null` / `is not null`
                op = "isnull" if m.group(6).split()[1] == "null" else "isnotnull"
                val = None
            else:
                op = m.group(4)
                lit = m.group(5).strip()
                if lit.startswith("'") and lit.endswith("'"):
                    val = lit[1:-1]
                elif re.fullmatch(r"-?\d+", lit):
                    val = int(lit)
                elif re.fullmatch(r"-?\d*\.\d+", lit):
                    val = float(lit)
                else:
                    raise ValueError(f"Filter {m.group(1)}: unhandled literal {lit!r}")
            c.filters[m.group(1)] = (m.group(2), m.group(3), op, val)
            if line.lstrip().startswith("MultiGrainFilter"):
                c.multigrain.add(m.group(1))
            continue
        m = _JOIN.match(line)
        if m:
            n, lt, lc, rt, rc = m.groups()
            if n in c.joins:
                raise ValueError(f"duplicate join {n}")
            c.joins[n] = Join(n, lt, lc, lt if rt == "{target}" else rt, rc,
                              tables=(lt, lt if rt == "{target}" else rt))
            continue
        m = _JOIN_HEAD.match(line)
        if m:
            # A general condition. The two sides are whichever tables it names; `{target}`
            # resolves to the other one, since a self-join names its own table once and
            # {target} once.
            name, body = m.group(1), m.group(2)
            if name in c.joins:
                raise ValueError(f"duplicate join {name}")
            cond = rhs.parse_condition(body)
            named = sorted(rhs.condition_tables(cond))
            real = [x for x in named if x != "{target}"]
            if "{target}" in named:
                real = real * 2
            if len(real) != 2:
                raise ValueError(
                    f"Join {name} names {len(real)} table(s) {real} -- a join must connect "
                    f"exactly two. A condition mentioning ONE is rejected by the engine too, "
                    f"with \"can only find one table in the join\"; use the {{target}} form.")
            c.joins[name] = Join(name, real[0], "", real[1], "", condition=cond,
                                 tables=tuple(real))
            continue
        # A Join this reader cannot parse is DROPPED, and a dropped join is invisible in
        # exactly the way a dropped filter was: nothing references it, so nothing complains,
        # and the store object counts as "present" in the density meter while no mapping can
        # use it and no service can execute it. Five of the six generated dense joins --
        # multi-column, non-equality, `or`, and both dynafunction forms -- were in that state.
        if line.strip().startswith("Join "):
            raise ValueError(
                f"Join form not modelled by this reader -- {line.strip()!r}. Extend it "
                f"deliberately; dropping it silently makes the join uncountable and "
                f"unexecutable while leaving it visible to a text-matching feature meter.")


def _split_cols(body: str) -> list[str]:
    """Split a Table(...) body on commas that are not inside VARCHAR(200)."""
    out, depth, cur = [], 0, []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            out.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        out.append("".join(cur).strip())
    return out


# `Class my::C { a: String[1]; b: Integer[1]; }` written on ONE line. The header pattern
# requires the declaration to end the line, so a one-liner matched nothing and the class
# simply did not exist -- no properties, no mapping, no service, and nothing saying why. The
# ninth silent drop of this shape in this reader.
#
# Split rather than matched with a second pattern: once the body is on its own lines every
# existing rule applies to it unchanged, including the guard that catches an unmodelled
# property form.
_ONELINE = re.compile(r"^(\s*)((?:'[^']*'\s*)?(?:Class|Association|Enum)\s+[^{]*?)\s*\{(.*)\}\s*$")


def _explode_oneliners(text: str) -> str:
    out = []
    for line in text.splitlines():
        m = _ONELINE.match(line)
        if m is None:
            out.append(line)
            continue
        indent, head, body = m.groups()
        out.append(f"{indent}{head}")
        out.append(f"{indent}{{")
        for part in body.split(";"):
            if part.strip():
                out.append(f"{indent}   {part.strip()};")
        out.append(f"{indent}}}")
    return "\n".join(out)


def _fold_class_headers(text: str) -> str:
    """Join a `Class` declaration that is spread over several lines into one.

    The reader is line-based, and every class in the corpus itself declares itself on a
    single line. A linked project need not: core-types writes a governed class as

        Class <<core_types::CtGovernance.reviewed>>
              {core_types::CtGovernance.owner = 'ref-data-team',
               core_types::CtGovernance.since = '2024-01-15'}
              core_types::CtMoney

    -- four lines, of which only the last carries the name. Without folding, the guard below
    fires on the first line and the whole build stops, which is the right failure but not a
    useful one.

    Only the HEADER is folded, never a body: accumulation stops at the opening brace, and a
    header that has not become parseable by then is left exactly as it was so the guard can
    still report it verbatim.
    """
    out, pending = [], None
    for raw in text.splitlines():
        if pending is None:
            if re.match(r"^\s*(?:'[^']*'\s*)?Class\s", raw) and not _CLASS.match(_strip(raw)):
                pending = raw
                continue
            out.append(raw)
            continue
        # A line that is EXACTLY `{` opens the body, so the header is over -- parseable or
        # not. It has to be exactly that: a tagged-value block opens with `{` too, and
        # treating that as the body is what the first version of this did.
        if _strip(raw).strip() == "{":
            out += [pending, raw]
            pending = None
            continue
        pending = pending.rstrip() + " " + raw.strip()
        if _CLASS.match(_strip(pending)):
            out.append(pending)
            pending = None
    if pending is not None:
        out.append(pending)
    return "\n".join(out)


def _fold_qualified_props(text: str) -> str:
    """Join a qualified property whose body is on its own lines.

    The corpus writes these on one line -- `rate() { ... } : Float[1];` -- and so does most
    of fee-core. But a body long enough to wrap gets written out:

        grossFee(notional: Float[1])
        {
           $notional * core_types::ctBasisPointsToRate($this.rateBasisPoints)
        } : Float[1];

    which is the same property in the same grammar, and the reader saw four lines of which
    none was a property. The trigger is deliberately narrow -- a line that is a name and a
    parameter list and NOTHING else -- so an ordinary property can never start an
    accumulation. Joining stops when the braces balance and the statement is terminated.
    """
    out, pending, depth = [], None, 0
    for raw in text.splitlines():
        if pending is None:
            if re.match(r"^\s*\w+\s*\([^)]*\)\s*$", _strip(raw)):
                pending, depth = raw, 0
                continue
            out.append(raw)
            continue
        s = _strip(raw).strip()
        pending = pending.rstrip() + " " + s
        depth += s.count("{") - s.count("}")
        if depth == 0 and pending.rstrip().endswith(";"):
            out.append(pending)
            pending = None
    if pending is not None:
        out.append(pending)
    return "\n".join(out)


def _parse_domain(text: str, c: Corpus) -> None:
    text = _explode_oneliners(text)
    text = _fold_class_headers(text)
    text = _fold_qualified_props(text)
    cur_class = cur_assoc = cur_enum = None
    cur_func = None
    ends: list[tuple[str, str, int, int | None]] = []
    for raw in text.splitlines():
        line = _strip(raw)
        if not line.strip():
            continue
        m = _CLASS.match(line)
        if m:
            cur_class, cur_assoc, cur_enum = m.group(2), None, None
            k = c.classes.setdefault(cur_class, Klass(cur_class))
            k.stereotypes = [s.strip() for s in (m.group(1) or "").split(",") if s.strip()]
            k.supertype = m.group(3)
            continue
        m = _ASSOC.match(line)
        if m:
            cur_assoc, cur_class, cur_enum = m.group(1), None, None
            ends = []
            continue
        m = _FUNC.match(line)
        if m:
            cur_func = Func(m.group(1), _FUNC_PARAM.findall(m.group(2)), m.group(3), "")
            cur_class = cur_assoc = cur_enum = None
            continue
        if cur_func is not None:
            s = line.strip()
            if s == "{":
                continue
            if s == "}":
                c.functions[cur_func.fqn] = cur_func
                cur_func = None
                continue
            cur_func.body += (" " if cur_func.body else "") + s
            continue
        if (re.match(r"^\s*(?:'[^']*'\s*)?Class\s", line) and not _CLASS.match(line)
                and cur_class is None):
            raise ValueError(
                f"Class declaration form not modelled by this reader -- {line.strip()!r}. "
                f"A class the reader cannot see has no properties, no mapping and no "
                f"service, and nothing reports why.")
        m = _ENUM.match(line)
        if m:
            cur_enum, cur_class, cur_assoc = m.group(1), None, None
            c.enums[cur_enum] = []
            continue
        if line.strip() == "}":
            if cur_assoc:
                if len(ends) != 2:
                    raise ValueError(f"association {cur_assoc} has {len(ends)} ends")
                (n0, t0, _, u0), (n1, t1, _, u1) = ends
                # The end named n0 has type t0, so it is reachable FROM t1.
                c.ends[(t1, n0)] = AssocEnd(t1, n0, t0, u0 is None or u0 > 1,
                                            assoc=cur_assoc)
                c.ends[(t0, n1)] = AssocEnd(t0, n1, t1, u1 is None or u1 > 1,
                                            assoc=cur_assoc)
            cur_class = cur_assoc = cur_enum = None
            continue
        if cur_enum is not None:
            v = line.strip().rstrip(",")
            if v and v != "{":
                c.enums[cur_enum].append(v)
            continue
        m = _DERIVED.match(line)
        if m and cur_class:
            lo, hi = _mult(m.group(5))
            c.classes[cur_class].derived[m.group(1)] = Derived(
                m.group(1), m.group(3).strip(), m.group(4), lo, hi,
                _PARAM.findall(m.group(2)))
            continue
        m = _PROP.match(line)
        if m:
            name, typ, mult = m.group(1), m.group(2), m.group(3)
            lo, hi = _mult(mult)
            if cur_class:
                c.classes[cur_class].props[name] = Prop(name, typ, lo, hi)
            elif cur_assoc:
                ends.append((name, typ, lo, hi))
            continue
        # A line inside a class body that ENDS LIKE A PROPERTY but matched neither _PROP nor
        # _DERIVED is a declaration form this reader cannot see. Dropping it is the failure
        # this file has now made six times -- `Class X extends Y`, `extends [id]`, join
        # chains, dyna-over-chain, scope blocks, decorated properties -- and every time the
        # symptom appeared somewhere far away, as a property that "did not exist".
        #
        # Constraint bodies are excluded by name: they sit between the class header and the
        # body and end in a comma or bracket, not a semicolon.
        if (cur_class and line.rstrip().endswith(";")
                and ":" in line and not line.lstrip().startswith(("~", "//", "*"))):
            raise ValueError(
                f"{cur_class}: property form not modelled by this reader -- "
                f"{line.strip()!r}. Extend it deliberately; a property the reader cannot "
                f"see is absent from every expectation computed over this class, and "
                f"nothing reports why.")


def _split_mappings(body: str) -> list[str]:
    """A ###Mapping section may hold several Mapping elements; skipping one means
    splitting them apart first."""
    out, cur = [], []
    for line in body.splitlines(True):
        if _MAPPING_NAME.match(line) and cur:
            out.append("".join(cur))
            cur = []
        cur.append(line)
    if cur:
        out.append("".join(cur))
    return out


def _parse_mapping(text: str, c: Corpus, mapping_name: str | None = None) -> None:
    if mapping_name:
        c.mapping_includes.setdefault(mapping_name, []).extend(
            re.findall(r"^\s*include\s+([\w:]+)\s*$", text, re.M))
    cur = None
    cur_id = None
    cur_op = None
    op_buf = ""
    set_tables: dict[str, str] = {}   # set-implementation id -> its ~mainTable
    # Initialised HERE as well as per class mapping: the generic closing-punctuation skip
    # below now consults it, and that skip runs before the first class mapping is seen.
    embedded: list[str] = []
    pending_embed = None
    scope_tbl = None
    in_assoc = False
    enum_map = None          # name of the EnumerationMapping currently being read
    for raw in text.splitlines():
        line = _strip(raw)
        if not line.strip():
            continue

        m = _ENUMMAP_HEAD.match(line)
        if m:
            enum_map, cur = m.group(2), None
            c.enum_maps.setdefault(enum_map, {})
            continue
        if enum_map is not None:
            if line.strip() == "}":
                enum_map = None
                continue
            m = _ENUMMAP_ROW.match(line)
            if m:
                value = m.group(1)
                # group(2) is the bracketed list, group(3) the single value.
                for code in (m.group(2) if m.group(2) is not None else m.group(3)).split(","):
                    code = code.strip().strip("'")
                    if code:
                        if code in c.enum_maps[enum_map]:
                            raise ValueError(
                                f"EnumerationMapping {enum_map}: source code {code!r} "
                                f"maps to both {c.enum_maps[enum_map][code]} and {value}")
                        c.enum_maps[enum_map][code] = value
                continue
            if line.strip() != "{":
                raise ValueError(f"unhandled EnumerationMapping line: {line!r}")
            continue
        m = _OPMAP.match(line)
        if m:
            cur, in_assoc, cur_op = None, False, m.group(1)
            continue
        if cur_op and ("OperationSetImplementation" in line or op_buf):
            # BUFFERED, not matched line by line. A merge operation carries a set-id list AND
            # a validation lambda, so its call spans several lines; matching one line at a
            # time saw only the qualified name and raised on a form that is perfectly valid.
            op_buf += " " + line.strip()
            # Keep buffering until the call is COMPLETE: either no `(` has been seen yet
            # (the qualified name sits on its own line) or the parens are still open.
            if "(" not in op_buf or op_buf.count("(") > op_buf.count(")"):
                continue
            m_op = _UNION.search(op_buf) or _MERGE.search(op_buf)
            if m_op is None:
                raise ValueError(
                    f"{cur_op}: Operation form not modelled by this reader -- "
                    f"{op_buf.strip()!r}. An unrecognised operation body falls through to "
                    f"the property parser, which reads its qualified name as a property.")
            ids = [i.strip(" []") for i in m_op.group(1).split(",") if i.strip(" []")]
            c.unions[cur_op] = [set_tables[i] for i in ids if i in set_tables]
            cur_op, op_buf = None, ""
            continue
        m = _CLSMAP.match(line)
        if m and "AssociationMapping" not in line:
            cur, in_assoc, cur_id = m.group(1), False, m.group(2)
            embedded, pending_embed, scope_tbl = [], None, None
            if cur_id:
                c.setid_class[cur_id] = cur
            # `extends [parentSetId]` INHERITS the parent set's main table and property
            # mappings -- an extending set declares only what it adds, and carries no
            # ~mainTable of its own. Without this the subclass had no table and no columns,
            # so inheritance was covered at compile time and by nothing else.
            parent_set = m.group(3)
            if parent_set:
                parent_cls = c.setid_class.get(parent_set)
                if parent_cls:
                    c.main_table.setdefault(cur, c.main_table.get(parent_cls, ""))
                    for pp, pcol in c.columns.get(parent_cls, {}).items():
                        c.columns.setdefault(cur, {}).setdefault(pp, pcol)
                    # A column inherited through `extends` keeps its TRANSFORMER. Copying
                    # the column and not the enum transformer read the raw source code off
                    # the row -- 'ZC' where the engine returns ZERO, 'AMER' where it returns
                    # AMERICAN -- and only a subtype whose enum property is declared on an
                    # ANCESTOR set shows it. Every enum in the corpus's own model is declared
                    # on the set that is queried, so nothing had ever inherited one.
                    for (pcls, pp), emap in list(c.enum_props.items()):
                        if pcls == parent_cls:
                            c.enum_props.setdefault((cur, pp), emap)
                    # Same argument for the other two transformer kinds.
                    for (pcls, pp), b in list(c.bindings.items()):
                        if pcls == parent_cls:
                            c.bindings.setdefault((cur, pp), b)
                    for (pcls, pp), ch in list(c.chains.items()):
                        if pcls == parent_cls:
                            c.chains.setdefault((cur, pp), ch)
            # WHICH mapping declared this class mapping. The reader is otherwise
            # mapping-agnostic, and deriving a service's mapping from the class's TABLE
            # instead got it wrong the moment a mapping spanned two stores: hier::
            # InstrumentReach is declared in IssuerMapping with a main table in HierDB, and
            # routing by database produced "Error mapping not found for class
            # InstrumentReach". Only the FIRST declaration is kept -- a class mapped twice
            # still collapses, which is the wider limitation this does not fix.
            if mapping_name and cur not in c.declared_in:
                c.declared_in[cur] = mapping_name
            continue
        if "AssociationMapping" in line:
            in_assoc = True
            continue
        # NOT while an embedded block is open. A nested block closes with a bare `)` and its
        # parent with `),`, and this generic skip swallowed the bare one -- so `ownership (
        # ... taxResidence ( ... ) )` popped once instead of twice and stayed open. Every
        # property declared after it was then attributed to Ownership, where it resolved
        # against nothing: core_account::Account's second sibling block and all six of its
        # outbound joins went to `unparsed`, and the class read as having no navigation.
        #
        # Only the comma made the difference, which is why one level of nesting worked and
        # a sibling after it did not.
        if line.strip() in ("}", ")", "    }") and not embedded:
            continue
        m = _CLS_FILTER.match(line)
        if m and cur:
            c.class_filter[cur] = m.group(1)
            continue
        m = _CLS_FILTER_CHAIN.match(line)
        if m and cur:
            c.class_filter_chain[cur] = (_CHAINJOIN.findall(m.group(1)), m.group(2))
            continue
        # A ~filter this reader cannot model changes what the class MEANS -- all() returns a
        # different row set -- so every expectation for that class would be silently wrong.
        # Dropping it is the worst available option and is exactly what happened: the
        # chain-filter form matched neither pattern and vanished, and the corpus only stayed
        # green because the filter excluded no rows. Raise instead.
        if line.strip().startswith("~filter") and cur:
            raise ValueError(
                f"{cur}: ~filter form not modelled by this reader -- {line.strip()!r}. "
                f"Extend the reader deliberately; ignoring it would change what all() "
                f"returns for {cur} with nothing pointing at the cause.")
        # ~distinct on a class mapping. Recorded here, before the column parsing, because
        # the property-shaped catch-all further down never sees a bare directive line -- so
        # the construct was skipped as noise and the oracle never deduped. Invisible until a
        # ~distinct set finally read a table with twelve rows per output row.
        if line.strip() == "~distinct" and cur:
            c.distinct_sets.add(cur)
            continue
        m = _MAIN.match(line)
        if m and cur:
            # For a union member, the per-id table is what the union needs; the class's
            # own main_table is set by whichever member comes last and is only used as a
            # fallback for callers that do not know about unions.
            # Quotes are stripped here, not in the pattern: a quoted name is the SAME table
            # as its unquoted key, and leaving them on made ~mainTable point at a table that
            # "is not declared" while sitting right there in the store.
            if cur_id:
                set_tables[cur_id] = m.group(1).strip('"')
            c.main_table[cur] = m.group(1).strip('"')
            continue
        if cur and in_assoc:
            for prop, join in _ENDMAP.findall(line):
                for (owner, name), end in c.ends.items():
                    if name == prop and _assoc_matches(cur, owner, end):
                        end.join = join
            continue
        if cur and cur in c.main_table:
            tbl = c.main_table[cur]
            m_sc = _SCOPE_TABLE.match(line)
            if m_sc:
                scope_tbl, pending_embed = m_sc.group(1), None
                continue
            if _SCOPE_DB.match(line):
                scope_tbl, pending_embed = "", None      # qualified inside
                continue
            if scope_tbl is not None:
                if line.strip() in ("(", ""):
                    continue
                if line.strip().startswith(")"):
                    scope_tbl = None
                    continue
                if scope_tbl:
                    # Value forms FIRST, with the scope's table supplied for bare columns.
                    # A dynafunction inside a scope -- `up: toUpper(A)` -- is accepted by
                    # the engine and matched none of the scope patterns, which recognise
                    # `prop: COL` only; it was therefore dropped, and a scope block could
                    # hold nothing but plain columns as far as this reader was concerned.
                    rest = _value_forms(line, c, cur, scope_tbl, bare=scope_tbl)
                    for sp, sc in _SCOPE_BARE.findall(rest):
                        c.columns.setdefault(cur, {}).setdefault(sp, sc)
                else:
                    rest = _value_forms(line, c, cur, tbl)
                    for sp, _st, sc in _SCOPE_QUAL.findall(rest):
                        c.columns.setdefault(cur, {}).setdefault(sp, sc)
                continue
            m_emb = _EMBED_OPEN.match(line)
            if m_emb:
                embedded.append(m_emb.group(1))
                continue
            if pending_embed and line.strip() == "(":
                embedded.append(pending_embed)
                pending_embed = None
                continue
            m_name = _EMBED_NAME.match(line)
            pending_embed = m_name.group(1) if m_name else None
            if m_name:
                continue
            if embedded:
                if line.strip().startswith(")"):
                    # POP one level, not all of them. `embedded` used to be a single name,
                    # so a block nested inside another replaced its parent rather than
                    # descending -- the inner mappings were then attributed to the OUTER
                    # class, where they resolved against nothing and landed in `unparsed`.
                    embedded.pop()
                    pending_embed = None
                    continue
                # An embedded property's sub-mappings read columns of the SAME row -- no
                # join, no association. So the embedded CLASS is given the parent's table
                # and its own columns, and the hop from parent to child is recorded as
                # embedded so navigation knows to stay on the row rather than look for an
                # association that does not exist.
                #
                # The OWNER is whatever class the enclosing blocks have descended to, so a
                # nested block hangs off its immediate parent rather than off the root.
                owner = cur
                for step in embedded[:-1]:
                    owner = c.embedded.get((owner, step), owner)
                name = embedded[-1]
                child = None
                kl = c.classes.get(owner)
                if kl is not None and name in kl.props:
                    child = kl.props[name].type
                if child is None:
                    for sub in _LEFTOVER_PROP.findall(line):
                        c.unparsed.append((cur, f"{name}.{sub}"))
                    continue
                # The hop is recorded from the BLOCK, not from the first column mapping
                # inside it. Recording it per-column meant a block whose every entry was a
                # dynafunction registered no embedded hop at all, so the property looked
                # absent rather than unmodelled.
                c.embedded[(owner, name)] = child
                c.main_table.setdefault(child, tbl)
                # An ENUM TRANSFORMER inside an embedded block. Handled here as well as on
                # the plain path, because _COLMAP left to itself reads
                # `curveType: EnumerationMapping CurveTypeMapping: [db]T.CURVE_TYPE` as a
                # property called CurveTypeMapping and drops curveType entirely -- so the
                # enum came out as a column that does not exist and the real property
                # resolved against nothing.
                for prop, mapping, e_tbl, col in _ENUMCOLMAP.findall(line):
                    if e_tbl != tbl:
                        raise ValueError(f"{child}.{prop} maps to {e_tbl}, not mainTable {tbl}")
                    c.columns.setdefault(child, {})[prop] = col
                    c.enum_props[(child, prop)] = mapping
                line = _ENUMCOLMAP.sub("", line)
                rest = _value_forms(line, c, child, tbl)
                for sub, _sc_tbl, sc_col in _COLMAP.findall(rest):
                    c.columns.setdefault(child, {})[sub] = sc_col
                for sub in _LEFTOVER_PROP.findall(_COLMAP.sub("", rest)):
                    if (sub not in c.columns.get(child, {})
                            and (child, sub) not in c.chains
                            and (child, sub) not in c.dyna):
                        c.unparsed.append((cur, f"{name}.{sub}"))
                continue
            # LOCAL properties first: their tail is a plain column mapping preceded by a
            # type and a multiplicity, so _COLMAP left to itself reads the TYPE as the
            # property name.
            for prop, l_tbl, col in _LOCALPROP.findall(line):
                if l_tbl != tbl:
                    raise ValueError(f"{cur}.+{prop} maps to {l_tbl}, not mainTable {tbl}")
                c.columns.setdefault(cur, {})[prop] = col
                c.local_props.add((cur, prop))
            line = _LOCALPROP.sub("", line)
            for prop, mapping, t, col in _ENUMCOLMAP.findall(line):
                if t != tbl:
                    raise ValueError(f"{cur}.{prop} maps to {t}, not mainTable {tbl}")
                c.columns.setdefault(cur, {})[prop] = col
                c.enum_props[(cur, prop)] = mapping
            line = _ENUMCOLMAP.sub("", line)
            # Binding transformers first: their tail is a plain column mapping preceded by
            # a binding path, so _COLMAP would fabricate a property from the binding's name.
            for prop, binding, btbl, bcol in _BINDINGMAP.findall(line):
                c.bindings[(cur, prop)] = (binding, btbl, bcol)
                # A Binding transformer reads a COMPLEX property out of ONE column holding
                # serialized JSON. So the hop behaves like an embedded one -- it stays on
                # the same row -- but the sub-properties are JSON KEYS rather than columns,
                # which is recorded separately so the oracle knows to deserialize instead of
                # looking for columns that do not exist.
                kl = c.classes.get(cur)
                child = kl.props[prop].type if kl and prop in kl.props else None
                if child:
                    c.embedded[(cur, prop)] = child
                    c.json_backed[child] = (btbl, bcol)
                    c.main_table.setdefault(child, btbl)
            line = _BINDINGMAP.sub("", line)
            line = _value_forms(line, c, cur, tbl)
            # Anything that still LOOKS like a property mapping after the recognised forms
            # have been stripped is a form this reader does not model -- a join chain, a
            # Binding transformer, an embedded block. Recorded rather than dropped.
            #
            # Silent dropping is a pattern that has now bitten three times: `Class X extends
            # Y` did not match _CLASS and the class simply did not exist; a mapping using
            # `extends [id]` matched no block in the density meter; and join-chain properties
            # vanished here. In every case the symptom appeared far from the cause. A
            # property the oracle cannot resolve should be visible in the reader, not
            # discovered when a service using it fails.
            for prop, t, col in ((a, b.strip('"'), d.strip('"'))
                                 for a, b, d in _COLMAP.findall(line)):
                if t != tbl:
                    raise ValueError(f"{cur}.{prop} maps to {t}, not mainTable {tbl}")
                c.columns.setdefault(cur, {})[prop] = col

            # CATCH-ALL, not a pattern list. Anything still shaped like `prop:` after every
            # recognised form has been stripped is a property mapping this reader does not
            # model, and it is recorded rather than dropped.
            #
            # The previous version matched only `@` and `Binding` right-hand sides, so a
            # dynafunction OVER a join chain -- `toUpper([db]@J1 > @J2 | [db]T.COL)` -- fell
            # through both the recognisers and the leftover check and vanished entirely.
            # That was the sixth silent drop in this reader. A catch-all cannot have a
            # seventh: a construct it does not recognise is, by construction, still shaped
            # like `prop:`.
            for leftover in _LEFTOVER_PROP.findall(_COLMAP.sub('', line)):
                if leftover in ("mainTable", "filter", "groupBy", "distinct",
                                "primaryKey", "src", "func"):
                    continue
                # Only if NOTHING has resolved this property. A scope() block writes bare
                # `prop: COL` with no [db]TABLE. prefix and this reader does not model
                # scope -- but the same property is usually also mapped flatly elsewhere,
                # and a property the oracle CAN resolve by another route is not a gap.
                # Without this the list was 653 entries, almost all of them scope-block
                # restatements of columns already known.
                if (leftover not in c.columns.get(cur, {})
                        and (cur, leftover) not in c.chains
                        and (cur, leftover) not in c.dyna):
                    c.unparsed.append((cur, leftover))


def _value_forms(line: str, c: "Corpus", owner: str, tbl: str,
                 bare: str | None = None) -> str:
    """Record every VALUE-expression property mapping on `line` against `owner`.

    Returns the line with the recognised forms stripped, so the caller can decide what to do
    with whatever is left.

    Extracted because the top-level path and the embedded-block path had drifted: the
    embedded path understood plain columns and nothing else, so a dynafunction or a join
    chain inside an embedded block went to `unparsed` -- 22 of the combination matrix's 48
    cells, all of which the reader understood perfectly well one nesting level up. Two
    copies of "how a property mapping is written" is one copy too many.
    """
    while True:
        m = _CALL_START.search(line)
        if m is None:
            break
        prop, _fn = m.group(1), m.group(2)
        close = rhs.find_call(line, line.index("(", m.end() - 1))
        node = rhs.parse(line[m.start(2):close + 1], default_table=bare)
        _tag, (fname, args) = node
        c.dyna[(owner, prop)] = (fname, args)
        # A plain-column argument on the main table keeps its entry in c.columns, so
        # everything that types or resolves a property by column continues to work for the
        # shapes that already existed. An expression whose every argument is a chain has no
        # such column and is typed from the function's return kind instead.
        #
        # A PLAIN argument must sit on the main table, because that is the row the oracle
        # holds. A value from another table is legal in the grammar but has to be written as
        # a chain, so the reader records how to REACH it rather than assuming the column is
        # somehow already in hand.
        local = []
        for atbl, acol in rhs.columns(node):
            if atbl != tbl:
                raise ValueError(
                    f"{owner}.{prop} uses {fname}() over {atbl}.{acol}, not mainTable "
                    f"{tbl} -- write it as a chain if it needs a join, since the oracle "
                    f"has no row of {atbl} to read it from")
            local.append(acol)
        if local:
            c.columns.setdefault(owner, {})[prop] = local[0]
        line = line[:m.start(1)] + line[close + 1:]
    # Join chains BEFORE plain columns: a chain's tail looks exactly like one.
    for prop, chaintext, ctbl, ccol in _CHAINMAP.findall(line):
        joins = _CHAINJOIN.findall(chaintext)
        if joins:
            c.chains[(owner, prop)] = (joins, ctbl, ccol)
    line = _CHAINMAP.sub("", line)

    # A class-typed property over a single join, recorded as an association end so every
    # navigation helper -- resolve, resolve_assoc, owner_of, to_many_on -- treats it the same
    # way it treats an Association. The declared multiplicity on the class decides to_many;
    # the mapping does not carry it.
    for prop, joinname in _JOINPROP.findall(line):
        if (owner, prop) in c.ends:
            continue
        kl = c.classes.get(owner)
        decl = kl.props.get(prop) if kl else None
        if decl is None or decl.type not in c.classes:
            continue
        c.ends[(owner, prop)] = AssocEnd(
            owner=owner, name=prop, target=decl.type,
            to_many=decl.upper != 1, join=joinname, assoc=None)
    return line


def _assoc_matches(assoc_fqn: str, owner: str, end: AssocEnd) -> bool:
    """An AssociationMapping names its ends by property only, so binding a property to the
    right association relies on the association's NAME carrying both class names.

    A SELF-association breaks that: org::Trader_Manager has owner == target == Trader, so
    the pair {Trader, Manager} never matches {Trader}. Handled explicitly — for a
    self-association it is enough that one half of the name is the class.
    """
    if end.assoc:
        return end.assoc == assoc_fqn
    short = assoc_fqn.split("::")[-1]
    a, _, b = short.partition("_")
    o, t = owner.split("::")[-1], end.target.split("::")[-1]
    if o == t:
        return o in (a, b)
    return {a, b} == {o, t} or (a in (o, t) and b in (o, t))


def sections(text: str) -> list[tuple[str, str]]:
    """Split a source into (section kind, body). A section header is a line that STARTS
    with ###; the mention of one inside a comment is not a header. Dispatching on
    `"###Mapping" in text` instead of this cost an afternoon — a comment in
    90-cross-domain-associations.pure explaining the header requirement contained the
    word, so the file was silently treated as a mapping and 44 associations vanished."""
    out, kind, buf = [], "Pure", []
    for line in text.splitlines():
        if line.startswith("###"):
            out.append((kind, "\n".join(buf)))
            kind, buf = line[3:].strip(), []
        else:
            buf.append(line)
    out.append((kind, "\n".join(buf)))
    return [(k, b) for k, b in out if b.strip()]


# Mappings the oracle deliberately does NOT model.
#
# reporting::EmbeddedFlatMapping maps trading::Trade to TRADE_FLAT with EMBEDDED property
# mappings. Parsing it with the line-based reader here would rebind trading::Trade's
# ~mainTable to TRADE_FLAT and attach the embedded columns to the wrong class, silently
# corrupting every expectation in the corpus.
#
# It is skipped rather than supported because it does not need to be supported: the whole
# claim of mapping invariance is that a query through this mapping returns what the
# canonical mapping returns. Its expectation IS the canonical expectation, so computing a
# second one would be circular at best and wrong at worst.
SKIP_MAPPINGS = {
    "reporting::EmbeddedFlatMapping",
    # An M2M mapping has no tables at all — it maps class to class. The line-based reader
    # here understands `prop: [db] TABLE.COL` and nothing else, so parsing it would find
    # no mainTable and attach nothing. Skipped for the same reason as the embedded
    # mapping: the canonical class's expectation is DERIVED from the source class it is
    # mapped from, which is what the invariance asserts.
    "canonical::M2MMapping",
    "canonical::MoneyMapping",
    # Both map trading::Trade onto TRADE_FLAT_PARTIAL with embedded blocks; parsing them
    # would rebind Trade's ~mainTable exactly as EmbeddedFlatMapping would. Their
    # expectations are mirrored from the canonical model, which is the claim under test.
    "reporting::PartialEmbeddedMapping",
    "reporting::OtherwiseMapping",
    "reporting::InlineFlatMapping",
    # AggregationAware has its own grammar -- Views / ~modelOperation / ~mainMapping with
    # NESTED Relational blocks. The line-based reader here would bind those inner blocks
    # to the wrong class. Its expectations are mirrored from equivalent queries on
    # trading::Trade, which is exactly the equivalence the mapping claims.
    "reporting::AggregationAwareMapping",
    # XStore relates classes by a PREDICATE, not a join, and declares local (+) properties
    # the reader does not model. It also re-maps trading::Trade onto the same table, which
    # would fight the canonical binding.
    "external::EntityMapping",
    # Relation class mappings bind to relation COLUMN names, not [db]Table.COLUMN, and
    # ModelJoin is a predicate rather than a join. Neither shape is modelled here.
    "modeljoin::JoinMapping",
}

# re.M is load-bearing: without it `search` over a multi-line chunk never matches, the
# skip-list silently does nothing, and trading::Trade quietly gets rebound to TRADE_FLAT.
_MAPPING_NAME = re.compile(r"^\s*Mapping\s+([\w:]+)\s*$", re.M)


def _materialise_view_schemas(c: Corpus) -> None:
    """Views are referenced by mappings exactly like tables, so they need a schema. Types
    are inherited from the base column; a count is an Integer whatever it counts."""
    for v in c.views.values():
        base = c.tables.get(v.base)
        if base is None:
            raise ValueError(f"View {v.name} is built on unknown table {v.base!r}")
        t = Table(v.name)
        for name, col in v.columns.items():
            src = base.columns.get(col.source)
            if src is None:
                raise ValueError(f"View {v.name}: {v.base}.{col.source} does not exist")
            typ = "INTEGER" if col.agg == "count" else src.type
            t.columns[name] = Column(name, typ, name in v.group_by
                                     or col.source in v.group_by)
        c.tables[v.name] = t


def load() -> Corpus:
    c = Corpus()
    # Linked projects FIRST: a corpus store includes a project store and a corpus mapping
    # includes a project mapping, so the project's tables and sets must already exist when
    # the corpus's own files are read.
    files = linked_files() + stress_sources()
    parsed = [(f, sections(f.read_text())) for f in files]
    # Three passes: stores define the tables mappings point at, and mappings bind
    # associations that must already exist.
    for _, secs in parsed:
        for kind, body in secs:
            if kind == "Relational":
                _parse_store(body, c)
    _materialise_view_schemas(c)
    for _, secs in parsed:
        for kind, body in secs:
            if kind == "Pure":
                _parse_domain(body, c)
    for _, secs in parsed:
        for kind, body in secs:
            if kind == "Mapping":
                for chunk in _split_mappings(body):
                    name = _MAPPING_NAME.search(chunk)
                    if name and name.group(1) in SKIP_MAPPINGS:
                        continue
                    _parse_mapping(chunk, c, name.group(1) if name else None)
    # A subclass has its supertype's ASSOCIATIONS. The reader registers an end against the
    # class the Association names, so a subtype could not navigate one at all -- resolving
    # `InterestRateSwap.legs` reported it as neither a mapped property nor an association,
    # even though `legs` is declared on OtcTrade and every subtype inherits it.
    #
    # Propagated here rather than handled in the resolver because six places consult
    # `c.ends` -- resolve, resolve_assoc, owner_of, to_many_on, the chain builder and the
    # generators -- and teaching each of them to walk the supertype chain is six chances to
    # walk it differently. Columns and derived properties are already inherited; this was
    # the third kind of inheritance and the last one missing.
    #
    # EMBEDDED hops inherit for the same reason and were the fourth kind to need it. A
    # subtype set written `Sub[sub] extends [base]` inherits the base's whole body,
    # including any embedded block -- so `QuotedPillar.curveRef` is as real as
    # `CurvePoint.curveRef`, and looking it up by the subtype's own name found nothing.
    for cls, klass in list(c.classes.items()):
        parent = klass.supertype
        while parent:
            for (owner, prop), end in list(c.ends.items()):
                if owner == parent and (cls, prop) not in c.ends:
                    c.ends[(cls, prop)] = end
            for (owner, prop), child in list(c.embedded.items()):
                if owner == parent and (cls, prop) not in c.embedded:
                    c.embedded[(cls, prop)] = child
            parent = c.classes[parent].supertype if parent in c.classes else None

    return c


# ---------------------------------------------------------------- self-check

def check(c: Corpus) -> list[str]:
    """Facts that must hold for the oracle to be trustworthy. Returned, not raised, so
    the caller can print all of them at once."""
    bad = []
    # The FIRST file parsed must declare its section. Every other headerless file inherits
    # from the one before it, which the guard below already covers; the first one inherits
    # from whatever the runner happens to concatenate ahead of it, which used to be nothing
    # and is now a linked project. A default that holds only while a file is first is not a
    # default, it is an accident waiting for something to be put in front of it.
    first = (linked_files() + stress_sources())[0]
    if not first.read_text().lstrip().startswith("###"):
        bad.append(f"{first.name} is parsed first and declares no ###Section; it would "
                   f"inherit from whatever is concatenated ahead of it")
    # A file that opens WITHOUT a `###Section` header inherits the section the PREVIOUS file
    # left active, because the runner parses the corpus as one concatenated unit. Twenty of
    # these files legitimately open with a bare `Class` -- they are pure domain models and
    # they follow other pure domain models, so the inherited section is `###Pure` and the
    # default is what they wanted anyway.
    #
    # It stops being harmless the moment such a file lands after one ending in `###Mapping`.
    # Then a `Class` is handed to the mapping parser, and the whole corpus fails with
    # "Unexpected token" naming no file, no line and no construct. Each file still parses
    # perfectly ALONE, so bisecting file-by-file reports every one of them healthy -- the
    # defect exists only in the ordering, which is exactly what this checks.
    # A table declared TWICE. The reader keys tables globally by name, so the second
    # declaration silently replaces the first and every property mapped to a column of the
    # replaced one resolves against the wrong table -- or, if the column names differ,
    # reports a column that plainly is in the file as missing. That is how a fresh
    # market-risk store collided with the risk domain's own RISK_FACTOR and STRESS_SCENARIO,
    # and the error pointed at the new file rather than at the duplication.
    # Counted per OCCURRENCE, not per file. The first version compared file names, so two
    # declarations in the SAME file -- which is what a 700-line store DDL invites -- slipped
    # through and cost the same debugging twice.
    # Keyed by (DATABASE, table), not by table alone. Two databases declaring the same table
    # is how store substitution is written -- hier::IssuerDB and hier::IssuerProdDB both
    # declare HIER_ISSUER on purpose -- so flagging that would be flagging a feature. Two
    # declarations inside ONE database is the mistake, and it is invisible: the reader keeps
    # one, and every property mapped to a column of the other reports as missing from a file
    # where it is plainly present.
    seen_tables: dict[tuple, list[str]] = {}
    for f in stress_sources():
        db = None
        for line in f.read_text().splitlines():
            m = re.match(r"^\s*Database\s+([\w:]+)", line)
            if m:
                db = m.group(1)
                continue
            m = re.match(r"^\s*Table\s+(\w+)\s*[\(]?\s*$|^\s*Table\s+(\w+)\s*\(", line)
            if m and db:
                seen_tables.setdefault((db, m.group(1) or m.group(2)), []).append(f.name)
    # A seeded value too long for its declared column. Without this the failure arrives as
    # an H2 insert error inside whichever service happens to load that table first -- for a
    # 22-character product type in a VARCHAR(20), that was a graph-fetch service on an
    # unrelated class, sixty-two errors deep and pointing nowhere near the seed.
    try:
        import flat
        for tname, rows in flat.all_tables(c).items():
            table = c.tables.get(tname)
            if table is None or not rows:
                continue
            for col, spec in table.columns.items():
                m = re.search(r"(?:VAR)?CHAR\s*\(\s*(\d+)", str(spec.type or ""), re.I)
                if not m:
                    continue
                width = int(m.group(1))
                worst = max((len(str(r[col])) for r in rows
                             if r.get(col) is not None), default=0)
                if worst > width:
                    bad.append(f"{tname}.{col} is VARCHAR({width}) and the seed holds a "
                               f"{worst}-character value")
    except Exception:
        pass

    for (db, name), where in sorted(seen_tables.items()):
        if len(where) > 1:
            bad.append(f"table {name} is declared {len(where)} times in {db} "
                       f"({', '.join(sorted(set(where)))}); the reader keeps one and "
                       f"silently drops the other")

    prev = "###Pure"
    for f in stress_sources():
        txt = f.read_text()
        first = next((ln for ln in txt.splitlines()
                      if ln.strip() and not ln.lstrip().startswith("//")), "")
        secs = re.findall(r"^###\w+", txt, re.M)
        if not first.startswith("###") and prev != "###Pure":
            bad.append(f"{f.name} opens with {first.strip()[:32]!r} and no ###Section header, "
                       f"but the previous file ends in {prev} -- it will be parsed as {prev}")
        prev = secs[-1] if secs else prev
    if not c.tables:
        bad.append("no tables parsed")
    if len(c.joins) < 100:
        bad.append(f"only {len(c.joins)} joins parsed")
    for cls, tbl in c.main_table.items():
        if tbl not in c.tables:
            bad.append(f"{cls} ~mainTable {tbl} is not a declared table")
    for cls, cols in c.columns.items():
        t = c.tables.get(c.main_table.get(cls, ""))
        if t is None:
            continue
        for prop, col in cols.items():
            if col not in t.columns:
                bad.append(f"{cls}.{prop} -> {t.name}.{col} does not exist")
    for (owner, name), end in c.ends.items():
        if end.join and end.join not in c.joins:
            bad.append(f"{owner}.{name} uses undeclared join {end.join}")
    for cls, name in c.class_filter.items():
        if name not in c.filters:
            bad.append(f"{cls} uses undeclared store filter {name}")
    for cls, members in c.unions.items():
        if len(members) < 2:
            bad.append(f"union mapping for {cls} resolved {len(members)} member tables; "
                       f"the set-implementation ids probably did not match")
        for m in members:
            if m not in c.tables:
                bad.append(f"union member {m} of {cls} is not a declared table")
    for (cls, prop), mapping in c.enum_props.items():
        if mapping not in c.enum_maps:
            bad.append(f"{cls}.{prop} uses undeclared EnumerationMapping {mapping}")
        elif not c.enum_maps[mapping]:
            bad.append(f"EnumerationMapping {mapping} has no source codes")
    return bad


if __name__ == "__main__":
    c = load()
    print(f"tables      {len(c.tables)}")
    print(f"joins       {len(c.joins)}")
    print(f"classes     {len(c.classes)}")
    print(f"enums       {len(c.enums)}")
    print(f"mapped cls  {len(c.main_table)}")
    print(f"assoc ends  {len(c.ends)}  ({sum(1 for e in c.ends.values() if e.join)} with a join)")
    problems = check(c)
    print(f"\nself-check: {'OK' if not problems else str(len(problems)) + ' PROBLEMS'}")
    for p in problems[:25]:
        print("  -", p)
