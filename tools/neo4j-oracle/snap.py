"""The #Cypher{...}# island's oracle: Neo4j Community's answers for the cypher-parity cases, snapped (README.md).

  snap.sh --write [--only GLOB]   run the cases, write core/src/test/resources/cypher-parity/neo4j-answers.jsonl
  snap.sh --check [--only GLOB]   run the cases, report DRIFT, UNSNAPPED, STALE and GONE (and FUNCTIONS when the
                                  server's function names are not cypher-functions.txt's); exit 1 on any
  snap.sh --functions             write core/src/test/resources/cypher-parity/cypher-functions.txt: the server's
                                  function names (SHOW FUNCTIONS), lower-cased, which CypherFunctions must list

The cases are the openCypher TCK's scenarios (core/src/test/resources/cypher-parity/tck, Apache 2.0, read by the
Gherkin subset below, which the Java reader CypherTck mirrors step for step: the two must produce the same keys and
the same sha inputs, or every answer is STALE) and lite's own (cypher-parity/lite/*.yaml). Each case runs on a
local Neo4j Community over Bolt (the Apache 2.0 Python driver; the standard library otherwise, run isolated with
python3 -I): the graph emptied, the case's setup run (under CYPHER 5, SETUP_CYPHER: the data the case is judged
over, not what is judged), the graph dumped (that dump is what the lanes seed their tables from: CI never interprets
a CREATE), then the query, under the Cypher version the case names or lite's default (25), its rows written in the
TCK's value grammar, or its error by Neo4j's code and message.

Neo4j Community is GPLv3: it runs here only, never on a classpath, never in CI; CI judges from the committed file.
"""

import argparse
import fnmatch
import hashlib
import json
import math
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
PARITY = os.path.join(ROOT, "core", "src", "test", "resources", "cypher-parity")
DEFAULT_CYPHER = "25"
# The version a setup statement runs under: the TCK's setups are openCypher 9's text, which Cypher 5 reads as written
# and Cypher 25 narrows (a variable created in a CREATE read in a property map of the same CREATE, measured: "Creating
# an entity (a) and referencing that entity in a property definition in the same CREATE is not allowed" for
# WithSkipLimit1/[1], WithSkipLimit2/[2] and With2/[1]). A setup only builds the graph; the query is what is judged,
# under its own version. A setup statement that names its version keeps it.
SETUP_CYPHER = "5"
FORMAT = 1
TIMEOUT_S = 60.0

# ------------------------------------------------------------------ the TCK's value grammar


def fmt(v):
    """A driver value in the TCK's value grammar (tck/README.adoc, "Values"): what the expected tables are written in,
    so the Java judge parses one grammar on both sides. Labels and map keys are sorted (the grammar compares them as
    sets and maps)."""
    if v is None:
        return "null"
    if v is True:
        return "true"
    if v is False:
        return "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if math.isnan(v):
            return "NaN"
        if math.isinf(v):
            return "Inf" if v > 0 else "-Inf"
        r = repr(v)
        return r if ("." in r or "e" in r or "n" in r) else r + ".0"
    if isinstance(v, str):
        return "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"
    if isinstance(v, list):
        return "[" + ", ".join(fmt(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ", ".join(k + ": " + fmt(v[k]) for k in sorted(v)) + "}"
    from neo4j.graph import Node, Path, Relationship   # the driver's types: a relationship's class is named by its type
    if isinstance(v, Node):
        return "(" + "".join(":" + l for l in sorted(v.labels)) + props(v) + ")"
    if isinstance(v, Relationship):
        return "[:" + v.type + props(v) + "]"
    if isinstance(v, Path):
        nodes = list(v.nodes)
        out = fmt(nodes[0])
        for i, r in enumerate(v.relationships):
            forward = r.start_node is not None and r.start_node.element_id == nodes[i].element_id
            out += ("-" + fmt(r) + "->" if forward else "<-" + fmt(r) + "-") + fmt(nodes[i + 1])
        return "<" + out + ">"
    # temporal, spatial, bytes: an opaque tagged text the judge compares as text (lite refuses these values today)
    return type(v).__name__ + "(" + fmt(str(v)) + ")"


def stored(v):
    """A stored value as the dump writes it: fmt's, but a date-time of a named zone carries the zone's name in brackets
    after its offset (Java's spelling, `DateTime('1984-11-11T12:31:14.645876123+01:00[Europe/Stockholm]')`): the
    driver's text is the offset alone, and the lanes store the zone the value names
    (docs/CYPHER_STUDY_2026_10_15.md §12.75). UTC, by any name, is the offset Z."""
    from neo4j.time import DateTime
    if isinstance(v, DateTime) and v.tzinfo is not None:
        zone = getattr(v.tzinfo, "zone", None) or getattr(v.tzinfo, "key", None)
        if zone is not None and zone != "UTC":
            return "DateTime(" + fmt(str(v) + "[" + zone + "]") + ")"
    if isinstance(v, list):
        return "[" + ", ".join(stored(x) for x in v) + "]"
    return fmt(v)


def props(entity):
    p = dict(entity)
    return (" {" + ", ".join(k + ": " + fmt(p[k]) for k in sorted(p)) + "}") if p else ""


class Values:
    """A reader of the TCK's value grammar, for parameter values ('And parameters are:')."""

    def __init__(self, text):
        self.s = text
        self.i = 0

    def ws(self):
        while self.i < len(self.s) and self.s[self.i] in " \t\n":
            self.i += 1

    def value(self):
        self.ws()
        c = self.s[self.i]
        if c == "'":
            return self.string()
        if c == "[":
            self.i += 1
            out = []
            self.ws()
            if self.s[self.i] == "]":
                self.i += 1
                return out
            while True:
                out.append(self.value())
                self.ws()
                if self.s[self.i] == ",":
                    self.i += 1
                    continue
                assert self.s[self.i] == "]", self.s[self.i:]
                self.i += 1
                return out
        if c == "{":
            self.i += 1
            out = {}
            self.ws()
            if self.s[self.i] == "}":
                self.i += 1
                return out
            while True:
                self.ws()
                m = re.match(r"[A-Za-z_][A-Za-z0-9_]*", self.s[self.i:])
                key = m.group(0)
                self.i += len(key)
                self.ws()
                assert self.s[self.i] == ":"
                self.i += 1
                out[key] = self.value()
                self.ws()
                if self.s[self.i] == ",":
                    self.i += 1
                    continue
                assert self.s[self.i] == "}"
                self.i += 1
                return out
        m = re.match(r"-?[0-9]+\.[0-9]+(e[+-]?[0-9]+)?|-?[0-9]+e[+-]?[0-9]+", self.s[self.i:])
        if m:
            self.i += len(m.group(0))
            return float(m.group(0))
        m = re.match(r"-?[0-9]+", self.s[self.i:])
        if m:
            self.i += len(m.group(0))
            return int(m.group(0))
        for word, val in (("true", True), ("false", False), ("null", None), ("NaN", float("nan")),
                          ("Inf", float("inf")), ("-Inf", float("-inf"))):
            if self.s.startswith(word, self.i):
                self.i += len(word)
                return val
        raise ValueError("value grammar: unexpected " + self.s[self.i:self.i + 20])

    def string(self):
        assert self.s[self.i] == "'"
        self.i += 1
        out = []
        while self.s[self.i] != "'":
            c = self.s[self.i]
            if c == "\\":
                self.i += 1
                c = self.s[self.i]
                out.append({"n": "\n", "t": "\t", "r": "\r"}.get(c, c))
            else:
                out.append(c)
            self.i += 1
        self.i += 1
        return "".join(out)


def parse_value(text):
    v = Values(text)
    out = v.value()
    v.ws()
    if v.i != len(v.s):
        raise ValueError("value grammar: trailing " + v.s[v.i:])
    return out


# ------------------------------------------------------------------ the Gherkin subset (mirrored by CypherTck.java)


def tck_cases(tck_dir):
    """Every TCK scenario as a case: key, query, setup statements, parameters, the Cypher version (none: the default),
    and what the scenario expects (rows, an error, or empty: the Java side judges from Neo4j's answer; this reader
    carries the expectation only for the census)."""
    graphs = {}
    gdir = os.path.join(tck_dir, "graphs")
    for name in sorted(os.listdir(gdir)):
        f = os.path.join(gdir, name, name + ".cypher")
        if os.path.isfile(f):
            graphs[name] = open(f, encoding="utf-8").read()
    cases = []
    fdir = os.path.join(tck_dir, "features")
    for dirpath, _, files in os.walk(fdir):
        for fn in sorted(files):
            if fn.endswith(".feature"):
                path = os.path.join(dirpath, fn)
                rel = os.path.relpath(path, fdir)[:-len(".feature")].replace(os.sep, "/")
                cases.extend(feature(open(path, encoding="utf-8").read().split("\n"), "tck/" + rel, graphs))
    return cases


def feature(lines, prefix, graphs):
    """The scenarios of one feature file. A Scenario Outline is one case per Examples row, its <name> placeholders
    substituted in every step; the key is the scenario's name, '#<row>' appended for an outline."""
    cases = []
    i = 0
    n = len(lines)

    def strip(s):
        return s.strip()

    while i < n:
        line = strip(lines[i])
        m = re.match(r"(Scenario Outline|Scenario):\s*(.*)", line)
        if not m:
            i += 1
            continue
        outline = m.group(1) == "Scenario Outline"
        name = m.group(2).strip()
        i += 1
        steps = []
        examples = []
        while i < n:
            line = strip(lines[i])
            if re.match(r"(Scenario Outline|Scenario):", line) or line.startswith("Feature:"):
                break
            if line.startswith("Examples:"):
                i += 1
                table = []
                while i < n and strip(lines[i]).startswith("|"):
                    table.append(cells(strip(lines[i])))
                    i += 1
                examples.append(table)
                continue
            m2 = re.match(r"(Given|And|When|Then|But)\s+(.*)", line)
            if m2:
                text = m2.group(2).strip()
                i += 1
                doc = None
                table = None
                if i < n and strip(lines[i]) == '"""':
                    indent = len(lines[i]) - len(lines[i].lstrip())
                    i += 1
                    body = []
                    while i < n and strip(lines[i]) != '"""':
                        body.append(lines[i][indent:] if lines[i][:indent].strip() == "" else lines[i].lstrip())
                        i += 1
                    i += 1
                    doc = "\n".join(body).rstrip("\n")
                elif i < n and strip(lines[i]).startswith("|"):
                    table = []
                    while i < n and strip(lines[i]).startswith("|"):
                        table.append(cells(strip(lines[i])))
                        i += 1
                steps.append((text, doc, table))
                continue
            i += 1
        if outline:
            for table in examples:
                header = table[0]
                for r, row in enumerate(table[1:], start=1):
                    subst = dict(zip(header, row))
                    cases.append(case(prefix, name + " #" + str(r), substitute(steps, subst), graphs))
        else:
            cases.append(case(prefix, name, steps, graphs))
    return cases


def cells(line):
    parts = line.strip().strip("|").split("|")
    return [p.strip() for p in parts]


def substitute(steps, subst):
    def sub(s):
        if s is None:
            return None
        for k, v in subst.items():
            s = s.replace("<" + k + ">", v)
        return s
    out = []
    for text, doc, table in steps:
        out.append((sub(text), sub(doc), None if table is None else [[sub(c) for c in row] for row in table]))
    return out


def case(prefix, name, steps, graphs):
    setup = []
    params = {}
    params_text = []
    procedures = []
    query = None
    expect = None
    skip = None
    version = None
    for text, doc, table in steps:
        if text in ("an empty graph", "any graph"):
            pass
        elif re.match(r"the (\S+) graph$", text):
            g = re.match(r"the (\S+) graph$", text).group(1)
            setup.append(graphs[g].rstrip().rstrip(";"))
        elif text == "having executed:":
            setup.append(doc.rstrip().rstrip(";"))
        elif text == "parameters are:":
            for row in table:
                params[row[0]] = parse_value(row[1])
                params_text.append(row[0] + "=" + row[1])
        elif text == "executing query:":
            if query is not None:
                skip = skip or "two queries"
            query = doc
        elif text == "executing control query:":
            skip = skip or "a control query"
        elif text.startswith("the result should be"):
            expect = {"kind": "rows", "ordered": "in order" in text, "ignoreListOrder": "ignoring element order" in text,
                      "columns": table[0] if table else [], "rows": table[1:] if table else []}
        elif text == "the result should be empty":
            expect = {"kind": "empty"}
        elif re.match(r"an? (\w+) should be raised at (compile time|runtime|any time): (\S+)", text):
            m = re.match(r"an? (\w+) should be raised at (compile time|runtime|any time): (\S+)", text)
            expect = {"kind": "error", "type": m.group(1), "phase": m.group(2), "detail": m.group(3)}
        elif text.startswith("there exists a procedure"):
            # a procedure the scenario declares (CypherTck.Procedure): its graph is Neo4j's, its answer the TCK's
            # own table (the harness judges a procedure case by its expectation; Neo4j runs no procedure of ours)
            procedures.append(procedure_text(text, table))
        elif text.startswith("having defined"):
            skip = skip or "a defined function"
        elif text in ("no side effects",) or text.startswith("the side effects should be"):
            pass
        else:
            skip = skip or ("an unread step: " + text)
    if query is None:
        skip = skip or "no query"
    return {"key": prefix + "/" + name, "query": query or "", "setup": setup, "params": params,
            "paramsText": "\n".join(params_text), "version": version, "expect": expect, "skip": skip,
            "proceduresText": "\n".join(procedures)}


def lite_procedures_text(block):
    """A lite case's procedures block (declaration lines, each followed by its table's lines) as the text."""
    if not block:
        return ""
    out = []
    declaration = None
    table = []
    for line in block.split("\n"):
        l = line.strip()
        if not l:
            continue
        if l.startswith("|"):
            table.append(cells(l))
        else:
            if declaration is not None:
                out.append(procedure_text(declaration, table))
            declaration = l
            table = []
    if declaration is not None:
        out.append(procedure_text(declaration, table))
    return "\n".join(out)


def procedure_text(text, table):
    """A procedure declaration as CypherTck.Procedure.text spells it: name(in :: T?, ...) :: (out :: T?, ...), then
    its rows (a table of one empty cell is none), each row's cells joined by |."""
    m = re.match(r"(?:there exists a procedure )?(\S+?)\((.*?)\)\s*::\s*\((.*?)\)\s*:?\s*$", text.strip())
    if not m:
        raise SystemExit("a procedure declaration the snapper does not read: " + text)
    def params(t):
        return ", ".join(p.strip().split("::")[0].strip() + " :: " + p.strip().split("::")[1].strip().replace("?", "") + "?"
                         for p in t.split(",") if p.strip())
    out = m.group(1) + "(" + params(m.group(2)) + ") :: (" + params(m.group(3)) + ")"
    for row in (table or [])[1:]:   # the first row is the header (the inputs' then the outputs' names)
        if not (len(row) == 1 and row[0] == ""):
            out += "\n" + "|".join(row)
    return out


# ------------------------------------------------------------------ lite's own cases


def lite_cases(lite_dir):
    """cypher-parity/lite/*.yaml: a YAML subset -- `tests:` then items `- id:`, `cypher:` (a quoted line or a `|`
    block), `graph:` (a `|` block of CREATE statements, or `graph: <name>` naming a graphs/ file), `version:`,
    `params:` (a flow map in the value grammar), `skip:`, `decision:`, `expect:`."""
    cases = []
    if not os.path.isdir(lite_dir):
        return cases
    graphs = {}
    gdir = os.path.join(os.path.dirname(lite_dir), "graphs")
    if os.path.isdir(gdir):
        for name in sorted(os.listdir(gdir)):
            if name.endswith(".cypher"):
                graphs[name[:-len(".cypher")]] = open(os.path.join(gdir, name), encoding="utf-8").read()
    for fn in sorted(os.listdir(lite_dir)):
        if not fn.endswith(".yaml"):
            continue
        file = fn[:-len(".yaml")]
        lines = open(os.path.join(lite_dir, fn), encoding="utf-8").read().split("\n")
        items = []
        i = 0
        while i < len(lines):
            line = lines[i]
            m = re.match(r"\s*-\s+id:\s*(\S+)\s*$", line)
            if m:
                item = {"id": m.group(1)}
                indent = len(line) - len(line.lstrip()) + 2
                i += 1
                while i < len(lines):
                    l2 = lines[i]
                    if l2.strip() == "" or l2.strip().startswith("#"):
                        i += 1
                        continue
                    if len(l2) - len(l2.lstrip()) < indent:
                        break
                    m2 = re.match(r"\s*([a-z]+):\s*(.*)$", l2)
                    key, rest = m2.group(1), m2.group(2).strip()
                    i += 1
                    if rest == "|":
                        body = []
                        while i < len(lines) and (lines[i].strip() == "" or len(lines[i]) - len(lines[i].lstrip()) > indent):
                            body.append(lines[i][indent + 2:] if len(lines[i]) >= indent + 2 else "")
                            i += 1
                        while body and body[-1].strip() == "":
                            body.pop()
                        item[key] = "\n".join(body)
                    else:
                        if (rest.startswith('"') and rest.endswith('"')) or (rest.startswith("'") and rest.endswith("'")):
                            rest = rest[1:-1]
                        item[key] = rest
                items.append(item)
                continue
            i += 1
        for it in items:
            graph = it.get("graph", "")
            setup = []
            if graph in graphs:
                setup.append(graphs[graph].rstrip().rstrip(";"))
            elif graph.strip():
                setup.append(graph.rstrip().rstrip(";"))
            params = parse_value(it["params"]) if it.get("params") else {}
            cases.append({"key": "lite/" + file + "/" + it["id"], "query": it.get("cypher", ""), "setup": setup,
                          "params": params, "paramsText": it.get("params") or "", "version": it.get("version"),
                          "expect": None, "skip": it.get("skip"), "decision": it.get("decision"),
                          "expectText": it.get("expect"),
                          "proceduresText": lite_procedures_text(it.get("procedures"))})
    return cases


# ------------------------------------------------------------------ the sha and the version


def cypher_version(c):
    m = re.match(r"\s*CYPHER\s+(5|25)\b", c["query"])
    if m:
        return m.group(1)
    return c.get("version") or DEFAULT_CYPHER


def sha(c):
    """What the answer is for: the query, its Cypher version, the setup, the parameters, and a lite case's decision
    and expect; a change to any makes the answer STALE (CypherAnswers.sha computes the same)."""
    parts = [c["query"], cypher_version(c), "\n;\n".join(c["setup"]), c["paramsText"], c.get("decision") or "",
             c.get("expectText") or ""]
    if c.get("proceduresText"):
        parts.append(c["proceduresText"])   # (a case declaring procedures, the study's §12.60)
    return hashlib.sha256("\n\x00\n".join(parts).encode("utf-8")).hexdigest()


# ------------------------------------------------------------------ running on Neo4j


def connect(url):
    from neo4j import GraphDatabase
    return GraphDatabase.driver(url)


FUNCTIONS_HEADER = ("# Neo4j's built-in functions on the pinned release (SHOW FUNCTIONS), lower-cased, one per line:"
                    " written by\n# tools/neo4j-oracle/snap.sh --functions, checked by every --check; CypherParityTest"
                    " holds\n# com.legend.protocol.cypher.CypherFunctions to this file.\n")


def function_names(driver):
    """The server's function names, lower-cased (Cypher's function names are case-insensitive), sorted."""
    with driver.session() as s:
        return sorted({r["name"].lower() for r in s.run("SHOW FUNCTIONS YIELD name RETURN name").data()})


def read_function_names(path):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip() and not l.startswith("#")]


def server_version(driver):
    with driver.session() as s:
        rows = s.run("CALL dbms.components() YIELD name, versions, edition RETURN name, versions[0] AS v, edition").data()
    for r in rows:
        if r["name"] == "Neo4j Kernel":
            return r["v"], r["edition"]
    raise RuntimeError("no Neo4j Kernel in dbms.components()")


def run_case(driver, c):
    """The case on an emptied graph: its setup, the graph's dump, then its query's rows or error."""
    out = {"id": c["key"], "cypher": cypher_version(c), "sha": sha(c)}
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n").consume()
        for stmt in c["setup"]:
            if not re.match(r"\s*CYPHER\b", stmt):
                stmt = "CYPHER " + SETUP_CYPHER + " " + stmt
            try:
                s.run(stmt).consume()
            except Exception as e:  # a setup Neo4j refuses: the case is unrunnable, recorded as such
                out["setupError"] = error_of(e)
                return out
        out["graph"] = dump(s)
        if c.get("proceduresText"):
            return out   # the query calls a procedure Neo4j has not: the graph alone is the oracle's
        query = c["query"]
        if not re.match(r"\s*CYPHER\b", query):
            query = "CYPHER " + cypher_version(c) + " " + query
        try:
            result = s.run(query, c["params"], timeout=TIMEOUT_S)
            columns = list(result.keys())
            rows = [[fmt(v) for v in rec.values()] for rec in result]
            out["columns"] = columns
            out["rows"] = rows
        except Exception as e:
            out["error"] = error_of(e)
    return out


def error_of(e):
    """Neo4j's error as its code and its message's first line (the driver's message carries the query and a caret
    below it; the GQL status too; the first line is the one a person reads)."""
    code = getattr(e, "code", None) or type(e).__name__
    message = getattr(e, "message", None) or str(e)
    m = re.search(r"\{message: (.*?)\}(?: \{gql_status|$)", message, re.S)
    if m:
        message = m.group(1)
    message = message.split("\n")[0][:500]
    # a node, relationship or path spelled by its internal id ("got: (1238)", "-[798]-", "Path{(1242)-[799]-(1243)}"):
    # the id is the run's, masked so that two snaps of one case agree
    message = re.sub(r"\((\d+)\)", "(#)", message)
    message = re.sub(r"-\[(\d+)\]-", "-[#]-", message)
    message = re.sub(r"\b(node|relationship)<\d+>", r"\1<#>", message)
    message = re.sub(r"\bwith id \d+\b", "with id #", message)
    return {"code": code, "message": message}


def dump(s):
    """The graph as the lanes seed it: each node's ordinal, labels and properties, each relationship's ordinal, ends,
    type and properties, every value in the value grammar."""
    nodes = []
    ids = {}
    for rec in s.run("MATCH (n) RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props ORDER BY id"):
        ids[rec["id"]] = len(nodes)
        nodes.append({"id": len(nodes), "labels": sorted(rec["labels"]),
                      "props": {k: stored(v) for k, v in sorted(rec["props"].items())}})
    rels = []
    for rec in s.run("MATCH (a)-[r]->(b) RETURN elementId(r) AS id, elementId(a) AS a, elementId(b) AS b, type(r) AS t,"
                     " properties(r) AS props ORDER BY id"):
        rels.append({"id": len(rels), "start": ids[rec["a"]], "end": ids[rec["b"]], "type": rec["t"],
                     "props": {k: stored(v) for k, v in sorted(rec["props"].items())}})
    return {"nodes": nodes, "rels": rels}


# ------------------------------------------------------------------ the answers file


def read_answers(path):
    meta = None
    answers = {}
    if not os.path.exists(path):
        return meta, answers
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            o = json.loads(line)
            if "meta" in o:
                meta = o["meta"]
            else:
                answers[o["id"]] = o
    return meta, answers


def write_answers(path, meta, answers):
    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps({"meta": meta}, ensure_ascii=False, separators=(",", ":")) + "\n")
        for key in sorted(answers):
            f.write(json.dumps(answers[key], ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n")


def same_answer(a, b, ignore_list_order=False):
    """Whether two snaps of one case agree: rows as a bag (Neo4j's order is its own where the query orders nothing;
    the Java judge compares in order only where the scenario says so), a list cell as a bag where the scenario
    ignores element order (a collect's order is Neo4j's own: Return6 [13] answers ['b', 'c'] or ['c', 'b'] by the
    run), the graph by its shape (a node's ordinal is the run's), the error and the version as they are."""
    for k in ("columns", "error", "setupError", "cypher"):
        if a.get(k) != b.get(k):
            return False
    rows_a = a.get("rows") or []
    rows_b = b.get("rows") or []
    if ignore_list_order:
        rows_a = [[list_bag(cell) for cell in row] for row in rows_a]
        rows_b = [[list_bag(cell) for cell in row] for row in rows_b]
    if sorted(map(json.dumps, rows_a)) != sorted(map(json.dumps, rows_b)):
        return False
    return graph_shape(a.get("graph")) == graph_shape(b.get("graph"))


def list_bag(cell):
    """A cell's text with every list's elements sorted, at every depth: the value grammar's `[a, b]`, split at the
    commas outside quotes and brackets."""
    if not (cell.startswith("[") and cell.endswith("]")):
        return cell
    parts = []
    depth = 0
    quote = None
    start = 1
    for i in range(1, len(cell) - 1):
        ch = cell[i]
        if quote:
            if ch == "\\":
                continue
            if ch == quote:
                quote = None
        elif ch in "'\"":
            quote = ch
        elif ch in "[{(":
            depth += 1
        elif ch in "]})":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(cell[start:i].strip())
            start = i + 1
    last = cell[start:len(cell) - 1].strip()
    if last or parts:
        parts.append(last)
    return "[" + ", ".join(sorted(list_bag(p) for p in parts)) + "]"


def graph_shape(g):
    """A dumped graph without its ordinals: the bag of nodes (labels, properties) and the bag of relationships
    (type, properties, the two nodes' labels and properties)."""
    if g is None:
        return None
    nodes = {n["id"]: (tuple(n["labels"]), tuple(sorted(n["props"].items()))) for n in g["nodes"]}
    rels = sorted((r["type"], tuple(sorted(r["props"].items())), nodes[r["start"]], nodes[r["end"]]) for r in g["rels"])
    return sorted(nodes.values()), rels


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--functions", action="store_true", help="write the server's function names and exit")
    ap.add_argument("--only", default=None, help="a glob over case keys")
    ap.add_argument("--url", default="bolt://127.0.0.1:7687")
    ap.add_argument("--parity", default=PARITY)
    ap.add_argument("--census", action="store_true", help="print the cases' census and exit (no server)")
    args = ap.parse_args()
    cases = tck_cases(os.path.join(args.parity, "tck")) + lite_cases(os.path.join(args.parity, "lite"))
    if args.only:
        cases = [c for c in cases if fnmatch.fnmatchcase(c["key"], args.only)]
    if args.census:
        kinds = {}
        for c in cases:
            k = "skip:" + c["skip"].split(":")[0] if c["skip"] else (c["expect"] or {}).get("kind", "lite")
            kinds[k] = kinds.get(k, 0) + 1
        print(len(cases), "cases", kinds)
        return 0
    functions_file = os.path.join(args.parity, "cypher-functions.txt")
    if args.functions:
        driver = connect(args.url)
        names = function_names(driver)
        driver.close()
        with open(functions_file, "w", encoding="utf-8") as f:
            f.write(FUNCTIONS_HEADER + "".join(n + "\n" for n in names))
        print("wrote", functions_file, "-", len(names), "names")
        return 0
    if not (args.write or args.check):
        ap.error("--write, --check or --functions")
    answers_file = os.path.join(args.parity, "neo4j-answers.jsonl")
    old_meta, old = read_answers(answers_file)
    driver = connect(args.url)
    version, edition = server_version(driver)
    print("Neo4j", version, edition, "at", args.url, "-", len(cases), "cases")
    runnable = [c for c in cases if not c["skip"]]
    live = {}
    for n, c in enumerate(runnable, start=1):
        live[c["key"]] = run_case(driver, c)
        if n % 100 == 0:
            print("  ", n, "of", len(runnable), file=sys.stderr)
    names = function_names(driver)
    driver.close()
    meta = {"tool": "tools/neo4j-oracle/snap.py", "format": FORMAT, "server": version, "edition": edition,
            "defaultCypher": DEFAULT_CYPHER}
    if args.write:
        merged = dict(old) if args.only else {}
        merged.update(live)
        write_answers(answers_file, meta, merged)
        errors = sum(1 for a in live.values() if "error" in a)
        setup_errors = sum(1 for a in live.values() if "setupError" in a)
        print("wrote", answers_file, "-", len(live), "answers:", len(live) - errors - setup_errors, "with rows,",
              errors, "errors,", setup_errors, "setup errors")
        return 0
    problems = []
    if old_meta and old_meta.get("server") != version:
        problems.append("VERSION: the answers were snapped on " + str(old_meta.get("server")) + ", this server is "
                        + version)
    for c in runnable:
        a = old.get(c["key"])
        if a is None:
            problems.append("UNSNAPPED: " + c["key"])
        elif a["sha"] != sha(c):
            problems.append("STALE: " + c["key"])
        elif not same_answer(a, live[c["key"]], bool((c.get("expect") or {}).get("ignoreListOrder"))):
            problems.append("DRIFT: " + c["key"])
    if read_function_names(functions_file) != names:
        problems.append("FUNCTIONS: the server's function names are not " + functions_file + "'s (snap.sh --functions)")
    keys = {c["key"] for c in runnable}
    for k in old:
        if (not args.only or fnmatch.fnmatchcase(k, args.only)) and k not in keys:
            problems.append("GONE: " + k)
    for p in problems:
        print(p)
    print(len(problems), "problems over", len(runnable), "cases")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
