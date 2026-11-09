#!/usr/bin/env python3
"""Extract plain text and tables from the raw EDGAR corpus. Stdlib only (plus fetch.py, whose manifest it shares).

    python3 tools/edgar-corpus/extract.py

Reads manifest.yaml's raw rows; writes, beside each raw file, and adds one manifest row per output (with its sha256 and
the raw file it came from):

    10-K   item_1a.txt  item_7.txt  item_7a.txt
    10-Q   item_1a.txt  item_2.txt  item_3.txt   Part II Item 1A; Part I Items 2 and 3: a 10-Q's numbers for the MD&A and
                                                 market-risk items a 10-K calls 7 and 7A (the row's ten_k_item says so)
    8-K    item_2.02.txt  item_8.01.txt          whichever of the two the filing has; a filing with neither has no output
    13F-HR holdings.csv  cover.txt

Sections are found by their headings, never by position; a section that should exist and cannot be found is an error.
Output is a pure function of the raw bytes: re-running reproduces every file and the manifest exactly.
"""
from __future__ import annotations

import csv
import io
import re
import sys
from collections import Counter
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from xml.etree import ElementTree

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fetch import CORPUS_ROOT, load_manifest, sha256_of, write_atomically, write_manifest  # noqa: E402

# A limit on the local disk a rebuild takes, and on nothing else: the filings are never committed, so the corpus's size
# does not reach the repository (the owner's ruling, 2026-11-08, replacing a 15 MB budget set when they were to be).
DISK_LIMIT_BYTES = 60_000_000

BLOCK_TAGS = {
    "address", "article", "blockquote", "center", "dd", "div", "dl", "dt", "footer", "h1", "h2", "h3", "h4", "h5", "h6",
    "header", "li", "ol", "p", "pre", "section", "table", "ul",
}
SKIPPED_TAGS = {"head", "ix:header", "script", "style", "title"}
PAGE_BREAK = "\f"
HIDDEN_STYLE = re.compile(r"display\s*:\s*none", re.I)
SPACES = re.compile(r"\s+")
INVISIBLE = re.compile(r"[\u200b\u200c\u200d\ufeff\u00ad]")
PAGE_ONLY = re.compile(r"\d{1,3}")
RUNNING_PART = re.compile(r"Parts? [IVX]+(?: and [IVX]+)?")


@dataclass
class Line:
    text: str
    is_row: bool = False


class TextExtractor(HTMLParser):
    """HTML to Lines: one Line per block or table row; a page-break <hr> becomes a PAGE_BREAK Line."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.lines: list[Line] = []
        self.buffer: list[str] = []
        self.row: list[str] | None = None
        self.skip_tag: str | None = None
        self.skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if self.skip_tag is not None:
            if tag == self.skip_tag:
                self.skip_depth += 1
            return
        style = dict(attrs).get("style") or ""
        if tag in SKIPPED_TAGS or HIDDEN_STYLE.search(style):
            self.skip_tag, self.skip_depth = tag, 1
            return
        if tag == "tr":
            self.finish_row()
            self.flush()
            self.row = []
        elif tag in ("td", "th"):
            self.finish_cell()  # older HTML leaves a <td> unclosed: the next one ends it
        elif tag == "br":
            self.buffer.append(" " if self.row is not None else "\n")
        elif tag == "hr":
            self.finish_row()
            self.flush()
            if "page-break" in style:
                self.lines.append(Line(PAGE_BREAK))
        elif tag in BLOCK_TAGS:
            self.block_boundary()

    def handle_endtag(self, tag: str) -> None:
        if self.skip_tag is not None:
            if tag == self.skip_tag:
                self.skip_depth -= 1
                if self.skip_depth == 0:
                    self.skip_tag = None
            return
        if tag in ("td", "th"):
            self.finish_cell()
        elif tag == "tr":
            self.finish_row()
        elif tag == "table":
            self.finish_row()
            self.flush()
        elif tag in BLOCK_TAGS:
            self.block_boundary()

    def handle_data(self, data: str) -> None:
        if self.skip_tag is None:
            self.buffer.append(data)

    def block_boundary(self) -> None:
        if self.row is not None:
            self.buffer.append(" ")  # a <div> or <p> inside a cell: the cell continues
        else:
            self.flush()

    @staticmethod
    def clean(text: str) -> str:
        return SPACES.sub(" ", INVISIBLE.sub("", text)).strip()

    def flush(self) -> None:
        for piece in "".join(self.buffer).split("\n"):
            text = self.clean(piece)
            if text:
                self.lines.append(Line(text))
        self.buffer = []

    def finish_cell(self) -> None:
        if self.row is not None:
            self.row.append(self.clean("".join(self.buffer)))
            self.buffer = []

    def finish_row(self) -> None:
        if self.row is None:
            return
        self.finish_cell()
        cells = [cell for cell in self.row if cell]
        self.row = None
        self.buffer = []
        joined: list[str] = []
        for cell in cells:
            if cell in (")", "%", ")%", "x") and joined:  # accounting tables put these in cells of their own
                joined[-1] += cell
            elif joined and joined[-1] in ("$", "(", "$("):
                joined[-1] += cell
            else:
                joined.append(cell)
        if joined:
            self.lines.append(Line("  ".join(joined), is_row=True))


def document_lines(path: Path) -> list[Line]:
    parser = TextExtractor()
    parser.feed(path.read_bytes().decode("utf-8"))
    parser.close()
    parser.finish_row()
    parser.flush()
    return without_page_furniture(parser.lines)


def without_page_furniture(lines: list[Line]) -> list[Line]:
    """Drop running headers and footers: a line beside a page break whose digit-blind text recurs at many breaks, or
    that names only the Part of the report (those occur too few times to recur: "Parts I and II" on one page)."""
    def shape(text: str) -> str:
        return re.sub(r"\d+", "#", text)

    def may_be_furniture(line: Line) -> bool:
        # A table row only if it is a page number, alone or beside a form name (JPMorgan's footers); a data row never is.
        cells = line.text.split("  ")
        return not line.is_row or (len(cells) <= 2 and any(PAGE_ONLY.fullmatch(cell) for cell in cells))

    def beside_a_break():
        for index, line in enumerate(lines):
            if line.text != PAGE_BREAK:
                continue
            # up to two lines each side: a footer may be a page number and a form name on separate lines
            for offset in (-2, -1, 1, 2):
                neighbour = index + offset
                if 0 <= neighbour < len(lines) and lines[neighbour].text != PAGE_BREAK and may_be_furniture(lines[neighbour]):
                    yield neighbour

    counts = Counter(shape(lines[i].text) for i in set(beside_a_break()) if len(lines[i].text) <= 100)
    furniture = {s for s, n in counts.items() if n >= 3}
    drop = {i for i in set(beside_a_break()) if shape(lines[i].text) in furniture or RUNNING_PART.fullmatch(lines[i].text)}
    return [line for i, line in enumerate(lines) if i not in drop and line.text != PAGE_BREAK]


# ---- finding sections by their headings ----

ITEM_HEADING = re.compile(r"(?i)^(?:part\s+[ivx]+[\s,.:–—-]*)?item\s+(\d+(?:\.\d+|[a-c])?)\b\s*([.:–—-]*)\s*(.*)$")
SIGNATURE = re.compile(r"(?i)^signatures?\b")
PAGE_AT_ROW_END = re.compile(r"\s\d{1,3}$")


@dataclass(frozen=True)
class Wanted:
    item: str
    title: re.Pattern
    file_name: str
    ten_k_item: str | None = None


RISK = re.compile(r"(?i)risk factors")
MDNA = re.compile(r"(?i)management.s discussion and analysis")
MARKET = re.compile(r"(?i)quantitative and qualitative")
ANY = re.compile(r"")

WANTED = {
    "10-K": (Wanted("1A", RISK, "item_1a.txt"), Wanted("7", MDNA, "item_7.txt"), Wanted("7A", MARKET, "item_7a.txt")),
    "10-Q": (
        Wanted("1A", RISK, "item_1a.txt", "1A"),
        Wanted("2", MDNA, "item_2.txt", "7"),
        Wanted("3", MARKET, "item_3.txt", "7A"),
    ),
    "8-K": (Wanted("2.02", ANY, "item_2.02.txt"), Wanted("8.01", ANY, "item_8.01.txt")),
}

# JPMorgan files its MD&A as a named part of the report, and puts under Item 7 / Item 2 / Item 7A / Item 3 only a pointer
# to it ("appears on pages 46-160"). These spans are those parts, by the headings the filing itself gives them. A span
# ends where the heading after it begins. Matching is exact and case-sensitive, which passes over the table of contents
# (it spells these in title case). The 10-Q has no Item 2 heading in its body at all, only the contents entry. Its Item
# 1A points to the 10-K, outside the document: it stays a stub.
POINTER = re.compile(r"(?i)\bappears on pages\b|^refer to the market risk management section\b")


@dataclass(frozen=True)
class Span:
    start: str
    end: str


BY_REFERENCE = {
    ("10-K", "7"): Span("INTRODUCTION", "Management\u2019s report on internal control over financial reporting"),
    ("10-K", "7A"): Span("MARKET RISK MANAGEMENT", "COUNTRY RISK MANAGEMENT"),
    ("10-Q", "2"): Span("Consolidated financial highlights (unaudited)", "Consolidated statements of income (unaudited)"),
    ("10-Q", "3"): Span("MARKET RISK MANAGEMENT", "COUNTRY RISK MANAGEMENT"),
}


def boundaries(lines: list[Line]) -> list[int]:
    """Every line that opens a new item (or the signatures): where the section before it ends."""
    found = []
    for index, line in enumerate(lines):
        match = ITEM_HEADING.match(line.text)
        is_heading = match is not None and len(line.text) <= 250 and (
            match.group(2) != "" or not match.group(3) or match.group(3)[0].isupper()
        )
        if is_heading or (SIGNATURE.match(line.text) and len(line.text) <= 40):
            found.append(index)
    return found


def is_contents_entry(lines: list[Line], stops: list[int], index: int) -> bool:
    """A table-of-contents entry: a page number on its own line, or closing a table row, before the next item starts."""
    limit = min(index + 5, next((stop for stop in stops if stop > index), len(lines)))
    for near in lines[index:limit]:
        if PAGE_ONLY.fullmatch(near.text) or (near.is_row and PAGE_AT_ROW_END.search(near.text)):
            return True
    return False


def body_size(lines: list[Line], start: int, end: int) -> int:
    return sum(len(line.text) for line in lines[start + 1:end])


def find_section(lines: list[Line], stops: list[int], wanted: Wanted) -> tuple[int, int] | None:
    """(heading index, end index): among lines headed by this item and its title, outside the table of contents, the
    one with the most text under it (a cross-reference earlier in a filing can also read like a heading)."""
    best = None
    for index, line in enumerate(lines):
        match = ITEM_HEADING.match(line.text)
        if match is None or match.group(1).upper() != wanted.item or index not in stops:
            continue
        if is_contents_entry(lines, stops, index):
            continue
        following = lines[index + 1].text if index + 1 < len(lines) else ""
        if not (wanted.title.search(line.text) or wanted.title.search(following)):
            continue
        end = next((stop for stop in stops if stop > index), None)
        if end is None:
            raise SystemExit(f"Item {wanted.item}: no heading follows it, so its end is unknown")
        if best is None or body_size(lines, index, end) > body_size(lines, *best):
            best = (index, end)
    return best


def line_index(lines: list[Line], text: str, after: int, why: str) -> int:
    for index in range(after, len(lines)):
        if lines[index].text == text:
            return index
    raise SystemExit(f"{why}: the heading {text!r} was not found after line {after}")


def render(lines: list[Line]) -> str:
    """Paragraphs separated by a blank line; a table's rows kept together."""
    out: list[str] = []
    for index, line in enumerate(lines):
        if index and not (line.is_row and lines[index - 1].is_row):
            out.append("")
        out.append(line.text)
    return "\n".join(out) + "\n"


def extract_sections(raw: dict) -> list[tuple[Wanted, str, str, bool]]:
    """(wanted, heading, text, followed a pointer) for each section this filing has."""
    form, name = raw["form"], raw["path"]
    lines = document_lines(CORPUS_ROOT / raw["path"])
    stops = boundaries(lines)
    declared = set(raw["items"].split(",")) if form == "8-K" else None
    out = []
    for wanted in WANTED[form]:
        if declared is not None and wanted.item not in declared:
            continue
        section = find_section(lines, stops, wanted)
        span = BY_REFERENCE.get((form, wanted.item)) if raw["cik"] == 19617 else None
        heading, body, followed = None, [], False
        if section is not None:
            heading = lines[section[0]].text
            body = lines[section[0] + 1:section[1]]
        if span is not None and (section is None or (body and POINTER.search(body[0].text))):
            first = line_index(lines, span.start, 0, f"{name}: Item {wanted.item}")
            last = line_index(lines, span.end, first + 1, f"{name}: Item {wanted.item}")
            followed = True
            if section is None:  # no Item heading to carry: the span's own heading opens the file
                heading, first = lines[first].text, first + 1
            body = body + lines[first:last]
        if heading is None:
            raise SystemExit(f"{name}: no section found for Item {wanted.item}")
        if not body:
            raise SystemExit(f"{name}: Item {wanted.item} is empty")
        out.append((wanted, heading, render([Line(heading), *body]), followed))
    return out


# ---- 13F ----

def local(element: ElementTree.Element) -> str:
    return element.tag.rsplit("}", 1)[-1]


def field(table: ElementTree.Element, name: str, path: Path, optional: bool = False) -> str:
    """An infoTable's field; absent is an error unless the 13F schema makes it optional (only putCall, here)."""
    for candidate in table.iter():
        if local(candidate) == name and (candidate.text or "").strip():
            return candidate.text.strip()
    if optional:
        return ""
    raise SystemExit(f"{path}: an infoTable without {name}")


def holdings_csv(path: Path) -> tuple[bytes, int]:
    root = ElementTree.fromstring(path.read_bytes())
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["issuer", "cusip", "value", "shares", "put_call", "discretion"])
    rows = principal = 0
    for table in root:
        if local(table) != "infoTable":
            raise SystemExit(f"{path}: unexpected element {local(table)}")
        principal += field(table, "sshPrnamtType", path) != "SH"
        writer.writerow([
            field(table, "nameOfIssuer", path),
            field(table, "cusip", path),
            field(table, "value", path),
            field(table, "sshPrnamt", path),
            field(table, "putCall", path, optional=True),
            field(table, "investmentDiscretion", path),
        ])
        rows += 1
    if rows == 0:
        raise SystemExit(f"{path}: no holdings")
    if principal:
        print(f"  note: {path.name}: {principal} of {rows} rows are PRN (principal amount), not shares")
    return buffer.getvalue().encode("utf-8"), rows


def cover_text(path: Path) -> bytes:
    """Every leaf of the 13F primary document as `dotted.path: value`, in document order; credentials are not filed."""
    root = ElementTree.fromstring(path.read_bytes())
    out: list[str] = []

    def walk(element: ElementTree.Element, prefix: str) -> None:
        children = list(element)
        name = f"{prefix}.{local(element)}" if prefix else local(element)
        if children:
            for child in children:
                walk(child, name)
        elif (element.text or "").strip():
            out.append(f"{name}: {element.text.strip()}")

    for child in root:
        walk(child, "")
    return ("\n".join(out) + "\n").encode("utf-8")


# ---- the run ----

def write_output(raw: dict, name: str, data: bytes, **extra) -> dict:
    relative = f"{Path(raw['path']).parent.as_posix()}/{name}"
    write_atomically(CORPUS_ROOT / relative, data)
    row = {"path": relative, "kind": "extracted"}
    row.update(extra)
    row.update({key: raw[key] for key in ("cik", "company", "form", "accession", "filed")})
    row.update({
        "derived_from": raw["path"],
        "source_url": raw["source_url"],
        "bytes": len(data),
        "sha256": sha256_of(data),
    })
    return row


def main() -> int:
    rows = load_manifest()
    raws = [row for row in rows if row["kind"] == "raw"]
    if not raws:
        raise SystemExit("no raw rows in manifest.yaml: run fetch.py first")
    for raw in raws:
        if not (CORPUS_ROOT / raw["path"]).is_file():
            raise SystemExit(f"{raw['path']}: not on disk; run fetch.py first")
        if sha256_of((CORPUS_ROOT / raw["path"]).read_bytes()) != raw["sha256"]:
            raise SystemExit(f"{raw['path']}: does not match its manifest sha256; run fetch.py")
    extracted: list[dict] = []
    without_items = []
    for raw in raws:
        role = raw["role"]
        path = CORPUS_ROOT / raw["path"]
        if role == "info_table":
            data, count = holdings_csv(path)
            extracted.append(write_output(raw, "holdings.csv", data, section="holdings", row_count=count))
        elif role == "primary_doc":
            extracted.append(write_output(raw, "cover.txt", cover_text(path), section="cover"))
        else:
            sections = extract_sections(raw)
            if not sections:
                without_items.append(raw)
            for wanted, heading, text, followed in sections:
                extra = {"section": f"item_{wanted.item}", "heading": heading}
                if wanted.ten_k_item:
                    extra["ten_k_item"] = wanted.ten_k_item
                if followed:
                    extra["followed_pointer"] = True
                extracted.append(write_output(raw, wanted.file_name, text.encode("utf-8"), **extra))
    previous = {row["path"] for row in rows if row["kind"] == "extracted"} - {row["path"] for row in extracted}
    for stale in sorted(previous):
        (CORPUS_ROOT / stale).unlink(missing_ok=True)
        print(f"removed stale output {stale}")
    changed = write_manifest(raws + extracted)
    print(f"extracted {len(extracted)} files, {sum(r['bytes'] for r in extracted):,} bytes; "
          f"manifest.yaml {'updated' if changed else 'unchanged'}")
    for raw in without_items:
        print(f"no target items in {raw['path']} (SEC-declared items: {raw['items']})")
    total = sum(row["bytes"] for row in raws + extracted)
    print(f"total raw + extracted: {total:,} bytes; disk limit {DISK_LIMIT_BYTES:,}: "
          f"{'within the limit' if total < DISK_LIMIT_BYTES else 'OVER THE LIMIT'}")
    return 0 if total < DISK_LIMIT_BYTES else 1


if __name__ == "__main__":
    sys.exit(main())
