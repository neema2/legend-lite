#!/usr/bin/env python3
"""Fetch the EDGAR corpus for the unstructured study, deterministically. Stdlib + requests only.

    python3 tools/edgar-corpus/fetch.py            # download, or re-download and compare
    python3 tools/edgar-corpus/fetch.py --verify   # local only: every file on disk against the manifest

The filings are a pointer corpus: never committed, rebuilt locally (the corpus's README.md). manifest.yaml is the record.

First run (no raw rows in manifest.yaml): resolve TARGETS against data.sec.gov/submissions -- the newest N filings of
exactly that form, filed on or before AS_OF -- then download each kept file and write the manifest.

Every later run, and a fresh checkout's first: the manifest is the pin. Each raw row's source_url is downloaded again and its sha256 compared to the
manifest and to the file on disk; nothing is written unless a file is missing (restored only if its sha256 matches).
Any mismatch is reported and the exit status is 1. The selection never moves on its own: a filing EDGAR publishes after
AS_OF does not enter the corpus.

SEC fair-access rules, enforced by Edgar: the User-Agent from the EDGAR_USER_AGENT environment variable, which names the
person running this and has no default; at most 10 requests/second (0.15 s between calls); the two hosts in ALLOWED_HOSTS
only (redirects are not followed). --verify makes no requests, so it needs no User-Agent.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit
from xml.etree import ElementTree

import requests

USER_AGENT_VARIABLE = "EDGAR_USER_AGENT"
ALLOWED_HOSTS = {"data.sec.gov", "www.sec.gov"}
SLEEP_BETWEEN_CALLS = 0.15
RETRY_STATUSES = {429, 500, 502, 503, 504}
RETRY_BACKOFF_SECONDS = (2, 4, 8)

# The selection's clock: "newest" means newest filed on or before this date, so a new filing cannot move the corpus.
AS_OF = "2026-11-08"

TARGETS = (
    (1067983, "Berkshire Hathaway", "13F-HR", 2),
    (1037389, "Renaissance Technologies", "13F-HR", 1),
    (320193, "Apple", "10-K", 1),
    (320193, "Apple", "10-Q", 2),
    (320193, "Apple", "8-K", 3),
    (19617, "JPMorgan", "10-K", 1),
    (19617, "JPMorgan", "10-Q", 1),
    (19617, "JPMorgan", "8-K", 2),
)

REPO_ROOT = Path(__file__).resolve().parents[2]
CORPUS_ROOT = REPO_ROOT / "projects" / "unstructured" / "corpus" / "edgar"
MANIFEST_PATH = CORPUS_ROOT / "manifest.yaml"

MANIFEST_HEADER = (
    "# The EDGAR corpus, one row per file. Written by tools/edgar-corpus/fetch.py (kind: raw) and extract.py\n"
    "# (kind: extracted); sorted by path, no timestamps, so a re-run reproduces it byte for byte. Paths are relative\n"
    "# to this directory. Do not edit by hand.\n"
)
SAFE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")

# www.sec.gov's CDN appends a bot-defence <script> to the HTML it serves, whose src path can rotate: it is not part of
# the filing, and a sha256 taken over it would stop verifying the day it changes. Removing it restores the filed bytes;
# first_run proves that against the size the filing index states.
CDN_SCRIPT = re.compile(rb'<script type="text/javascript"\s+src="/[^"]*"></script>(?=</body></html>\s*$)')


# ---- the manifest: a fixed YAML shape (a list of flat mappings), written and read without a YAML library ----

def load_manifest() -> list[dict]:
    """The manifest's rows; [] if it does not exist. Each value is a JSON scalar, which is also a YAML scalar."""
    if not MANIFEST_PATH.exists():
        return []
    rows: list[dict] = []
    for number, line in enumerate(MANIFEST_PATH.read_text(encoding="utf-8").splitlines(), 1):
        if not line or line.startswith("#") or line == "files:":
            continue
        match = re.fullmatch(r"(- |  )([a-z_0-9]+): (.*)", line)
        if match is None:
            raise SystemExit(f"{MANIFEST_PATH}:{number}: not a manifest row: {line!r}")
        if match.group(1) == "- ":
            rows.append({})
        rows[-1][match.group(2)] = json.loads(match.group(3))
    return rows


def manifest_text(rows: list[dict]) -> str:
    lines = [MANIFEST_HEADER.rstrip("\n"), "files:"]
    for row in sorted(rows, key=lambda r: r["path"]):
        for index, (key, value) in enumerate(row.items()):
            lines.append(f"{'- ' if index == 0 else '  '}{key}: {json.dumps(value, ensure_ascii=False)}")
    return "\n".join(lines) + "\n"


def write_manifest(rows: list[dict]) -> bool:
    """Write the manifest if its text changed; True if it did."""
    text = manifest_text(rows)
    if MANIFEST_PATH.exists() and MANIFEST_PATH.read_text(encoding="utf-8") == text:
        return False
    write_atomically(MANIFEST_PATH, text.encode("utf-8"))
    return True


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_atomically(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_bytes(data)
    os.replace(temporary, path)


# ---- the one door to the network ----

def user_agent() -> str:
    """The User-Agent the SEC requires. It names a person, so it is theirs to give: no default (AGENTS.md invariant 4)."""
    value = os.environ.get(USER_AGENT_VARIABLE)
    if value is None or not value.strip():
        raise SystemExit(
            f"{USER_AGENT_VARIABLE} is not set. The SEC requires a User-Agent naming who is asking; run:\n"
            f'  {USER_AGENT_VARIABLE}="Name email@example.org" python tools/edgar-corpus/fetch.py'
        )
    return value.strip()


class Edgar:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent(), "Accept-Encoding": "gzip, deflate"})
        self.calls = 0
        self.called_before = False

    def get(self, url: str) -> bytes:
        host = urlsplit(url).hostname
        if host not in ALLOWED_HOSTS:
            raise SystemExit(f"refusing a host outside the SEC rules: {url}")
        for backoff in (*RETRY_BACKOFF_SECONDS, None):
            if self.called_before:
                time.sleep(SLEEP_BETWEEN_CALLS)
            self.called_before = True
            self.calls += 1
            response = self.session.get(url, timeout=120, allow_redirects=False)
            if response.status_code == 200:
                return response.content
            if response.status_code not in RETRY_STATUSES or backoff is None:
                raise SystemExit(f"HTTP {response.status_code} for {url}")
            print(f"  HTTP {response.status_code}, retrying in {backoff}s: {url}", file=sys.stderr)
            time.sleep(backoff)
        raise AssertionError("unreachable")


# ---- resolving the targets (first run only) ----

def select_filings(edgar: Edgar) -> list[dict]:
    """The filings TARGETS names: newest first, exact form match (an amendment such as 10-K/A is a different form)."""
    submissions: dict[int, dict] = {}
    selected = []
    for cik, company, form, count in TARGETS:
        if cik not in submissions:
            submissions[cik] = json.loads(edgar.get(f"https://data.sec.gov/submissions/CIK{cik:010d}.json"))
        recent = submissions[cik]["filings"]["recent"]
        rows = sorted(
            (i for i, f in enumerate(recent["form"]) if f == form and recent["filingDate"][i] <= AS_OF),
            key=lambda i: (recent["filingDate"][i], recent["accessionNumber"][i]),
            reverse=True,
        )
        if len(rows) < count:
            raise SystemExit(f"CIK {cik}: wanted {count} x {form} filed by {AS_OF}, the recent list has {len(rows)}")
        for i in rows[:count]:
            selected.append({
                "cik": cik,
                "company": company,
                "form": form,
                "accession": recent["accessionNumber"][i],
                "filed": recent["filingDate"][i],
                "report_period": recent["reportDate"][i],
                "items": recent["items"][i],
                "primary_document": recent["primaryDocument"][i],
            })
    return selected


def base_url(filing: dict) -> str:
    return f"https://www.sec.gov/Archives/edgar/data/{filing['cik']}/{filing['accession'].replace('-', '')}/"


def as_filed(name: str, data: bytes) -> bytes:
    """The document as filed: an .htm without the CDN's script tag, anything else untouched."""
    return CDN_SCRIPT.sub(b"", data) if name.lower().endswith(".htm") else data


def is_information_table(xml: bytes) -> bool:
    return ElementTree.fromstring(xml).tag.rsplit("}", 1)[-1] == "informationTable"


def files_to_keep(edgar: Edgar, filing: dict) -> list[tuple[str, str, bytes | None, int]]:
    """(role, name, bytes if already read, filed size) of the files this filing keeps, read off its filing index."""
    index = json.loads(edgar.get(base_url(filing) + "index.json"))
    items = index["directory"]["item"]
    names = [item["name"] for item in items]
    sizes = {item["name"]: item["size"] for item in items}
    for name in names:
        if SAFE_NAME.fullmatch(name) is None:
            raise SystemExit(f"{filing['accession']}: an unexpected file name in the index: {name!r}")
    if filing["form"] != "13F-HR":
        name = filing["primary_document"]
        if not name.lower().endswith(".htm") or name not in names:
            raise SystemExit(f"{filing['accession']}: the primary document {name!r} is not an .htm in the index")
        return [("primary", name, None, int(sizes[name]))]
    # The submissions JSON names a 13F's primary document by its XSL rendering (xslForm13F_X02/primary_doc.xml); the
    # filed XML is primary_doc.xml at the filing's root. The info table's name varies by filer: it is the other XML.
    if "primary_doc.xml" not in names:
        raise SystemExit(f"{filing['accession']}: no primary_doc.xml in the index")
    tables = []
    for name in names:
        if name.lower().endswith(".xml") and name != "primary_doc.xml":
            data = edgar.get(base_url(filing) + name)
            if is_information_table(data):
                tables.append((name, data))
    if len(tables) != 1:
        raise SystemExit(f"{filing['accession']}: expected one information table, found {[n for n, _ in tables]}")
    return [
        ("primary_doc", "primary_doc.xml", None, int(sizes["primary_doc.xml"])),
        ("info_table", tables[0][0], tables[0][1], int(sizes[tables[0][0]])),
    ]


def first_run(edgar: Edgar) -> list[dict]:
    rows = []
    for filing in select_filings(edgar):
        directory = f"{filing['form']}/{filing['cik']}_{filing['accession']}"
        print(f"{filing['company']} {filing['form']} {filing['accession']} filed {filing['filed']}")
        for role, name, data, filed_size in files_to_keep(edgar, filing):
            url = base_url(filing) + name
            data = as_filed(name, edgar.get(url) if data is None else data)
            if len(data) != filed_size:
                raise SystemExit(f"{url}: {len(data)} bytes, the filing index states {filed_size}")
            relative = f"{directory}/{name}"
            write_atomically(CORPUS_ROOT / relative, data)
            print(f"  {relative}  {len(data):,} bytes")
            row = {
                "path": relative,
                "kind": "raw",
                "role": role,
                "cik": filing["cik"],
                "company": filing["company"],
                "form": filing["form"],
                "accession": filing["accession"],
                "filed": filing["filed"],
                "report_period": filing["report_period"],
            }
            if filing["items"]:
                row["items"] = filing["items"]
            row.update({"source_url": url, "bytes": len(data), "sha256": sha256_of(data)})
            rows.append(row)
    return rows


# ---- checking against the pin ----

def compare_with_sec(edgar: Edgar, rows: list[dict]) -> int:
    """Re-download every raw row and compare; returns the number of problems. Writes only a missing, verified file."""
    problems = restored = 0
    for row in rows:
        data = as_filed(row["path"], edgar.get(row["source_url"]))
        remote = sha256_of(data)
        local_path = CORPUS_ROOT / row["path"]
        local = sha256_of(local_path.read_bytes()) if local_path.exists() else None
        if remote != row["sha256"]:
            problems += 1
            print(f"MISMATCH (SEC)   {row['path']}\n  manifest {row['sha256']}\n  fetched  {remote}")
        elif local is None:
            write_atomically(local_path, data)
            restored += 1
            print(f"RESTORED         {row['path']}")
        elif local != row["sha256"]:
            problems += 1
            print(f"MISMATCH (disk)  {row['path']}\n  manifest {row['sha256']}\n  on disk  {local}")
    print(f"verified {len(rows) - problems} of {len(rows)} raw files against the manifest by sha256; "
          f"{restored} restored, {problems} mismatched, {edgar.calls} requests")
    return problems


def verify_disk(rows: list[dict]) -> int:
    """--verify: every manifest row against the disk, and every file on disk against the manifest. No network."""
    if not rows:
        raise SystemExit(f"no rows in {MANIFEST_PATH}")
    missing = mismatched = unlisted = 0
    for row in rows:
        path = CORPUS_ROOT / row["path"]
        if not path.is_file():
            missing += 1
            print(f"MISSING   {row['path']}")
            continue
        data = path.read_bytes()
        if len(data) != row["bytes"] or sha256_of(data) != row["sha256"]:
            mismatched += 1
            print(f"MISMATCH  {row['path']}")
    listed = {row["path"] for row in rows} | {"manifest.yaml", "README.md"}
    for path in sorted(CORPUS_ROOT.rglob("*")):
        relative = path.relative_to(CORPUS_ROOT).as_posix()
        if path.is_file() and relative not in listed:
            unlisted += 1
            print(f"UNLISTED  {relative}")
    print(f"{len(rows) - missing - mismatched} of {len(rows)} manifest files verified by sha256; "
          f"{missing} missing, {mismatched} mismatched, {unlisted} unlisted")
    return 1 if missing or mismatched or unlisted else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch or verify the EDGAR pointer corpus.")
    parser.add_argument("--verify", action="store_true", help="check every file on disk against the manifest; no network")
    arguments = parser.parse_args()
    rows = load_manifest()
    if arguments.verify:
        return verify_disk(rows)
    edgar = Edgar()
    raw = [row for row in rows if row["kind"] == "raw"]
    if raw:
        print(f"manifest pins {len(raw)} raw files: verifying (nothing is re-selected)")
        problems = compare_with_sec(edgar, raw)
        if write_manifest(rows):
            print("manifest.yaml: reformatted")
            problems += 1
        else:
            print("manifest.yaml: unchanged")
        return 1 if problems else 0
    new_rows = first_run(edgar)
    write_manifest(new_rows)
    print(f"wrote {len(new_rows)} raw files, {sum(r['bytes'] for r in new_rows):,} bytes, {edgar.calls} requests; "
          f"manifest.yaml written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
