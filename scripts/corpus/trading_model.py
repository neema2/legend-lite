"""
The one trading model as one text (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md DH34): what a page holds to plan the
corpus's classes through stress::AllMapping -- the linked projects, then every stress file, in the order legend-lite's
own suites load them (StressCorpus.files: each project's model, store and mapping; then the stress files by name), with
the files legend-lite cannot build a model from left out (StressExclusions, read from its own source so the two cannot
drift) and the corpus's services and test data left out too (a page plans; it runs no testSuite and seeds nothing) --
the test data a hand-written file holds beside its model too, every ###Data section (2026-10-27, the timeseries line:
each is one service test's seed, and a page fills a project's database from the Data it carries, which two of these
contend with -- hier::IssuerData and hier::IssuerProdData fill one table two Databases declare).

  run by Bazel (//scripts/corpus:trading_model): --out FILE --exclusions StressExclusions.java --inputs FILE...

Measured (2026-10-25): 2.27 MB, held by the WebAssembly planner in 4.4 s, a class query then planned in 5 ms.
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

# The generated files a page needs none of: the services and their test data. The runtimes and connections some
# hand-written files name live in 96-98, which stay.
SERVICES_AND_DATA = {"92-services.pure", "93-testdata.pure", "94-canonical-services.pure", "94-fanout-services.pure",
                     "94-gap-services.pure", "95-function-tests.pure", "99-intraday-services.pure",
                     "99-m2m-services.pure", "99-trader-services.pure", "99-typed-services.pure",
                     "99-unreached-services.pure", "99-vocab-services.pure"}
SECTIONS = ("model.pure", "store.pure", "mapping.pure")


def excluded(java: str) -> set[str]:
    """The keys of StressExclusions.EXCLUDED: each a quoted file name at the start of its entry."""
    names = set(re.findall(r'^\s*"(\d+-[\w-]+\.pure)",\s*$', java, re.M))
    if not names:
        raise SystemExit("trading_model.py: no exclusions found in StressExclusions.java -- has its shape changed?")
    return names


def without_data(text: str) -> str:
    """The text without its ###Data sections: each from its header to the next section's (or the text's end)."""
    out, keep = [], True
    for line in text.splitlines(keepends=True):
        if line.startswith("###"):
            keep = line.rstrip() != "###Data"
        if keep:
            out.append(line)
    return "".join(out)


def main() -> None:
    argv = sys.argv
    out = Path(argv[argv.index("--out") + 1])
    exclusions = excluded(Path(argv[argv.index("--exclusions") + 1]).read_text(encoding="utf-8"))
    rest = argv[argv.index("--inputs") + 1:]
    inputs = [Path(p) for p in rest[:next((i for i, a in enumerate(rest) if a.startswith("--")), len(rest))]]
    layout = json.loads(Path(os.environ["STRESS_LAYOUT"]).read_text(encoding="utf-8"))
    parts = []
    for project in layout["linked_projects"]:
        for section in SECTIONS:
            parts += [p.read_text(encoding="utf-8") for p in inputs
                      if p.name == section and p.parent.name == project]
    stress = sorted((p for p in inputs if p.parent.name == "stress" and p.suffix == ".pure"), key=lambda p: p.name)
    names = {p.name for p in stress}
    missing = (set(layout["generated"]) - SERVICES_AND_DATA) - names
    if missing:
        raise SystemExit(f"trading_model.py: generated stress files not given: {sorted(missing)}")
    parts += [without_data(p.read_text(encoding="utf-8")) for p in stress
              if p.name not in exclusions and p.name not in SERVICES_AND_DATA]
    out.write_text("\n".join(parts) + "\n", encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
