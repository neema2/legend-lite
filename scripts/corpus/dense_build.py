"""
Writes the three stress files that build.py does not: 59-dense-mapping.pure (dense_mapping),
60-dense-store.pure (dense_store) and 64-combinations.pure (combos).

  scripts/corpus/dense_build.py --out DIR

They are generated from the corpus WITHOUT themselves (model.DENSE_GENERATED) and without
build.py's outputs (model.GENERATED), and each chooses from a committed seed list rather than
from whatever the corpus holds, so adding a stress file does not move them. Under Bazel this
is an action; //core:update_generated writes the files back and diff-tests them.
"""
from __future__ import annotations

import sys
from pathlib import Path

import model


def generate() -> dict[str, str]:
    model.EXCLUDE |= model.DENSE_GENERATED
    import combos
    import dense_mapping
    import dense_store
    import flat

    c = model.load()
    seeded = {t for t, rows in flat.all_tables(c).items() if rows}
    return {"59-dense-mapping.pure": dense_mapping.build(c, seeded),
            "60-dense-store.pure": dense_store.build(c),
            "64-combinations.pure": combos.build_source()}


def main() -> None:
    out = Path(sys.argv[sys.argv.index("--out") + 1])
    out.mkdir(parents=True, exist_ok=True)
    for name, text in generate().items():
        (out / name).write_text(text, encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
