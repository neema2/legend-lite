"""The answer key is the file it was frozen as. A label changes only with a dated entry in DECISIONS.md and a new answer_key.sha256."""
import hashlib
import sys
from pathlib import Path


def main(key: str, recorded: str) -> int:
    actual = hashlib.sha256(Path(key).read_bytes()).hexdigest()
    frozen = Path(recorded).read_text(encoding="utf-8").strip()
    if actual != frozen:
        print(f"answer_key.yaml has sha256 {actual}; answer_key.sha256 holds {frozen}: a label changes only with a dated "
              "entry in DECISIONS.md and a new answer_key.sha256", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
