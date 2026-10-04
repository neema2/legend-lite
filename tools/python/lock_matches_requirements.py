"""Every pin in requirements.in is in requirements_lock.txt at the same version, with hashes (Bazel workplan
P1-07). Offline: the gate's check that the lock was regenerated after requirements.in changed. The
network re-resolution, //tools/python:requirements.test, stays a manual run."""

import re
import sys

from python.runfiles import runfiles


def pins(text):
    out = {}
    for line in text.splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            m = re.fullmatch(r"([A-Za-z0-9_.\-]+)==([^\s;\\]+)", line)
            if not m:
                raise SystemExit(f"requirements.in: not an exact pin: {line!r}")
            out[m.group(1).lower().replace("_", "-")] = m.group(2)
    return out


def locked(text):
    out = {}
    current = None
    for line in text.splitlines():
        m = re.match(r"^([A-Za-z0-9_.\-]+)==(\S+)", line)
        if m:
            current = m.group(1).lower().replace("_", "-")
            out[current] = [m.group(2).rstrip(" \\"), 0]
        elif current and "--hash=sha256:" in line:
            out[current][1] += 1
    return out


def main():
    r = runfiles.Create()
    wanted = pins(open(r.Rlocation(sys.argv[1]), encoding="utf-8").read())
    lock = locked(open(r.Rlocation(sys.argv[2]), encoding="utf-8").read())
    problems = []
    for name, version in sorted(wanted.items()):
        if name not in lock:
            problems.append(f"{name}=={version} is not in the lock")
        elif lock[name][0] != version:
            problems.append(f"{name}: requirements.in says {version}, the lock {lock[name][0]}")
        elif lock[name][1] == 0:
            problems.append(f"{name}: the lock carries no hashes")
    if problems:
        print("\n".join(problems))
        print("regenerate: bazel run //tools/python:requirements.update")
        sys.exit(1)
    print(f"ok: {len(wanted)} pins locked with hashes")


if __name__ == "__main__":
    main()
