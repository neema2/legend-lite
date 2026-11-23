"""Every pin in pyproject.toml is in uv.lock at the same version, with hashes (Bazel workplan P1-07; uv's lock
since 2026-11-23). Offline: the gate's check that the lock was regenerated after pyproject.toml changed. The
network re-resolution, `uv lock --check`, stays a manual run."""

import re
import sys
import tomllib

from python.runfiles import runfiles


def normal(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def pins(project):
    out = {}
    for requirement in project["project"]["dependencies"]:
        m = re.fullmatch(r"([A-Za-z0-9_.\-]+)==([^\s;]+)", requirement)
        if not m:
            raise SystemExit(f"pyproject.toml: not an exact pin: {requirement!r}")
        out[normal(m.group(1))] = m.group(2)
    return out


def locked(lock):
    """Each registry package's versions (one per resolution fork), and how many of its files carry no hash."""
    out = {}
    for package in lock["package"]:
        if "registry" not in package.get("source", {}):
            continue
        files = package.get("wheels", []) + ([package["sdist"]] if "sdist" in package else [])
        unhashed = sum(1 for f in files if not f.get("hash", "").startswith("sha256:"))
        out.setdefault(normal(package["name"]), []).append((package["version"], len(files), unhashed))
    return out


def load(r, path):
    with open(r.Rlocation(path), "rb") as f:
        return tomllib.load(f)


def main():
    r = runfiles.Create()
    wanted = pins(load(r, sys.argv[1]))
    lock = locked(load(r, sys.argv[2]))
    problems = []
    for name, version in sorted(wanted.items()):
        versions = sorted({v for v, _, _ in lock.get(name, [])})
        if not versions:
            problems.append(f"{name}=={version} is not in the lock")
        elif versions != [version]:
            problems.append(f"{name}: pyproject.toml says {version}, the lock {', '.join(versions)}")
    for name, entries in sorted(lock.items()):
        for version, files, unhashed in entries:
            if files == 0 or unhashed:
                problems.append(f"{name}=={version}: the lock carries no hash for {unhashed or 'any'} of its files")
    if problems:
        print("\n".join(problems))
        print("regenerate: bazel run //:uv_lock.update")
        sys.exit(1)
    print(f"ok: {len(wanted)} pins locked, {len(lock)} packages with hashes")


if __name__ == "__main__":
    main()
