#!/usr/bin/env python3
"""Test stub for the `gh` CLI — backs "GitHub Releases" on the local filesystem (FAKE_REL)."""
import fnmatch
import json
import os
import shutil
import sys

FAKE_REL = os.environ.get("FAKE_REL", "")


def _repo(argv):
    if "-R" in argv:
        return argv[argv.index("-R") + 1]
    return os.environ.get("GH_REPO", "main/encoder")


def _tag_dir(repo, tag):
    return os.path.join(FAKE_REL, repo, tag)


def _list_assets(repo, tag):
    d = _tag_dir(repo, tag)
    out = []
    if os.path.isdir(d):
        for name in os.listdir(d):
            p = os.path.join(d, name)
            if os.path.isfile(p):
                out.append({"name": name, "state": "uploaded", "size": os.path.getsize(p)})
    return out


def main():
    argv = sys.argv[1:]
    if not argv:
        return 1
    cmd = argv[0]
    if cmd == "api":
        if "user" in argv:
            print("fake-user")
            return 0
        print("{}")
        return 0
    if cmd == "secret":
        return 0
    if cmd in ("workflow", "run"):
        if "list" in argv:
            print("[]")
        elif "view" in argv:
            print('{"jobs": []}')
        return 0
    if cmd != "release":
        print("fake-gh: unhandled " + " ".join(argv), file=sys.stderr)
        return 1
    sub = argv[1]
    repo = _repo(argv)
    rest = [a for a in argv[2:] if not a.startswith("-")]
    flags = argv[2:]

    if sub == "view":
        tag = rest[0]
        field = ""
        if "--json" in flags:
            field = flags[flags.index("--json") + 1]
        assets = _list_assets(repo, tag)
        if not os.path.isdir(_tag_dir(repo, tag)):
            print(f"release not found: {tag}", file=sys.stderr)
            return 1
        if field == "assets":
            print(json.dumps({"assets": assets}))
        elif field == "id":
            print('{"id": 123456}')
        else:
            print(json.dumps({"assets": assets, "id": 123456}))
        return 0
    if sub == "create":
        tag = rest[0]
        os.makedirs(_tag_dir(repo, tag), exist_ok=True)
        return 0
    if sub == "upload":
        tag = rest[0]
        os.makedirs(_tag_dir(repo, tag), exist_ok=True)
        for f in rest[1:]:
            if os.path.isfile(f):
                shutil.copyfile(f, os.path.join(_tag_dir(repo, tag), os.path.basename(f)))
        return 0
    if sub == "download":
        tag = rest[0]
        pattern = flags[flags.index("-p") + 1]
        destdir = flags[flags.index("-D") + 1]
        os.makedirs(destdir, exist_ok=True)
        d = _tag_dir(repo, tag)
        matched = [f for f in os.listdir(d) if fnmatch.fnmatch(f, pattern)] if os.path.isdir(d) else []
        if not matched:
            print(f"no assets matched {pattern} in {repo}/{tag}", file=sys.stderr)
            return 1
        for name in matched:
            shutil.copyfile(os.path.join(d, name), os.path.join(destdir, name))
        return 0
    if sub == "delete":
        shutil.rmtree(_tag_dir(repo, rest[0]), ignore_errors=True)
        return 0
    if sub == "delete-asset":
        tag, name = rest[0], rest[1]
        p = os.path.join(_tag_dir(repo, tag), name)
        if os.path.exists(p):
            os.remove(p)
        return 0
    if sub == "list":
        base = os.path.join(FAKE_REL, repo)
        tags = [d for d in os.listdir(base) if os.path.isdir(os.path.join(base, d))] if os.path.isdir(base) else []
        print(json.dumps([{"tagName": t} for t in tags]))
        return 0
    print("fake-gh: unhandled release " + sub, file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
