#!/usr/bin/env python3
"""Choose the next server-YYYY.MM.DD.N tag and the commit it releases. Read-only: tags and pushes nothing."""
import argparse
import datetime
import os
import re
import subprocess

TAG = re.compile(r"server-(\d{4})\.(\d{2})\.(\d{2})\.(\d+)\Z")


def key(tag):
    return tuple(int(part) for part in TAG.fullmatch(tag).groups())


def next_tag(tags, released, today):
    """The next unused N for today's UTC date, and the latest FINAL release. A tag whose Release run
    failed stays burned (its N is never reused) but is not a release. Refuses a tag dated after today."""
    tags = sorted((t for t in tags if TAG.fullmatch(t)), key=key)
    stem = "server-" + today.strftime("%Y.%m.%d")
    used = [key(t)[3] for t in tags if t.startswith(stem + ".")]
    tag = f"{stem}.{max(used, default=0) + 1}"
    if tags and key(tags[-1]) >= key(tag):
        raise SystemExit(f"tag {tags[-1]} sorts after today's {tag}; check the runner clock")
    final = sorted((t for t in released if TAG.fullmatch(t) and t in tags), key=key)
    return tag, (final[-1] if final else None)


def git(*args):
    return subprocess.run(["git", *args], check=True, text=True, capture_output=True).stdout.strip()


def is_ancestor(older, newer):
    return subprocess.run(["git", "merge-base", "--is-ancestor", older, newer]).returncode == 0


def plan(pin, force, released, today):
    main = git("rev-parse", "origin/main")
    sha = git("rev-parse", "--verify", (pin or main) + "^{commit}")
    if not is_ancestor(sha, main):
        raise SystemExit(f"{sha} is not on main; releases ship only merged code")
    tag, latest = next_tag(git("tag", "-l", "server-*").split(), released, today)
    if latest:
        released = git("rev-parse", latest + "^{commit}")
        if is_ancestor(sha, released) and not force:
            raise SystemExit(f"{sha} is already in {latest} ({released}); main has not moved. Set force to cut it anyway.")
    return {"tag": tag, "sha": sha, "latest": latest or ""}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sha", default="", help="commit on main to release (default: origin/main)")
    parser.add_argument("--force", choices=["true", "false"], default="false")
    parser.add_argument("--released", required=True, help="file listing the tags of final (published, not pre-release) releases")
    args = parser.parse_args()
    with open(args.released) as released:
        names = released.read().split()
    chosen = plan(args.sha, args.force == "true", names, datetime.datetime.now(datetime.timezone.utc).date())
    for name, value in chosen.items():
        print(f"{name}={value}")
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.writelines(f"{name}={value}\n" for name, value in chosen.items())


if __name__ == "__main__":
    main()
