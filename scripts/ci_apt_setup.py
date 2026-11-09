#!/usr/bin/env python3
"""Configure ephemeral Ubuntu CI runners before dependency installation."""
from pathlib import Path
import re


# Match complete URI tokens only; never rewrite third-party repositories,
# suites, components, signing keys, or trust settings. Bypass mirror+file so
# package downloads cannot independently reselect the slow Azure HTTP mirror.
UBUNTU_URI = re.compile(
    r"(?<!\S)(?:mirror\+file:/etc/apt/apt-mirrors\.txt|"
    r"https?://(?:azure\.archive|archive|security)\.ubuntu\.com/ubuntu/?)"
    r"(?=\s|$)"
)
APT_CONFIG = '''// Ephemeral CI dependency setup; fail on partial/stale index updates.
Acquire::Retries "1";
Acquire::http::Timeout "15";
Acquire::https::Timeout "15";
APT::Update::Error-Mode "any";
'''


def configure(root: Path) -> None:
    sources = [root / "sources.list"]
    for pattern in ("*.list", "*.sources"):
        sources.extend(sorted((root / "sources.list.d").glob(pattern)))
    updates = []
    recognized = 0
    for source in sources:
        if not source.is_file():
            continue
        lines = []
        for line in source.read_text().splitlines(keepends=True):
            if re.match(r"^\s*(?:URIs:|deb(?:-src)?\s)", line):
                line, count = UBUNTU_URI.subn("https://archive.ubuntu.com/ubuntu/", line)
                recognized += count
            lines.append(line)
        updates.append((source, "".join(lines)))
    if not recognized:
        raise ValueError("No recognized Ubuntu APT source; inspect runner image before proceeding")
    for source, content in updates:
        if source.read_text() != content:
            source.write_text(content)
    # Sort after the image's zz-retries; Playwright's sudo apt-get reads this too.
    (root / "apt.conf.d/zzzz-coralswarm-ci").write_text(APT_CONFIG)
    print("CI APT: official HTTPS Ubuntu archive; retries=1, HTTP(S) timeout=15s, update errors fatal")


if __name__ == "__main__":
    configure(Path("/etc/apt"))
