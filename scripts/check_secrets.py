"""Reject likely committed credentials without printing matched values."""

from __future__ import annotations

import argparse
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MAX_SCANNED_BYTES = 2_000_000


@dataclass(frozen=True)
class SecretPattern:
    """Named secret signature used for safe findings."""

    name: str
    expression: re.Pattern[str]


@dataclass(frozen=True)
class Finding:
    """Secret finding that deliberately excludes the matched value."""

    path: Path
    line_number: int
    pattern_name: str


SECRET_PATTERNS = (
    SecretPattern(
        "private key material",
        re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    ),
    SecretPattern(
        "AWS access key",
        re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
    ),
    SecretPattern(
        "AWS secret access key assignment",
        re.compile(
            r"(?im)^\s*AWS_SECRET_ACCESS_KEY\s*=\s*[A-Za-z0-9/+=]{32,}\s*$"
        ),
    ),
    SecretPattern(
        "Alchemy credential-bearing endpoint",
        re.compile(
            r"(?:https?|wss?)://[^\s\"']*\.g\.alchemy\.com/v2/"
            r"(?!<|\$|\{)[A-Za-z0-9_-]{12,}",
            re.IGNORECASE,
        ),
    ),
    SecretPattern(
        "Ankr credential-bearing endpoint",
        re.compile(
            r"https?://rpc\.ankr\.com/[A-Za-z0-9_-]+/"
            r"(?!<|\$|\{)[A-Za-z0-9_-]{12,}",
            re.IGNORECASE,
        ),
    ),
    SecretPattern(
        "Snowflake password assignment",
        re.compile(
            r"(?im)^\s*(?:DBT_ENV_SECRET_)?SNOWFLAKE_PASSWORD\s*=\s*"
            r"(?!<|\$|\{)[^\s#]{8,}\s*$"
        ),
    ),
    SecretPattern(
        "private key passphrase assignment",
        re.compile(
            r"(?im)^\s*SNOWFLAKE_PRIVATE_KEY_PASSPHRASE\s*=\s*"
            r"(?!<|\$|\{)[^\s#]{8,}\s*$|"
            r"^\s*private_key_passphrase\s*=\s*[\"']"
            r"(?!<|\$|\{|not-used-in-this-test)[^\"']{8,}[\"']\s*$"
        ),
    ),
)

PROHIBITED_PATHS = {
    Path(".streamlit/secrets.toml"),
}


def repository_files(*, include_untracked: bool) -> tuple[Path, ...]:
    """Return Git-controlled inputs without traversing ignored files."""
    command = ["git", "ls-files", "-z", "--cached"]
    if include_untracked:
        command.extend(("--others", "--exclude-standard"))
    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
    )
    return tuple(
        Path(value.decode())
        for value in result.stdout.split(b"\0")
        if value
    )


def scan_paths(paths: Iterable[Path], *, root: Path = PROJECT_ROOT) -> list[Finding]:
    """Scan repository-relative text files and return value-free findings."""
    findings: list[Finding] = []
    for relative_path in sorted(set(paths)):
        if relative_path in PROHIBITED_PATHS:
            findings.append(Finding(relative_path, 1, "tracked Streamlit secrets"))
            continue

        path = root / relative_path
        if not path.is_file() or path.stat().st_size > MAX_SCANNED_BYTES:
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for pattern in SECRET_PATTERNS:
            for match in pattern.expression.finditer(content):
                line_number = content.count("\n", 0, match.start()) + 1
                findings.append(
                    Finding(relative_path, line_number, pattern.name)
                )
    return findings


def format_findings(findings: Sequence[Finding]) -> str:
    """Format only location and signature name, never secret content."""
    return "\n".join(
        f"{finding.path}:{finding.line_number}: {finding.pattern_name}"
        for finding in findings
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the repository secret-safety gate."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--all-files",
        action="store_true",
        help="include untracked, non-ignored files in addition to tracked files",
    )
    args = parser.parse_args(argv)
    findings = scan_paths(
        repository_files(include_untracked=args.all_files),
    )
    if findings:
        print("Potential committed secrets detected:")
        print(format_findings(findings))
        return 1
    print("Secret safety check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
