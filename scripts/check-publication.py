"""Check staged publication files without printing matched secret values."""
from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTROLLERS = {"AGENTS.md", "GOAL.md", "STATE.md", "LOOP.md", "VERIFY.md", "CONTEXT_INDEX.md", "KICKOFF_PROMPT.md", "PACKAGE_MANIFEST.json"}
SECRET = re.compile(rb"(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|sk-(?:proj-|ant-)?[A-Za-z0-9_-]{24,}|AKIA[A-Z0-9]{16}|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|li_at\s*[=:]\s*['\"]?[A-Za-z0-9_-]{24,})")


def check(root=ROOT):
    paths = subprocess.check_output(["git", "diff", "--cached", "--name-only", "--diff-filter=ACM", "-z"], cwd=root).decode().split("\0")
    findings = []
    for name in filter(None, paths):
        parts = Path(name).parts
        if (Path(name).name == "AGENTS.md" or name in CONTROLLERS or
                any(part in {".local", ".venv", "node_modules"} for part in parts) or
                name.startswith(("docs/results/", "docs/work/")) or
                Path(name).name.startswith(".env") and Path(name).name != ".env.example"):
            findings.append({"path": name, "kind": "private_or_controller_path"})
        content = subprocess.check_output(["git", "show", f":{name}"], cwd=root)
        for match in SECRET.finditer(content):
            findings.append({"path": name, "kind": "credential_literal", "line": content[:match.start()].count(b"\n") + 1})
    print(json.dumps({"files_checked": len(list(filter(None, paths))), "findings": findings}, indent=2))
    return bool(findings)


if __name__ == "__main__":
    raise SystemExit(check(Path.cwd()))
