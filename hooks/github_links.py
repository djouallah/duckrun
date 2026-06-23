"""MkDocs hook: rewrite repo-relative doc links to GitHub blob URLs.

The docs under docs/ link to source files outside the docs tree with relative
paths (e.g. ../tests/..., ../.github/workflows/cores.yml). The previous custom
generator rewrote those to github.com/.../blob/main/... so they resolve on the
published site; MkDocs would otherwise leave them as dangling relative links.
This restores that behavior without editing every markdown file.

Sibling .md links and images stay untouched — MkDocs resolves those itself.
"""
from __future__ import annotations

import posixpath
import re

REPO_BLOB = "https://github.com/djouallah/duckrun/blob/main"

# Markdown inline links whose target steps out of docs/ with a leading "../".
_LINK = re.compile(r"(\]\()(\.\./[^)\s]+)(\))")


def _rewrite(target: str) -> str:
    path, _, anchor = target.partition("#")
    # The docs live in docs/, so "../tests/x" resolves to the repo-root "tests/x".
    repo_rel = posixpath.normpath(posixpath.join("docs", path))
    return f"{REPO_BLOB}/{repo_rel}" + (f"#{anchor}" if anchor else "")


def on_page_markdown(markdown: str, **_kwargs) -> str:
    return _LINK.sub(lambda m: m.group(1) + _rewrite(m.group(2)) + m.group(3), markdown)
