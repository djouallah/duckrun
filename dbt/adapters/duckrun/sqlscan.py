"""Shared quote-aware SQL scanning primitives.

The DML router (``delta_dml``) and the write plugin (``delta_plugin``) both need to inspect or
rewrite raw SQL *outside* string literals, quoted identifiers, dollar-quoted bodies, and SQL
comments — never a regex over the raw text, which fires inside literals and corrupts them. These primitives are the one
shared implementation; ``delta_dml`` re-exports ``_dollar_quote_end`` so its existing scanners and
``session.py`` keep importing it from there.
"""
from __future__ import annotations

import re
from typing import Callable, Optional, Tuple

# PostgreSQL/DuckDB dollar-quoting: ``$tag$ … $tag$`` (the tag is optional, so ``$$ … $$`` too). The
# body is OPAQUE — no escaping — so a ``;``, a quote, or a comment marker inside it is literal text,
# NOT a structural token. Every scanner must skip a dollar-quoted run wholesale.
_DOLLAR_OPEN = re.compile(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$")

# A bare SQL identifier token (unquoted).
_IDENT = re.compile(r"[A-Za-z_]\w*")


def _dollar_quote_end(s: str, i: int) -> Optional[int]:
    """If a dollar-quote opens at ``s[i]`` (``$$`` or ``$tag$``), return the index just past its
    matching close; else None (not a dollar-quote, or no close found — leave it to normal scanning)."""
    m = _DOLLAR_OPEN.match(s, i)
    if not m:
        return None
    delim = m.group(0)
    close = s.find(delim, m.end())
    return close + len(delim) if close != -1 else None


def _rewrite_outside_quotes(s: str, cb: Callable[[str, int], Optional[Tuple[str, int]]]) -> str:
    """Scan ``s`` copying it verbatim, skipping ``'…'`` / ``"…"``, ``$tag$…$tag$`` runs, ``--`` line
    comments, and ``/* … */`` block comments (nested, as Postgres/DuckDB nest them). At each
    position that begins an identifier boundary (the previous char is not part of an identifier and
    not a ``.`` qualifier dot), call ``cb(s, i)``: if it returns ``(replacement, new_i)`` the span
    ``s[i:new_i]`` is replaced by ``replacement`` and scanning resumes at ``new_i``; if it returns
    None the char is copied. The single rewrite core behind the identifier helpers below."""
    out, i, n, quote = [], 0, len(s), None
    while i < n:
        ch = s[i]
        if quote:
            out.append(ch)
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch in ("'", '"'):
            quote = ch
            out.append(ch)
            i += 1
            continue
        if ch == "-" and s.startswith("--", i):
            nl = s.find("\n", i)
            end = n if nl == -1 else nl + 1
            out.append(s[i:end])  # comment text is not SQL — copy verbatim, rewrite nothing inside
            i = end
            continue
        if ch == "/" and s.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if s.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif s.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            out.append(s[i:j])
            i = j
            continue
        if ch == "$":
            de = _dollar_quote_end(s, i)
            if de is not None:
                out.append(s[i:de])  # opaque dollar-quoted body — copy verbatim, scan nothing inside
                i = de
                continue
        # An identifier starts here only at a boundary: not glued to a preceding identifier char and
        # not immediately after a '.' (which would make it the RHS of an existing qualifier).
        if not (i and (s[i - 1].isalnum() or s[i - 1] == "_" or s[i - 1] == ".")):
            res = cb(s, i)
            if res is not None:
                rep, ni = res
                out.append(rep)
                i = ni
                continue
        out.append(ch)
        i += 1
    return "".join(out)


def qualify_identifiers(expr: str, columns, prefix: str = "target") -> str:
    """Prefix each bare reference to a known ``columns`` name with ``<prefix>.``, leaving string
    literals, quoted identifiers, already-qualified references, and non-column tokens (functions like
    ``current_date``) untouched. The quote-aware replacement for a regex that would also rewrite a
    column name appearing *inside* a string literal."""
    if not expr or not columns:
        return expr
    cols = {str(c).lower() for c in columns}

    def cb(s: str, i: int):
        m = _IDENT.match(s, i)
        if not m:
            return None
        word = m.group(0)
        end = m.end()
        # A token immediately followed by '.' is itself a qualifier/alias, not a bare column.
        j = end
        while j < len(s) and s[j] in " \t":
            j += 1
        if j < len(s) and s[j] == ".":
            return None
        if word.lower() in cols:
            return prefix + "." + word, end
        return None

    return _rewrite_outside_quotes(expr, cb)


def strip_qualifier(expr: str, alias: str) -> str:
    """Remove every ``<alias>.`` qualifier (case-insensitive) that sits outside quotes — e.g. drop
    dbt's ``DBT_INTERNAL_DEST.`` so a predicate evaluates against the target table directly. The
    quote-aware replacement for ``re.sub(r'\\bALIAS\\.', '', expr)``, which would also fire inside a
    string literal."""
    if not expr:
        return expr
    al = alias.lower()

    def cb(s: str, i: int):
        m = _IDENT.match(s, i)
        if not m or m.group(0).lower() != al:
            return None
        end = m.end()
        if end < len(s) and s[end] == ".":  # only strip when it is actually a qualifier
            return "", end + 1
        return None

    return _rewrite_outside_quotes(expr, cb)
