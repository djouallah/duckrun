"""
Inject a generated Markdown card into README.md between marker comments, in place.

    python tests/tools/inject_readme.py README.md card.md CONFORMANCE

replaces whatever sits between `<!-- CONFORMANCE:START -->` and `<!-- CONFORMANCE:END -->`
with the contents of card.md. The markers themselves are preserved so the next run can find
them again. Exit status is 0 whether or not the file changed; the workflow decides what to do
with the diff.
"""
import sys


def main(readme_path: str, card_path: str, marker: str) -> int:
    start = f"<!-- {marker}:START -->"
    end = f"<!-- {marker}:END -->"

    with open(readme_path, encoding="utf-8") as f:
        readme = f.read()
    with open(card_path, encoding="utf-8") as f:
        card = f.read().strip("\n")

    i = readme.find(start)
    j = readme.find(end)
    if i == -1 or j == -1 or j < i:
        sys.stderr.write(f"markers {start!r}/{end!r} not found (or out of order) in {readme_path}\n")
        return 1

    new = readme[:i] + start + "\n\n" + card + "\n\n" + readme[j:]
    if new != readme:
        with open(readme_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(new)
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.stderr.write("usage: inject_readme.py <readme> <card.md> <MARKER>\n")
        sys.exit(2)
    sys.exit(main(sys.argv[1], sys.argv[2], sys.argv[3]))
