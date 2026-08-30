# Learnings

Patterns and pitfalls discovered while working on this codebase. Read before changing
identifier handling or front-matter generation.

## Gmail message IDs are time-ordered — never truncate them

Gmail's `messages.id` is a 16-char hex string whose high bits are effectively a delivery
timestamp. It looks like a hash, so a prefix looks safe. It is not.

Truncating concentrates collisions among emails that arrive close together, which is the
common case for newsletters. Empirically, over 17,007 live messages:

| Prefix length | Colliding groups |
|---|---|
| 8 | **6** |
| 10 / 12 / 14 / 16 | 0 |

A uniformly-random 32-bit prefix predicts ~0.03 collisions at that corpus size. The real
count was ~180× higher, and every colliding pair arrived within ~2 hours of the other —
most within 3 minutes. Birthday-bound intuition does not apply to sequential IDs.

**Rule:** use the full `message_id` everywhere — filenames, directory names, database keys,
front matter. It is the PRIMARY KEY of `messages`, so uniqueness is free.

**Why it mattered:** `MarkdownWriter` used `message_id[:8]`. Downstream, `ingestor-tools`
read that prefix back off the filename and located raw bodies with `startswith`, so both
members of a colliding pair landed in one directory. `newsletters-web` then keyed one
record per directory and picked `sorted(glob("*.html"))[0]` — publishing one newsletter's
body under another newsletter's headline for 5 emails, and dropping 1 entirely.

## Escape backslashes before quotes in YAML double-quoted scalars

`converter._escape_yaml` must do `.replace("\\", "\\\\")` *then* `.replace('"', '\\"')`.

Inside a double-quoted YAML scalar, `\\` is a literal backslash. Escaping only quotes
leaves a raw `\` that swallows the following escape and terminates the string early:

```python
raw = '"\\"Mr. and Mrs. Psmith’s Bookshelf\\"" <thepsmiths@substack.com>'
yaml.safe_load(f'from: "{raw.replace(chr(34), chr(92) + chr(34))}"')   # ParserError
```

Real Gmail `From:` headers contain backslashes — RFC 2822 quoted-strings escape inner
quotes that way. 16 live files were unparseable, and because `ingestor-tools` calls
`yaml.safe_load` and returns `None` on failure, those emails were skipped silently and
never appeared downstream. The failure mode is invisible without a real YAML parse.

**Rule:** never hand-roll per-field escaping. Route every front-matter value through
`_escape_yaml`, and assert round-trips with an actual parser in tests, not string matching.

## Quote identifiers in generated YAML

~30 live Gmail IDs are all digits (e.g. `1637675546614607`). Unquoted, `yaml.safe_load`
returns an `int`, and every downstream `id == message_id` comparison fails for exactly
those messages — a silent, data-dependent bug. Emit `id: "{message_id}"`.

## `output/` is not in this repo

`.env` points `GMAIL_OUTPUT_MARKDOWN_DIR` / `GMAIL_OUTPUT_RAW_DIR` at `../output/...`,
shared with the sibling `ingestor-tools`, `newsletters`, and `newsletters-web` projects.
The in-repo `output/` directory is an empty placeholder. `messages.markdown_path` stores
these paths **relative**, so they only resolve when the CWD is the repo root — anchor to
`Path(__file__).parent.parent` in scripts rather than trusting the caller's CWD.

## The DB is a complete rename oracle

`messages` holds the full `message_id` as PRIMARY KEY alongside `markdown_path`,
`raw_text_path`, and `raw_html_path`. Any renaming migration is therefore derivable
entirely from SQLite — no Gmail API calls and no re-conversion. `scripts/migrate_full_message_ids.py`
is the worked example: back up the DB via `sqlite3.Connection.backup()` (WAL-safe, unlike
a file copy), journal the planned renames before touching anything, then rename → update
DB → rewrite files, with every phase idempotent.

## Expect drift between the DB and disk

3 rows are marked `converted` but their markdown *and* raw files are gone — deleted outside
the pipeline. `convert-pending` will never notice, because it only looks at rows still in
`fetched` state. Any script walking `markdown_path` must tolerate missing files rather than
assuming the DB describes reality.
