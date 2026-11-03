# PostgreSQL Data-Inspector TUI Pilot Decision (Requirement 83)

- **Plugin / profile type**: `voidb-plugin-postgres` / PostgreSQL profiles
  (host, port, username, password, database, optional `ssl_mode`).
- **Selected state**: `Accepted pilot` (following the retained Redis pilot as
  the canonical data-TUI implementation).
- **Command**: `voidb-plugin-postgres tui` (plugin-owned, CLI-launched), also
  routed as `postgres tui` through `voidb-cli`.
- **Scope**: schema-aware data triage. This is new plugin-owned UI work built
  on the existing `PostgresService` channel contract; it adds no backend
  operations and does not depend on removed router-hosted factories, shared
  legacy data widgets, or shell-owned plugin UI state.

This record satisfies the Requirement 83 data inspector TUI acceptance gate in
`voidb/docs/data-inspector-cli-first-decisions.md`, which previously recorded
PostgreSQL as `CLI-only` ("PostgreSQL lacks a plugin-specific inspector UX
brief and retained fixture/transcript evidence that beats repeated CLI
commands"). That brief, fixture, and PTY evidence are produced here in the
same requirement, with the Redis pilot (`voidb-plugin-redis`,
`docs/data-inspector-tui-pilot-decision.md`) as the accepted family pattern.

## Mutation scope decision (recorded up front)

**Slice 1 ships READ-ONLY.** The service layer exposes
`CrudCommand::SaveChanges` (a batch grid save that couples pending cell edits
with row-deletion indices over a full page snapshot and requires PK columns)
and `CrudCommand::InsertRow`, but **no bounded single-row delete operation**.
Wiring a destructive pilot mutation through the batch grid-save surface would
need its own confirmation and redaction evidence, so per the
prefer-read-only-if-in-doubt rule this pilot stages no plans, binds no
mutation keys, and records every mutation workflow (row delete, cell edit,
insert) as `later candidate`. `--readonly` is still accepted as a launch flag
and reported in preflight; there is no mutation surface to block because none
exists. The CLI (`postgres query`, `voidb-cli invoke run postgres.exec …`)
remains the supported general path for writes.

## Named human workflow

**Schema-aware data triage on a live database.** An operator responds to a
data-quality or over-retention incident:

1. **Start**: the operator connects through an existing profile
   (`postgres tui --profile prod-analytics --purpose triage`). The TUI opens
   read-only on the tables view; the first viewport is the active schema's
   table list with name/type/row-estimate columns (from the service's
   information_schema + pg_class listing), not a full dump.
2. **Navigate schemas**: the operator presses `s`, types to filter the schema
   list (`ListSchemas`), and Enter switches (for example `public` →
   `analytics`); the table list refreshes (`ListTables`) with the new
   schema's tables.
3. **Page**: the operator moves to `orders` and presses Enter/i; the
   full-screen TABLE DATA VIEWER loads one bounded page (`LoadTableData` with
   `LIMIT 10/OFFSET 0` plus `COUNT(*)`) — page title shows
   `page 1 rows 1-10/23`.
4. **Inspect**: `/` narrows the loaded page live with a match count;
   Enter on a row opens the typed row-detail viewer (one line per column:
   `tags (text[]): {hardware,sensor}`, `attributes (jsonb): {…}`,
   `last_review (uuid): (null)`); `n`/`b` page forward/back through offsets
   with the previous offset kept in a history stack.
5. **Structure**: pressing `2` shows the selected table's columns (with
   nullable/PK/default markers), indexes (unique/type), and foreign keys
   (`LoadStructure`) without leaving the workflow.
6. **Success state**: without leaving the TUI, the operator has walked a
   table page-by-page, read the typed cells of suspicious rows, and confirmed
   the structural context. A failing read (permission denied on a finance
   table) shows a redacted target error in a non-modal banner and the
   operator keeps working.

**Why the CLI is materially worse here**: the same loop requires
`postgres tables -s <schema>` per schema, then one
`postgres query -s <schema> "SELECT * FROM <table> LIMIT 10 OFFSET n"`
invocation per page, plus `postgres describe` per structure question — each
a fresh process paying connection setup and re-deriving column names, types,
and NULL/JSON/array rendering from scratch, with no shared schema/cursor
context and shell quoting on every filter. The TUI keeps the active schema,
table selection, offset paging state, per-page filter, and typed row viewer
in one viewport with persistent per-view selection memory. Generic "browse
data" is not the claim; the claim is cursor-preserving read triage over
repeated page-inspect cycles on the same table set.

## Gate-by-gate evidence

| Gate | Evidence in this implementation |
|---|---|
| Named human workflow | Above; exercised end to end in fixture mode and by the PTY transcript (schema switch to `analytics`, `public.orders` paged 1→2→3 with a partial final page, viewer filter `pending` → 3 matches, row detail with typed columns, structure view, clean quit). |
| Capability-backed bounded reads | Reads are the existing service-layer operations only: `SchemaCommand::ListSchemas`, `SchemaCommand::ListTables { schema }`, `SchemaCommand::LoadStructure { schema, table }`, and `LoadCommand::LoadTableData { schema, table, page_size, offset }` (LIMIT/OFFSET + `COUNT(*)` bounded in `service/data_loader.rs`). The TUI adds no new backend operations; no Crud/Browser command is ever sent. |
| Pagination and large-result safety | One LIMIT/OFFSET page is rendered at a time (`page N rows X-Y/T` in the viewer title); `n` fetches `offset + PAGE_SIZE` (pages are **replaced**, not appended, so the viewport stays bounded; the previous offset is kept in a history stack for `b`); empty state (`audit_events`, 0 rows), partial page (final 3-row page of `orders`), and end-of-table state are all visible; row-detail bodies are bounded by `BoundedLines` at 2,000 lines / 2 MiB with dropped-line counters, and grid cells are capped at 32 chars with an ellipsis. |
| Redaction and error shape | Preflight (`--format json`) reports auth as `"auth": "password"|"none"` with `"secret_material": "redacted"` and no connection URL; `secret_leak_markers` scan covers `postgres_password_value`, `postgres_secret_data_value`, `plaintext_credential_value`, `raw_connection_url` and passes (`passed: true`, 0 markers) in `build_postgres_tui_evidence` output; banner/status text additionally passes through a defensive `redact_error_text` that rewrites any embedded `password=…` material (the connection factory already never logs the connection string, T-03-01) and truncates to 180 chars. Errors render in the non-modal banner and keep the family `target error:` category text carried by the service error contract (validation/auth/permission/transport/timeout/plugin/target categories come from the shared capability error contract; fixture and preflight evidence carry the `target error` marker family). |
| Mutation safety | **First viewport is read-only** — the tables browser binds no mutation keys. **Slice 1 ships READ-ONLY**: no staged plans, no delete/edit/insert surface; the strongest form of the gate. `--readonly` is accepted, reported in the header and preflight, and asserted by tests. Mutation policy is recorded in `evidence["mutation_policy"]` (`slice: "read_only"`) and `evidence["mutation_transcript"]` with the rationale and the deferred `later candidate` workflows. |
| Fixture and target-error evidence | `fixtures/postgres_tui_operations.json` covers two schemas (`public`, `analytics`) with base tables and a view, 47 rows including NULL, jsonb, `text[]` array, boolean, numeric, timestamp, and uuid cells, a truncated `orders` table (23 rows over 3 pages), an empty `audit_events` table, and a deliberately erroring `public.payments` table (`target error: permission denied for table public.payments (fixture)`). Startup carries a `target error: permission denied for schema finance …` banner; the PTY proves the TUI stays fully responsive across it (all 34 assertions pass with the banner present on the first frame). |
| PTY lifecycle evidence | Real PTY run (`pty.fork()` + `TIOCSWINSZ` `HHHH` at 36x130, winsize set pre-exec): 34/34 assertions across 3 consecutive runs. First frame markers (`PostgreSQL Data TUI`, `1:tables`, `2:structure`, `fixture-postgres-ops`, fixture tables, hint footer, target-error banner); quit exits 0; resize to 24x100 keeps content visible and reports `resized postgres view to 100x24`; raw-mode cleanup is delegated to `ratatui::restore()` after the loop (raw input is not used: `raw_input = false`). |
| No legacy dependency | The command lives in the plugin (`src/tui.rs`), is launched from the plugin CLI (`postgres tui`), consumes only `PostgresService` channel events, and contains no reference to removed router-hosted data-inspector adapters or shared legacy widgets. The agent-session/agent_live machinery is untouched. |

## Interaction surface

- Views and hotkeys: `1` tables browser, `2` structure (Tab cycles); pending
  schema/table/structure fetches show the family `⟳` marker in the header
  tabs. **Hotkey 3 (server view) is deferred**: the postgres service layer
  has no server-info command (Redis's `ServerCommand` has no postgres
  counterpart), so there is nothing bounded to render — recorded as
  `later candidate` in `evidence["deferred"]`.
- Tables browser: name/type/rows(estimate) columns; `s` opens the schema
  prompt (type-to-filter, Up/Down, Enter switches); `/` filters the table
  list client-side with a live match count; `r` refreshes.
- Enter/i on a table opens the full-screen TABLE DATA VIEWER: one bounded
  LIMIT/OFFSET page at a time with `j/k`/PgUp/PgDn scroll, `n` next page /
  `b` back (replace semantics, offset history), `/` live page filter with
  match count, and Enter on a row opening the typed row-detail viewer (one
  line per column; NULL renders `(null)`, jsonb renders the stored document,
  arrays render PG literals).
- Structure view: columns list (nullable/PK/default) plus an indexes and
  foreign-keys panel for the selected table.
- Confirm-quit fires while any fetch is active; the error banner is
  non-modal (any key acknowledges while the key keeps its meaning).
- No mutation keys are bound anywhere (read-only pilot).

## Deferred inside the accepted pilot

- **Server view (hotkey 3)**: `later candidate` — no server-info command
  exists in the service layer. If a bounded `ServerCommand` (activity,
  locks, settings) is added later, it can slot into hotkey 3 with its own
  fixture and PTY evidence.
- **Mutation workflows (row delete, cell edit, row insert)**:
  `later candidate` — see the mutation scope decision above. A future slice
  needs a bounded row-delete service operation (or a dry-run/preview shape
  over `SaveChanges`), typed table-name confirmation, rescan evidence, and
  its own PTY destructive-confirmation transcript.
- **Arbitrary SQL console**: intentionally not shipped. `CrudCommand::
  ExecuteQuery` executes arbitrary statements (including writes), so a TUI
  console over it would violate the mutation-safety gate; the CLI remains
  the supported path for ad-hoc SQL.
- Until those ship, docs and release copy must keep pointing to the CLI
  (`postgres query`, `postgres tables`, `postgres describe`, and
  `voidb-cli invoke run postgres.<capability> …`) as the supported general
  path for writes and arbitrary queries; `plugin.toml` declares
  `[ui] tui = true, raw_input = false` for this bounded pilot only.

## Validation commands and evidence

All run in `voidb-plugin-postgres` with `VOIDB_DISABLE_KEYRING=1` (no OS
keyring touch):

```bash
VOIDB_DISABLE_KEYRING=1 cargo test            # 50 passed (33 pre-existing + 17 TUI)
cargo clippy --all-targets -- -D warnings     # 0 warnings
cargo fmt --all && cargo fmt --all -- --check # clean (CI enforces this)
cargo run --example export_contract -- --check # manifest/contract drift green
```

TUI interaction tests (in `src/tui.rs#[cfg(test)]`): fixture evidence +
secret-leak scan, preflight redaction, error-text password redaction,
non-modal startup banner, tab hotkeys with structure load, schema prompt
switch both ways, table filter match count, data-viewer paging with offset
history and partial/end states, viewer filter match count, row-detail typed
cells (array/jsonb/uuid/NULL), structure view columns/indexes/FKs plus
erroring-table banner, empty table state, erroring-table responsiveness,
readonly launch with no mutation surface, confirm-quit, page-key movement,
and fixture-cell conversion parity with the service value shapes.

Preflight and evidence:

```bash
target/debug/voidb-plugin-postgres tui --fixture fixtures/postgres_tui_operations.json --format json
target/debug/voidb-plugin-postgres tui --fixture fixtures/postgres_tui_operations.json --evidence evidence.json
```

Evidence JSON: `kind: postgres_tui_fixture_evidence`, quality gate
`retained_tui_quality_gate("postgres", ["fixture-postgres-ops",
"fixture-postgres-local"], ["target error"], 80, 24, 10_000)`, 19 coverage
entries including the new interactions (`schema_switch_prompt`,
`table_paging_offset_history`, `viewer_filter_match_count`,
`row_detail_viewer`, `typed_cell_rendering`, `readonly_no_mutation_surface`,
…), `secret_leak_scan.passed = true`.

PTY lifecycle record (macOS, Python 3.9 stdlib only, `struct "HHHH"`, 36 rows
x 130 cols): 34/34 assertions, 3 consecutive clean runs, exit code 0 each
run; script retained out-of-repo at `/tmp/postgres_tui_pty_smoke.py` (run
`python3 -I /tmp/postgres_tui_pty_smoke.py`). Covered: first frame
(non-blank, marker-checked), banner acknowledgement, full-screen data viewer
with offset paging forward/back and partial final page, viewer filter with
match count and clear, typed row detail, structure view with indexes/FKs,
schema prompt switch both ways, erroring-table banner with the viewer
refusing to open, live resize with content retention, normal `q` quit with
clean exit 0 and terminal restore. One PTY finding was fixed during the
loop: the data-viewer popup was reduced to the family 84% height so status
messages below the overlay stay visible (paging/filter status assertions
were occluded at 88%). Local Python is 3.9 (no tomllib), so the python
provenance tests cannot run on this machine (CI runs 3.11+); this does not
affect the PTY evidence, which uses only stdlib.

## Migration note

This pilot is new plugin-owned UI work. It does not restore, wrap, or depend
on the router-hosted PostgreSQL data-inspector adapters removed in
Requirement 77, and it does not reintroduce shared legacy data widgets or
shell-owned plugin UI state. The default TUI guidance for PostgreSQL
profiles remains the CLI/capability path; `postgres tui` is a bounded,
profile-scoped, READ-ONLY triage pilot whose supported mutations are none.
Release docs must not advertise the deferred server view, mutation
workflows, or a SQL console as available in the TUI.
