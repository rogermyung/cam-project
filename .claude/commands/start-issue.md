# Start Issue

Start work on a CAM module issue by reading its spec from PLAN.md, creating the branch, and commenting on the GitHub issue.

## Usage
`/start-issue <module-number>`

Example: `/start-issue 3`

## Steps

Given the module number in the arguments (e.g. `3` for M3), do the following:

1. **Read PLAN.md** — find the `## M<N>` section. Extract the Goal, Data Source, Key Functions, Schema Mapping, Test Requirements, and Acceptance Criteria.

2. **Check dependencies** — use GitHub issues as the source of truth for module status. The issue number for M<N> is N+1 (M0=issue #1, M3=issue #4, etc.). Run:
   ```bash
   gh issue list --limit 20 --state all
   ```
   A module is **complete** if its GitHub issue is CLOSED. Cross-reference the dependency list from the Module Index table in PLAN.md. If any dependency issue is still OPEN, stop and tell the user which dependencies are missing.

   After confirming all deps are met, **sync CLAUDE.md** to reflect reality: for each module whose issue is CLOSED but still shows ⬜ TODO or 🔄 In Progress in the CLAUDE.md Module Status table, update it to ✅ Complete and fill in the PR number (run `gh pr list --state merged --limit 20 --json number,title` to find the right PR).

3. **Check current branch** — run `git branch --show-current`. If not already on the correct branch, create it:
   ```bash
   git checkout main && git pull
   git checkout -b module/m<N>-<short-name>
   ```
   The short-name comes from the module title (e.g. M3 — OSHA Ingestion → `osha-ingestion`).

4. **Comment on the GitHub issue** — the issue number matches the module number + 1 (M3 = issue #4, M0 = issue #1, etc.). Run:
   ```bash
   gh issue edit <issue-number>  --add-assignee @me
   gh issue edit <issue-number> --add-label "in-progress"
   gh issue comment <issue-number> --body "Starting implementation of M<N> — <Title>. Branch: \`module/m<N>-<short-name>\`. Will implement <key functions> per PLAN.md spec."
   ```

5. **Mark in-progress in CLAUDE.md** — update the Module Status table row for this module from `⬜ TODO` to `🔄 In Progress` and record the issue number:
   ```
   | M<N> — <Title> | #<issue-number> | 🔄 In Progress | — |
   ```
   This is the record used to check status later: a `🔄 In Progress` row with a known issue number means work has started but the issue is not yet closed.

6. **Summarise the spec** — print a concise summary of what needs to be built: key functions, schema mapping, test requirements, and acceptance criteria.

7. **Begin implementation** — start with the fixture files (in `tests/fixtures/<source>/`), then implement the module in `cam/ingestion/<source>.py`, then write `tests/unit/test_<source>.py`. Follow all conventions in CLAUDE.md.
