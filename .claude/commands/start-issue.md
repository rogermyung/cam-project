# Start Issue

Start work on a CAM module issue by reading its spec from PLAN.md, creating the branch, and commenting on the GitHub issue.

## Usage
`/start-issue <module-number>`

Example: `/start-issue 3`

## Steps

Given the module number in the arguments (e.g. `3` for M3), do the following:

1. **Read PLAN.md** — find the `## M<N>` section. Extract the Goal, Data Source, Key Functions, Schema Mapping, Test Requirements, and Acceptance Criteria.

2. **Check dependencies** — look at the Module Index table in PLAN.md. Confirm all listed dependencies for this module are marked complete (✅). If any dependency is not complete, stop and tell the user which dependencies are missing.

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

5. **Summarise the spec** — print a concise summary of what needs to be built: key functions, schema mapping, test requirements, and acceptance criteria.

6. **Begin implementation** — start with the fixture files (in `tests/fixtures/<source>/`), then implement the module in `cam/ingestion/<source>.py`, then write `tests/unit/test_<source>.py`. Follow all conventions in CLAUDE.md.
