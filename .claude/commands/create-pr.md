# Create PR

Lint, format, test, commit any remaining changes, push, and open a pull request for the current module branch.

## Steps

1. **Identify the current module** from `git branch --show-current`. Parse the module number and name from the branch (e.g. `module/m3-osha-ingestion` → M3, issue #4).

2. **Lint and format** — run both ruff commands on all changed files:
   ```bash
   git diff --name-only HEAD | grep '\.py$'   # list changed Python files
   .venv/bin/ruff check <files> --fix
   .venv/bin/ruff format <files>
   ```

3. **Run tests** for the module:
   ```bash
   PYTHONPATH=. .venv/bin/python -m pytest tests/unit/test_<module>.py -v --no-cov
   ```
   If any tests fail, fix them before continuing. Do not create a PR with failing tests.

4. **Stage and commit** any remaining unstaged changes:
   ```bash
   git status   # check what's unstaged
   git add <relevant files>
   git commit -m "M<N>: <Title> — <brief summary>

   Closes #<issue>

   Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
   ```

5. **Push** the branch:
   ```bash
   git push -u origin <branch-name>
   ```

6. **Create the PR** using `gh pr create`:
   ```bash
   gh pr create --base main --title "M<N>: <Title>" --body "$(cat <<'EOF'
   ## Summary
   - <bullet points covering what was implemented>

   ## Key functions
   - <list the public functions delivered>

   ## Test plan
   - [ ] <test class 1>
   - [ ] <test class 2>
   - [ ] Coverage ≥ 80%

   🤖 Generated with [Claude Code](https://claude.com/claude-code)
   EOF
   )"
   ```

7. **Print the PR URL** so the user can review it.
