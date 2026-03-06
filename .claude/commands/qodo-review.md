# Qodo Review

Fetch and display all action-required review comments left by qodo-merge-pro[bot] on the current branch's open PR.

## Steps

1. **Find the PR number** for the current branch:
   ```bash
   gh pr view --json number,url,title
   ```

2. **Fetch all PR review comments** from the qodo bot:
   ```bash
   gh api repos/{owner}/{repo}/pulls/<pr-number>/reviews \
     --jq '.[] | select(.user.login | test("qodo"; "i")) | {id: .id, state: .state, submitted_at: .submitted_at}'
   ```
   Then fetch the individual review comments:
   ```bash
   gh api repos/{owner}/{repo}/pulls/<pr-number>/comments \
     --jq '.[] | select(.user.login | test("qodo"; "i")) | {path: .path, line: .original_line, body: .body}'
   ```
   Also fetch issue-level comments (top-level PR comments):
   ```bash
   gh api repos/{owner}/{repo}/issues/<pr-number>/comments \
     --jq '.[] | select(.user.login | test("qodo"; "i")) | .body'
   ```

3. **Parse and display** the results grouped by type:
   - **Action-required inline comments**: show file path, line number, and comment body
   - **General review comments**: show the full body

4. **Summarise** how many action-required items were found and which files they affect.

5. **Ask the user** whether to resolve each comment automatically or skip.

Note: replace `{owner}/{repo}` with the actual values from `gh repo view --json nameWithOwner --jq .nameWithOwner`.
