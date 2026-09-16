# Delivery

Every change reaches `main` through a short-lived branch and a squash-merged PR. `main` is protected: the `check` CI job (`.github/workflows/ci.yml`) must pass, and direct pushes are rejected.

1. **Branch** off an up-to-date `main`, one branch per issue (e.g. `fix/36-null-key-message-wording`). The issue carries `ready-for-agent`.
2. **Commit** test-first (see `AGENTS.md`). Run the same checks CI runs before pushing: `gofmt -l .`, `go mod tidy -diff`, `go vet ./...`, `go test -race ./...`.
3. **Push** with gh as the git credential helper; plain git has no GitHub credentials here:
   `git -c credential.helper= -c credential.helper='!gh auth git-credential' push -u origin <branch>`
4. **Open the PR** with `gh pr create`. The title is a conventional commit subject without an issue number: the squash commit appends the PR number itself. The body starts with `Closes #N` and states what was verified.
5. **Merge** once `check` is green: `gh pr merge <n> --squash --delete-branch`. If GitHub reports the branch out of date, run `gh pr update-branch <n>` and wait for `check` again.
6. **Finish** on `main`: switch back, pull, and confirm the issue is closed and the branch is gone locally and on GitHub.
