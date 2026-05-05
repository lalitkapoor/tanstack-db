---
name: notion-fork-release
description: Cut a release on the Notion fork of tanstack-db (lalitkapoor/tanstack-db, branch `notion`). Use when the user asks to "cut a release", "release notion.N", "ship the notion fork", or similar. Bumps 7 package.json files with a `-notion.N` prerelease suffix, commits, tags `notion.N`, packs `.tgz` artifacts to `_artifacts/release-notion.N/`, and creates a GitHub release with the artifacts attached.
---

# Notion fork release

The Notion fork (`origin/notion` on `lalitkapoor/tanstack-db`) tracks `upstream/main` and carries Notion-specific patches on top. Releases are cut as bundled GitHub releases with `.tgz` artifacts that the Notion app installs from release URLs.

## Versioning scheme (don't re-litigate)

- **Per-package version**: `<upstream-base>-notion.<N>` (e.g. `0.6.5-notion.1`). Semver-valid prerelease — npm `^` ranges exclude prereleases by default, so upstream consumers won't accidentally pull the fork.
- **Counter `N`**: monotonic across all Notion releases. Don't reset when upstream bumps. `notion.1`, `notion.2`, ... globally.
- **Tag**: `notion.N` (just the counter, no upstream version). Notion identity comes first; tag list reads cleanly. Upstream base lives in tgz filenames + release title.
- **Artifact dir**: `_artifacts/release-notion.N/` (gitignored).
- **Packs all 7 packages** even if not all changed — keeps the cross-dep graph internally consistent (e.g. `react-db@0.1.83` depending on upstream `@tanstack/db@^0.6.5` would skip the prerelease fork).

## The 7 packages

These are the packages the Notion app consumes. Always pack all 7 at the same `-notion.N`:

| Package | Workspace path |
|---|---|
| `@tanstack/db` | `packages/db` |
| `@tanstack/react-db` | `packages/react-db` |
| `@tanstack/query-db-collection` | `packages/query-db-collection` |
| `@tanstack/offline-transactions` | `packages/offline-transactions` |
| `@tanstack/browser-db-sqlite-persistence` | `packages/browser-db-sqlite-persistence` |
| `@tanstack/db-sqlite-persistence-core` | `packages/db-sqlite-persistence-core` |
| `@tanstack/electron-db-sqlite-persistence` | `packages/electron-db-sqlite-persistence` |

External deps on `@tanstack/db-ivm`, `@tanstack/pacer-lite`, `@tanstack/query-core` are pulled from the public npm registry and **stay at upstream versions** — don't fork them.

## Before starting

Confirm with the user:
1. **Counter `N`** — last release tag is `notion.<previous>`; default to `<previous>+1`. Check via `gh release list --repo lalitkapoor/tanstack-db` or `git tag -l 'notion.*' | sort -V | tail -1`.
2. **What's already on `notion` branch** — assume the user has already squash-merged the patches they want included, with both the PR number and source branch name in each squash commit message. Run `git log upstream/main..notion --oneline` to confirm.
3. **Upstream base version** — read `packages/db/package.json` on `notion` branch. That's the `@tanstack/db` upstream base.

## Merging PR branches into `notion`

When a PR branch needs to be included in the Notion fork release, squash it into
`notion` instead of creating a merge commit. Include both the PR number and
source branch name in the squash commit message:

```bash
git switch notion
git merge --squash <pr-branch>
git commit -m "<commit subject> (#<PR number>, branch: <pr-branch>)"
```

The PR number and branch name must be in the commit message so release notes and
later branch audits can map every Notion fork patch back to the PR and exact
source branch it came from.

## Steps

### 1. Verify branch state

```bash
git fetch upstream main && git fetch origin
git switch notion
git log upstream/main..notion --oneline   # expect: the patches to ship
git status --short                          # expect: clean (untracked _artifacts/ ok)
```

### 2. Bump 7 package.json versions

For each of the 7 packages, change `"version": "<upstream>"` → `"version": "<upstream>-notion.N"`. Use `Edit` with each package's exact `name` + `version` lines for safe targeting. Don't touch `workspace:*` deps — `pnpm pack` substitutes them automatically.

Current upstream bases (verify before bumping in case upstream advanced):

| Package | Upstream base (as of notion.1) |
|---|---|
| `@tanstack/db` | `0.6.5` |
| `@tanstack/react-db` | `0.1.83` |
| `@tanstack/query-db-collection` | `1.0.36` |
| `@tanstack/offline-transactions` | `1.0.30` |
| `@tanstack/browser-db-sqlite-persistence` | `0.1.9` |
| `@tanstack/db-sqlite-persistence-core` | `0.1.9` |
| `@tanstack/electron-db-sqlite-persistence` | `0.1.9` |

### 3. Commit and tag

```bash
git add packages/db/package.json packages/react-db/package.json \
        packages/query-db-collection/package.json \
        packages/offline-transactions/package.json \
        packages/browser-db-sqlite-persistence/package.json \
        packages/db-sqlite-persistence-core/package.json \
        packages/electron-db-sqlite-persistence/package.json

git commit -m "chore(release): cut notion.N" \
           -m "<list of bumped versions and included PRs>"

git tag -a notion.N -m "notion.N (based on upstream @tanstack/db@<base>)"
git push origin notion
git push origin notion.N
```

### 4. Pack the 7 packages

```bash
mkdir -p _artifacts/release-notion.N

for p in db react-db query-db-collection offline-transactions \
         browser-db-sqlite-persistence db-sqlite-persistence-core \
         electron-db-sqlite-persistence; do
  pnpm --filter "@tanstack/$p" pack \
       --pack-destination "$PWD/_artifacts/release-notion.N"
done
```

Verify `workspace:*` substitution succeeded:

```bash
for tgz in _artifacts/release-notion.N/*.tgz; do
  echo "=== $(basename $tgz) ==="
  tar -xzOf "$tgz" package/package.json | python3 -c "
import json, sys
p = json.load(sys.stdin)
print(f\"  name: {p['name']}  version: {p['version']}\")
for key in ['dependencies', 'peerDependencies']:
    for dep, ver in p.get(key, {}).items():
        if dep.startswith('@tanstack/'):
            print(f\"  {key}: {dep} -> {ver}\")
"
done
```

Expect: cross-deps among the 7 → `<base>-notion.N`. External `@tanstack/*` (db-ivm, pacer-lite, query-core) → registry versions, **not** `-notion.N`.

### 5. Generate SHASUMS256

```bash
cd _artifacts/release-notion.N && shasum -a 256 *.tgz > SHASUMS256.txt && cd -
```

### 6. Create GitHub release

Use absolute paths to the artifacts (fish shell strict-globs files with dots in path).

```bash
gh release create notion.N \
  --repo lalitkapoor/tanstack-db \
  --target notion \
  --title "notion.N (based on upstream @tanstack/db@<base>)" \
  --notes-file - \
  /abs/path/to/_artifacts/release-notion.N/<each-of-7-tgzs> \
  /abs/path/to/_artifacts/release-notion.N/SHASUMS256.txt <<'EOF'
<release notes — see template below>
EOF
```

**Release notes template:**

```markdown
Based on upstream `@tanstack/db@<base>`.

## Packages

| Package | Version |
|---|---|
| `@tanstack/db` | `<base>-notion.N` |
| ... 6 more rows ... |

## Notion patches on top of upstream

- **<commit subject>** (#<PR>, branch: `<pr-branch>`) — affects `<package>`
- ...

## Verifying integrity

Hashes are in `SHASUMS256.txt`. After download:

```
shasum -a 256 -c SHASUMS256.txt
```
```

### 7. Confirm

Report the release URL (`https://github.com/lalitkapoor/tanstack-db/releases/tag/notion.N`) plus the install pattern for the Notion app:

```json
"@tanstack/db": "https://github.com/lalitkapoor/tanstack-db/releases/download/notion.N/tanstack-db-<base>-notion.N.tgz"
```

Remind: pin exact versions (no `^`).

## Gotchas

- **Fish shell glob with dots**: `_artifacts/release-notion.N/*.tgz` fails in fish. Use bash subshell or absolute paths to each file.
- **Working dir persists across Bash calls**: if you `cd` into the artifacts dir to run `shasum`, `cd -` back before the `gh release create`.
- **Don't run `pnpm install` after bumping versions** — not needed for `pnpm pack`, and would dirty the lockfile with prerelease references.
- **PR squashes happen separately**: this skill assumes patches are already on the `notion` branch. If they aren't, squash-merge them first (`git merge --squash <pr-branch>` then commit with `(#<PR number>, branch: <pr-branch>)` in the message), then start at step 1.
- **Hooks may run on commit**: husky/lint-staged is configured. Don't bypass with `--no-verify`. If a hook fails on the release commit, fix the underlying issue.

## Reference: notion.1 (the first cut)

- Tag: `notion.1`
- Upstream base: `@tanstack/db@0.6.5`
- Bundled patches: PR #7 (sqlite JSON paths), PR #10 (tracked source subscriptions)
- URL: https://github.com/lalitkapoor/tanstack-db/releases/tag/notion.1
