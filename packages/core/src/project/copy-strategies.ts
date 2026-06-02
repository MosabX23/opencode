import path from "path"
import { Effect } from "effect"
import { AbsolutePath } from "../schema"
import { AppFileSystem } from "../filesystem"
import { Git } from "../git"
import { type Copy, type Strategy, type StrategyID } from "./copy"
import type { PathUnavailableError } from "./copy"

export function makeStrategies(input: {
  git: Git.Interface
  fs: AppFileSystem.Interface
  canonical: (path: AbsolutePath) => Effect.Effect<AbsolutePath, PathUnavailableError>
}) {
  const repo = (sourcePath: AbsolutePath) => ({ directory: sourcePath, store: sourcePath }) satisfies Git.Repo

  const gitWorktree: Strategy = {
    id: "git_worktree",
    create: Effect.fn("ProjectCopy.GitWorktree.create")(function* (options) {
      yield* input.git.worktreeCreate({ repo: repo(options.sourcePath), path: options.path })
      return { path: yield* input.canonical(options.path) }
    }),
    remove: Effect.fn("ProjectCopy.GitWorktree.remove")(function* (path) {
      yield* input.git.worktreeRemove({ repo: repo(path), path })
    }),
    list: Effect.fn("ProjectCopy.GitWorktree.list")(function* (path) {
      const entries = yield* input.git.worktreeList(repo(path))
      return yield* Effect.forEach(entries, (entry) =>
        entry === path ? Effect.succeed(undefined) : input.canonical(entry).pipe(Effect.map((path) => ({ path }))),
      ).pipe(Effect.map((items) => items.filter((item): item is Copy => item !== undefined)))
    }),
    detect: Effect.fn("ProjectCopy.GitWorktree.detect")(function* (inputPath) {
      return yield* input.fs.isFile(path.join(inputPath, ".git"))
    }),
  }

  return new Map<StrategyID, Strategy>([[gitWorktree.id, gitWorktree]])
}
