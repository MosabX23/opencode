import { describe, expect } from "bun:test"
import { $ } from "bun"
import path from "path"
import { eq } from "drizzle-orm"
import { Effect, Layer } from "effect"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Hash } from "@opencode-ai/core/util/hash"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Database } from "@opencode-ai/core/database/database"
import { ProjectPathTable } from "@opencode-ai/core/project/path.sql"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { ProjectV2 } from "@opencode-ai/core/project"
import { Project } from "@/project/project"
import { tmpdirScoped } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

const it = testEffect(Layer.mergeAll(Project.defaultLayer, Database.defaultLayer, CrossSpawnSpawner.defaultLayer))

function paths(projectID: ProjectV2.ID) {
  return Database.Service.use(({ db }) =>
    db
      .select()
      .from(ProjectPathTable)
      .where(eq(ProjectPathTable.project_id, projectID))
      .all()
      .pipe(
        Effect.orDie,
        Effect.map((rows) =>
          rows.map((row) => ({ path: row.path, type: row.type })).toSorted((a, b) => a.path.localeCompare(b.path)),
        ),
      ),
  )
}

describe("Project path persistence", () => {
  it.live("stores the first opened checkout path", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped({ git: true })
      const project = yield* Project.Service

      const result = yield* project.fromDirectory(tmp)

      expect(yield* paths(result.project.id)).toEqual([{ path: tmp, type: "main" }])
    }),
  )

  it.live("stores a repeatedly opened checkout path only once", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped({ git: true })
      const project = yield* Project.Service

      const result = yield* project.fromDirectory(tmp)
      const next = yield* project.fromDirectory(tmp)

      expect(next.project.id).toBe(result.project.id)
      expect(yield* paths(result.project.id)).toEqual([{ path: tmp, type: "main" }])
    }),
  )

  it.live("stores an opened linked worktree path", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped({ git: true })
      const project = yield* Project.Service
      const main = yield* project.fromDirectory(tmp)
      const worktree = path.join(tmp, "..", path.basename(tmp) + "-project-path-worktree")
      yield* Effect.addFinalizer(() =>
        Effect.promise(() => $`git worktree remove ${worktree}`.cwd(tmp).quiet().nothrow()).pipe(Effect.ignore),
      )
      yield* Effect.promise(() => $`git worktree add ${worktree} -b project-path-${Date.now()}`.cwd(tmp).quiet())

      yield* project.fromDirectory(worktree)

      expect(yield* paths(main.project.id)).toEqual(
        [
          { path: tmp, type: "main" as const },
          { path: worktree, type: "git_worktree" as const },
        ].toSorted((a, b) => a.path.localeCompare(b.path)),
      )
    }),
  )

  it.live("stores only the linked copy when first opened from an external linked worktree", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped({ git: true })
      const worktree = path.join(tmp, "..", path.basename(tmp) + "-project-path-first-worktree")
      yield* Effect.addFinalizer(() =>
        Effect.promise(() => $`git worktree remove ${worktree}`.cwd(tmp).quiet().nothrow()).pipe(Effect.ignore),
      )
      yield* Effect.promise(() => $`git worktree add --detach ${worktree} HEAD`.cwd(tmp).quiet())
      const project = yield* Project.Service

      const result = yield* project.fromDirectory(worktree)

      expect(yield* paths(result.project.id)).toEqual([{ path: worktree, type: "git_worktree" }])
    }),
  )

  it.live("stores a separately opened clone as a secondary path", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped({ git: true })
      const bare = tmp + "-project-path-bare"
      const clone = tmp + "-project-path-clone"
      yield* Effect.addFinalizer(() =>
        Effect.promise(() => $`rm -rf ${bare} ${clone}`.quiet().nothrow()).pipe(Effect.ignore),
      )
      yield* Effect.promise(() => $`git clone --bare ${tmp} ${bare}`.quiet())
      yield* Effect.promise(() => $`git clone ${bare} ${clone}`.quiet())
      const project = yield* Project.Service
      const main = yield* project.fromDirectory(tmp)

      yield* project.fromDirectory(clone)

      expect(yield* paths(main.project.id)).toEqual(
        [
          { path: tmp, type: "main" as const },
          { path: clone, type: "root" as const },
        ].toSorted((a, b) => a.path.localeCompare(b.path)),
      )
    }),
  )

  it.live("stores only the materialized worktree for a bare repository", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped({ git: true })
      const bare = tmp + "-project-path-bare-store.git"
      const worktree = tmp + "-project-path-bare-worktree"
      yield* Effect.addFinalizer(() =>
        Effect.promise(() => $`rm -rf ${bare} ${worktree}`.quiet().nothrow()).pipe(Effect.ignore),
      )
      yield* Effect.promise(() => $`git clone --bare ${tmp} ${bare}`.quiet())
      yield* Effect.promise(() => $`git worktree add ${worktree} HEAD`.cwd(bare).quiet())
      const project = yield* Project.Service

      const result = yield* project.fromDirectory(worktree)

      expect(yield* paths(result.project.id)).toEqual([{ path: worktree, type: "git_worktree" }])
    }),
  )

  it.live("records the active path under its newly resolved project id", () =>
    Effect.gen(function* () {
      const tmp = yield* tmpdirScoped({ git: true })
      const project = yield* Project.Service
      yield* project.fromDirectory(tmp)
      const remoteID = ProjectV2.ID.make(Hash.fast("git-remote:github.com/project-path-test/collision"))
      const { db } = yield* Database.Service
      yield* db
        .insert(ProjectTable)
        .values({
          id: remoteID,
          worktree: AbsolutePath.make("/tmp/existing"),
          vcs: "git",
          time_created: Date.now(),
          time_updated: Date.now(),
          sandboxes: [],
        })
        .run()
        .pipe(Effect.orDie)
      yield* Effect.promise(() =>
        $`git remote add origin git@github.com:project-path-test/collision.git`.cwd(tmp).quiet(),
      )

      yield* project.fromDirectory(tmp)

      expect(yield* paths(remoteID)).toEqual([{ path: tmp, type: "main" }])
    }),
  )
})
