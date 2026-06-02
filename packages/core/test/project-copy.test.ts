import { describe, expect } from "bun:test"
import { $ } from "bun"
import fs from "fs/promises"
import path from "path"
import { eq } from "drizzle-orm"
import { sql } from "drizzle-orm"
import { Effect, Fiber, Layer, Stream } from "effect"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import { Git } from "@opencode-ai/core/git"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { Project } from "@opencode-ai/core/project"
import { ProjectPathTable } from "@opencode-ai/core/project/path.sql"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { ProjectCopy } from "@opencode-ai/core/project/copy"
import { tmpdir } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"

const databaseLayer = Database.layerFromPath(":memory:")
const eventLayer = EventV2.layer.pipe(Layer.provide(databaseLayer))
const copyLayer = ProjectCopy.layer.pipe(
  Layer.provide(databaseLayer),
  Layer.provide(eventLayer),
  Layer.provide(AppFileSystem.defaultLayer),
  Layer.provide(Git.defaultLayer),
)
const it = testEffect(Layer.mergeAll(copyLayer, databaseLayer, eventLayer))

function abs(input: string) {
  return AbsolutePath.make(input)
}

const ensureProjectPathTable = Database.Service.use(({ db }) =>
  db.run(sql`
    CREATE TABLE IF NOT EXISTS project_path (
      project_id text NOT NULL,
      path text NOT NULL,
      type text NOT NULL,
      time_created integer NOT NULL DEFAULT 0,
      PRIMARY KEY (project_id, path)
    )
  `),
)

async function initRepo(directory: string) {
  await $`git init`.cwd(directory).quiet()
  await $`git config core.fsmonitor false`.cwd(directory).quiet()
  await $`git config commit.gpgsign false`.cwd(directory).quiet()
  await $`git config user.email test@opencode.test`.cwd(directory).quiet()
  await $`git config user.name Test`.cwd(directory).quiet()
  await $`git commit --allow-empty -m root`.cwd(directory).quiet()
}

function setup() {
  return Effect.gen(function* () {
    const root = yield* Effect.acquireRelease(
      Effect.promise(() => tmpdir()),
      (dir) => Effect.promise(() => dir[Symbol.asyncDispose]()),
    )
    yield* Effect.promise(() => initRepo(root.path))
    const sourcePath = abs(yield* Effect.promise(() => fs.realpath(root.path)))
    const projectID = Project.ID.make("copy-project")
    const { db } = yield* Database.Service
    yield* ensureProjectPathTable
    yield* db
      .insert(ProjectTable)
      .values({ id: projectID, worktree: root.path, sandboxes: [], time_created: 1, time_updated: 1 })
      .run()
      .pipe(Effect.orDie)
    yield* db
      .insert(ProjectPathTable)
      .values({ project_id: projectID, path: sourcePath, type: "main" })
      .run()
      .pipe(Effect.orDie)
    return { root, sourcePath, projectID, db }
  })
}

function stored(projectID: Project.ID) {
  return Database.Service.use(({ db }) =>
    db
      .select({ path: ProjectPathTable.path, type: ProjectPathTable.type })
      .from(ProjectPathTable)
      .where(eq(ProjectPathTable.project_id, projectID))
      .all()
      .pipe(
        Effect.orDie,
        Effect.map((rows) => rows.toSorted((a, b) => a.path.localeCompare(b.path))),
      ),
  )
}

describe("ProjectCopy", () => {
  it.live("detects linked git worktrees but not root checkouts", () =>
    Effect.gen(function* () {
      const input = yield* setup()
      const copy = yield* ProjectCopy.Service
      const target = abs(`${input.root.path}-copy-detected`)
      yield* Effect.addFinalizer(() =>
        Effect.promise(() => fs.rm(target, { recursive: true, force: true })).pipe(Effect.ignore),
      )
      yield* Effect.promise(() => $`git worktree add --detach ${target} HEAD`.cwd(input.root.path).quiet())

      expect(yield* copy.detect({ path: input.sourcePath })).toBeUndefined()
      expect(yield* copy.detect({ path: target })).toBe("git_worktree")
    }),
  )

  it.live("creates and removes a git worktree path", () =>
    Effect.gen(function* () {
      const input = yield* setup()
      const copy = yield* ProjectCopy.Service
      const events = yield* EventV2.Service
      const target = abs(`${input.root.path}-copy-created`)
      yield* Effect.addFinalizer(() =>
        Effect.promise(() => fs.rm(target, { recursive: true, force: true })).pipe(Effect.ignore),
      )
      const fiber = yield* events
        .subscribe(ProjectCopy.Event.Updated)
        .pipe(Stream.take(1), Stream.runCollect, Effect.forkScoped)
      yield* Effect.yieldNow

      const created = yield* copy.create({
        projectID: input.projectID,
        strategy: "git_worktree",
        sourcePath: input.sourcePath,
        path: target,
      })
      expect(yield* stored(input.projectID)).toEqual(
        [
          { path: input.sourcePath, type: "main" as const },
          { path: created.path, type: "git_worktree" as const },
        ].toSorted((a, b) => a.path.localeCompare(b.path)),
      )
      expect(Array.from(yield* Fiber.join(fiber))[0]?.data).toEqual({ projectID: input.projectID })

      yield* copy.remove({ projectID: input.projectID, path: created.path })

      expect(yield* stored(input.projectID)).toEqual([{ path: input.sourcePath, type: "main" as const }])
      expect(yield* Effect.promise(() => Bun.file(target).exists())).toBe(false)
    }),
  )

  it.live("does not publish an event when refresh finds no path changes", () =>
    Effect.gen(function* () {
      const input = yield* setup()
      const copy = yield* ProjectCopy.Service
      const events = yield* EventV2.Service
      const event = yield* events.subscribe(ProjectCopy.Event.Updated).pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.forkScoped,
        Effect.flatMap((fiber) =>
          Effect.gen(function* () {
            yield* Effect.yieldNow
            yield* copy.refresh({ projectID: input.projectID })
            return yield* Fiber.join(fiber).pipe(Effect.timeoutOption("50 millis"))
          }),
        ),
      )

      expect(event._tag).toBe("None")
    }),
  )

  it.live("refresh discovers and prunes an externally managed git worktree", () =>
    Effect.gen(function* () {
      const input = yield* setup()
      const copy = yield* ProjectCopy.Service
      const events = yield* EventV2.Service
      const target = abs(`${input.root.path}-copy-external`)
      yield* Effect.addFinalizer(() =>
        Effect.promise(() => fs.rm(target, { recursive: true, force: true })).pipe(Effect.ignore),
      )
      yield* Effect.promise(() => $`git worktree add --detach ${target} HEAD`.cwd(input.root.path).quiet())
      const fiber = yield* events
        .subscribe(ProjectCopy.Event.Updated)
        .pipe(Stream.take(1), Stream.runCollect, Effect.forkScoped)
      yield* Effect.yieldNow

      yield* copy.refresh({ projectID: input.projectID })

      const discovered = abs(yield* Effect.promise(() => fs.realpath(target)))
      expect(yield* stored(input.projectID)).toEqual(
        [
          { path: input.sourcePath, type: "main" as const },
          { path: discovered, type: "git_worktree" as const },
        ].toSorted((a, b) => a.path.localeCompare(b.path)),
      )
      expect(Array.from(yield* Fiber.join(fiber))[0]?.data).toEqual({ projectID: input.projectID })

      yield* Effect.promise(() => $`git worktree remove --force ${target}`.cwd(input.root.path).quiet())
      yield* copy.refresh({ projectID: input.projectID })
      expect(yield* stored(input.projectID)).toEqual([{ path: input.sourcePath, type: "main" as const }])
    }),
  )

  it.live("refresh with no roots is a no-op", () =>
    Effect.gen(function* () {
      const copy = yield* ProjectCopy.Service

      yield* ensureProjectPathTable
      yield* copy.refresh({ projectID: Project.ID.make("missing-project") })
    }),
  )
})
