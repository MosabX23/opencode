export * as ProjectCopy from "./copy"

import { and, eq, inArray } from "drizzle-orm"
import { Context, Effect, Layer, Schema } from "effect"
import { AbsolutePath } from "../schema"
import { AppFileSystem } from "../filesystem"
import { Git } from "../git"
import { Database } from "../database/database"
import { EventV2 } from "../event"
import { Project } from "../project"
import { ProjectPathTable } from "./path.sql"
import { makeStrategies } from "./copy-strategies"

export const StrategyID = Schema.Literal("git_worktree")
export type StrategyID = typeof StrategyID.Type

export const DetectInput = Schema.Struct({
  path: AbsolutePath,
}).annotate({ identifier: "ProjectCopy.DetectInput" })
export type DetectInput = typeof DetectInput.Type

export const CreateInput = Schema.Struct({
  projectID: Project.ID,
  strategy: StrategyID,
  sourcePath: AbsolutePath,
  path: AbsolutePath,
}).annotate({ identifier: "ProjectCopy.CreateInput" })
export type CreateInput = typeof CreateInput.Type

export const RemoveInput = Schema.Struct({
  projectID: Project.ID,
  path: AbsolutePath,
}).annotate({ identifier: "ProjectCopy.RemoveInput" })
export type RemoveInput = typeof RemoveInput.Type

export const RefreshInput = Schema.Struct({
  projectID: Project.ID,
}).annotate({ identifier: "ProjectCopy.RefreshInput" })
export type RefreshInput = typeof RefreshInput.Type

export const Copy = Schema.Struct({
  path: AbsolutePath,
}).annotate({ identifier: "ProjectCopy.Copy" })
export type Copy = typeof Copy.Type

export type PathType = "main" | "root" | StrategyID

export class SourcePathNotFoundError extends Schema.TaggedErrorClass<SourcePathNotFoundError>()(
  "ProjectCopy.SourcePathNotFoundError",
  { path: AbsolutePath },
) {}

export class DestinationExistsError extends Schema.TaggedErrorClass<DestinationExistsError>()(
  "ProjectCopy.DestinationExistsError",
  { path: AbsolutePath },
) {}

export class PathUnavailableError extends Schema.TaggedErrorClass<PathUnavailableError>()(
  "ProjectCopy.PathUnavailableError",
  { path: AbsolutePath },
) {}

export class StrategyNotFoundError extends Schema.TaggedErrorClass<StrategyNotFoundError>()(
  "ProjectCopy.StrategyNotFoundError",
  { path: AbsolutePath },
) {}

export type Error =
  | SourcePathNotFoundError
  | DestinationExistsError
  | PathUnavailableError
  | StrategyNotFoundError
  | Git.WorktreeError

export interface Strategy {
  readonly id: StrategyID
  readonly create: (input: {
    sourcePath: AbsolutePath
    path: AbsolutePath
  }) => Effect.Effect<Copy, Git.WorktreeError | PathUnavailableError>
  readonly remove: (path: AbsolutePath) => Effect.Effect<void, Git.WorktreeError | PathUnavailableError>
  readonly list: (path: AbsolutePath) => Effect.Effect<Copy[], Git.WorktreeError | PathUnavailableError>
  readonly detect: (path: AbsolutePath) => Effect.Effect<boolean>
}

export const Event = {
  Updated: EventV2.define({
    type: "project.paths.updated",
    schema: { projectID: Project.ID },
  }),
}

export interface Interface {
  readonly detect: (input: DetectInput) => Effect.Effect<StrategyID | undefined>
  readonly create: (input: CreateInput) => Effect.Effect<Copy, Error>
  readonly remove: (input: RemoveInput) => Effect.Effect<void, Error>
  readonly refresh: (input: RefreshInput) => Effect.Effect<void, Error>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/ProjectCopy") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* AppFileSystem.Service
    const git = yield* Git.Service
    const events = yield* EventV2.Service
    const db = (yield* Database.Service).db

    const canonical = Effect.fnUntraced(function* (input: AbsolutePath) {
      const resolved = AbsolutePath.make(AppFileSystem.resolve(input))
      if (!(yield* fs.isDir(resolved))) return yield* new PathUnavailableError({ path: input })
      return resolved
    })

    const registry = makeStrategies({ git, fs, canonical })

    const source = Effect.fnUntraced(function* (input: AbsolutePath, projectID: Project.ID) {
      const sourcePath = yield* canonical(input)
      const row = yield* db
        .select({ path: ProjectPathTable.path })
        .from(ProjectPathTable)
        .where(and(eq(ProjectPathTable.project_id, projectID), eq(ProjectPathTable.path, sourcePath)))
        .get()
        .pipe(Effect.orDie)
      if (!row) return yield* new SourcePathNotFoundError({ path: sourcePath })
      return sourcePath
    })

    const insert = Effect.fnUntraced(function* (projectID: Project.ID, copyPath: AbsolutePath, type: StrategyID) {
      return yield* db
        .transaction(
          (tx) =>
            Effect.gen(function* () {
              const row = yield* tx
                .select({ path: ProjectPathTable.path })
                .from(ProjectPathTable)
                .where(and(eq(ProjectPathTable.project_id, projectID), eq(ProjectPathTable.path, copyPath)))
                .get()
              if (row) return false
              yield* tx.insert(ProjectPathTable).values({ project_id: projectID, path: copyPath, type }).run()
              return true
            }),
          { behavior: "immediate" },
        )
        .pipe(Effect.orDie)
    })

    const removeStored = Effect.fnUntraced(function* (projectID: Project.ID, copyPath: AbsolutePath) {
      return (
        (yield* db
          .delete(ProjectPathTable)
          .where(and(eq(ProjectPathTable.project_id, projectID), eq(ProjectPathTable.path, copyPath)))
          .returning({ path: ProjectPathTable.path })
          .get()
          .pipe(Effect.orDie)) !== undefined
      )
    })

    const changed = Effect.fnUntraced(function* (projectID: Project.ID, update: boolean) {
      if (update) yield* events.publish(Event.Updated, { projectID })
    })

    const strategy = (id: StrategyID) => registry.get(id) as Strategy

    const detect = Effect.fn("ProjectCopy.detect")(function* (input: DetectInput) {
      for (const strategy of registry.values()) {
        if (yield* strategy.detect(input.path)) return strategy.id
      }
      return undefined
    })

    const create = Effect.fn("ProjectCopy.create")(function* (input: CreateInput) {
      if (yield* fs.existsSafe(input.path)) return yield* new DestinationExistsError({ path: input.path })
      const result = yield* strategy(input.strategy).create({
        path: input.path,
        sourcePath: yield* source(input.sourcePath, input.projectID),
      })
      yield* changed(input.projectID, yield* insert(input.projectID, result.path, input.strategy))
      return result
    })

    const remove = Effect.fn("ProjectCopy.remove")(function* (input: RemoveInput) {
      const copyPath = yield* canonical(input.path)
      const id = yield* detect({ path: copyPath })
      if (!id) return yield* new StrategyNotFoundError({ path: copyPath })
      yield* strategy(id).remove(copyPath)
      yield* changed(input.projectID, yield* removeStored(input.projectID, copyPath))
    })

    const refresh = Effect.fn("ProjectCopy.refresh")(function* (input: RefreshInput) {
      const roots = yield* db
        .select({ path: ProjectPathTable.path })
        .from(ProjectPathTable)
        .where(and(eq(ProjectPathTable.project_id, input.projectID), inArray(ProjectPathTable.type, ["main", "root"])))
        .all()
        .pipe(Effect.orDie)
      const sourcePaths = yield* Effect.forEach(roots, (item) => canonical(AbsolutePath.make(item.path)), {
        concurrency: "unbounded",
      })
      const discovered = yield* Effect.forEach(
        sourcePaths,
        (sourcePath) =>
          Effect.forEach(registry.values(), (strategy) =>
            strategy
              .list(sourcePath)
              .pipe(Effect.map((items) => items.map((item) => ({ ...item, type: strategy.id })))),
          ),
        { concurrency: "unbounded" },
      ).pipe(Effect.map((sets) => new Map(sets.flat(2).map((item) => [item.path, item] as const)).values().toArray()))
      const stored = yield* db
        .select({ path: ProjectPathTable.path })
        .from(ProjectPathTable)
        .where(eq(ProjectPathTable.project_id, input.projectID))
        .all()
        .pipe(Effect.orDie)
      const inserted = yield* Effect.forEach(discovered, (item) => insert(input.projectID, item.path, item.type)).pipe(
        Effect.map((items) => items.some(Boolean)),
      )
      const removed = yield* Effect.forEach(stored, (item) =>
        fs
          .isDir(item.path)
          .pipe(
            Effect.flatMap((exists) =>
              exists ? Effect.succeed(false) : removeStored(input.projectID, AbsolutePath.make(item.path)),
            ),
          ),
      ).pipe(Effect.map((items) => items.some(Boolean)))
      yield* changed(input.projectID, inserted || removed)
    })

    return Service.of({ detect, create, remove, refresh })
  }),
)

export const defaultLayer = layer.pipe(
  Layer.provide(Database.defaultLayer),
  Layer.provide(AppFileSystem.defaultLayer),
  Layer.provide(Git.defaultLayer),
  Layer.provide(EventV2.defaultLayer),
)
