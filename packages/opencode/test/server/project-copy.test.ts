import { afterEach, describe, expect } from "bun:test"
import { $ } from "bun"
import fs from "fs/promises"
import path from "path"
import { sql } from "drizzle-orm"
import { Effect, Layer } from "effect"
import { HttpClientResponse } from "effect/unstable/http"
import { AppFileSystem } from "@opencode-ai/core/filesystem"
import { Database } from "@opencode-ai/core/database/database"
import { Snapshot } from "@/snapshot"
import { InstanceBootstrap } from "@/project/bootstrap-service"
import { InstanceStore } from "@/project/instance-store"
import { resetDatabase } from "../fixture/db"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { httpApiLayer, requestInDirectory } from "./httpapi-layer"

afterEach(async () => {
  await disposeAllInstances()
  await resetDatabase()
})

const noopBootstrap = Layer.succeed(InstanceBootstrap.Service, InstanceBootstrap.Service.of({ run: Effect.void }))
const testInstanceStore = InstanceStore.defaultLayer.pipe(Layer.provide(noopBootstrap))
const it = testEffect(
  Layer.mergeAll(
    AppFileSystem.defaultLayer,
    Database.defaultLayer,
    Snapshot.defaultLayer,
    testInstanceStore,
    httpApiLayer,
  ),
)

function request(directory: string, url: string, init: RequestInit = {}) {
  return requestInDirectory(url, directory, init)
}

function json<T>(response: HttpClientResponse.HttpClientResponse) {
  return response.json.pipe(Effect.map((value) => value as T))
}

describe("project paths and copies endpoints", () => {
  it.instance(
    "lists paths and manages git worktree copies",
    () =>
      Effect.gen(function* () {
        const test = yield* TestInstance
        yield* Database.Service.use(({ db }) =>
          db
            .run(
              sql`
            CREATE TABLE IF NOT EXISTS project_path (
              project_id text NOT NULL,
              path text NOT NULL,
              type text NOT NULL,
              time_created integer NOT NULL DEFAULT 0,
              PRIMARY KEY (project_id, path)
            )
          `,
            )
            .pipe(Effect.orDie),
        )
        const current = yield* request(test.directory, "/project/current")
        const projectID = (yield* json<{ id: string }>(current)).id
        const base = `/project/${projectID}`
        const copies = `/experimental/project/${projectID}/copy`
        const createdPath = path.join(test.directory, "..", path.basename(test.directory) + "-http-copy")
        yield* Effect.addFinalizer(() =>
          Effect.promise(() => fs.rm(createdPath, { recursive: true, force: true })).pipe(Effect.ignore),
        )

        const initial = yield* request(test.directory, `${base}/paths`)
        expect(initial.status).toBe(200)
        expect(yield* json<string[]>(initial)).toEqual([test.directory])

        const create = yield* request(test.directory, copies, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ strategy: "git_worktree", path: createdPath }),
        })
        expect(create.status).toBe(200)
        const created = yield* json<{ path: string }>(create)
        expect(created.path).toContain("-http-copy")

        const listed = yield* request(test.directory, `${base}/paths`)
        expect(yield* json<string[]>(listed)).toContain(created.path)

        const remove = yield* request(test.directory, copies, {
          method: "DELETE",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ path: created.path }),
        })
        expect(remove.status).toBe(204)

        const externalPath = path.join(test.directory, "..", path.basename(test.directory) + "-http-refresh")
        yield* Effect.addFinalizer(() =>
          Effect.promise(() => fs.rm(externalPath, { recursive: true, force: true })).pipe(Effect.ignore),
        )
        yield* Effect.promise(() => $`git worktree add --detach ${externalPath} HEAD`.cwd(test.directory).quiet())
        const refresh = yield* request(test.directory, `${copies}/refresh`, {
          method: "POST",
        })
        expect(refresh.status).toBe(204)
        const refreshed = yield* request(test.directory, `${base}/paths`)
        expect((yield* json<string[]>(refreshed)).length).toBe(2)
      }),
    { git: true },
  )
})
