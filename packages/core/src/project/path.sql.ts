import { integer, primaryKey, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { ProjectTable } from "./sql"
import { ProjectV2 } from "../project"

export const ProjectPathTable = sqliteTable(
  "project_path",
  {
    project_id: text()
      .$type<ProjectV2.ID>()
      .notNull()
      .references(() => ProjectTable.id, { onDelete: "cascade" }),
    path: text().notNull(),
    type: text().$type<"main" | "root" | "git_worktree">().notNull(),
    time_created: integer()
      .notNull()
      .$default(() => Date.now()),
  },
  (table) => [primaryKey({ columns: [table.project_id, table.path] })],
)
