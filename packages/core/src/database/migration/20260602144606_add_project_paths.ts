import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260602144606_add_project_paths",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`project_path\` (
          \`project_id\` text NOT NULL,
          \`path\` text NOT NULL,
          \`type\` text NOT NULL,
          \`time_created\` integer NOT NULL,
          CONSTRAINT \`project_path_pk\` PRIMARY KEY(\`project_id\`, \`path\`),
          CONSTRAINT \`fk_project_path_project_id_project_id_fk\` FOREIGN KEY (\`project_id\`) REFERENCES \`project\`(\`id\`) ON DELETE CASCADE
        );
      `)
    })
  },
} satisfies DatabaseMigration.Migration
