import { config } from "@/lib/config";
import { drizzle } from "drizzle-orm/node-postgres";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";

declare global {
  var db: NodePgDatabase | undefined;
}

let db: NodePgDatabase;

if (process.env.NODE_ENV === "production") {
  db = drizzle(config.db.connectionString);
} else {
  if (!global.db) {
    global.db = drizzle(config.db.connectionString);
  }

  db = global.db;
}

export { db };
