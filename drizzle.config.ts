import { config } from "@/lib/config";
import { defineConfig } from "drizzle-kit";

export default defineConfig({
  out: "./lib/db/migrations",
  schema: "./lib/db/schemas/*.ts",
  dialect: "postgresql",
  dbCredentials: {
    url: config.db.connectionString,
  },
});
