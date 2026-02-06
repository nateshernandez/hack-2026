import { db } from "@/lib/db/db-client";
import { operationalSchemaEmbeddingsTable } from "@/lib/db/schemas/operational-schema-embeddings-schema";
import { embed } from "ai";
import { cosineDistance, desc, sql } from "drizzle-orm";
import { z } from "zod";

export const description = `
  Search for relevant database tables in the operational Databricks data warehouse using semantic similarity.
  Use this tool to discover which tables contain the data you need before writing queries.
  Returns table schemas including column names, data types, primary/foreign keys, relationships between tables, and sample enum values for categorical columns.
  Results are ranked by semantic relevance to your search query, with higher similarity scores indicating better matches.
`.trim();

export const inputSchema = z.object({
  query: z
    .string()
    .describe(
      "Natural language description of the data you're looking for. Examples: 'customer orders and purchases', 'user authentication and sessions', 'product inventory and pricing'"
    ),
  limit: z
    .number()
    .int()
    .min(1)
    .max(20)
    .default(10)
    .optional()
    .describe(
      "Maximum number of table schemas to return. Defaults to 10, maximum 20."
    ),
  minSimilarity: z
    .number()
    .min(0)
    .max(1)
    .default(0.3)
    .optional()
    .describe(
      "Minimum similarity score threshold (0-1). Only tables with similarity above this threshold will be returned. Defaults to 0.3."
    ),
});

export async function execute({
  query,
  limit = 10,
  minSimilarity = 0.3,
}: z.infer<typeof inputSchema>) {
  const { embedding } = await embed({
    model: "openai/text-embedding-3-small",
    value: query,
  });

  const similarityScore = sql<number>`1 - (${cosineDistance(
    operationalSchemaEmbeddingsTable.embedding,
    embedding
  )})`;

  const tables = await db
    .select({
      tableName: operationalSchemaEmbeddingsTable.tableName,
      schemaDescription: operationalSchemaEmbeddingsTable.schemaDescription,
      similarityScore,
    })
    .from(operationalSchemaEmbeddingsTable)
    .where(sql`${similarityScore} > ${minSimilarity}`)
    .orderBy(desc(similarityScore))
    .limit(limit);

  return { tables };
}
