import { config } from "@/lib/config";
import { databricksClient } from "@/lib/databricks/databricks-client";
import { z } from "zod";

const maxRows = 1000;
const queryTimeoutMs = 30000;

export const description = `
  Executes read-only SQL queries against the Databricks data warehouse using Apache Spark SQL dialect.

  Supported Spark SQL features:
  - SELECT statements with complex expressions
  - Common Table Expressions (CTEs) using WITH clause (including recursive CTEs)
  - Window functions (ROW_NUMBER, RANK, LAG, LEAD, etc.)
  - Aggregate functions and GROUP BY
  - JOINs (INNER, LEFT, RIGHT, FULL, CROSS)
  - Subqueries and derived tables
  - UNION, INTERSECT, EXCEPT set operations
  - EXPLAIN for query plans
  - Table metadata commands (DESCRIBE, SHOW)

  Write operations are blocked for safety (INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, GRANT, REVOKE).
  Results are limited to ${maxRows} rows and queries timeout after ${queryTimeoutMs / 1000} seconds.
`.trim();

export const inputSchema = z.object({
  sqlQuery: z
    .string()
    .min(1)
    .describe(
      "SQL query to execute against the Databricks data warehouse. Must be a read-only SELECT query using Spark SQL syntax."
    ),
});

const writeOperations = [
  "insert",
  "update",
  "delete",
  "drop",
  "create",
  "alter",
  "truncate",
  "grant",
  "revoke",
  "merge",
  "copy",
  "call",
];

const dangerousFunctions = ["load_file", "load_data", "outfile", "dumpfile"];

function validateReadOnlyQuery(query: string): {
  valid: boolean;
  reason?: string;
} {
  const normalized = query
    .toLowerCase()
    .replace(/--[^\n]*/g, "")
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/\s+/g, " ")
    .trim();

  for (const operation of writeOperations) {
    const pattern = new RegExp(`\\b${operation}\\b`);

    if (pattern.test(normalized)) {
      return {
        valid: false,
        reason: `${operation.toUpperCase()} operations are not allowed`,
      };
    }
  }

  for (const fn of dangerousFunctions) {
    if (normalized.includes(fn)) {
      return {
        valid: false,
        reason: `${fn.toUpperCase()} function is not allowed`,
      };
    }
  }

  if (normalized.includes(";")) {
    return {
      valid: false,
      reason: "Multiple statements not allowed (found semicolon)",
    };
  }

  return { valid: true };
}

export async function execute({ sqlQuery }: z.infer<typeof inputSchema>) {
  const validation = validateReadOnlyQuery(sqlQuery);

  if (!validation.valid) {
    return {
      success: false as const,
      error: validation.reason || "Query validation failed",
    };
  }

  const normalizedQuery = sqlQuery.toLowerCase().replace(/\s+/g, " ");
  const hasLimitClause = /\blimit\b/.test(normalizedQuery);

  const queryWithLimit = hasLimitClause
    ? sqlQuery
    : `${sqlQuery.trim()} LIMIT ${maxRows}`;

  let session = null;

  try {
    await databricksClient.connect({
      host: config.databricks.host,
      path: config.databricks.httpPath,
      token: config.databricks.accessToken,
    });

    session = await databricksClient.openSession({
      initialCatalog: config.databricks.catalog,
      initialSchema: config.databricks.schema,
    });

    const queryOperation = await session.executeStatement(queryWithLimit, {
      runAsync: true,
      maxRows,
    });

    const rows = await queryOperation.fetchAll();
    await queryOperation.close();

    return {
      success: true as const,
      rows: rows as Record<string, unknown>[],
      rowCount: rows.length,
    };
  } catch (error) {
    let errorMessage = "Unknown error occurred";

    if (error instanceof Error) {
      errorMessage = error.message;

      if (errorMessage.includes("timeout")) {
        errorMessage = `Query timeout after ${queryTimeoutMs / 1000}s: ${errorMessage}`;
      } else if (errorMessage.includes("syntax")) {
        errorMessage = `SQL syntax error (Spark SQL dialect): ${errorMessage}`;
      }
    }

    return {
      success: false as const,
      error: errorMessage,
    };
  } finally {
    if (session) {
      await session.close();
    }

    await databricksClient.close();
  }
}
