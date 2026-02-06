import {
  description as executeOperationalQueryDescription,
  execute as executeOperationalQueryExecute,
  inputSchema as executeOperationalQueryInputSchema,
} from "@/lib/ai/core/execute-operational-query";
import {
  description as searchOperationalSchemaDescription,
  execute as searchOperationalSchemaExecute,
  inputSchema as searchOperationalSchemaInputSchema,
} from "@/lib/ai/core/search-operational-schema";
import { createMcpHandler } from "mcp-handler";

const handler = createMcpHandler(
  (server) => {
    server.registerTool(
      "search_bigtime_operational_schema",
      {
        title: "Search BigTime Operational Schema",
        description: searchOperationalSchemaDescription,
        inputSchema: {
          query: searchOperationalSchemaInputSchema.shape.query,
          limit: searchOperationalSchemaInputSchema.shape.limit,
          minSimilarity: searchOperationalSchemaInputSchema.shape.minSimilarity,
        },
        annotations: {
          title: "Search BigTime Operational Schema",
          readOnlyHint: true,
          openWorldHint: true,
        },
      },
      async (args) => {
        const result = await searchOperationalSchemaExecute(args);
        return {
          content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
        };
      }
    );

    server.registerTool(
      "execute_bigtime_operational_query",
      {
        title: "Execute BigTime Operational Query",
        description: executeOperationalQueryDescription,
        inputSchema: {
          sqlQuery: executeOperationalQueryInputSchema.shape.sqlQuery,
        },
        annotations: {
          title: "Execute BigTime Operational Query",
          readOnlyHint: true,
          openWorldHint: true,
        },
      },
      async (args) => {
        const result = await executeOperationalQueryExecute(args);
        return {
          content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
        };
      }
    );
  },
  {
    serverInfo: {
      name: "bigtime-analytics",
      version: "0.1.0",
    },
  },
  {
    basePath: "/api",
    maxDuration: 60,
  }
);

export { handler as GET, handler as POST };
