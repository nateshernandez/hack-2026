import { DBSQLClient } from "@databricks/sql";
import type IDBSQLLogger from "@databricks/sql/dist/contracts/IDBSQLLogger";
import { LogLevel } from "@databricks/sql/dist/contracts/IDBSQLLogger";
import "server-only";

declare global {
  var databricksClient: DBSQLClient | undefined;
}

const databricksLogger: IDBSQLLogger = {
  log: (level: LogLevel, message: string): void => {
    if (level === LogLevel.error && !message.includes("LZ4")) {
      console.error(message);
    }
  },
};

let databricksClient: DBSQLClient;

if (process.env.NODE_ENV === "production") {
  databricksClient = new DBSQLClient({
    logger: databricksLogger,
  });
} else {
  if (!global.databricksClient) {
    global.databricksClient = new DBSQLClient({
      logger: databricksLogger,
    });
  }

  databricksClient = global.databricksClient;
}

export { databricksClient };
