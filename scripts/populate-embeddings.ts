import { config } from "@/lib/config";
import { databricksClient } from "@/lib/databricks/databricks-client";
import { db } from "@/lib/db/db-client";
import {
  NewOperationalSchemaEmbedding,
  operationalSchemaEmbeddingsTable,
} from "@/lib/db/schemas/operational-schema-embeddings-schema";
import { DBSQLClient } from "@databricks/sql";
import { embed } from "ai";

type DBSQLSession = Awaited<ReturnType<DBSQLClient["openSession"]>>;

interface ColumnInfo {
  columnName: string;
  dataType: string;
  isNullable: boolean;
  isPrimaryKey: boolean;
  isForeignKey: boolean;
}

interface ForeignKeyInfo {
  columnName: string;
  referencedTable: string;
  referencedColumn: string;
}

interface EnumValue {
  columnName: string;
  values: string[];
}

interface TableMetadata {
  tableName: string;
  columns: ColumnInfo[];
  primaryKeys: string[];
  foreignKeys: ForeignKeyInfo[];
  enumValues: EnumValue[];
}

interface ShowTablesRow {
  tableName: string;
}

interface DescribeTableRow {
  col_name: string;
  data_type: string;
}

interface TablePropertyRow {
  key: string;
  value: string;
}

interface DistinctValueRow {
  value: string | null;
}

async function getTables(session: DBSQLSession): Promise<string[]> {
  const queryOperation = await session.executeStatement(
    `SHOW TABLES IN \`${config.databricks.catalog}\`.\`${config.databricks.schema}\``,
    {
      runAsync: true,
      maxRows: 10000,
    }
  );

  const tableRows = await queryOperation.fetchAll();
  await queryOperation.close();

  return tableRows.map((row) => (row as ShowTablesRow).tableName);
}

async function getColumns(
  session: DBSQLSession,
  tableName: string
): Promise<ColumnInfo[]> {
  const fullTableName = `\`${config.databricks.catalog}\`.\`${config.databricks.schema}\`.\`${tableName}\``;

  const queryOperation = await session.executeStatement(
    `DESCRIBE TABLE EXTENDED ${fullTableName}`,
    {
      runAsync: true,
      maxRows: 10000,
    }
  );

  const descriptionRows = await queryOperation.fetchAll();
  await queryOperation.close();

  const columns: ColumnInfo[] = [];

  for (const row of descriptionRows) {
    const descriptionRow = row as DescribeTableRow;

    if (!descriptionRow.col_name || descriptionRow.col_name.startsWith("#")) {
      break;
    }

    if (descriptionRow.col_name.trim() === "") {
      continue;
    }

    columns.push({
      columnName: descriptionRow.col_name,
      dataType: descriptionRow.data_type || "string",
      isNullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
    });
  }

  return columns;
}

async function getPrimaryKeys(
  session: DBSQLSession,
  tableName: string
): Promise<string[]> {
  try {
    const fullTableName = `\`${config.databricks.catalog}\`.\`${config.databricks.schema}\`.\`${tableName}\``;

    const queryOperation = await session.executeStatement(
      `SHOW TBLPROPERTIES ${fullTableName}`,
      {
        runAsync: true,
        maxRows: 10000,
      }
    );

    const propertyRows = await queryOperation.fetchAll();
    await queryOperation.close();

    for (const row of propertyRows) {
      const propertyRow = row as TablePropertyRow;

      if (
        propertyRow.key === "primaryKey" ||
        propertyRow.key === "primary_key"
      ) {
        return propertyRow.value
          .split(",")
          .map((columnName) => columnName.trim());
      }
    }

    return [];
  } catch {
    return [];
  }
}

async function getForeignKeys(
  session: DBSQLSession,
  tableName: string
): Promise<ForeignKeyInfo[]> {
  try {
    const fullTableName = `\`${config.databricks.catalog}\`.\`${config.databricks.schema}\`.\`${tableName}\``;

    const queryOperation = await session.executeStatement(
      `DESCRIBE TABLE EXTENDED ${fullTableName}`,
      {
        runAsync: true,
        maxRows: 10000,
      }
    );

    const descriptionRows = await queryOperation.fetchAll();
    await queryOperation.close();

    const foreignKeys: ForeignKeyInfo[] = [];
    let inDetailSection = false;

    for (const row of descriptionRows) {
      const descriptionRow = row as DescribeTableRow;

      if (descriptionRow.col_name === "# Detailed Table Information") {
        inDetailSection = true;
        continue;
      }

      if (inDetailSection && descriptionRow.col_name?.includes("Foreign Key")) {
        const foreignKeyPattern = descriptionRow.data_type?.match(
          /(\w+)\s*->\s*(\w+)\.(\w+)/
        );

        if (foreignKeyPattern) {
          foreignKeys.push({
            columnName: foreignKeyPattern[1],
            referencedTable: foreignKeyPattern[2],
            referencedColumn: foreignKeyPattern[3],
          });
        }
      }
    }

    return foreignKeys;
  } catch {
    return [];
  }
}

async function detectEnumValues(
  session: DBSQLSession,
  tableName: string,
  columns: ColumnInfo[]
): Promise<EnumValue[]> {
  const textTypes = ["string", "text", "varchar", "char"];

  const candidateColumns = columns.filter((column) =>
    textTypes.includes(column.dataType.toLowerCase())
  );

  const enumValues: EnumValue[] = [];

  for (const column of candidateColumns) {
    try {
      const fullTableName = `\`${config.databricks.catalog}\`.\`${config.databricks.schema}\`.\`${tableName}\``;

      const queryOperation = await session.executeStatement(
        `SELECT DISTINCT \`${column.columnName}\` AS value
           FROM ${fullTableName}
          WHERE \`${column.columnName}\` IS NOT NULL
          LIMIT 20`,
        {
          runAsync: true,
          maxRows: 20,
        }
      );

      const distinctRows = await queryOperation.fetchAll();
      await queryOperation.close();

      const distinctValues = distinctRows
        .map((row) => (row as DistinctValueRow).value)
        .filter(
          (value): value is string => value !== null && value !== undefined
        );

      if (distinctValues.length >= 2 && distinctValues.length <= 10) {
        enumValues.push({
          columnName: column.columnName,
          values: distinctValues.sort(),
        });
      }
    } catch {
      continue;
    }
  }

  return enumValues;
}

async function extractTableMetadata(
  session: DBSQLSession,
  tableName: string
): Promise<TableMetadata> {
  const [columns, primaryKeys, foreignKeys] = await Promise.all([
    getColumns(session, tableName),
    getPrimaryKeys(session, tableName),
    getForeignKeys(session, tableName),
  ]);

  const primaryKeySet = new Set(primaryKeys);
  const foreignKeySet = new Set(
    foreignKeys.map((foreignKey) => foreignKey.columnName)
  );

  for (const column of columns) {
    column.isPrimaryKey = primaryKeySet.has(column.columnName);
    column.isForeignKey = foreignKeySet.has(column.columnName);
  }

  const enumValues = await detectEnumValues(session, tableName, columns);

  return { tableName, columns, primaryKeys, foreignKeys, enumValues };
}

function formatTableContent(metadata: TableMetadata): string {
  const lines: string[] = [`Table: ${metadata.tableName}`];

  const columnDescriptions = metadata.columns.map((column) => {
    const constraints: string[] = [];

    if (column.isPrimaryKey) constraints.push("primary key");
    if (column.isForeignKey) constraints.push("foreign key");
    if (!column.isNullable && !column.isPrimaryKey)
      constraints.push("not null");

    const suffix = constraints.length > 0 ? `, ${constraints.join(", ")}` : "";

    return `${column.columnName} (${column.dataType}${suffix})`;
  });

  lines.push(`Columns: ${columnDescriptions.join(", ")}`);

  if (metadata.foreignKeys.length > 0) {
    const foreignKeyDescriptions = metadata.foreignKeys.map(
      (foreignKey) =>
        `${metadata.tableName}.${foreignKey.columnName} → ${foreignKey.referencedTable}.${foreignKey.referencedColumn}`
    );

    lines.push(`Foreign Keys: ${foreignKeyDescriptions.join(", ")}`);
  }

  if (metadata.primaryKeys.length > 0) {
    lines.push(`Primary Keys: ${metadata.primaryKeys.join(", ")}`);
  }

  if (metadata.enumValues.length > 0) {
    const enumDescriptions = metadata.enumValues.map(
      (enumValue) =>
        `${enumValue.columnName} can be ${enumValue.values.map((value) => `'${value}'`).join(", ")}`
    );

    lines.push(`Sample Values: ${enumDescriptions.join("; ")}`);
  }

  return lines.join("\n");
}

async function main(): Promise<void> {
  let session: DBSQLSession | null = null;

  try {
    await databricksClient.connect({
      host: config.databricks.host,
      path: config.databricks.httpPath,
      token: config.databricks.accessToken,
    });

    console.log("Connected to Databricks.");

    session = await databricksClient.openSession({
      initialCatalog: config.databricks.catalog,
      initialSchema: config.databricks.schema,
    });

    console.log("Opened Databricks session.");

    await db.execute("SELECT 1");
    console.log("Connected to analytics database.");

    await db.delete(operationalSchemaEmbeddingsTable);
    console.log("Cleared existing embeddings.");

    const tableNames = await getTables(session);
    console.log(`Found ${tableNames.length} tables to process.\n`);

    const embeddings: NewOperationalSchemaEmbedding[] = [];

    for (let i = 0; i < tableNames.length; i++) {
      const tableName = tableNames[i];
      process.stdout.write(`[${i + 1}/${tableNames.length}] ${tableName}... `);

      const metadata = await extractTableMetadata(session, tableName);
      const content = formatTableContent(metadata);

      const { embedding } = await embed({
        model: "openai/text-embedding-3-small",
        value: content,
      });

      embeddings.push({ schemaDescription: content, tableName, embedding });
      console.log("done");
    }

    await db.transaction(async (tx) => {
      for (const record of embeddings) {
        await tx.insert(operationalSchemaEmbeddingsTable).values(record);
      }
    });

    console.log(`\nInserted ${embeddings.length} embeddings.`);
  } catch (error) {
    console.error("Fatal error:", error);
    process.exit(1);
  } finally {
    if (session) {
      await session.close();
    }

    await databricksClient.close();
  }
}

main();
