import { client, connectDB } from '../db';
import { cleanedClient, connectCleanedDB } from '../cleandb';
import crypto from 'crypto';

const TABLE_NAME = 'ArbTransactionMeta';

export async function fetchAndInsertArbTransactionMeta() {
  await connectDB();
  await connectCleanedDB();

  await cleanedClient.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS "idx_arbtransactionmeta_row_hash"
    ON "ArbTransactionMeta" ("row_hash");
  `);

  const trackerResult = await cleanedClient.query(
    `SELECT "last_processed_id" FROM "pipeline_tracker" WHERE "table_name" = $1`,
    [TABLE_NAME]
  );
  const lastProcessedId = trackerResult.rows[0]?.last_processed_id ?? 0;

  const result = await client.query(
    `SELECT * FROM "ArbTransactionMeta" WHERE "id" > $1 ORDER BY "id" ASC`,
    [lastProcessedId]
  );
  const rows = result.rows;

  if (rows.length === 0) {
    console.log("No new rows.");
    await client.end();
    await cleanedClient.end();
    return;
  }

  const allColumns = [
    "id", "dateTime", "idealTradeBuy", "idealTradeSell", "idealProfit",
    "expectedTradeBuy", "expectedTradeSell", "expectedidealProfit", "base", "quote",
    "buyExchange", "buyExpectedVwap", "buyExpectedBase", "buyExpectedQuote",
    "buyOrderId", "buyStatusId", "buyStatusfee", "buyStatusExecutedBase", "buyStatusExecutedQuote", "buyStatusVwap",
    "sellExchange", "sellExpectedVwap", "sellExpectedBase", "sellExpectedQuote",
    "sellOrderId", "sellStatusId", "sellStatusFee", "sellStatusExecutedBase", "sellStatusExecutedQuote", "sellStatusVwap",
    "deltaBase", "deltaQuote", "deltaBaseValue", "totalFees", "estimatedDeltaValue"
  ];
  const insertColumns = [...allColumns, "row_hash"];

  const hashedRows = rows.map(row => ({
    ...row,
    row_hash: generateRowHash(row, allColumns)
  }));

  const batchSize = 100;
  for (let i = 0; i < hashedRows.length; i += batchSize) {
    const batch = hashedRows.slice(i, i + batchSize);
    const values: string[] = [];
    const params: any[] = [];

    batch.forEach((row, idx) => {
      const base = idx * insertColumns.length;
      const placeholders = insertColumns.map((_, j) => `$${base + j + 1}`);
      values.push(`(${placeholders.join(', ')})`);
      insertColumns.forEach(col => params.push(row[col]));
    });

    await cleanedClient.query(`
      INSERT INTO "ArbTransactionMeta" (${insertColumns.map(c => `"${c}"`).join(', ')})
      VALUES ${values.join(', ')}
      ON CONFLICT ("row_hash") DO NOTHING;
    `, params);

    console.log(`✅ Inserted batch ${i / batchSize + 1} of ${Math.ceil(hashedRows.length / batchSize)}`);
  }

  const maxId = Math.max(...hashedRows.map(r => r.id));
  await cleanedClient.query(`
    INSERT INTO "pipeline_tracker" ("table_name", "last_processed_id")
    VALUES ($1, $2)
    ON CONFLICT ("table_name") DO UPDATE SET "last_processed_id" = EXCLUDED."last_processed_id";
  `, [TABLE_NAME, maxId]);

  await client.end();
  await cleanedClient.end();
  console.log(`✅ ArbTransactionMeta: ${hashedRows.length} rows inserted. Last ID: ${maxId}`);
}

function generateRowHash(row: Record<string, any>, columns: string[]) {
  return crypto.createHash('sha256').update(
    columns.map(c => String(row[c] ?? '')).join('|')
  ).digest('hex');
}

fetchAndInsertArbTransactionMeta().catch(console.error);
