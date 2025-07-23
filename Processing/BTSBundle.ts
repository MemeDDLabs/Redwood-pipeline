import { client, connectDB } from '../db';
import { cleanedClient, connectCleanedDB } from '../cleandb';
import crypto from 'crypto';

const TABLE_NAME = 'BTSBundle';

export async function insertBTSBundle() {
  await connectDB();
  await connectCleanedDB();

  await cleanedClient.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS "idx_btsbundle_row_hash"
    ON "BTSBundle" ("row_hash");
  `);

  const trackerResult = await cleanedClient.query(
    `SELECT "last_processed_ts" FROM "pipeline_tracker" WHERE "table_name" = $1`,
    [TABLE_NAME]
  );
  const lastProcessedTs = trackerResult.rows[0]?.last_processed_ts ?? new Date(0);

  const result = await client.query(
    `SELECT * FROM "BTSBundle" WHERE "dateCaptured" > $1 ORDER BY "dateCaptured" ASC`,
    [lastProcessedTs]
  );
  const rows = result.rows;

  if (rows.length === 0) {
    console.log("No new rows.");
    await client.end();
    await cleanedClient.end();
    return;
  }

  const allColumns = [
    "id", "confidence", "reasons", "suspiciousWallets", "timeClustering",
    "similarAmounts", "freshWallets", "coordinatedBehavior", "totalBuyers",
    "suspiciousBuyers", "tokenAddress", "dateCaptured", "isBundle"
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
      INSERT INTO "BTSBundle" (${insertColumns.map(c => `"${c}"`).join(', ')})
      VALUES ${values.join(', ')}
      ON CONFLICT ("row_hash") DO NOTHING;
    `, params);

    console.log(`✅ Inserted batch ${i / batchSize + 1} of ${Math.ceil(hashedRows.length / batchSize)}`);
  }

  const maxTs = new Date(Math.max(...hashedRows.map(r => new Date(r.dateCaptured).getTime())));
  await cleanedClient.query(`
    INSERT INTO "pipeline_tracker" ("table_name", "last_processed_ts")
    VALUES ($1, $2)
    ON CONFLICT ("table_name") DO UPDATE SET "last_processed_ts" = EXCLUDED."last_processed_ts";
  `, [TABLE_NAME, maxTs]);

  await client.end();
  await cleanedClient.end();
  console.log(`✅ BTSBundle: ${hashedRows.length} rows inserted. Last TS: ${maxTs.toISOString()}`);
}

function generateRowHash(row: Record<string, any>, columns: string[]) {
  return crypto.createHash('sha256').update(
    columns.map(c => Array.isArray(row[c]) ? row[c].join(',') : String(row[c] ?? '')).join('|')
  ).digest('hex');
}

insertBTSBundle().catch(console.error);
