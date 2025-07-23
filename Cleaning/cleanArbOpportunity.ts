import { client, connectDB } from '../db';
import { cleanedClient, connectCleanedDB } from '../cleandb';
import crypto from 'crypto';

export async function fetchCleanAndInsertArbOpportunity() {
  await connectDB();
  await connectCleanedDB();

  // Ensure unique index exists on row_hash
  await cleanedClient.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_cleanarb_row_hash
    ON "CleanArbOpportunity"("row_hash");
  `);

  // Ensure tracker entry exists
  await cleanedClient.query(`
    INSERT INTO pipeline_tracker (table_name, last_processed_id)
    VALUES ('CleanArbOpportunity', 0)
    ON CONFLICT (table_name) DO NOTHING;
  `);

  // Get last processed ID
  const trackerRes = await cleanedClient.query(
    `SELECT last_processed_id FROM pipeline_tracker WHERE table_name = 'CleanArbOpportunity'`
  );
  const lastProcessedId = trackerRes.rows[0]?.last_processed_id || 0;

  // Fetch only new rows
  const result = await client.query(
    `SELECT * FROM "ArbOpportunity" WHERE id > $1 ORDER BY id ASC`,
    [lastProcessedId]
  );
  let rows = result.rows;

  if (rows.length === 0) {
    console.log('No new rows to process.');
    await client.end();
    await cleanedClient.end();
    return;
  }

  // Process and hash rows
  rows = convertTimestamps(rows);
  rows = removeNulls(rows);
  rows = removeDuplicates(rows);

  rows = rows.map(row => ({
    ...row,
    minExchange: capitalize(row.minExchange),
    maxExchange: capitalize(row.maxExchange),
    row_hash: generateRowHash(row)
  }));

  // Validate that every row has a row_hash
  for (const row of rows) {
    if (!row.row_hash) {
      throw new Error('❌ Row hash missing for: ' + JSON.stringify(row));
    }
  }

  // Insert in batches
  const batchSize = 100;
  let maxId = lastProcessedId;

  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    const values: string[] = [];
    const params: (string | number)[] = [];

    batch.forEach((row, idx) => {
      const baseIndex = idx * 7;

      values.push(
        `($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4}, $${baseIndex + 5}, $${baseIndex + 6}, $${baseIndex + 7})`
      );

      params.push(
        row.symbol,
        row.stableSymbol,
        row.minExchange,
        row.maxExchange,
        row.profit,
        row.timestamp.toISOString(),
        row.row_hash
      );

      if (row.id > maxId) {
        maxId = row.id;
      }
    });

    const insertQuery = `
      INSERT INTO "CleanArbOpportunity"
        ("symbol", "stableSymbol", "minExchange", "maxExchange", "profit", "timestamp", "row_hash")
      VALUES ${values.join(', ')}
      ON CONFLICT ON CONSTRAINT unique_row_hash_cleanarb DO NOTHING;
    `;

    const updateTrackerQuery = `
      UPDATE pipeline_tracker
      SET last_processed_id = $1
      WHERE table_name = 'CleanArbOpportunity';
    `;
    const updateParams = [maxId];

    // Transaction: insert batch and update tracker
    try {
      await cleanedClient.query('BEGIN');
      await cleanedClient.query(insertQuery, params);
      if (maxId > lastProcessedId) {
        await cleanedClient.query(updateTrackerQuery, updateParams);
        console.log(`✅ Tracker updated to ID ${maxId}`);
      }
      await cleanedClient.query('COMMIT');
      console.log(`✅ Inserted batch of ${batch.length} rows`);
    } catch (err) {
      await cleanedClient.query('ROLLBACK');
      console.error('❌ Transaction failed. Batch not inserted or tracker not updated:', err);
      throw err;
    }
  }

  await client.end();
  await cleanedClient.end();
  console.log('✅ Cleaned data inserted into CleanArbOpportunity');
}

// Helpers
function convertTimestamps(data: any[]) {
  return data.map(row => ({
    ...row,
    timestamp: new Date(row.timestamp),
  }));
}

function removeNulls(data: any[]) {
  return data.filter(row =>
    row.symbol !== null &&
    row.stableSymbol !== null &&
    row.minExchange !== null &&
    row.maxExchange !== null &&
    row.profit !== null &&
    row.timestamp !== null
  );
}

function removeDuplicates(data: any[]) {
  const seen = new Set();
  return data.filter(row => {
    const key = JSON.stringify({
      symbol: row.symbol,
      stableSymbol: row.stableSymbol,
      minExchange: row.minExchange,
      maxExchange: row.maxExchange,
      profit: row.profit,
      timestamp: row.timestamp.toISOString(),
    });

    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function capitalize(value: string): string {
  return value ? value.charAt(0).toUpperCase() + value.slice(1) : value;
}

function generateRowHash(row: any): string {
  const rowString = [
    row.symbol,
    row.stableSymbol,
    row.minExchange,
    row.maxExchange,
    row.profit,
    row.timestamp.toISOString()
  ].join('|');

  return crypto.createHash('sha256').update(rowString).digest('hex');
}

// Run
fetchCleanAndInsertArbOpportunity().catch(console.error);
