import { client, connectDB } from '../db';
import { cleanedClient, connectCleanedDB } from '../cleandb';
import crypto from 'crypto';

export async function cleanArbOpportunityHistory() {
  await connectDB();
  await connectCleanedDB();

  // Ensure unique index exists on row_hash
  await cleanedClient.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_cleanarb_row_hash
    ON "CleanArbOpportunityHistory"("row_hash")
  `);

  // Get last processed ID from tracker (if exists)
  const trackerRes = await cleanedClient.query(
    `SELECT last_processed_id FROM pipeline_tracker WHERE table_name = 'CleanArbOpportunityHistory'`
  );
  const lastProcessedId = trackerRes.rows[0]?.last_processed_id || 0;

  const result = await client.query(
    `SELECT * FROM "ArbOpportunityHistory" WHERE id > $1 ORDER BY id ASC`,
    [lastProcessedId]
  );

  let rows = result.rows;
  if (rows.length === 0) {
    console.log('No new rows to process.');
    await client.end();
    await cleanedClient.end();
    return;
  }

  // Process
  rows = convertTimestamps(rows);
  rows = removeNulls(rows);
  rows = normaliseExchanges(rows);
  rows = removeDuplicates(rows);
  rows = rows.map(row => ({
    ...row,
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
      INSERT INTO "CleanArbOpportunityHistory"
        ("symbol", "stableSymbol", "minExchange", "maxExchange", "profit", "timestamp", "row_hash")
      VALUES ${values.join(', ')}
      ON CONFLICT ("row_hash") DO NOTHING;
    `;

    const lastTimestamp = batch[batch.length - 1].timestamp.toISOString();
    const updateTrackerQuery = `
      INSERT INTO pipeline_tracker (table_name, last_processed_id, last_processed_ts)
      VALUES ($1, $2, $3)
      ON CONFLICT (table_name) DO UPDATE SET
        last_processed_id = EXCLUDED.last_processed_id,
        last_processed_ts = EXCLUDED.last_processed_ts;
    `;
    const updateParams = ['CleanArbOpportunityHistory', maxId, lastTimestamp];

    // Insert batch + update tracker
    try {
      await cleanedClient.query('BEGIN');
      await cleanedClient.query(insertQuery, params);
      await cleanedClient.query(updateTrackerQuery, updateParams);
      await cleanedClient.query('COMMIT');
      console.log(`✅ Inserted batch and updated tracker to ID ${maxId}, TS ${lastTimestamp}`);
    } catch (err) {
      await cleanedClient.query('ROLLBACK');
      console.error('❌ Transaction failed. Batch not inserted or tracker not updated:', err);
      throw err;
    }
  }

  await client.end();
  await cleanedClient.end();
  console.log('✅ Cleaned data inserted into CleanArbOpportunityHistory');
}

// Utility functions
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

function normaliseExchanges(data: any[]) {
  return data.map(row => ({
    ...row,
    minExchange: normaliseExchangeName(row.minExchange, 'min'),
    maxExchange: normaliseExchangeName(row.maxExchange, 'max'),
  }));
}

function normaliseExchangeName(name: string, type: 'min' | 'max'): string {
  if (!name) return name;
  const lower = name.toLowerCase();
  if (type === 'min') {
    if (lower === 'blockchaincom') return 'Blockchain';
    if (lower === 'btcturk') return 'BtcTurk';
  }
  if (type === 'max') {
    if (lower === 'btcturk') return 'BtcTurk';
  }
  return name.charAt(0).toUpperCase() + name.slice(1);
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

cleanArbOpportunityHistory().catch(console.error);
