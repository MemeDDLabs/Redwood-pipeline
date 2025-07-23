import { client, connectDB } from '../db';
import { cleanedClient, connectCleanedDB } from '../cleandb';

// Main function: fetch raw, clean, then insert cleaned data
export async function fetchCleanAndInsertBTCopyTraderTransactionData() {
  await connectDB();
  await connectCleanedDB();

  // Query the original BTCopyTraderTransaction table
  const result = await client.query('SELECT * FROM "BTCopyTraderTransaction"');
  let rows = result.rows;

  // Process data: convert timestamp + uppercase side
  rows = convertTimestampsAndUppercaseSides(rows);

  // Insert in batches for efficiency
  const batchSize = 100;
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);

    const values: string[] = [];
    const params: (string | number | null)[] = [];

    batch.forEach((row, idx) => {
      const baseIndex = idx * 9; 

      values.push(
        `($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4}, $${baseIndex + 5}, $${baseIndex + 6}, $${baseIndex + 7}, $${baseIndex + 8}, $${baseIndex + 9})`
      );
      params.push(
        row.id,
        row.timestamp ? row.timestamp.toISOString() : null,
        row.side,
        row.traderAddress,
        row.amountIn,
        row.amountOut,
        row.dex,
        row.tokenIn,
        row.tokenOut,
        row.txHash
      );
    });

    const query = `
      INSERT INTO "CleanBTCopyTraderTransaction"
        ("id", "timestamp", "side", "traderAddress", "amountIn", "amountOut", "dex", "tokenIn", "tokenOut", "txHash")
      VALUES ${values.join(', ')}
      ON CONFLICT DO NOTHING
    `;

    await cleanedClient.query(query, params);
  }

  await client.end();
  await cleanedClient.end();
  console.log('✅ Cleaned data inserted into CleanBTCopyTraderTransaction');
}

// Convert timestamp strings to Date objects and ensure side is uppercase
function convertTimestampsAndUppercaseSides(data: any[]) {
  return data.map(row => ({
    ...row,
    timestamp: row.timestamp ? new Date(row.timestamp) : null,
    side: row.side ? row.side.toUpperCase() : row.side,
  }));
}

fetchCleanAndInsertBTCopyTraderTransactionData().catch(console.error);
