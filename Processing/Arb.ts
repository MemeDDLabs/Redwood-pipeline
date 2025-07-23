import { client, connectDB } from '../db';
import { cleanedClient, connectCleanedDB } from '../cleandb';
import crypto from 'crypto';

// ✅ Only include columns that are NOT expected or ideal
// ✅ Correct casing on 'buyStatusfee' and 'sellStatusfee'
const selectedColumns = [
  "id", "dateTime", "base", "quote",
  "buyExchange", "buyOrderId", "buyStatusId", "buyStatusfee",
  "buyStatusExecutedBase", "buyStatusExecutedQuote", "buyStatusVwap",
  "sellExchange", "sellOrderId", "sellStatusId", "sellStatusfee",
  "sellStatusExecutedBase", "sellStatusExecutedQuote", "sellStatusVwap",
  "deltaBase", "deltaQuote", "deltaBaseValue", "totalFees", "estimatedDeltaValue"
];

// Add 'rowHash' for deduplication
const insertColumns = [...selectedColumns, "rowHash"];

async function copyArbTransactionMeta() {
  try {
    await connectDB();
    await connectCleanedDB();

    const columnSQL = selectedColumns.map(col => `"${col}"`).join(', ');
    const { rows } = await client.query(`SELECT ${columnSQL} FROM "ArbTransactionMeta"`);

    if (rows.length === 0) {
      console.log('No rows to copy.');
      return;
    }

    const processedRows = rows.map(row => {
      const valuesToHash = selectedColumns.map(col => row[col]);
      const hash = crypto
        .createHash('sha256')
        .update(JSON.stringify(valuesToHash))
        .digest('hex');

      return { ...row, rowHash: hash };
    });

    const values: any[] = [];
    const valuePlaceholders = processedRows.map((row, rowIndex) => {
      const placeholders = insertColumns.map((_, colIndex) => `$${rowIndex * insertColumns.length + colIndex + 1}`);
      insertColumns.forEach(col => values.push(row[col]));
      return `(${placeholders.join(', ')})`;
    });

    const insertQuery = `
      INSERT INTO "ArbTransactionMeta" (${insertColumns.map(col => `"${col}"`).join(', ')})
      VALUES ${valuePlaceholders.join(', ')}
      ON CONFLICT ("rowHash") DO NOTHING;
    `;

    await cleanedClient.query(insertQuery, values);
    console.log(`✅ Inserted ${processedRows.length} rows (deduplicated by hash).`);
  } catch (err) {
    console.error('❌ Error copying ArbTransactionMeta:', err);
  } finally {
    await client.end();
    await cleanedClient.end();
  }
}

copyArbTransactionMeta();
