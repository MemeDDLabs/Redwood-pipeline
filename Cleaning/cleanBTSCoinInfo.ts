import { client, connectDB } from '../db';
import { cleanedClient, connectCleanedDB } from '../cleandb';
import crypto from 'crypto';

const BATCH_SIZE = 500;

async function copyBTSCoinInfo() {
  try {
    await connectDB();
    await connectCleanedDB();

    const result = await client.query(`SELECT * FROM "BTSCoinInfo"`);
    const rows = result.rows;

    if (rows.length === 0) {
      console.log('⚠️ No rows to copy from BTSCoinInfo.');
      return;
    }

    const allColumns = Object.keys(rows[0]).filter(
      col => col !== 'row_hash' && col.toLowerCase() !== 'datecaptured'
    );
    const insertColumns = [...allColumns, 'row_hash'];

    const processedRows = rows.map(row => {
      const valuesToHash = allColumns.map(col => row[col]);
      const hash = crypto
        .createHash('sha256')
        .update(JSON.stringify(valuesToHash))
        .digest('hex');
      return { ...row, row_hash: hash };
    });

    for (let i = 0; i < processedRows.length; i += BATCH_SIZE) {
      const batch = processedRows.slice(i, i + BATCH_SIZE);

      const values: any[] = [];
      const valuePlaceholders = batch.map((row, rowIndex) => {
        const placeholders = insertColumns.map(
          (_, colIndex) => `$${rowIndex * insertColumns.length + colIndex + 1}`
        );
        insertColumns.forEach(col => values.push(row[col]));
        return `(${placeholders.join(', ')})`;
      });

      const insertQuery = `
        INSERT INTO "CleanBTSCoinInfo" (${insertColumns.map(col => `"${col}"`).join(', ')})
        VALUES ${valuePlaceholders.join(', ')}
        ON CONFLICT ("row_hash") DO NOTHING;
      `;

      await cleanedClient.query(insertQuery, values);
      console.log(`✅ Inserted batch ${i / BATCH_SIZE + 1} (${batch.length} rows)`);
    }

    // Track progress in pipeline_tracker using last ID
    const lastId = rows[rows.length - 1]?.id;
    if (lastId !== undefined) {
      await cleanedClient.query(`
        INSERT INTO pipeline_tracker (table_name, last_processed_id)
        VALUES ($1, $2)
        ON CONFLICT (table_name)
        DO UPDATE SET last_processed_id = EXCLUDED.last_processed_id;
      `, ['CleanBTSCoinInfo', lastId]);

      console.log(`📌 Updated pipeline_tracker → CleanBTSCoinInfo: ID ${lastId}`);
    } else {
      console.warn('⚠️ No ID found in last row — pipeline_tracker not updated.');
    }

  } catch (err) {
    console.error('❌ Error copying BTSCoinInfo:', err);
  } finally {
    await client.end();
    await cleanedClient.end();
  }
}

copyBTSCoinInfo();
