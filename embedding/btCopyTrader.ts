import { cleanedClient } from "../cleandb"; // adjust path as needed
import { PgVector } from "@mastra/pg";
import { embedMany } from "ai";
import { google } from "@ai-sdk/google";
import dotenv from "dotenv";

dotenv.config();

// Setup pgvector connection
const pgVector = new PgVector({
  connectionString: process.env.CLEANDB_CONNECTION_STRING!,
});

async function embedProcessedBTCopyTrader() {
  try {
    await cleanedClient.connect();
    console.log("Connected to Cleaned DB ✅");

    // Fetch rows from ProcessedBTCopyTrader
    const res = await cleanedClient.query("SELECT * FROM ProcessedBTCopyTrader");
    const rows = res.rows;

    // Create pgvector index if needed
    try {
      await pgVector.createIndex({
        indexName: "processed_bt_copy_trader_embedding_index",
        dimension: 1536, // text-embedding-004 dimension
      });
    } catch (e) {
      console.log("ℹ️ Vector index may already exist.");
    }

    for (const row of rows) {
      // Construct descriptive text summarising the trade
      const text = `BTC Copy Trader transaction on ${row.timestamp}: ${row.side} by trader ${row.traderaddress}.
        Input: ${row.amountin} ${row.tokenin}, Output: ${row.amountout} ${row.tokenout}.
        Executed on DEX ${row.dex}, transaction hash: ${row.txhash}.`;

      // Skip empty text
      if (!text.trim()) {
        console.log(`⚠️ Skipped empty text for trade id ${row.id}`);
        continue;
      }

      try {
        // Get embedding
        const { embeddings } = await embedMany({
          model: google.textEmbeddingModel("text-embedding-004"),
          values: [text],
        });

        // Insert into EmbeddedBTCopyTrader table
        await cleanedClient.query(
          `INSERT INTO EmbeddedBTCopyTrader (trade_id, text, embedding, metadata)
           VALUES ($1, $2, $3, $4)`,
          [
            row.id,
            text,
            embeddings[0], // single embedding vector
            JSON.stringify({
              timestamp: row.timestamp,
              side: row.side,
              traderAddress: row.traderaddress,
              amountIn: row.amountin,
              tokenIn: row.tokenin,
              amountOut: row.amountout,
              tokenOut: row.tokenout,
              dex: row.dex,
              txHash: row.txhash,
            }),
          ]
        );

        console.log(`✅ Embedded & saved BTC Copy Trader trade id ${row.id}`);
      } catch (err) {
        console.error(`❌ Embedding failed for BTC Copy Trader trade id ${row.id}:`, err);
      }
    }
  } catch (err) {
    console.error("❌ Error in embedding pipeline:", err);
  } finally {
    await cleanedClient.end();
    console.log("🔒 Cleaned DB connection closed.");
  }
}

// Run it
embedProcessedBTCopyTrader().catch(console.error);
