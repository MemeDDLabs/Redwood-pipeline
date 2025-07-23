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

async function embedProcessedArb() {
  try {
    await cleanedClient.connect();
    console.log("Connected to Cleaned DB ✅");

    // Fetch rows from ProcessedArb
    const res = await cleanedClient.query("SELECT * FROM ProcessedArb");
    const rows = res.rows;

    // Create pgvector index if needed
    try {
      await pgVector.createIndex({
        indexName: "processed_arb_embedding_index",
        dimension: 1536, // text-embedding-004 dimension
      });
    } catch (e) {
      console.log("Vector index may already exist.");
    }

    for (const row of rows) {
      // Construct descriptive text summarising buy/sell arbitrage details
      const text = `Arbitrage transaction traded on ${row.datetraded}.
        Buy: ${row.buyBase}/${row.buyQuote} on ${row.buyExchange}, volume ${row.buyVolume}, vwap ${row.buyVwap}, fee ${row.buyFee}.
        Sell: ${row.sellBase}/${row.sellQuote} on ${row.sellExchange}, volume ${row.sellVolume}, vwap ${row.sellVwap}, fee ${row.sellFee}.
        Profit: ${row.idealProfit}, dollar amount: ${row.dollarAmount}, result: ${row.win_loss}.`;

      // Skip empty text
      if (!text.trim()) {
        console.log(` Skipped empty text for transaction id ${row.id}`);
        continue;
      }

      try {
        // Get embedding
        const { embeddings } = await embedMany({
          model: google.textEmbeddingModel("text-embedding-004"),
          values: [text],
        });

        // Insert into EmbeddedArbTransaction table
        await cleanedClient.query(
          `INSERT INTO EmbeddedArbTransaction (transaction_id, text, embedding, metadata)
           VALUES ($1, $2, $3, $4)`,
          [
            row.id,
            text,
            embeddings[0], // single embedding vector
            JSON.stringify({
              buy: {
                base: row.buyBase,
                quote: row.buyQuote,
                exchange: row.buyExchange,
                volume: row.buyVolume,
                vwap: row.buyVwap,
                fee: row.buyFee,
              },
              sell: {
                base: row.sellBase,
                quote: row.sellQuote,
                exchange: row.sellExchange,
                volume: row.sellVolume,
                vwap: row.sellVwap,
                fee: row.sellFee,
              },
              dateTraded: row.datetraded,
              idealProfit: row.idealProfit,
              dollarAmount: row.dollarAmount,
              win_loss: row.win_loss,
            }),
          ]
        );

        console.log(`✅ Embedded & saved ProcessedArb transaction id ${row.id}`);
      } catch (err) {
        console.error(`❌ Embedding failed for ProcessedArb transaction id ${row.id}:`, err);
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
embedProcessedArb().catch(console.error);
