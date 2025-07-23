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

async function embedCleanArbOpportunityHistory() {
  try {
    await cleanedClient.connect();
    console.log("Connected to Cleaned DB ✅");

    // Fetch rows from CleanArbOpportunityHistory
    const res = await cleanedClient.query("SELECT * FROM CleanArbOpportunityHistory");
    const rows = res.rows;

    // Create pgvector index if needed
    try {
      await pgVector.createIndex({
        indexName: "arb_opportunity_history_embedding_index",
        dimension: 1536, // text-embedding-004 dimension
      });
    } catch (e) {
      console.log("ℹ️ Vector index may already exist.");
    }

    for (const row of rows) {
      // Construct descriptive text
      const text = `${row.symbol} arbitrage opportunity in ${row.stablesymbol}: buy from ${row.minexchange}, sell on ${row.maxexchange}, profit ${row.profit}, timestamp ${row.timestamp}`;

      // Skip empty text
      if (!text.trim()) {
        console.log(`⚠️ Skipped empty text for opportunity id ${row.id}`);
        continue;
      }

      try {
        // Get embedding
        const { embeddings } = await embedMany({
          model: google.textEmbeddingModel("text-embedding-004"),
          values: [text],
        });

        // Insert into EmbeddedArbOpportunityHistory table
        await cleanedClient.query(
          `INSERT INTO EmbeddedArbOpportunityHistory (opportunity_id, text, embedding, metadata)
           VALUES ($1, $2, $3, $4)`,
          [
            row.id,
            text,
            embeddings[0], // single embedding vector
            JSON.stringify({
              symbol: row.symbol,
              stableSymbol: row.stablesymbol,
              minExchange: row.minexchange,
              maxExchange: row.maxexchange,
              profit: row.profit,
              timestamp: row.timestamp,
            }),
          ]
        );

        console.log(`✅ Embedded & saved opportunity history id ${row.id}`);
      } catch (err) {
        console.error(`❌ Embedding failed for opportunity history id ${row.id}:`, err);
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
embedCleanArbOpportunityHistory().catch(console.error);
