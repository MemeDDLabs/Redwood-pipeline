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

async function embedCleanBTSCoinInfo() {
  try {
    await cleanedClient.connect();
    console.log("Connected to Cleaned DB ✅");

    // Fetch rows from CleanBTSCoinInfo
    const res = await cleanedClient.query("SELECT * FROM CleanBTSCoinInfo");
    const rows = res.rows;

    // Create pgvector index if needed
    try {
      await pgVector.createIndex({
        indexName: "bts_coin_info_embedding_index",
        dimension: 1536, // text-embedding-004 dimension
      });
    } catch (e) {
      console.log("ℹ️ Vector index may already exist.");
    }

    for (const row of rows) {
      // Construct descriptive text
      const text = `Token ${row.tokenaddress} with coin price ${row.coinprice}, dev pubkey ${row.devpubkey}, dev capital ${row.devcapital}, dev holder percentage ${row.devholderpercentage}, token supply ${row.tokensupply}, total holders supply ${row.totalholderssupply}`;

      // Skip empty text
      if (!text.trim()) {
        console.log(`⚠️ Skipped empty text for coin info id ${row.id}`);
        continue;
      }

      try {
        // Get embedding
        const { embeddings } = await embedMany({
          model: google.textEmbeddingModel("text-embedding-004"),
          values: [text],
        });

        // Insert into EmbeddedBTSCoinInfo table
        await cleanedClient.query(
          `INSERT INTO EmbeddedBTSCoinInfo (coininfo_id, text, embedding, metadata)
           VALUES ($1, $2, $3, $4)`,
          [
            row.id,
            text,
            embeddings[0], // single embedding vector
            JSON.stringify({
              tokenAddress: row.tokenaddress,
              coinPrice: row.coinprice,
              devPubkey: row.devpubkey,
              devCapital: row.devcapital,
              devholderPercentage: row.devholderpercentage,
              tokenSupply: row.tokensupply,
              totalHoldersSupply: row.totalholderssupply,
            }),
          ]
        );

        console.log(`✅ Embedded & saved coin info id ${row.id}`);
      } catch (err) {
        console.error(`❌ Embedding failed for coin info id ${row.id}:`, err);
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
embedCleanBTSCoinInfo().catch(console.error);
