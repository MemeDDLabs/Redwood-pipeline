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

async function embedProcessedBTS() {
  try {
    await cleanedClient.connect();
    console.log("Connected to Cleaned DB ✅");

    // Fetch rows from ProcessedBTS
    const res = await cleanedClient.query("SELECT * FROM ProcessedBTS");
    const rows = res.rows;

    // Create pgvector index if needed
    try {
      await pgVector.createIndex({
        indexName: "processed_bts_embedding_index",
        dimension: 1536, // text-embedding-004 dimension
      });
    } catch (e) {
      console.log(" Vector index may already exist.");
    }

    for (const row of rows) {
      // Construct descriptive text summarising buy/sell details
      const text = `Processed BTS transaction for token ${row.tokenaddress} (${row.symbol}) named ${row.name}.
        Buy: ${row.buy_amount} at price ${row.buy_price} by wallet ${row.buy_walletaddress} on ${row.buy_timestamp}, totalling ${row.buyamountindollars} USD.
        Sell: ${row.sell_amount} at price ${row.sell_price} by wallet ${row.sell_walletaddress} on ${row.sell_timestamp}, totalling ${row.sell_amountindollars} USD.
        Dollar profit: ${row.dollarprofit}, overall profit: ${row.profit}, result: ${row.win_loss}.`;

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

        // Insert into EmbeddedBTSTransaction table
        await cleanedClient.query(
          `INSERT INTO EmbeddedBTSTransaction (transaction_id, text, embedding, metadata)
           VALUES ($1, $2, $3, $4)`,
          [
            row.id,
            text,
            embeddings[0], // single embedding vector
            JSON.stringify({
              tokenAddress: row.tokenaddress,
              symbol: row.symbol,
              name: row.name,
              buy: {
                amount: row.buy_amount,
                price: row.buy_price,
                walletAddress: row.buy_walletaddress,
                timestamp: row.buy_timestamp,
                amountInDollars: row.buyamountindollars,
              },
              sell: {
                amount: row.sell_amount,
                price: row.sell_price,
                walletAddress: row.sell_walletaddress,
                timestamp: row.sell_timestamp,
                amountInDollars: row.sell_amountindollars,
              },
              dollarProfit: row.dollarprofit,
              profit: row.profit,
              win_loss: row.win_loss,
              BTSCoinInfoId: row.btscoininfoid,
            }),
          ]
        );

        console.log(`✅ Embedded & saved Processed BTS transaction id ${row.id}`);
      } catch (err) {
        console.error(`❌ Embedding failed for Processed BTS transaction id ${row.id}:`, err);
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
embedProcessedBTS().catch(console.error);
