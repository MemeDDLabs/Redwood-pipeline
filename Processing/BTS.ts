import { client, connectDB } from '../db';
import { cleanedClient, connectCleanedDB } from '../cleandb';
import Decimal from 'decimal.js';
import axios from 'axios';
import pLimit from 'p-limit';
import axiosRetry from 'axios-retry';
import dotenv from 'dotenv';
import crypto from 'crypto';

axiosRetry(axios, {
  retries: 3,
  retryDelay: axiosRetry.exponentialDelay,
  retryCondition: (error) => axiosRetry.isNetworkError(error) || axiosRetry.isRetryableError(error),
});

interface BTSTransaction {
  id: number;
  timestamp: Date | string | null;
  amount: string;
  price: string;
  walletAddress: string;
  tokenAddress: string;
  BTSCoinInfoId: number;
  type: string;
  amountInDollars: string;
}

async function processBTSTransactionData() {
  try {
    console.log(" Connecting to databases...");
    await connectDB();
    await connectCleanedDB();

    await cleanedClient.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_processedbts_row_hash
      ON "ProcessedBTS" ("row_hash");
    `);

    await cleanedClient.query(`
      INSERT INTO pipeline_tracker (table_name, last_processed_id)
      VALUES ('ProcessedBTS', 0)
      ON CONFLICT (table_name) DO NOTHING;
    `);

    console.log("Starting paginated processing (no LIMIT)...");
    const batchSize = 5000;
    let lastId = (
      await cleanedClient.query(`SELECT last_processed_id FROM pipeline_tracker WHERE table_name = 'ProcessedBTS'`)
    ).rows[0]?.last_processed_id || 0;

    const limit = pLimit(10);

    while (true) {
      console.log(` Fetching rows with id > ${lastId}, batch size ${batchSize}...`);
      const result = await client.query(
        `SELECT * FROM "BTSTransaction" WHERE id > $1 ORDER BY id ASC LIMIT $2`,
        [lastId, batchSize]
      );
      const rows = convertTimestamps(result.rows as BTSTransaction[]);

      if (rows.length === 0) {
        console.log(" All rows processed.");
        break;
      }

      const grouped = rows.reduce((acc, row) => {
        if (!row.tokenAddress) return acc;
        if (!acc[row.tokenAddress]) acc[row.tokenAddress] = [];
        acc[row.tokenAddress].push(row);
        return acc;
      }, {} as Record<string, BTSTransaction[]>);

      const pairedTradesPromises = Object.entries(grouped).map(([tokenAddress, trades]) =>
        limit(async () => {
          const buy = trades.find((t) => t.type?.toLowerCase() === 'buy');
          const sell = trades.find((t) => t.type?.toLowerCase() === 'sell');
          const partialSell = trades.find((t) => t.type?.toLowerCase() === 'partial_sell');

          if (!buy || !sell) return null;

          const buyAmount = new Decimal(buy.amount);
          const buyAmountInDollars = new Decimal(buy.amountInDollars || '0');

          const partialAmount = new Decimal(partialSell?.amount || 0);
          const partialAmountDollars = new Decimal(partialSell?.amountInDollars || 0);

          const sellAmount = new Decimal(sell.amount);
          const sellAmountInDollars = new Decimal(sell.amountInDollars || 0);

          const totalSellAmount = partialAmount.plus(sellAmount);
          const totalSellAmountInDollars = partialAmountDollars.plus(sellAmountInDollars);

          const profit = totalSellAmount.minus(buyAmount).toDecimalPlaces(8);
          const dollarProfit = totalSellAmountInDollars.minus(buyAmountInDollars).toDecimalPlaces(8);
          const win_loss = dollarProfit.gt(0) ? 'WIN' : 'LOSS';

          let symbol = null;
          let name = null;

          try {
            const baseUrl = process.env.COIN_INFO_API_URL;
            const apiRes = await axios.get(`${baseUrl}/${tokenAddress}/0`);
            symbol = apiRes.data.token?.metadata?.symbol || null;
            name = apiRes.data.token?.metadata?.name || null;
          } catch (apiErr: any) {
            console.error(`API error for ${tokenAddress}:`, apiErr.message);
          }

          if (!name || !symbol) {
            console.warn(`Skipping ${tokenAddress} due to missing name/symbol`);
            return null;
          }

          const row = {
            tokenAddress,
            buy_amount: buyAmount.toString(),
            buy_price: buy.price,
            buy_walletAddress: buy.walletAddress,
            buy_timestamp: buy.timestamp ? new Date(buy.timestamp).toISOString() : null,
            buy_amountInDollars: buyAmountInDollars.toString(),
            partial_sell_amount: partialSell?.amount || null,
            partial_sell_price: partialSell?.price || null,
            partial_sell_walletAddress: partialSell?.walletAddress || null,
            partial_sell_amountInDollars: partialSell?.amountInDollars || null,
            partial_sell_timestamp: partialSell?.timestamp ? new Date(partialSell.timestamp).toISOString() : null,
            partial_sell_botId: partialSell?.BTSCoinInfoId || null,
            sell_amount: totalSellAmount.toString(),
            sell_price: sell.price,
            sell_walletAddress: sell.walletAddress,
            sell_timestamp: sell.timestamp ? new Date(sell.timestamp as string | Date).toISOString() : null,
            sell_amountInDollars: totalSellAmountInDollars.toString(),
            dollarProfit: dollarProfit.toString(),
            profit: profit.toString(),
            win_loss,
            symbol,
            name,
            BTSCoinInfoId: buy.BTSCoinInfoId
          };

          return {
            ...row,
            row_hash: generateRowHash(row),
            lastSourceId: Math.max(buy.id, sell.id, partialSell?.id || 0)
          };
        })
      );

      const pairedTrades = (await Promise.all(pairedTradesPromises)).filter(trade => trade !== null);

      for (const row of pairedTrades) {
        if (!row!.row_hash) throw new Error('❌ Missing row_hash in row: ' + JSON.stringify(row));
      }

      if (pairedTrades.length > 0) {
        console.log(` Inserting ${pairedTrades.length} paired trades into ProcessedBTS...`);

        const insertQuery = `
          INSERT INTO "public"."ProcessedBTS"
          (
            "tokenAddress",
            "buy_amount", "buy_price", "buy_walletAddress", "buy_timestamp", "buy_amountInDollars",
            "partial_sell_amount", "partial_sell_price", "partial_sell_walletAddress", "partial_sell_amountInDollars", "partial_sell_timestamp", "partial_sell_botId",
            "sell_amount", "sell_price", "sell_walletAddress", "sell_timestamp", "sell_amountInDollars",
            "dollarProfit", "profit", "win_loss", "symbol", "name", "BTSCoinInfoId", "row_hash"
          )
          VALUES ${pairedTrades.map((_, i) => `(
            $${i * 24 + 1},$${i * 24 + 2},$${i * 24 + 3},$${i * 24 + 4},$${i * 24 + 5},$${i * 24 + 6},
            $${i * 24 + 7},$${i * 24 + 8},$${i * 24 + 9},$${i * 24 + 10},$${i * 24 + 11},$${i * 24 + 12},
            $${i * 24 + 13},$${i * 24 + 14},$${i * 24 + 15},$${i * 24 + 16},$${i * 24 + 17},
            $${i * 24 + 18},$${i * 24 + 19},$${i * 24 + 20},$${i * 24 + 21},$${i * 24 + 22},$${i * 24 + 23},$${i * 24 + 24}
          )`).join(",")}
          ON CONFLICT (row_hash) DO NOTHING;
        `;

        const insertValues = pairedTrades.flatMap(trade => [
          trade.tokenAddress,
          trade.buy_amount, trade.buy_price, trade.buy_walletAddress, trade.buy_timestamp, trade.buy_amountInDollars,
          trade.partial_sell_amount, trade.partial_sell_price, trade.partial_sell_walletAddress, trade.partial_sell_amountInDollars, trade.partial_sell_timestamp, trade.partial_sell_botId,
          trade.sell_amount, trade.sell_price, trade.sell_walletAddress, trade.sell_timestamp, trade.sell_amountInDollars,
          trade.dollarProfit, trade.profit, trade.win_loss, trade.symbol, trade.name, trade.BTSCoinInfoId,
          trade.row_hash
        ]);

        const maxProcessedId = Math.max(...pairedTrades.map(r => r.lastSourceId ?? 0));

        try {
          await cleanedClient.query('BEGIN');
          await cleanedClient.query(insertQuery, insertValues);
          await cleanedClient.query(`
            UPDATE pipeline_tracker
            SET last_processed_id = $1
            WHERE table_name = 'ProcessedBTS';
          `, [maxProcessedId]);
          await cleanedClient.query('COMMIT');
          console.log(`✅ Inserted and tracker updated to ID ${maxProcessedId}`);
        } catch (err) {
          await cleanedClient.query('ROLLBACK');
          console.error('❌ Transaction failed:', err);
          throw err;
        }

        lastId = maxProcessedId;
      } else {
        lastId = rows[rows.length - 1].id;
      }

      console.log(` Batch up to id ${lastId} processed and inserted.`);
    }

    console.log('✅ All data paginated, processed, and inserted successfully.');

  } catch (err: any) {
    console.error('❌ Error in BTS pipeline:', err.message);
  } finally {
    await client.end();
    await cleanedClient.end();
  }
}

function convertTimestamps(data: BTSTransaction[]): BTSTransaction[] {
  return data.map(row => ({
    ...row,
    timestamp: row.timestamp ? new Date(row.timestamp) : null,
  }));
}

function generateRowHash(row: Record<string, any>): string {
  const rowString = [
    row.tokenAddress,
    row.buy_amount,
    row.buy_price,
    row.buy_walletAddress,
    row.buy_timestamp,
    row.sell_amount,
    row.sell_price,
    row.sell_walletAddress,
    row.sell_timestamp,
    row.profit,
    row.symbol,
    row.name
  ].join('|');

  return crypto.createHash('sha256').update(rowString).digest('hex');
}

processBTSTransactionData().catch(console.error);