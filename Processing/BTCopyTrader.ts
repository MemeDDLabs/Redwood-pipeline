import { cleanedClient, connectCleanedDB } from '../cleandb';
import Decimal from 'decimal.js';

async function processBTCopyTrader() {
  try {
    await connectCleanedDB();
    console.log("🔄 Processing BTCopyTrader bot...");

    const res = await cleanedClient.query(`
      SELECT * FROM "public"."CleanBTCopyTraderTransaction"
      ORDER BY "traderAddress", "timestamp";
    `);

    const rows = res.rows;
    const pairedTrades: any[] = [];

    // Group by traderAddress
    const grouped = rows.reduce((acc, row) => {
      const traderAddress = row.traderAddress;
      if (!traderAddress) return acc;

      if (!acc[traderAddress]) acc[traderAddress] = [];
      acc[traderAddress].push(row);
      return acc;
    }, {} as Record<string, any[]>);

    for (const [traderAddress, trades] of Object.entries(grouped)) {
      if (!trades || !Array.isArray(trades)) continue;

      // Find buy-sell pairs
      const buys = trades.filter(t => t.side === 'BUY');
      const sells = trades.filter(t => t.side === 'SELL');

      for (const buy of buys) {
        // Find matching sell where tokenIn of sell = tokenOut of buy
        const sell = sells.find(s => s.tokenIn === buy.tokenOut && new Decimal(s.amountIn).equals(new Decimal(buy.amountOut)));

        if (!sell) continue;

        const buyCost = new Decimal(buy.amountIn || '0');   // USDT spent to buy BTC
        const sellRevenue = new Decimal(sell.amountOut || '0'); // USDT received from selling BTC

        const profit = sellRevenue.minus(buyCost);
        const win_loss = profit.gt(0) ? 'WIN' : 'LOSS';

        pairedTrades.push({
          traderAddress,
          tokenBought: buy.tokenOut,
          amountBought: buy.amountOut,
          buy_cost: buyCost.toString(),
          sell_revenue: sellRevenue.toString(),
          profit: profit.toString(),
          win_loss,
          buy_timestamp: buy.timestamp,
          sell_timestamp: sell.timestamp
        });
      }
    }

    console.log(`✅ Processed ${pairedTrades.length} paired BTCopyTrader trades. Inserting into ProcessedBTCopyTrader...`);

    // Insert into ProcessedBTCopyTrader table
    for (const trade of pairedTrades) {
      await cleanedClient.query(`
        INSERT INTO "public"."ProcessedBTCopyTrader"
        ("traderAddress", "tokenBought", "amountBought", "buy_cost", "sell_revenue", "profit", "win_loss", "buy_timestamp", "sell_timestamp")
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
      `, [
        trade.traderAddress,
        trade.tokenBought,
        trade.amountBought,
        trade.buy_cost,
        trade.sell_revenue,
        trade.profit,
        trade.win_loss,
        trade.buy_timestamp,
        trade.sell_timestamp
      ]);
    }

    console.log(`✅ Inserted ${pairedTrades.length} trades into ProcessedBTCopyTrader.`);
    await cleanedClient.end();
  } catch (err) {
    console.error('❌ Error processing BTCopyTrader:', err);
    await cleanedClient.end();
  }
}

processBTCopyTrader();
