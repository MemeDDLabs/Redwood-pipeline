# Processing & Pipeline Scripts

This section explains all the scripts used for cleaning, processing, and embedding data to support the Looker Studio dashboards. Scripts are organized by directory and purpose.

---

## 📁 /Cleaning/

Scripts for cleaning and preprocessing raw data before inserting it into the cleaned Looker Studio database.

- `cleanArbOpportunity.ts`: Cleans raw arbitrage opportunity data and prepares it for embedding or analysis.
- `cleanArbOpportunityHistory.ts`: Processes historical records of arbitrage opportunities.
- `cleanBTCopyTrader.ts`: Placeholder script created for when BTCopyTrader data becomes available. No data is available yet.
- `cleanBTSCoinInfo.ts`: Cleans BTS-related token info.
- `cleanBTSFailedCoinInfo.ts`: Cleans metadata related to failed sniper transactions.
- `cleanBTSBundle.ts`: Cleans bundled sniper token data.
- `fetchAndInsertArbTransactionMeta.ts`: Loads metadata about arbitrage transactions into the cleaned database.

---

## 📁 /embedding/

Scripts that generate vector embeddings for LLM-based semantic search, using models such as `text-embedding-004`. These are all placeholder scripts. The data has changed significantly and is expected to continue changing. As a result, these scripts will need to be updated.

- `arbOpportunity.ts`: Embeds cleaned arbitrage opportunity data.
- `arbOpportunityHistory.ts`: Embeds historical arbitrage opportunities.
- `arbTransactions.ts`: Embeds individual arbitrage transactions.
- `btCopyTrader.ts`: Present for future use. No BTCopyTrader data is available yet.
- `btsCoinInfo.ts`: Embeds sniper token metadata.
- `btsTransaction.ts`: Embeds BTS (sniper) transaction data.

---

## 📁 /Processing/

Scripts responsible for inserting, transforming, and enriching data in the cleaned database used by Looker Studio. These are the main scripts used to process data used in Looker Studio.

- `Arb.ts`: Inserts processed arbitrage trades into the `ProcessedArb` table. This script is currently outdated and should be rewritten once new data becomes available.
- `ArbMeta.ts`: Processes metadata about arbitrage transactions.
- `BTCopyTrader.ts`: Placeholder for processing BTCopyTrader data. No data available yet.
- `BTS.ts`: Main processing script for sniper bot trades. Pairs `buy`/`sell` and `partial_sell` transactions, calculates various things such as `dollarProfit`, and determines `win/loss` outcome.
- `BTSBundle.ts`: Processes sniper bundles. These are used in the Failed Sniper Dashboard.
- `BTSFailedCoinInfo.ts`: Processes metadata for sniper trades that failed to execute. Also used in the Failed Sniper Dashboard.
- `BTCoinAfterEight.ts`: Loads BTCoin data added after a specific timestamp. Also used in the Failed Sniper Dashboard.

---

## 📝 Notes

- The sniper bot (BTS) is the most complete and actively maintained data source.
- The arbitrage bot (Arb) currently has no new data. Old data is used for testing purposes only.
- The `Arb.ts` script is outdated and should be rewritten when new Arb data becomes available.
- Buy and sell transactions in `ProcessedBTS` are paired using the `tokenAddress`.
- Calculated fields such as `dollarProfit`, `win_loss`, and holding duration are added during the processing stage (e.g., in `BTS.ts`).
- Cleaning scripts mostly just move data from the raw database to the cleaned database.
- Processing scripts enrich the data with logic, calculations, and formatting needed for dashboards.
- The `pipeline_tracker` table is used by each script to track either the `last_processed_id` or `last_processed_ts` for incremental data loading.

**Views supporting Looker Studio:**

- `ProcessedBTS_WithCoinInfo` – Used in the Sniper Bot Dashboard.
- `FailedBTSTransactions` – Joins `BTCoinAfterEight`, `BTSFailedCoinInfo`, and `BTSBundle`. Will be used for Failed Sniper Bot dashboard.

---

##  Database Structure

### Main Tables and Views

- `ProcessedBTS`: Cleaned and processed sniper bot trades (includes `buy`/`sell`/`partial_sell` pairs and other calculated fields).
- `ProcessedBTS_WithCoinInfo`: A view that joins `ProcessedBTS` with `CleanBTSCoinInfo` on `tokenAddress`. Used in the Sniper Bot Dashboard.
- `FailedBTSTransactions`: A view that joins `BTCoinAfterEight`, `BTSFailedCoinInfo`, and `BTSBundle`. Will be used for Failed Sniper Bot dashboard.
- `CleanBTSCoinInfo`: Cleaned metadata about each token: liquidity ratios, dev holdings, token supply, and other useful metrics.
- `BTSFailedCoinInfo`: Metadata for failed sniper transactions.
- `BTSBundle`: Info about tokens launched as part of a bundle.
- `ProcessedArb`: Cleaned arbitrage bot trade data. Currently outdated due to lack of new input data.
- `ArbTransactionMeta`: Metadata about arbitrage transactions. Useful for future expansion.
- `CleanArbOpportunity`: Cleaned arbitrage opportunities, prepared for embedding and dashboard use.
- `CleanArbOpportunityHistory`: Historical snapshots of arbitrage opportunities.
- `BTCoinAfterEight`: BTCoin data that came in after a specific timestamp, used in the Failed Sniper Dashboard view.
- `pipeline_tracker`: A tracking table used to store the latest processed `id` or `timestamp` for each script. Prevents duplicate processing and supports incremental loads.
