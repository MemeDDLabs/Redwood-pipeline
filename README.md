Project Handover: Trading Dashboard
This document summarizes the dashboards developed using Looker Studio. It is designed to help the next person continue working with minimal disruption.

📊 Looker Studio Dashboards
Dashboards Created
Dashboard Name	Link	Description
Bot Dashboard	Bot Dashboard	Contains the bot performance report across three pages: 1) Arbitrage Bot, 2) Sniper Bot, and 3) Failed Sniper Bot (in progress). Each table includes links that redirect users to the corresponding Arb Info or BTS Info dashboards, filtered by id (for Arb) or tokenAddress (for Sniper). These destination dashboards are in separate reports for restricted access.
Arb Info	Arb Info	Provides detailed information on a specific arbitrage transaction selected by the user from the Bot Dashboard.
BTS Info	BTS Info	Provides detailed information on a specific sniper transaction selected by the user from the Bot Dashboard.

🧩 Parameters Used
HYPERLINK() formulas are used in the tables to navigate users to the corresponding Info pages.

These links apply dynamic filters — tokenAddress for the Sniper Bot and id for the Arb Bot — ensuring that each Info page displays data specific to the selected transaction.

📐 Custom Fields in Looker Studio (BTS Data)
These custom fields were created to enhance analysis and interactivity for the BTS (Sniper Bot) dashboard:

Holding Duration: Time between token purchase and sale, showing trade speed and holding strategies.

Partial Sell: Indicates whether a partial sell occurred (✅ Yes or No).

URL: Clickable ℹ️ icon that links to the filtered BTS Info page.

Win Loss Sort Order: Enables sorting by win/loss status using numeric values.

Wins: Binary win counter (1 = win, 0 = loss) for win rate calculation.

Total Profit: Aggregates profit across all trades, used in visuals.

Win Rate: Percentage of trades that were wins.

📐 Custom Fields in Looker Studio (Arb Bot Data)
These fields were added to support deeper analysis and better UX in the Arbitrage Bot dashboard:

Buy/Sell Route: Combines buy and sell exchanges into a readable route (e.g., Raydium → Orca).

Clickable Date: Clickable date field linking to Arb Info filtered by trade ID.

Losses: Binary loss counter (1 = loss, 0 = win) for metrics and rate calculations.

Token Pair: Concise representation of the two tokens involved in the trade (e.g., SOL → USDC).

Win Loss Label: Displays "WIN" or "LOSS" for categorization and filtering.

Wins: Binary win counter (same logic as in BTS).

Win Rate: Win percentage based on total trades.

Total Profit: Aggregated dollar profit for visualizations and KPIs.

📌 Notes
Dashboards use read-only access to a dedicated Looker Studio Postgres DB.

Visuals include profit analysis, liquidity ratios, token supply, holding duration, partial sell flags, and more.

⚠️ Important: If you access the Arb Info or BTS Info pages directly without a filter applied, the dashboard may display incorrect or misleading data.
To view these pages as intended, go to the Bot Dashboard and click on a row. This will redirect you to the filtered Info page with the correct context applied.

