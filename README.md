# Project Handover: Trading Dashboard
This document summarizes the dashboards developed using Looker Studio. It is designed to help the next person continue working with minimal disruption.

## 📊 Looker Studio Dashboards

### Bot Dashboard

Link: https://lookerstudio.google.com/reporting/157892b8-84e0-44ee-bbbe-98dbc2008954

Description:
Contains the bot performance report across three pages:

1) Arbitrage Bot

2) Sniper Bot

3) Failed Sniper Bot (in progress)

Each has a table that contains links that redirect users to the corresponding Arb Info or BTS Info dashboards, filtered by id (for Arb) or tokenAddress (for Sniper). These destination dashboards are in separate reports for restricted access.

### Arb Info Dashboard

Link: https://lookerstudio.google.com/reporting/b646d0f7-7ca6-4929-8b8d-d2c60bffe015

Description: Provides detailed information on a specific arbitrage transaction selected by the user from the Bot Dashboard.

### BTS Info Dashboard

Link: https://lookerstudio.google.com/reporting/e28e15dd-18ae-4d53-980d-4864d71f093f

Description: Provides detailed information on a specific sniper transaction selected by the user from the Bot Dashboard.

## 🧩 Parameters Used

HYPERLINK() formulas are used in the tables to navigate users to the corresponding Info pages.

These links apply dynamic filters — tokenAddress for the Sniper Bot and id for the Arb Bot — ensuring that each Info page displays data specific to the selected transaction.

## 📐 Custom Fields in Looker Studio (BTS Data)
These custom fields were created to enhance analysis and interactivity for the BTS (Sniper Bot) dashboard:

1) Holding Duration: Shows the time between buy and sell timestamps to reveal holding behavior.

2) Partial Sell: Flags whether a trade had a partial sell.

3) URL: ℹ️ icon links to filtered BTS Info page for that transaction.

4) Win Loss Sort Order: Numeric value to help sort WINs above LOSSES.

5) Wins: Binary counter for WIN trades.

6) Total Profit: Total profit across all sniper trades.

7) Win Rate: Percentage of winning trades.

## 📐 Custom Fields in Looker Studio (Arb Bot Data)
These fields support trade analysis and a better user experience in the Arbitrage Bot dashboard:

1) buy sell route: Combines buy and sell exchanges into one readable route.

2) Clickable Date: Clickable date linking to Arb Info page filtered by trade ID.

3) Losses: Binary counter for LOSS trades.

4) Token Pair: Summarizes traded tokens, e.g., SOL → USDC.

5) Win Loss Label: "WIN" or "LOSS" tag used for visuals and filters.

6) Wins: Binary counter for WIN trades.

7) Win Rate: Percentage of arbitrage trades that were wins.

8) Total Profit: Combined profit across all arbitrage trades.

## 📝 Notes
- Dashboards use read-only access to a dedicated Looker Studio Postgres database.

- Visuals include profit analysis, liquidity ratios, token supply, holding duration, partial sell indicators, and more.

- ⚠️ If you access the Arb Info or BTS Info pages directly without a filter applied, the dashboard may display incorrect or misleading data. To view these pages correctly, go to the Bot Dashboard and click on a row. This will redirect you with the correct filter applied.
