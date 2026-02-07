# Binance_Data_Analysis
Utilized Binance API to retrieve historical data for perpetual futures contracts (Funding Rates, Open Interest, Long/Short Ratio). Performed visualization analysis to identify correlations between price and the above indicators.

Program Description:

1获取U本位合约数据binance.py
After the user enters the name of the perpetual futures contracts, the following data can be retrieved and saved as an Excel file: Open Interest, Open Value, Number of Large Traders (Long/Short Ratio), Number of Large Traders' Long/Short Positions (Long/Short Ratio), Total Market Long/Short Ratio, Price (Open, High, Low, Close) and Volume, Basis, Basis Rate, Funding Rate. 
Selectable time periods are: '5m', '15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d'.
Note: This program can only retrieve data for the most recent month because Binance only retains open interest and long/short ratio data for one month.

2画图.py
After entering the path to the Excel file in program 1, the following data can be plotted as a function of price: Open Interest, Number of Large Traders' Long/Short Ratio, Number of Large Traders' Long/Short Positions (Long/Short Ratio), Total Market Long/Short Ratio, Basis/Basis Rate, Funding Rate.
