# 📈 Technical Analysis Tracking System

Comprehensive web application for technical analysis with 40+ indicators and automated trading signals.

## 🚀 Features

- **📊 40+ Technical Indicators**: Comprehensive analysis including Ichimoku, MA, EMA, RSI, MACD, and more
- **🎯 Automated Trading Signals**: Buy/Sell/Neutral signals based on proven technical analysis rules
- **📅 Date Selection**: Choose specific analysis dates with trading day validation
- **🏢 Multi-Sector Analysis**: Filter and analyze stocks by sector
- **📥 Export Functionality**: Download results as CSV or Excel with formatting
- **🖥️ Multipage Interface**: Expandable structure for future features

## 📋 Technical Indicators Covered

### 📈 Trend Indicators
- Ichimoku Cloud (9,26,52,26) - Base Line, Conversion Line, Leading Span A & B
- Simple Moving Averages (10, 20, 30, 50, 100, 200)
- Exponential Moving Averages (10, 13, 20, 30, 50, 100, 200)
- Volume Weighted Moving Average (20)
- Hull Moving Average (9)

### 📊 Momentum Oscillators
- RSI (14)
- Stochastic (14,3,3) - %K and %D
- Stochastic RSI (3,3,14,14) - K and D
- CCI (20)
- ADX (14)
- Williams %R (14)
- Ultimate Oscillator (7,14,28)

### ⚡ Other Indicators
- MACD (12,26) - Level and Signal
- Awesome Oscillator
- Momentum (10)
- Bull Bear Power (13)
- Directional Movement Index (14) - Positive and Negative
- Previous day comparisons for all oscillators

## 🛠️ Installation

1. **Clone or download the project**
2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Ensure your stock list file exists:**
   - File: `TA_Tracking_List.csv`
   - Format: `Sector,Ticker,Exchange`

## 🚀 Usage

1. **Start the application:**
   ```bash
   streamlit run main.py
   ```

2. **Navigate to Summary Table:**
   - Click "📊 Summary Table" in the sidebar

3. **Configure Analysis:**
   - **Select Date**: Choose analysis date (defaults to last trading day)
   - **Filter by Sector**: Choose specific sector or "All"
   - **Select Stocks**: Pick stocks for analysis

4. **Run Analysis:**
   - Click "🔄 Run Analysis"
   - Wait for indicators calculation and signal evaluation

5. **Export Results:**
   - Download as CSV or Excel
   - Results include color-coded signals

## 📂 Project Structure

```
ta-tracking/
├── main.py                    # Main entry point
├── requirements.txt           # Dependencies
├── TA_Tracking_List.csv      # Stock list
├── src/
│   ├── data_fetcher.py       # Yahoo Finance integration
│   ├── indicators/
│   │   ├── calculator.py     # Technical indicators
│   │   └── signals.py        # Signal evaluation
│   └── utils/
│       ├── stock_loader.py   # CSV stock list loader
│       └── export_utils.py   # Export functionality
├── pages/
│   ├── 1_📊_Summary_Table.py # Main analysis page
│   └── 2_📈_Charts.py        # Future charts (placeholder)
└── README.md
```

## 🎯 Trading Signal Rules

### Moving Average Signals
- **Buy**: Price > MA
- **Sell**: Price < MA
- **Neutral**: Price = MA

### Ichimoku Signals
- **Buy**: Leading Span A > Leading Span B AND Base Line > Leading Span A AND Conversion Line > Base Line AND Price > Conversion Line
- **Sell**: Opposite conditions

### Oscillator Signals
- **RSI**: Buy when RSI < 30 and rising, Sell when RSI > 70 and falling
- **Stochastic**: Buy when %K and %D < 20 and %K > %D, Sell when %K and %D > 80 and %K < %D
- **MACD**: Buy when MACD > Signal line, Sell when MACD < Signal line

## 📊 Data Sources

- **Stock Data**: Yahoo Finance API
- **Stock List**: `TA_Tracking_List.csv` (Sector, Ticker, Exchange format)
- **Indicators**: TA-Lib Python library

## 🔧 Configuration

### Stock List Format
CSV file with columns:
- `Sector`: Stock sector (e.g., "CK", "BDS")
- `Ticker`: Stock symbol (e.g., "SSI", "VND")
- `Exchange`: Trading exchange (e.g., "HOSE", "HNX")

### Date Handling
- Automatic weekend adjustment
- Trading day validation
- Vietnam market timezone support

## 🚀 Future Enhancements

- **📈 Interactive Charts**: Candlestick charts with indicator overlays
- **📊 Portfolio Analysis**: Multi-stock comparative analysis
- **🔄 Real-time Updates**: Live data streaming
- **📱 Mobile Optimization**: Responsive design improvements
- **🎨 Custom Indicators**: User-defined technical indicators

## 🐛 Troubleshooting

### Common Issues

1. **"No data found" errors:**
   - Check internet connection
   - Verify ticker symbols in CSV file
   - Ensure selected date is a trading day

2. **Import errors:**
   - Run `pip install -r requirements.txt`
   - Check Python version compatibility (3.8+)

3. **CSV file not found:**
   - Ensure `TA_Tracking_List.csv` exists in project root
   - Check file format matches requirements

## 📝 License

This project is for educational and analysis purposes. Please ensure compliance with data provider terms of service.

## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Commit changes
4. Submit pull request

## 📞 Support

For technical support or feature requests, please create an issue in the project repository.

---

**Built with ❤️ using Streamlit | Data provided by Yahoo Finance**