# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick Start Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the application
streamlit run main.py

# Test vnstock integration
python -c "from src.vnstock_fetcher import test_vnstock_connection; print(test_vnstock_connection())"

# Test VNMIDCAP Investing.com integration
python -c "from src.investing_fetcher import test_investing_connection; print(test_investing_connection())"
```

## Architecture Overview

This is a **Streamlit 2-page web application** for technical analysis of Vietnamese and US financial markets. The architecture follows a **simplified structure** with comprehensive indicator calculation and signal evaluation optimized for TradingView compatibility.

### Core Data Flow Architecture

1. **Data Source Router** (`src/data_fetcher.py`):
   - Routes Vietnamese symbols (stocks + VNINDEX) → vnstock API
   - Routes **VNMIDCAP exclusively** → Investing.com API (no fallback)
   - Routes US symbols/indices → Yahoo Finance API
   - Implements intelligent fallback between sources (except VNMIDCAP)

2. **Triple API Integration**:
   - **vnstock** (`src/vnstock_fetcher.py`): Vietnamese market data
     - TCBS source for stocks and VNINDEX
     - VCI source with fallback to TCBS
   - **Investing.com** (`src/investing_fetcher.py`): VNMIDCAP exclusive data source
     - Uses `investiny` Python library (investing_id='995069' for VN Mid Cap)
     - Historical data from 2015 to present (2,500+ records)
     - Date format: MM/DD/YYYY (US format)
     - Columns: Date, Open, High, Low, Close, Volume
     - 30-minute cache TTL for efficiency
   - **Yahoo Finance**: US indices and fallback for Vietnamese stocks

3. **Processing Pipeline**:
   - Stock selection → Data fetching → Indicator calculation → Signal evaluation → Export

### Technical Indicators System

**Calculation Engine** (`src/indicators/calculator.py`):
- Calculates 40+ technical indicators using the `ta` library with **TradingView compatibility**
- **Key TradingView Adjustments**:
  - StochRSI: Scale 0-100 (multiply by 100)
  - Momentum: Absolute difference (Close - Close[N])  
  - MACD: Scale x1000 for display compatibility
  - Bull/Bear Power: Scale x1000
  - Awesome Oscillator: Keep original scale (no multiplication)
- Includes Ichimoku Cloud (9,26,52,26), MA/EMA series, RSI, MACD, Stochastic, etc.
- Implements custom Hull MA calculation
- Handles previous-day comparisons for oscillators
- Date sorting ensures proper chronological calculations

**Signal Evaluation** (`src/indicators/signals.py`):
- Converts raw indicator values into Buy/Sell/Neutral signals
- Implements complex rules (e.g., Ichimoku multi-condition logic)
- **Signal Counting** (`src/utils/signal_counter.py`):
  - Oscillator signals: RSI, Stochastic, CCI, ADX, AO, Momentum, MACD, StochRSI, Williams %R, BBP, UO
  - Moving Average signals: All SMA/EMA, Ichimoku, VWMA, Hull MA
  - Provides buy/sell counts by category

### Vietnamese Market Specifics

**Key Mapping**: `VNMID` → `VNMIDCAP` (handled in `format_ticker_for_vnstock()`)

**Exchange Handling**:
- HOSE/HNX/UPCOM stocks use vnstock with `.VN` suffix fallback
- **VNINDEX** uses vnstock exclusively
- **VNMIDCAP** uses Investing.com exclusively (no vnstock fallback)
- Exchange validation ensures proper data source selection

**VNMIDCAP Investing.com Integration**:
- **Source**: Investing.com via `investiny` Python library
- **Investing ID**: `995069` (VN Mid Cap / VNIMC symbol)
- **Historical Coverage**: From 2015 to present (~2,500+ daily records)
- **Data Format**: Standard numeric format with US date (MM/DD/YYYY)
- **Columns**: Date, Open, High, Low, Close, Volume
- **Data Quality**: Daily updates, chronologically sorted, 100% accuracy verified
- **Cache**: 30-minute TTL (data updates once per day)
- **Error Handling**: No fallback - fails cleanly if Investing.com unavailable
- **Backup**: Google Sheets fetcher preserved as `google_sheets_simple.py.backup`

### Streamlit App Structure

**Main Page** (`main.py`):
- **New Sector Summary Table**: Modern HTML table with Vietnamese sector names
  - Variable top/bottom counts per sector (1-3 stocks depending on sector size)
  - Green/red background colors for Top cao điểm/Top thấp điểm columns
  - Breakthrough groups (±10 rating points) displayed with merged columns
  - Single table appearance with overflow handling for long text
- **AG-Grid Modern Table** with Balham light theme and fixed headers
- **Smart Refresh System**: Manual data loading with status indicators
- **Parallel Processing**: 15 concurrent workers with real-time progress
  - Progress bar updates smoothly from 0% → 100%
  - Displays current ticker being analyzed: "🚀 Analyzing {ticker}... ({X}/{total})"
  - Completion message: "✅ Completed analyzing {N} stocks!"
- **Historical Rating System**: Shows 3-day rating history (T, T-1, T-2)
- Date picker with intelligent trading day default
- **Scrollable Display**: Fixed height (600px) with sticky headers and pinned Ticker column
- **Advanced Gradient System**: Dynamic color gradients with separate scaling for different column groups
- **Totals Row**: Summary statistics with totals for STRENGTH and Rating columns

**Charts Page** (`pages/1_📈_Charts.py`):
- Reserved for future charting functionality
- Placeholder for interactive technical analysis charts

**Session State Management**:
- Caches analysis results in `st.session_state.analysis_results`
- **Smart Refresh Logic**: Only loads data when explicitly requested
- **First Load Flag**: Prevents unnecessary data reloading on UI changes
- Date change detection with user warnings

### Stock List Configuration

**CSV Format** (`TA_Tracking_List.csv`):
```csv
Sector,Ticker,Exchange
CK,SSI,HOSE
Index,VNINDEX,
Index,VNMID,
Index,^GSPC,
Index,^VIX,
```

**Special Handling**:
- `VNMID` → `VNMIDCAP` mapping in vnstock
- US indices use `^` prefix (^GSPC for S&P 500, ^VIX for VIX)
- Vietnamese indices (VNINDEX, VNMID) use vnstock exclusively
- All entries must have proper Sector designation to avoid NaN sorting errors

### Display & Visualization

**AG-Grid Modern Table Structure**:
- **Core Columns**: Sector, Ticker (pinned), Price, % Change
- **Close vs MA Series**: vs MA5, MA10, MA20, MA50, MA200 (with dynamic gradient)
- **Strength Indicators**: ST Strength, LT Strength (with gradient backgrounds)
- **Historical Ratings**: 
  - Rating1: T, T-1, T-2 (with separate gradient scaling)
  - Rating2: T, T-1, T-2 (with separate gradient scaling)
- **Trend Indicator**: MA50>MA200 (Yes/No with color coding)
- **Totals Row**: Bottom row with sums for numeric columns

**Advanced Gradient System**:
- **Dynamic Scaling**: Each column group has separate min/max calculation
  - Close vs MA: Scaled to actual data range (excludes totals row)
  - STRENGTH columns: Independent scaling for ST/LT values
  - Rating1 columns: Scaled to Rating1 data range only
  - Rating2 columns: Scaled to Rating2 data range only
- **Light Colors**: Max alpha 0.5 for subtle appearance
- **Totals Row Exclusion**: Summary row has uniform background, no gradients

**Visual Features**:
- **Scrollable Design**: 600px height with horizontal/vertical scrolling
- **Fixed Elements**: Headers stay visible when scrolling, Ticker column pinned left
- **Center-Aligned Headers**: All column headers centered with 10px font
- **Compact Cells**: 12px font, optimized spacing
- **No Pagination**: Single-page display with scrollbars
- **Visual Separators**: Blank columns (15-20px) between logical groups

### Export System

**Future Enhancement** (`src/utils/export_utils.py`):
- CSV: Plain text with all indicator values and signals
- Excel: Color-coded signals (Buy=Green, Sell=Red, Neutral=Yellow)
- Auto-generated filenames with timestamps

### Historical Rating System

**3-Day Rating Implementation**:
- **Current Day**: Calculated from latest indicators using standard pipeline
- **-1 Day**: Uses `get_latest_indicators()` with previous day's timestamp
- **-2 Day**: Uses `get_latest_indicators()` with -2 day's timestamp
- **Consistency**: Rating -1d (when viewing day N) = Rating Current (when viewing day N-1)

**Rating Logic**:
```python
# All days use identical calculation pipeline:
indicators = get_latest_indicators(df_with_indicators, target_date)
signals = evaluate_all_signals(indicators)
osc_buy, osc_sell, ma_buy, ma_sell = count_signals(signals)
rating1, rating2 = calculate_ratings(osc_buy, osc_sell, ma_buy, ma_sell)
```

**Performance Optimization**:
- No additional API calls for historical ratings
- Reuses existing dataframe historical data
- Efficient date-based indicator extraction

### Parallel Processing System

**Parallel Stock Analysis** (`src/utils/parallel_processor.py`):
- **ThreadPoolExecutor Implementation**: Analyzes multiple stocks concurrently
  - Default: 15 parallel workers (optimized for API rate limits)
  - Processes 66 stocks in ~30-40 seconds (vs ~3 minutes sequential)
  - 80% performance improvement in data loading
- **Real-time Progress Tracking**:
  - Callback-based progress updates
  - Shows: "🚀 Analyzing {ticker}... ({X}/{total})"
  - Smooth progress bar from 0% → 100%
- **Error Resilience**:
  - Individual stock failures don't block other stocks
  - Maintains original stock order in results
  - Aggregates errors for batch display
- **Key Functions**:
  - `analyze_single_stock()`: Core analysis logic extracted from main loop
  - `analyze_stocks_parallel()`: Parallel orchestrator with progress callback
  - Identical calculation logic to sequential version (zero regression risk)

**Performance Metrics**:
- **Data Fetching**: 80% faster (3 min → 30-40 sec for 66 stocks)
- **User Experience**: Real-time progress updates, no frozen UI
- **Scalability**: Can handle 100+ stocks efficiently with worker tuning

## Development Notes

### TradingView Compatibility
- Indicator values match TradingView within acceptable tolerance (data source differences)
- Scaling adjustments ensure proper display compatibility
- Signal logic follows TradingView conventions

### AG-Grid Implementation
- **Theme**: Balham light theme for modern appearance
- **Custom Renderers**: JavaScript-based cell formatting
  - Dynamic gradient renderer with separate scaling per column group
  - Percentage formatter with 1 decimal place
  - Integer formatter for strength columns
  - Text renderers with consistent 12px font
- **Scrolling Features**: Fixed 600px height with sticky headers
- **Column Pinning**: Ticker column pinned to left for horizontal scrolling
- **User Experience**: No pagination, no filter dropdowns, center-aligned headers
- **Performance**: Separate gradient calculations exclude totals row for accurate scaling

### Data Source Priorities
1. **Investing.com** for VNMIDCAP exclusively (no fallback)
2. **vnstock** for Vietnamese symbols (VNINDEX, all VN stocks)
3. **Yahoo Finance** for US indices (^GSPC, ^VIX) and VN stock fallback
4. Intelligent fallback handling with user warnings (except VNMIDCAP)
5. `get_last_trading_date()` for smart date defaults

### Caching Strategy
- **Data Fetching Cache**: 5-minute TTL on vnstock and Yahoo Finance functions
- **Investing.com Cache**: 30-minute TTL (VNMIDCAP data updates once per day)
- **Session State**: Caches analysis results to avoid re-fetching on UI changes
- **Smart Refresh**: Manual refresh button prevents unnecessary data reloading
- Date sorting before calculations ensures consistency

### Vietnamese Market Integration
- **VNMID mapping**: Automatically converts VNMID → VNMIDCAP for routing to Investing.com
- **Investing.com Integration**: VNMIDCAP fetched via `investiny` library
  - Direct API access to Investing.com historical data
  - Standard numeric format (no conversion needed)
  - US date format parsing (mm/dd/yyyy → datetime)
  - Automatic data quality checks and sorting
  - 100% data accuracy verified against Google Sheets
- **vnstock Integration**: VNINDEX and stocks use TCBS source with VCI fallback
- **Retry mechanism**: 2 attempts per source with 1-second delay for connection reliability
- **Error prevention**: All CSV entries must have proper Sector values

### Sector Analysis System

**New Sector Summary Implementation** (`src/utils/sector_analysis.py`):
- **Vietnamese Sector Names**: Maps English codes to Vietnamese display names with grouped sectors
  - CK → Chứng khoán
  - BDS → Bất động sản
  - DTC, VLXD → Xây dựng & ĐTC, VLXD (grouped)
  - DAU, HK, AGRI → Dầu, Hàng không, Agri (grouped)
  - XK → Xuất khẩu
  - NH → Ngân hàng
  - FAV → FAV
- **Variable Counts per Sector**: Different top/bottom counts based on grouped sectors
  - Major sectors (CK, BDS, Xây dựng & ĐTC VLXD, Dầu Hàng không Agri, NH, FAV): top 3, bottom 3
  - Medium sectors (XK): top 2, bottom 2
- **Stock Display Format**: Shows both ticker and rating1 value (e.g., "SSI (10), VCB (8)")
- **Breakthrough Detection**: Identifies stocks with Rating Change ≥ ±10 points
  - Format: "TICKER (prev_rating -> current_rating)"
  - Displayed in separate breakthrough groups below main sectors
- **Single Table Display**:
  - Main sectors: 3 columns (Sector | Top cao điểm | Top thấp điểm)
  - Color scheme: Green text for positive rating1 values, red text for negative rating1 values
  - Breakthrough groups: Green text for up moves, red text for down moves
  - White background with !important to override Streamlit dark theme
  - No scroll, text overflow handling for long stock lists

### Functions Available

**Core Functions**:
- `analyze_sectors_new(df_results)`: New sector analysis with Vietnamese names and variable counts
- `create_sector_dataframe(sector_analysis)`: Creates DataFrame for display
- `create_sector_html_table(sector_analysis)`: Creates HTML table with merged cells (fallback)

**Key Features**:
- Automatic sector mapping from English to Vietnamese
- Breakthrough detection with ±10 rating threshold
- Smart text overflow handling for long stock lists
- Single table display with conditional styling

### Error Handling Patterns
- Duplicate column prevention in dataframe construction
- Safe numeric formatting with empty string fallback (no "N/A" display)
- Graceful degradation for missing indicator data
- Progress tracking with individual stock error handling
- Connection retry logic for cloud deployment stability