import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import streamlit as st
from .vnstock_fetcher import fetch_vnstock_data, is_vietnamese_symbol, format_ticker_for_vnstock
from .investing_fetcher import fetch_vnmidcap_from_investing


@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_stock_data(ticker: str, end_date: datetime, period_days: int = 365, exchange: str = '') -> Optional[pd.DataFrame]:
    """
    Fetch stock data from appropriate source (Yahoo Finance or vnstock)
    
    Args:
        ticker: Stock ticker (original format)
        end_date: End date for data fetching
        period_days: Number of days to fetch (for indicator calculations)
        exchange: Stock exchange (for determining data source)
    
    Returns:
        DataFrame with OHLCV data or None if error
    """
    try:
        # Special handling for VNMIDCAP using Investing.com ONLY
        if ticker in ['VNMID', 'VNMIDCAP'] or (ticker == 'VNMID' and exchange == ''):
            df = fetch_vnmidcap_from_investing(period_days)

            if df is not None and not df.empty:
                # Filter data up to the specified end_date
                df = df[df['Date'].dt.date <= end_date.date()]
                return df
            else:
                # Only use Investing.com for VNMIDCAP - no fallback
                st.error(f"Failed to fetch VNMIDCAP data from Investing.com for {ticker}")
                return None
        
        # Check if should use vnstock for Vietnamese market (excluding VNMIDCAP)
        elif is_vietnamese_symbol(ticker, exchange):
            # Use vnstock for Vietnamese stocks and indices
            vnstock_ticker = format_ticker_for_vnstock(ticker)
            df = fetch_vnstock_data(vnstock_ticker, period_days)
            
            if df is not None and not df.empty:
                # Filter data up to the specified end_date
                df = df[df['Date'].dt.date <= end_date.date()]
                return df
            else:
                # Fallback to Yahoo Finance if vnstock fails
                pass
        
        # Use Yahoo Finance (for US indices or fallback)
        start_date = end_date - timedelta(days=period_days)
        
        # Format ticker for Yahoo Finance
        if exchange in ['HOSE', 'HNX', 'UPCOM']:
            yahoo_ticker = f"{ticker}.VN"
        else:
            yahoo_ticker = ticker
        
        stock = yf.Ticker(yahoo_ticker)
        df = stock.history(start=start_date, end=end_date + timedelta(days=1))
        
        if df.empty:
            st.warning(f"No data found for {ticker}")
            return None
            
        # Reset index to make Date a column
        df.reset_index(inplace=True)
        
        # Rename columns to standard format
        df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits']
        
        # Filter data up to the specified end_date
        df = df[df['Date'].dt.date <= end_date.date()]
        
        return df
        
    except Exception as e:
        st.error(f"Error fetching data for {ticker}: {str(e)}")
        return None


def get_last_trading_date() -> datetime:
    """Get the last trading date (exclude weekends)"""
    today = datetime.now()
    
    # If today is weekend, go back to Friday
    if today.weekday() == 5:  # Saturday
        return today - timedelta(days=1)
    elif today.weekday() == 6:  # Sunday
        return today - timedelta(days=2)
    else:
        return today


def validate_trading_date(selected_date: datetime) -> datetime:
    """Validate and adjust trading date if needed"""
    # Ensure it's not in the future
    last_trading_date = get_last_trading_date()
    if selected_date > last_trading_date:
        return last_trading_date
    
    # Adjust for weekends
    if selected_date.weekday() == 5:  # Saturday
        return selected_date - timedelta(days=1)
    elif selected_date.weekday() == 6:  # Sunday
        return selected_date - timedelta(days=2)
    
    return selected_date