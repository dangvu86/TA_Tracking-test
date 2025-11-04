import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
from typing import Optional
from vnstock import Vnstock
import time


@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_vnstock_data(ticker: str, days: int = 365) -> Optional[pd.DataFrame]:
    """
    Fetch stock/index data from vnstock API
    
    Args:
        ticker: Stock ticker or index name
        days: Number of days to fetch
    
    Returns:
        DataFrame with OHLCV data or None if error
    """
    try:
        vnstock = Vnstock()
        
        # Calculate date range
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        # VNMIDCAP should not reach here - handled exclusively by Google Sheets
        if ticker == 'VNMIDCAP':
            st.warning(f"VNMIDCAP should use Google Sheets exclusively, not vnstock")
            return None

        # Use TCBS as primary source with VCI fallback for other tickers
        sources_to_try = ['TCBS', 'VCI']
        
        df = None
        last_error = None
        
        for source in sources_to_try:
            try:
                stock_obj = vnstock.stock(symbol=ticker, source=source)
                # Add timeout and retry for connection issues
                max_retries = 2
                for attempt in range(max_retries):
                    try:
                        df = stock_obj.quote.history(start=start_date, end=end_date)
                        if df is not None and not df.empty:
                            break  # Success, exit retry loop
                    except Exception as retry_error:
                        if attempt < max_retries - 1:
                            time.sleep(1)  # Wait 1 second before retry
                            continue
                        else:
                            raise retry_error
                
                if df is not None and not df.empty:
                    break  # Success, exit source loop
                    
            except Exception as e:
                last_error = e
                continue  # Try next source
        
        if df is None or df.empty:
            raise last_error if last_error else Exception("No data available")
        
        if df is not None and not df.empty:
            # Rename columns to match Yahoo Finance format
            column_mapping = {
                'time': 'Date',
                'open': 'Open',
                'high': 'High',
                'low': 'Low', 
                'close': 'Close',
                'volume': 'Volume'
            }
            
            df = df.rename(columns=column_mapping)
            
            # Ensure Date column is datetime
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'])
            
            # Add missing columns for compatibility
            if 'Volume' not in df.columns:
                df['Volume'] = 0
                
            # Add dividend and stock split columns (for compatibility)
            df['Dividends'] = 0
            df['Stock Splits'] = 0
            
            # Sort by date and reset index
            df = df.sort_values('Date').reset_index(drop=True)
            
            return df
            
    except Exception as e:
        st.warning(f"Error fetching vnstock data for {ticker}: {str(e)}")
        return None


def is_vietnamese_symbol(ticker: str, exchange: str) -> bool:
    """Check if symbol should use vnstock (Vietnamese market, excluding VNMIDCAP)"""
    # Vietnamese indices available on vnstock (VNMIDCAP excluded - uses Google Sheets)
    vn_indices = ['VNINDEX']
    if ticker in vn_indices:
        return True

    # VNMID maps to VNMIDCAP which uses Google Sheets exclusively
    if ticker in ['VNMID', 'VNMIDCAP']:
        return False

    # Vietnamese stock exchanges
    if exchange in ['HOSE', 'HNX', 'UPCOM']:
        return True

    return False


def get_available_vn_indices() -> list:
    """Get list of Vietnamese indices available on vnstock (VNMIDCAP uses Google Sheets)"""
    return ['VNINDEX']


def format_ticker_for_vnstock(ticker: str) -> str:
    """Format ticker for vnstock API"""
    # Handle special cases
    if ticker == 'VNMID':
        return 'VNMIDCAP'  # Map VNMID to VNMIDCAP
    
    return ticker


def get_vnstock_source(ticker: str) -> str:
    """Get the correct vnstock source for ticker (VNMIDCAP not supported here)"""
    return 'TCBS'  # Primary source for vnstock data


def test_vnstock_connection() -> bool:
    """Test vnstock API connection"""
    try:
        test_df = fetch_vnstock_data("VNINDEX", days=5)
        return test_df is not None and not test_df.empty
    except:
        return False