import pandas as pd
import streamlit as st
from datetime import datetime
from typing import Optional
import requests
from io import StringIO
import re


@st.cache_data(ttl=1800)  # Cache for 30 minutes (data updates once per day)
def fetch_vnmidcap_from_sheets() -> Optional[pd.DataFrame]:
    """
    Fetch VNMIDCAP data from Google Sheets - Simplified approach
    
    Returns:
        DataFrame with OHLCV data or None if error
    """
    try:
        # Get data from Google Sheets (updated link)
        sheet_id = "1-aoYbQDjBeOuzqT8LuURuOuEF4K0qsSA"
        csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
        
        response = requests.get(csv_url, timeout=10)
        response.raise_for_status()
        
        # Handle encoding
        content = response.content.decode('utf-8', errors='ignore')
        
        # Parse CSV without headers
        csv_data = StringIO(content)
        df_raw = pd.read_csv(csv_data, header=None)
        
        # Find the first row that looks like actual data (date + numeric values)
        data_row = None
        for i in range(min(50, len(df_raw))):
            row = df_raw.iloc[i]
            
            if row.isna().all():
                continue
                
            first_val = str(row.iloc[0]).strip() if not pd.isna(row.iloc[0]) else ""
            
            # Look for date pattern DD/MM/YYYY
            if re.match(r'\d{1,2}/\d{1,2}/\d{4}', first_val):
                data_row = i
                break
        
        if data_row is None:
            st.error("Could not find VNMIDCAP data starting row")
            return None
        
        # Extract data starting from the found row
        df = df_raw.iloc[data_row:].copy()
        df = df.reset_index(drop=True)
        df = df.dropna(how='all')
        
        if df.empty:
            st.error("No data found")
            return None
        
        # Correct column mapping based on user specification:
        # Col 0: Date
        # Col 1: %change (skip)
        # Col 2: Open 
        # Col 3: High
        # Col 4: Low
        # Col 5: Close
        # Col 6: Volume
        
        # Select and rename columns correctly
        if len(df.columns) >= 7:
            # Create new dataframe with correct column order (avoid pandas warnings)
            df = pd.DataFrame({
                'Date': df.iloc[:, 0].copy(),      # Date column
                'Open': df.iloc[:, 2].copy(),      # Open price (col 2)
                'High': df.iloc[:, 3].copy(),      # High price (col 3) 
                'Low': df.iloc[:, 4].copy(),       # Low price (col 4)
                'Close': df.iloc[:, 5].copy(),     # Close price (col 5)
                'Volume': df.iloc[:, 6].copy()     # Volume (col 6)
            })
        else:
            st.error("Not enough columns in Google Sheets data")
            return None
        
        # Convert Date column (US format: mm/dd/yyyy)
        try:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=False)
        except:
            st.error("Could not parse dates")
            return None
        
        # Remove invalid dates
        df = df.dropna(subset=['Date'])
        
        # Convert price columns (handle Vietnamese number format: 2.492,45 -> 2492.45)
        price_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        for col in price_cols:
            if col in df.columns:
                # Convert to string
                df[col] = df[col].astype(str)
                # Handle Vietnamese number format: remove thousand separators (dots), replace comma with dot
                df[col] = df[col].str.replace(r'\.(?=\d{3})', '', regex=True)  # Remove dots used as thousand separators
                df[col] = df[col].str.replace(',', '.', regex=False)           # Replace comma decimal separator with dot
                df[col] = df[col].str.replace(' ', '', regex=False)            # Remove spaces
                # Convert to numeric
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Fill missing OHLC with Close if needed
        if 'Close' in df.columns and not df['Close'].isna().all():
            for col in ['Open', 'High', 'Low']:
                if col not in df.columns or df[col].isna().all():
                    df[col] = df['Close']
        
        # Add Volume if not present
        if 'Volume' not in df.columns:
            df['Volume'] = 0
        
        # Add compatibility columns
        df['Dividends'] = 0
        df['Stock Splits'] = 0
        
        # Remove rows with no price data
        df = df.dropna(subset=['Close'])
        
        # Sort by date
        df = df.sort_values('Date').reset_index(drop=True)
        
        if df.empty:
            st.error("No valid data after processing")
            return None
        
        # Check data freshness
        latest_date = df['Date'].max()
        days_old = (datetime.now() - latest_date.to_pydatetime()).days
        
        if days_old > 30:
            st.warning(f"VNMIDCAP data may be outdated. Latest: {latest_date.strftime('%Y-%m-%d')}")
        
        return df
        
    except requests.exceptions.RequestException as e:
        st.error(f"Error accessing Google Sheets: {str(e)}")
        return None
    except Exception as e:
        st.error(f"Error processing VNMIDCAP data: {str(e)}")
        return None


def test_google_sheets_connection() -> bool:
    """Test Google Sheets connection"""
    try:
        df = fetch_vnmidcap_from_sheets()
        return df is not None and not df.empty
    except:
        return False


def get_vnmidcap_data_info() -> dict:
    """Get VNMIDCAP data information"""
    try:
        df = fetch_vnmidcap_from_sheets()
        if df is not None and not df.empty:
            return {
                'total_records': len(df),
                'date_range': f"{df['Date'].min().strftime('%Y-%m-%d')} to {df['Date'].max().strftime('%Y-%m-%d')}",
                'latest_close': df['Close'].iloc[-1] if 'Close' in df.columns else None,
                'columns': list(df.columns)
            }
        return {'error': 'No data available'}
    except Exception as e:
        return {'error': str(e)}
