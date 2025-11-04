import pandas as pd
import streamlit as st
from datetime import datetime
from typing import Optional


@st.cache_data(ttl=1800)  # Cache for 30 minutes (data updates once per day)
def fetch_vnmidcap_from_investing(period_days: int = 365) -> Optional[pd.DataFrame]:
    """
    Fetch VNMIDCAP data from Investing.com via investiny library

    Args:
        period_days: Number of days to fetch (for indicator calculations)

    Returns:
        DataFrame with OHLCV data or None if error
    """
    try:
        from investiny import historical_data
        from datetime import timedelta

        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)

        # Format dates for investiny (MM/DD/YYYY)
        from_date = start_date.strftime('%m/%d/%Y')
        to_date = end_date.strftime('%m/%d/%Y')

        # Fetch data from Investing.com
        # investing_id='995069' corresponds to VN Mid Cap (VNIMC)
        data = historical_data(
            investing_id='995069',
            from_date=from_date,
            to_date=to_date
        )

        if not data or not isinstance(data, dict):
            st.error("No data returned from Investing.com for VNMIDCAP")
            return None

        # Convert to DataFrame
        df = pd.DataFrame(data)

        if df.empty:
            st.error("Empty DataFrame from Investing.com for VNMIDCAP")
            return None

        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y', errors='coerce')

        # Remove invalid dates
        df = df.dropna(subset=['date'])

        if df.empty:
            st.error("No valid dates in VNMIDCAP data")
            return None

        # Rename columns to match expected format
        df = df.rename(columns={
            'date': 'Date',
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume'
        })

        # Add compatibility columns
        df['Dividends'] = 0
        df['Stock Splits'] = 0

        # Sort by date
        df = df.sort_values('Date').reset_index(drop=True)

        # Check data freshness
        latest_date = df['Date'].max()
        days_old = (datetime.now() - latest_date.to_pydatetime()).days

        if days_old > 7:
            st.warning(f"VNMIDCAP data may be outdated. Latest: {latest_date.strftime('%Y-%m-%d')}")

        return df

    except ImportError:
        st.error("investiny library not installed. Run: pip install investiny")
        return None
    except Exception as e:
        st.error(f"Error fetching VNMIDCAP from Investing.com: {str(e)}")
        return None


def test_investing_connection() -> bool:
    """Test Investing.com connection"""
    try:
        df = fetch_vnmidcap_from_investing(period_days=30)
        return df is not None and not df.empty
    except:
        return False


def get_vnmidcap_data_info() -> dict:
    """Get VNMIDCAP data information from Investing.com"""
    try:
        df = fetch_vnmidcap_from_investing(period_days=365)
        if df is not None and not df.empty:
            return {
                'total_records': len(df),
                'date_range': f"{df['Date'].min().strftime('%Y-%m-%d')} to {df['Date'].max().strftime('%Y-%m-%d')}",
                'latest_close': df['Close'].iloc[-1] if 'Close' in df.columns else None,
                'columns': list(df.columns),
                'source': 'Investing.com (investiny)'
            }
        return {'error': 'No data available'}
    except Exception as e:
        return {'error': str(e)}
