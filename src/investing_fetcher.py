import pandas as pd
import streamlit as st
from datetime import datetime
from typing import Optional
import time
import traceback


@st.cache_data(ttl=1800)  # Cache for 30 minutes (data updates once per day)
def fetch_vnmidcap_from_investing(period_days: int = 365, max_retries: int = 3) -> Optional[pd.DataFrame]:
    """
    Fetch VNMIDCAP data from Investing.com via investiny library with retry mechanism

    Args:
        period_days: Number of days to fetch (for indicator calculations)
        max_retries: Maximum number of retry attempts (default: 3)

    Returns:
        DataFrame with OHLCV data or None if error
    """
    try:
        from investiny import historical_data
        from datetime import timedelta
    except ImportError as e:
        st.error(f"⚠️ investiny library not installed. Run: pip install investiny")
        st.error(f"Import error details: {str(e)}")
        return None

    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=period_days)

    # Format dates for investiny (MM/DD/YYYY)
    from_date = start_date.strftime('%m/%d/%Y')
    to_date = end_date.strftime('%m/%d/%Y')

    # Retry logic with exponential backoff
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                wait_time = 2 ** attempt  # Exponential backoff: 2s, 4s, 8s
                st.info(f"🔄 Retry attempt {attempt + 1}/{max_retries} for VNMIDCAP (waiting {wait_time}s)...")
                time.sleep(wait_time)

            # Fetch data from Investing.com
            # investing_id='995069' corresponds to VN Mid Cap (VNIMC)
            st.info(f"📡 Fetching VNMIDCAP from Investing.com (ID: 995069, {from_date} to {to_date})...")

            data = historical_data(
                investing_id='995069',
                from_date=from_date,
                to_date=to_date
            )

            # Validate response
            if not data:
                error_msg = f"❌ No data returned from Investing.com for VNMIDCAP (attempt {attempt + 1}/{max_retries})"
                if attempt == max_retries - 1:
                    st.error(error_msg)
                    st.error("💡 Possible causes: Network issue, API rate limit, or IP blocked by Investing.com")
                else:
                    st.warning(error_msg)
                continue

            if not isinstance(data, dict):
                error_msg = f"❌ Invalid data type from Investing.com: {type(data)} (attempt {attempt + 1}/{max_retries})"
                if attempt == max_retries - 1:
                    st.error(error_msg)
                else:
                    st.warning(error_msg)
                continue

            # Convert to DataFrame
            df = pd.DataFrame(data)

            if df.empty:
                error_msg = f"❌ Empty DataFrame from Investing.com for VNMIDCAP (attempt {attempt + 1}/{max_retries})"
                if attempt == max_retries - 1:
                    st.error(error_msg)
                    st.error(f"Raw data keys: {list(data.keys()) if data else 'None'}")
                else:
                    st.warning(error_msg)
                continue

            # Convert date column to datetime
            df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y', errors='coerce')

            # Remove invalid dates
            df = df.dropna(subset=['date'])

            if df.empty:
                error_msg = f"❌ No valid dates in VNMIDCAP data (attempt {attempt + 1}/{max_retries})"
                if attempt == max_retries - 1:
                    st.error(error_msg)
                    st.error(f"Raw dates sample: {list(data.get('date', []))[:5] if isinstance(data, dict) else 'N/A'}")
                else:
                    st.warning(error_msg)
                continue

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
                st.warning(f"⚠️ VNMIDCAP data may be outdated. Latest: {latest_date.strftime('%Y-%m-%d')} ({days_old} days old)")
            else:
                st.success(f"✅ VNMIDCAP data loaded successfully: {len(df)} records, latest {latest_date.strftime('%Y-%m-%d')}")

            return df

        except Exception as e:
            error_msg = f"❌ Error fetching VNMIDCAP (attempt {attempt + 1}/{max_retries}): {str(e)}"

            if attempt == max_retries - 1:
                # Last attempt - show full error details
                st.error(error_msg)
                st.error(f"Exception type: {type(e).__name__}")
                st.error("Full traceback:")
                st.code(traceback.format_exc())
                st.error("💡 Troubleshooting tips:")
                st.error("1. Check if Streamlit Cloud IP is blocked by Investing.com")
                st.error("2. Verify investiny library version (should be >=0.7.0)")
                st.error("3. Check network connectivity from Streamlit Cloud")
                st.error("4. Consider using alternative data source (Yahoo Finance, vnstock)")
                return None
            else:
                st.warning(error_msg)
                continue

    # All retries exhausted
    st.error(f"❌ Failed to fetch VNMIDCAP after {max_retries} attempts")
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
