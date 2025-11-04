"""
Parallel processing utilities for stock analysis
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np
from datetime import datetime
import streamlit as st

from ..data_fetcher import fetch_stock_data
from ..indicators.calculator import calculate_all_indicators, get_latest_indicators
from ..indicators.signals import evaluate_all_signals
from .signal_counter import count_signals, calculate_price_change, calculate_ratings


def analyze_single_stock(ticker: str, sector: str, exchange: str, selected_date_dt: datetime) -> Dict:
    """
    Analyze a single stock - extracted from main.py for parallel processing

    Args:
        ticker: Stock ticker
        sector: Sector name
        exchange: Exchange name
        selected_date_dt: Selected date as datetime

    Returns:
        Dictionary with analysis results
    """
    try:
        # Fetch data
        df = fetch_stock_data(ticker, selected_date_dt, exchange=exchange)

        if df is None or df.empty:
            # Return row with no data
            return {
                'Sector': sector,
                'Ticker': ticker,
                'Price': np.nan,
                '% Change': np.nan,
                'Osc Buy': 0,
                'Osc Sell': 0,
                'MA Buy': 0,
                'MA Sell': 0,
                'error': None
            }

        # Calculate indicators
        df_with_indicators = calculate_all_indicators(df)
        indicators = get_latest_indicators(df_with_indicators, pd.Timestamp(selected_date_dt))

        if not indicators:
            # Return row with no data
            return {
                'Sector': sector,
                'Ticker': ticker,
                'Price': np.nan,
                '% Change': np.nan,
                'Osc Buy': 0,
                'Osc Sell': 0,
                'MA Buy': 0,
                'MA Sell': 0,
                'error': None
            }

        # Evaluate signals
        signals = evaluate_all_signals(indicators)

        # Count signals
        osc_buy, osc_sell, ma_buy, ma_sell = count_signals(signals)

        # Calculate ratings for current day
        rating1_current, rating2_current = calculate_ratings(osc_buy, osc_sell, ma_buy, ma_sell)

        # Calculate ratings for previous days using EXACT same logic as current day
        df_sorted = df_with_indicators.sort_values('Date')

        # Initialize previous day ratings
        rating1_prev1, rating2_prev1 = 'N/A', 'N/A'
        rating1_prev2, rating2_prev2 = 'N/A', 'N/A'

        # Calculate -1 day rating using IDENTICAL method as current day
        if len(df_sorted) >= 2:
            try:
                prev_date = df_sorted.iloc[-2]['Date']
                prev_indicators = get_latest_indicators(df_with_indicators, pd.Timestamp(prev_date))
                if prev_indicators:
                    prev_signals = evaluate_all_signals(prev_indicators)
                    prev_osc_buy, prev_osc_sell, prev_ma_buy, prev_ma_sell = count_signals(prev_signals)
                    rating1_prev1, rating2_prev1 = calculate_ratings(prev_osc_buy, prev_osc_sell, prev_ma_buy, prev_ma_sell)
            except:
                rating1_prev1, rating2_prev1 = 'N/A', 'N/A'

        # Calculate -2 day rating using IDENTICAL method as current day
        if len(df_sorted) >= 3:
            try:
                prev2_date = df_sorted.iloc[-3]['Date']
                prev2_indicators = get_latest_indicators(df_with_indicators, pd.Timestamp(prev2_date))
                if prev2_indicators:
                    prev2_signals = evaluate_all_signals(prev2_indicators)
                    prev2_osc_buy, prev2_osc_sell, prev2_ma_buy, prev2_ma_sell = count_signals(prev2_signals)
                    rating1_prev2, rating2_prev2 = calculate_ratings(prev2_osc_buy, prev2_osc_sell, prev2_ma_buy, prev2_ma_sell)
            except:
                rating1_prev2, rating2_prev2 = 'N/A', 'N/A'

        # Get price info
        current_price = indicators.get('Price', np.nan)

        # Calculate price change (using previous day's close)
        if len(df_sorted) >= 2:
            prev_price = df_sorted.iloc[-2]['Close']
        else:
            prev_price = current_price

        price_change = calculate_price_change(current_price, prev_price)

        # Create result row with basic info
        result_row = {
            'Sector': sector,
            'Ticker': ticker,
            'Price': current_price,
            '% Change': price_change,
            'Osc Buy': osc_buy,
            'Osc Sell': osc_sell,
            'MA Buy': ma_buy,
            'MA Sell': ma_sell,
            'MA5': indicators.get('SMA_5', np.nan),
            'Close_vs_MA5': indicators.get('Close_vs_MA5', np.nan),
            'Close_vs_MA10': indicators.get('Close_vs_MA10', np.nan),
            'Close_vs_MA20': indicators.get('Close_vs_MA20', np.nan),
            'Close_vs_MA50': indicators.get('Close_vs_MA50', np.nan),
            'Close_vs_MA200': indicators.get('Close_vs_MA200', np.nan),
            'STRENGTH_ST': indicators.get('STRENGTH_ST', np.nan),
            'STRENGTH_LT': indicators.get('STRENGTH_LT', np.nan),
            'Rating_1_Current': rating1_current,
            'Rating_1_Prev1': rating1_prev1,
            'Rating_1_Prev2': rating1_prev2,
            'Rating_2_Current': rating2_current,
            'Rating_2_Prev1': rating2_prev1,
            'Rating_2_Prev2': rating2_prev2,
            'MA50_GT_MA200': 'Yes' if indicators.get('MA50_GT_MA200', False) else 'No',
            'error': None
        }

        # Add all signal details
        result_row.update(signals)

        # Add all indicator values (exclude duplicates already in result_row)
        for key, value in indicators.items():
            if key not in result_row:
                result_row[key] = value

        return result_row

    except Exception as e:
        # Return row with error
        return {
            'Sector': sector,
            'Ticker': ticker,
            'Price': np.nan,
            '% Change': np.nan,
            'Osc Buy': 0,
            'Osc Sell': 0,
            'MA Buy': 0,
            'MA Sell': 0,
            'error': str(e)
        }


def analyze_stocks_parallel(stock_df: pd.DataFrame, selected_date_dt: datetime,
                           max_workers: int = 15, progress_callback=None) -> Tuple[List[Dict], List[str]]:
    """
    Analyze multiple stocks in parallel using ThreadPoolExecutor

    Args:
        stock_df: DataFrame with columns ['Ticker', 'Sector', 'Exchange']
        selected_date_dt: Selected date as datetime
        max_workers: Maximum number of parallel workers (default: 15)
        progress_callback: Optional callback function(completed, total, ticker) for progress updates

    Returns:
        Tuple of (results list, errors list)
    """
    results = []
    errors = []

    # Create list of tasks
    tasks = []
    for _, row in stock_df.iterrows():
        tasks.append({
            'ticker': row['Ticker'],
            'sector': row['Sector'],
            'exchange': row['Exchange']
        })

    total_tasks = len(tasks)
    completed_count = 0

    # Process in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_task = {}
        for task in tasks:
            future = executor.submit(
                analyze_single_stock,
                task['ticker'],
                task['sector'],
                task['exchange'],
                selected_date_dt
            )
            future_to_task[future] = task

        # Collect results as they complete
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            try:
                result = future.result()
                results.append(result)

                # Track errors
                if result.get('error'):
                    errors.append(f"{task['ticker']}: {result['error']}")

                # Update progress
                completed_count += 1
                if progress_callback:
                    progress_callback(completed_count, total_tasks, task['ticker'])

            except Exception as e:
                # Handle unexpected errors
                error_result = {
                    'Sector': task['sector'],
                    'Ticker': task['ticker'],
                    'Price': np.nan,
                    '% Change': np.nan,
                    'Osc Buy': 0,
                    'Osc Sell': 0,
                    'MA Buy': 0,
                    'MA Sell': 0,
                    'error': str(e)
                }
                results.append(error_result)
                errors.append(f"{task['ticker']}: {str(e)}")

                # Update progress even on error
                completed_count += 1
                if progress_callback:
                    progress_callback(completed_count, total_tasks, task['ticker'])

    # Sort results by original order (same as stock_df)
    ticker_order = {row['Ticker']: idx for idx, row in stock_df.iterrows()}
    results.sort(key=lambda x: ticker_order.get(x['Ticker'], 999))

    return results, errors
