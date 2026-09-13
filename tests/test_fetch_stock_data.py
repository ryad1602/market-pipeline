import sys
import os
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from fetch_stock_data import TICKERS

def test_tickers_list_not_empty():
    assert len(TICKERS) > 0

def test_tickers_are_strings():
    for ticker in TICKERS:
        assert isinstance(ticker, str)

def test_tickers_no_duplicates():
    assert len(TICKERS) == len(set(TICKERS))
