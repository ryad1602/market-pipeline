import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from fetch_crypto_data import COIN_TO_BINANCE


def test_coin_mapping_has_no_duplicates():
    """Chaque coin ne doit apparaître qu'une seule fois dans le mapping."""
    symbols = list(COIN_TO_BINANCE.values())
    assert len(symbols) == len(set(symbols))


@patch("fetch_crypto_data.requests.get")
@patch("fetch_crypto_data.save_to_db")
def test_fetch_crypto_data_parses_binance_response(mock_save_to_db, mock_get):
    """
    Vérifie que la fonction transforme correctement une réponse Binance simulée,
    sans jamais appeler le vrai réseau.
    """
    from fetch_crypto_data import fetch_crypto_data

    fake_response = MagicMock()
    fake_response.json.return_value = [
        {
            "symbol": "BTCUSDT",
            "lastPrice": "65000.50",
            "quoteVolume": "1000000.0",
            "priceChangePercent": "2.5",
        }
    ]
    fake_response.raise_for_status.return_value = None
    mock_get.return_value = fake_response

    result = fetch_crypto_data(["bitcoin"])

    assert result == 1
    mock_save_to_db.assert_called_once()
