import json
import os
import websocket
from datetime import datetime, timezone
from kafka import KafkaProducer

# Correspondance entre nos noms (CoinGecko) et les symboles Binance
COIN_TO_BINANCE = {
    "bitcoin": "btcusdt",
    "ethereum": "ethusdt",
    "solana": "solusdt",
    "ripple": "xrpusdt",
    "cardano": "adausdt",
    "dogecoin": "dogeusdt",
    "polkadot": "dotusdt",
    "chainlink": "linkusdt",
    "litecoin": "ltcusdt",
    "avalanche-2": "avaxusdt",
}

def get_producer():
    bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
    return KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

producer = get_producer()

def on_message(ws, message):
    """
    Appelée automatiquement à chaque message reçu du WebSocket.
    """
    payload = json.loads(message)
    trade = payload["data"]

    symbol = trade["s"]  # ex: "BTCUSDT"
    record = {
        "schema_version": 1,
        "symbol": symbol,
        "price": float(trade["p"]),
        "quantity": float(trade["q"]),
        "trade_time": trade["T"],
        "received_at": datetime.now(timezone.utc).isoformat(),
    }

    # La clé du message = le symbole : garantit que tous les messages
    # d'une même crypto restent dans l'ordre (même partition Kafka)
    producer.send("crypto-trades", key=symbol, value=record)
    print(f"{symbol} : {record['price']} USDT")

def on_error(ws, error):
    print(f"Erreur WebSocket : {error}")

def on_close(ws, close_status_code, close_msg):
    print("Connexion WebSocket fermée")

def on_open(ws):
    print("Connexion WebSocket ouverte, en écoute...")

if __name__ == "__main__":
    streams = "/".join(f"{symbol}@trade" for symbol in COIN_TO_BINANCE.values())
    url = f"wss://stream.binance.com:9443/stream?streams={streams}"

    ws = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever()
