# INSTALL azure-eventhub before running!
# pip install azure-eventhub

from azure.eventhub import EventHubProducerClient, EventData
from datetime import datetime, timezone
import hashlib
import random
import time
import json
import uuid
from collections import deque

EVENTHUB_NAME = "XXX"
CONNECTION_STR = (
    "XXX"
)

TICKERS = {
    "AAPL": 195.12,
    "MSFT": 448.67,
    "GOOGL": 182.45,
    "AMZN": 178.52,
    "TSLA": 262.80,
}

CURRENCY_DEFAULT = "USD"
ERROR_PROB = 0.01
QUOTE_SPREAD_BPS = (5, 25)
VOLUME_RANGE = (100, 5_000)
CRAZY_VOLUME = 10_000_000
SLEEP_SECS = 5

EXCHANGE = "NASDAQ"
EVENT_TYPES = ["TRADE", "QUOTE", "CANCEL"]
TRANSACTION_TYPES = ["BUY", "SELL"]

# ────────── helpers ────────── #
def sha256_id(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def drift(base: float) -> float:
    return base * (1 + random.uniform(-0.0045, 0.0055))  # ±0.5 %


def inject_error(price: float, volume: int, currency: str) -> tuple[float, int, str]:
    """Wprowadź celowy błąd z prawdopodobieństwem ERROR_PROB."""
    if random.random() >= ERROR_PROB:
        return price, volume, currency
    match random.choice(["NEG_PRICE", "BAD_CCY", "CRAZY_VOL"]):
        case "NEG_PRICE":
            price = -abs(price)
        case "BAD_CCY":
            currency = "EUR"
        case "CRAZY_VOL":
            volume = CRAZY_VOLUME
    return price, volume, currency


def next_quote(mid: float) -> tuple[float, float]:
    spread = mid * random.uniform(*QUOTE_SPREAD_BPS) / 10_000
    return round(mid - spread / 2, 2), round(mid + spread / 2, 2)


# mini-order-book – do 100 aktywnych order_id na symbol
ORDER_BOOK: dict[str, deque[str]] = {sym: deque(maxlen=100) for sym in TICKERS}


def new_order_id(symbol: str) -> str:
    oid = str(uuid.uuid4())
    ORDER_BOOK[symbol].append(oid)
    return oid


def random_existing_order_id(symbol: str) -> str:
    return random.choice(list(ORDER_BOOK[symbol])) if ORDER_BOOK[symbol] else str(uuid.uuid4())


# ────────── unified-schema builder ────────── #
def build_event(symbol: str) -> dict:
    """Zwraca rekord z pełnym, spójnym schematem."""
    mid = drift(TICKERS[symbol])
    event_type = random.choices(EVENT_TYPES, weights=[0.6, 0.3, 0.1])[0]
    latency = random.randint(1, 500)  # ms

    # Pola wspólne
    payload = {
        "timestamp": now_iso(),
        "symbol": symbol,
        "exchange": EXCHANGE,
        "event_type": event_type,
        "latency_ms": latency,
        # Pola specyficzne – zapełniamy None, potem ewentualnie nadpisujemy
        "order_id": None,
        "transaction_type": None,
        "price": None,
        "volume": None,
        "bid_price": None,
        "ask_price": None,
        "bid_size": None,
        "ask_size": None,
        "canceled_order_id": None,
        "currency": CURRENCY_DEFAULT,
        "trade_id": None,          # tylko dla TRADE
    }

    if event_type == "TRADE":
        price = round(mid, 2)
        volume = random.randint(*VOLUME_RANGE)
        price, volume, currency = inject_error(price, volume, CURRENCY_DEFAULT)
        order_id = new_order_id(symbol)
        payload.update(
            {
                "order_id": order_id,
                "transaction_type": random.choice(TRANSACTION_TYPES),
                "price": price,
                "volume": volume,
                "currency": currency,
            }
        )
    elif event_type == "QUOTE":
        bid, ask = next_quote(mid)
        bid, _, currency = inject_error(bid, 0, CURRENCY_DEFAULT)
        _, ask, currency = inject_error(ask, 0, currency)
        order_id = new_order_id(symbol)
        payload.update(
            {
                "order_id": order_id,
                "bid_price": bid,
                "ask_price": ask,
                "bid_size": random.randint(*VOLUME_RANGE),
                "ask_size": random.randint(*VOLUME_RANGE),
                "currency": currency,
            }
        )
    else:  # CANCEL
        payload["canceled_order_id"] = random_existing_order_id(symbol)

    # Identyfikatory
    payload["event_id"] = sha256_id(payload)
    if event_type == "TRADE":
        payload["trade_id"] = payload["event_id"]

        # aktualizacja referencyjnej ceny
        if payload["price"] and payload["price"] > 0:
            TICKERS[symbol] = payload["price"]

    return payload


def main() -> None:
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME
    )
    try:
        while True:
            batch = producer.create_batch()
            for symbol in TICKERS:
                evt = build_event(symbol)
                batch.add(EventData(json.dumps(evt)))
                print(json.dumps(evt, indent=2))
            producer.send_batch(batch)
            print("Batch sent!\n")
            time.sleep(SLEEP_SECS)
    except KeyboardInterrupt:
        print("Stopped by user")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
