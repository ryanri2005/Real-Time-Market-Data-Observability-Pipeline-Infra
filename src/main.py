import asyncio
import json
import logging
from datetime import datetime, timezone

from prometheus_client import Counter, Gauge, start_http_server
from websockets.asyncio.client import connect


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

WS_URL = "wss://advanced-trade-ws.coinbase.com"
PRODUCT_ID = "BTC-USD"
METRICS_PORT = 8000

INITIAL_RECONNECT_DELAY = 1
MAX_RECONNECT_DELAY = 60


# ---------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------

MESSAGES_RECEIVED = Counter(
    "coinbase_messages_received_total",
    "Total number of messages received from the Coinbase WebSocket.",
)

MESSAGE_LATENCY = Gauge(
    "coinbase_message_latency_seconds",
    "Estimated age of a Coinbase message at the client in seconds.",
)

WEBSOCKET_RECONNECTS = Counter(
    "coinbase_websocket_reconnects_total",
    "Total number of WebSocket reconnection attempts after connection loss.",
)


# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# Coinbase ticker subscription
# ---------------------------------------------------------

SUBSCRIBE_MESSAGE = {
    "type": "subscribe",
    "product_ids": [PRODUCT_ID],
    "channel": "ticker",
}


# ---------------------------------------------------------
# Timestamp handling
# ---------------------------------------------------------

def calculate_latency(exchange_timestamp: str) -> float | None:
    """
    Calculate the elapsed time between Coinbase's message timestamp
    and the local UTC clock.

    Returns:
        Estimated message age in seconds, or None if the timestamp
        cannot be parsed or appears to be ahead of the local clock.
    """

    try:
        exchange_time = datetime.fromisoformat(
            exchange_timestamp.replace("Z", "+00:00")
        )

        if exchange_time.tzinfo is None:
            exchange_time = exchange_time.replace(tzinfo=timezone.utc)

        local_time = datetime.now(timezone.utc)

        latency = (local_time - exchange_time).total_seconds()

        if latency < 0:
            return None

        return latency

    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------
# Message processing
# ---------------------------------------------------------

def process_message(raw_message: str) -> None:
    """
    Parse one Coinbase WebSocket message and update Prometheus
    metrics for ticker data.
    """

    MESSAGES_RECEIVED.inc()

    try:
        message = json.loads(raw_message)
    except json.JSONDecodeError:
        logger.warning("Received invalid JSON from Coinbase.")
        return

    if message.get("channel") != "ticker":
        return

    exchange_timestamp = message.get("timestamp")

    if exchange_timestamp is None:
        logger.warning("Ticker message did not contain a timestamp.")
        return

    for event in message.get("events", []):
        for ticker in event.get("tickers", []):

            if ticker.get("product_id") != PRODUCT_ID:
                continue

            latency = calculate_latency(exchange_timestamp)

            if latency is not None:
                MESSAGE_LATENCY.set(latency)


# ---------------------------------------------------------
# WebSocket consumer
# ---------------------------------------------------------

async def consume_market_data() -> None:
    """
    Maintain a persistent Coinbase WebSocket connection.

    Connection failures trigger automatic reconnection with
    exponential backoff capped at MAX_RECONNECT_DELAY seconds.
    """

    reconnect_delay = INITIAL_RECONNECT_DELAY

    while True:
        try:
            logger.info("Connecting to Coinbase WebSocket...")

            async with connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
                max_size=1_048_576,
            ) as websocket:

                await websocket.send(
                    json.dumps(SUBSCRIBE_MESSAGE)
                )

                logger.info(
                    "Connected and subscribed to %s ticker channel.",
                    PRODUCT_ID,
                )

                # Reset the backoff after a successful connection.
                reconnect_delay = INITIAL_RECONNECT_DELAY

                async for raw_message in websocket:
                    process_message(raw_message)

        except asyncio.CancelledError:
            logger.info("Market-data consumer cancelled.")
            raise

        except Exception as exc:
            logger.warning(
                "WebSocket connection lost: %s",
                exc,
            )

            WEBSOCKET_RECONNECTS.inc()

            logger.info(
                "Reconnecting in %d seconds.",
                reconnect_delay,
            )

            await asyncio.sleep(reconnect_delay)

            reconnect_delay = min(
                reconnect_delay * 2,
                MAX_RECONNECT_DELAY,
            )


# ---------------------------------------------------------
# Application entry point
# ---------------------------------------------------------

def main() -> None:
    """
    Start the Prometheus metrics server and the asynchronous
    market-data consumer.
    """

    start_http_server(METRICS_PORT)

    logger.info(
        "Prometheus metrics available at http://localhost:%d/metrics",
        METRICS_PORT,
    )

    asyncio.run(consume_market_data())


if __name__ == "__main__":
    main()
