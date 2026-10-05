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
    "Total number of Coinbase ticker messages received.",
)

MESSAGE_LATENCY = Gauge(
    "coinbase_message_latency_seconds",
    "Seconds between the Coinbase ticker timestamp and local UTC time.",
)

WEBSOCKET_RECONNECTS = Counter(
    "coinbase_websocket_reconnects_total",
    "Total number of WebSocket reconnection attempts.",
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
# Coinbase subscription
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
    Calculate the elapsed time between Coinbase's timestamp and
    the local UTC clock.

    Returns:
        Latency in seconds, or None when the timestamp is invalid
        or appears to be ahead of the local clock.
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
    Parse a Coinbase WebSocket message and update Prometheus
    metrics for ticker events.
    """

    try:
        message = json.loads(raw_message)
    except json.JSONDecodeError:
        logger.warning("Received invalid JSON from Coinbase.")
        return

    if message.get("channel") != "ticker":
        return

    MESSAGES_RECEIVED.inc()

    for event in message.get("events", []):
        for ticker in event.get("tickers", []):
            if ticker.get("product_id") != PRODUCT_ID:
                continue

            exchange_timestamp = ticker.get("time")

            if exchange_timestamp is None:
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

    If the connection fails, the service waits using exponential
    backoff before attempting to reconnect.
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

                # A successful connection resets the backoff.
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
    Start Prometheus metrics and the asynchronous market-data
    consumer.
    """

    start_http_server(METRICS_PORT)

    logger.info(
        "Prometheus metrics available on port %d.",
        METRICS_PORT,
    )

    asyncio.run(consume_market_data())


if __name__ == "__main__":
    main()
