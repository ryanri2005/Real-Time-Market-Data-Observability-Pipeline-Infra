import asyncio
import json
import logging
from datetime import datetime, timezone

import websockets
from prometheus_client import Counter, Gauge, start_http_server


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

WS_URL = "wss://advanced-trade-ws.coinbase.com"
PRODUCT_ID = "BTC-USD"
METRICS_PORT = 8000


# ---------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------

MESSAGES_RECEIVED = Counter(
    "coinbase_messages_received",
    "Total number of ticker messages received from Coinbase",
)

MESSAGE_LATENCY = Gauge(
    "coinbase_message_latency_seconds",
    "Estimated exchange-to-client message latency in seconds",
)

WEBSOCKET_RECONNECTS = Counter(
    "coinbase_websocket_reconnects",
    "Total number of WebSocket reconnection attempts",
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
# Coinbase WebSocket subscription
# ---------------------------------------------------------

SUBSCRIBE_MESSAGE = {
    "type": "subscribe",
    "product_ids": [PRODUCT_ID],
    "channel": "ticker",
}


# ---------------------------------------------------------
# Message processing
# ---------------------------------------------------------

def calculate_latency(exchange_timestamp: str) -> float | None:
    """
    Calculate the difference between the local UTC time and the
    timestamp supplied by Coinbase.

    Returns latency in seconds.
    """

    try:
        exchange_time = datetime.fromisoformat(
            exchange_timestamp.replace("Z", "+00:00")
        )

        if exchange_time.tzinfo is None:
            exchange_time = exchange_time.replace(tzinfo=timezone.utc)

        local_time = datetime.now(timezone.utc)

        latency = (local_time - exchange_time).total_seconds()

        # A clock difference or malformed timestamp can produce
        # a negative value. Do not publish negative latency.
        if latency < 0:
            return None

        return latency

    except (TypeError, ValueError):
        return None


def process_message(raw_message: str) -> None:
    """
    Parse one Coinbase WebSocket message and update Prometheus metrics.
    """

    try:
        message = json.loads(raw_message)
    except json.JSONDecodeError:
        logger.warning("Received invalid JSON message")
        return

    MESSAGES_RECEIVED.inc()

    exchange_timestamp = message.get("timestamp")

    if exchange_timestamp is None:
        return

    latency = calculate_latency(exchange_timestamp)

    if latency is not None:
        MESSAGE_LATENCY.set(latency)


# ---------------------------------------------------------
# WebSocket consumer
# ---------------------------------------------------------

async def consume_market_data() -> None:
    """
    Maintain a persistent connection to Coinbase.

    Connection failures trigger an automatic reconnect with
    exponential backoff.
    """

    reconnect_delay = 1

    while True:
        try:
            logger.info("Connecting to Coinbase WebSocket...")

            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
            ) as websocket:

                await websocket.send(json.dumps(SUBSCRIBE_MESSAGE))

                logger.info(
                    "Connected. Subscribed to %s ticker channel.",
                    PRODUCT_ID,
                )

                # Reset the backoff after a successful connection.
                reconnect_delay = 1

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
                "Reconnecting in %d seconds...",
                reconnect_delay,
            )

            await asyncio.sleep(reconnect_delay)

            reconnect_delay = min(
                reconnect_delay * 2,
                60,
            )


# ---------------------------------------------------------
# Application entry point
# ---------------------------------------------------------

def main() -> None:
    """
    Start the Prometheus metrics server and market-data consumer.
    """

    start_http_server(METRICS_PORT)

    logger.info(
        "Prometheus metrics available at "
        "http://localhost:%d/metrics",
        METRICS_PORT,
    )

    asyncio.run(consume_market_data())


if __name__ == "__main__":
    main()
