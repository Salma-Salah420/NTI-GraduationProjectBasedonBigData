import asyncio
import json
import os
from datetime import datetime, timezone

import websockets
from kafka import KafkaProducer


AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

KAFKA_TOPIC = "ais-events"

API_KEY = os.getenv("AISSTREAM_API_KEY")


if not API_KEY:
    raise RuntimeError("AISSTREAM_API_KEY is not set")


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


async def stream_ais():

    async with websockets.connect(AISSTREAM_URL) as websocket:

        subscription = {
            "APIKey": API_KEY,
            "BoundingBoxes": [
                [
                    [-10.0, -30.0],
                    [60.0, 60.0],
                ]
            ],
            "FilterMessageTypes": [
                "PositionReport",
            ],
        }

        await websocket.send(json.dumps(subscription))

        print("Connected to AISStream")
        print("Subscription sent")
        print("Streaming AIS PositionReports to Kafka...")

        async for message in websocket:

            print("RAW AISSTREAM MESSAGE:")
            print(message)

            data = json.loads(message)

            if "error" in data:
                print(f"AISStream ERROR: {data['error']}")
                continue

            if data.get("MessageType") != "PositionReport":
                print(f"Skipping message type: {data.get('MessageType')}")
                continue

            position = data["Message"]["PositionReport"]

            event = {
                "mmsi": position.get("UserID"),
                "base_date_time": datetime.now(timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "longitude": position.get("Longitude"),
                "latitude": position.get("Latitude"),
                "sog": position.get("Sog"),
                "cog": position.get("Cog"),
            }

            producer.send(
                KAFKA_TOPIC,
                value=event,
            )

            producer.flush()

            print(
                f"Sent AIS event: "
                f"MMSI={event['mmsi']} "
                f"lat={event['latitude']} "
                f"lon={event['longitude']}"
            )


if __name__ == "__main__":
    asyncio.run(stream_ais())