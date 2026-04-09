import asyncio
from datetime import datetime, timezone
import json
import logging

from sqlalchemy import insert
from sqlalchemy.exc import IntegrityError

from .models_events import ProcessedEvent

from .database import AsyncSessionLocal

from .events import EventEnvelope

from .rabbit import RabbitMq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("price-consumer")

RABBITMQ_URL = "amqp://user:password@rabbitmq:5672/"

CONSUMER_NAME = "price-service"
QUEUE_NAME = "price_updates"
BINDING_KEY = "price.*"


async def handle_event(envelope: EventEnvelope) -> None:
    logger.info("Handle price event: %s", envelope.data)


async def main() -> None:
    await RabbitMq.connect()
    assert RabbitMq.connection is not None
    assert RabbitMq.channel is not None

    channel = RabbitMq.channel
    exchange = RabbitMq.events_exchange
    assert exchange is not None

    queue = await channel.declare_queue(QUEUE_NAME, durable=True)
    await queue.bind(exchange, routing_key=BINDING_KEY)

    logger.info("Consuming %s with binding %s", QUEUE_NAME, BINDING_KEY)

    async with queue.iterator() as q:
        async for message in q:
            try:
                payload = json.loads(message.body.decode("utf-8"))
                envelope = EventEnvelope.model_validate(payload)
                logger.info(
                    "Got event_id=%s type=%s producer=%s data=%s",
                    envelope.event_id, envelope.event_type, envelope.producer, envelope.data
                )

                async with AsyncSessionLocal() as session:
                    try:
                        async with session.begin():
                            await session.execute(
                                insert(ProcessedEvent).values(
                                    event_id=envelope.event_id,
                                    consumer=CONSUMER_NAME,
                                    event_type=envelope.event_type,
                                    processed_at=datetime.now(timezone.utc),
                                )
                            )
                            await handle_event(envelope)

                    except IntegrityError:
                        await session.rollback()
                        logger.info("Duplicate event %s -> skip", envelope.event_id)

                await message.ack()

            except Exception:
                logger.exception("Processing failed -> reject to DLQ")
                await message.reject(requeue=False)


if __name__ == "__main__":
    asyncio.run(main())
