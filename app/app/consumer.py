import asyncio
import json
import logging

from .events import EventEnvelope

from .rabbit import RabbitMq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("price-consumer")

RABBITMQ_URL = "amqp://user:password@rabbitmq:5672/"

QUEUE_NAME = "price_updates"
BINDING_KEY = "price.*"


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
            async with message.process(requeue=False):
                payload = json.loads(message.body.decode("utf-8"))
                envelope = EventEnvelope.model_validate(payload)
                logger.info(
                    "Got event_id=%s type=%s producer=%s data=%s",
                    envelope.event_id, envelope.event_type, envelope.producer, envelope.data
                )

if __name__ == "__main__":
    asyncio.run(main())
