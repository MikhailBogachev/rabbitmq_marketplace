import asyncio
import logging

import aio_pika


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("price-consumer")


RABBITMQ_URL = "amqp://user:password@rabbitmq:5672/"
QUEUE_NAME = "price_updates"


async def main() -> None:
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()

    await channel.set_qos(prefetch_count=10)

    queue = await channel.declare_queue(QUEUE_NAME, durable=True)

    logger.info("Waiting for messages...")

    async with queue.iterator() as q:
        async for message in q:
            try:
                async with message.process(requeue=False):
                    body = message.body.decode()
                    logger.info("Got message: %s", body)
            except Exception:
                logger.exception("Failde to process message")


if __name__ == "__main__":
    asyncio.run(main())
