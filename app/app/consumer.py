import asyncio
import json
import logging

from .rabbit import RabbitMq, QUEUE_NAME

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("price-consumer")


async def main() -> None:
    await RabbitMq.connect()
    assert RabbitMq.queue is not None

    queue = RabbitMq.queue
    logger.info("Consuming from queue: %s", QUEUE_NAME)

    async with queue.iterator() as q:
        async for message in q:
            try:
                payload = json.loads(message.body.decode("utf-8"))
                logger.info("Got message: %s", payload)

                if payload.get("force_fail"):
                    raise RuntimeError("Forced fail for DLQ test")

                await message.ack()
            except Exception:
                logger.exception("Processing failed -> reject requeue=false (DLQ)")
                await message.reject(requeue=False)


if __name__ == "__main__":
    asyncio.run(main())
