import asyncio
import json
import logging

import aio_pika

from .rabbit import RETRY_30S_KEY, RETRY_5S_KEY, RabbitMq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("price-consumer")


def get_retry_count(message: aio_pika.IncomingMessage) -> int:
    val = (message.headers or {}).get("x-retry-count", 0)
    try:
        return int(val)
    except Exception:
        return 0


async def main() -> None:
    await RabbitMq.connect()
    assert RabbitMq.queue is not None

    queue = RabbitMq.queue
    logger.info("Waiting for messages...")

    async with queue.iterator() as q:
        async for message in q:
            raw = message.body.decode("utf-8")
            try:
                payload = json.loads(raw)
                if not isinstance(payload, dict):
                    payload = {"message": payload}
            except json.JSONDecodeError:
                payload = {"message": raw}

            retry_count = get_retry_count(message)
            payload["_retry_count"] = retry_count

            try:
                logger.info("Got message (retry=%s): %s", retry_count, payload)

                # имитация ошибки
                if payload.get("force_fail"):
                    raise RuntimeError("Forced fail")

                await message.ack()

            except Exception:
                logger.exception("Processing failed")

                next_retry = retry_count + 1
                # payload["force_fail"] = payload.get("force_fail", True)

                new_headers = dict(message.headers or {})
                new_headers["x-retry-count"] = next_retry

                if next_retry == 1:
                    await asyncio.sleep(10)
                    await RabbitMq.publish_retry(payload, RETRY_5S_KEY, headers=new_headers)
                    await message.ack()
                    continue

                if next_retry == 2:
                    await asyncio.sleep(10)
                    await RabbitMq.publish_retry(payload, RETRY_30S_KEY, headers=new_headers)
                    await message.ack()
                    continue

                await message.reject(requeue=False)


if __name__ == "__main__":
    asyncio.run(main())
