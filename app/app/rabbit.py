from __future__ import annotations

import json
from typing import Any, Optional

import aio_pika
import aiormq
from aio_pika import ExchangeType, Message


RABBITMQ_URL = "amqp://user:password@rabbitmq:5672/"

# Producer topology
EXCHANGE_NAME = "price_exchange"
ROUTING_KEY = "price.update"

# Main queue (consumer reads from it)
QUEUE_NAME = "price_updates"

# DLQ / DLX
DLX_NAME = "price_dlx"
DLQ_NAME = "price_updates.dlq"
DLQ_ROUTING_KEY = "price.update.dlq"


class RabbitMq:
    connection: Optional[aio_pika.RobustConnection] = None
    channel: Optional[aio_pika.RobustChannel] = None

    exchange: Optional[aio_pika.RobustExchange] = None
    dlx: Optional[aio_pika.RobustExchange] = None

    queue: Optional[aio_pika.RobustQueue] = None

    @classmethod
    async def connect(cls) -> None:
        """
        Creates AMQP connection, declares RabbitMQ topology:
        - exchange for normal events
        - DLX exchange
        - DLQ queue bound to DLX
        - main queue bound to normal exchange + configured to dead-letter to DLX
        """
        # Already connected
        if cls.connection and not cls.connection.is_closed:
            return

        cls.connection = await aio_pika.connect_robust(RABBITMQ_URL)
        cls.channel = await cls.connection.channel()
        await cls.channel.set_qos(prefetch_count=10)

        # 1) Main exchange (producer publishes here)
        cls.exchange = await cls.channel.declare_exchange(
            EXCHANGE_NAME,
            ExchangeType.DIRECT,
            durable=True,
        )

        # 2) Dead-letter exchange
        cls.dlx = await cls.channel.declare_exchange(
            DLX_NAME,
            ExchangeType.DIRECT,
            durable=True,
        )

        # 3) Dead-letter queue (where rejected messages go)
        dlq = await cls.channel.declare_queue(
            DLQ_NAME,
            durable=True,
        )
        await dlq.bind(cls.dlx, routing_key=DLQ_ROUTING_KEY)

        # 4) Main queue with DLQ settings
        cls.queue = await cls.channel.declare_queue(
            QUEUE_NAME,
            durable=True,
            arguments={
                "x-dead-letter-exchange": DLX_NAME,
                "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
            },
        )

        # 5) Bind main queue to main exchange
        await cls.queue.bind(cls.exchange, routing_key=ROUTING_KEY)

    @classmethod
    async def close(cls) -> None:
        if cls.connection and not cls.connection.is_closed:
            await cls.connection.close()

        cls.connection = None
        cls.channel = None
        cls.exchange = None
        cls.dlx = None
        cls.queue = None

    @classmethod
    async def publish_price_update(cls, payload: dict[str, Any]) -> None:
        """
        Publishes JSON message to EXCHANGE_NAME with ROUTING_KEY.
        """
        if cls.exchange is None:
            await cls.connect()

        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        msg = Message(
            body=body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )

        assert cls.exchange is not None
        await cls.exchange.publish(msg, routing_key=ROUTING_KEY)

    @classmethod
    async def dev_delete_queues(cls) -> None:
        """
        Optional helper for development:
        deletes main queue + DLQ.
        Use with extreme caution. In real systems you don't do this automatically.
        """
        if cls.channel is None:
            await cls.connect()
        assert cls.channel is not None

        for qname in (QUEUE_NAME, DLQ_NAME):
            q = aio_pika.Queue(cls.channel, qname)
            try:
                await q.delete(if_unused=False, if_empty=False)
            except aiormq.exceptions.ChannelNotFoundEntity:
                pass
