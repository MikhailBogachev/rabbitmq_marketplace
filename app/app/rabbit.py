from __future__ import annotations

import asyncio
import json
from typing import Any, Optional

import aio_pika
import aiormq
from aio_pika import ExchangeType, Message
from aio_pika.exceptions import DeliveryError


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


# Retry
RETRY_EXCHANGE_NAME = "price_retry_exchange"
RETRY_5S_QUEUE = "price_updates.retry.5s"
RETRY_30S_QUEUE = "price_updates.retry.30s"
RETRY_5S_KEY = "price.update.retry.5s"
RETRY_30S_KEY = "price.update.retry.30s"

# Unroutable
UNROUTABLE_EXCHANGE = "price_unroutable_exchange"
UNROUTABLE_QUEUE = "price_unroutable"


class RabbitMq:
    connection: Optional[aio_pika.RobustConnection] = None
    channel: Optional[aio_pika.RobustChannel] = None

    exchange: Optional[aio_pika.RobustExchange] = None
    dlx: Optional[aio_pika.RobustExchange] = None
    retry_exchange: Optional[aio_pika.RobustExchange] = None

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
        cls.channel = await cls.connection.channel(publisher_confirms=True)
        await cls.channel.set_qos(prefetch_count=10)

        unroutable_exchange = await cls.channel.declare_exchange(
            UNROUTABLE_EXCHANGE,
            ExchangeType.FANOUT,
            durable=True,
        )
        unroutable_queue = await cls.channel.declare_queue(
            UNROUTABLE_QUEUE,
            durable=True,
        )
        await unroutable_queue.bind(unroutable_exchange)

        # Main exchange (producer publishes here)
        cls.exchange = await cls.channel.declare_exchange(
            EXCHANGE_NAME,
            ExchangeType.DIRECT,
            durable=True,
            arguments={"alternate-exchange": UNROUTABLE_EXCHANGE}
        )

        # Dead-letter exchange
        cls.dlx = await cls.channel.declare_exchange(
            DLX_NAME,
            ExchangeType.DIRECT,
            durable=True,
        )

        # Retry exchange
        cls.retry_exchange = await cls.channel.declare_exchange(
            RETRY_EXCHANGE_NAME, ExchangeType.DIRECT, durable=True
        )

        # Dead-letter queue (where rejected messages go)
        dlq = await cls.channel.declare_queue(
            DLQ_NAME,
            durable=True,
        )
        await dlq.bind(cls.dlx, routing_key=DLQ_ROUTING_KEY)

        # Main queue with DLQ settings
        cls.queue = await cls.channel.declare_queue(
            QUEUE_NAME,
            durable=True,
            arguments={
                "x-dead-letter-exchange": DLX_NAME,
                "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
            },
        )
        await cls.queue.bind(cls.exchange, routing_key=ROUTING_KEY)

        retry_5s = await cls.channel.declare_queue(
            RETRY_5S_QUEUE,
            durable=True,
            arguments={
                "x-message-ttl": 5_000,
                "x-dead-letter-exchange": DLX_NAME,
                "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
            }
        )
        await cls.queue.bind(cls.retry_exchange, routing_key=RETRY_5S_KEY)

        retry_30s = await cls.channel.declare_queue(
            RETRY_30S_QUEUE,
            durable=True,
            arguments={
                "x-message-ttl": 5_000,
                "x-dead-letter-exchange": DLX_NAME,
                "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
            }
        )
        await cls.queue.bind(cls.retry_exchange, routing_key=RETRY_30S_KEY)


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
    def _make_message(
        cls,
        payload: dict[str, Any],
        headers: Optional[dict[str, Any]] = None,
    ) -> Message:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        return Message(
            body=body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            headers=headers or {},
        )


    @classmethod
    async def publish_price_update(
        cls,
        payload: dict[str, Any],
        headers: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Publishes JSON message to EXCHANGE_NAME with ROUTING_KEY.
        """
        if cls.exchange is None:
            await cls.connect()
        assert cls.exchange is not None

        msg = cls._make_message(payload, headers=headers)
        await cls.exchange.publish(msg, routing_key=ROUTING_KEY)


    @classmethod
    async def publish_retry(
        cls,
        payload: dict[str, Any],
        retry_key: str,
        headers: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Publish message into retry exchange with routing key:
        - price.update.retry.5s
        - price.update.retry.30s
        """
        if cls.retry_exchange is None:
            await cls.connect()
        assert cls.retry_exchange is not None

        msg = cls._make_message(payload, headers=headers)
        await cls.retry_exchange.publish(msg, routing_key=retry_key)


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
