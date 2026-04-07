from __future__ import annotations

import json
import logging
from typing import Any, Optional

import aio_pika
from aio_pika import ExchangeType, Message

from .events import EventEnvelope


RABBITMQ_URL = "amqp://user:password@rabbitmq:5672/"


# Nain events bus
EVENTS_EXCHANGE = "events"

# Unroutable
UNROUTABLE_EXCHANGE = "events_unroutable_exchange"
UNROUTABLE_QUEUE = "events_unroutable"

# DLQ / DLX
DLX_NAME = "events_dlx"
DLQ_ROUTING_KEY = "deadletter"

# Retry
RETRY_EXCHANGE_NAME = "events_retry_exchange"
RETRY_5S_QUEUE = "events.retry.5s"
RETRY_30S_QUEUE = "events.retry.30s"
RETRY_5S_KEY = "retry.5s"
RETRY_30S_KEY = "retry.30s"

PRODUCER_NAME = "catalog-service"


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("rabbit")


class RabbitMq:
    connection: Optional[aio_pika.RobustConnection] = None
    channel: Optional[aio_pika.RobustChannel] = None

    events_exchange: Optional[aio_pika.RobustExchange] = None
    dlx: Optional[aio_pika.RobustExchange] = None
    retry_exchange: Optional[aio_pika.RobustExchange] = None

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

        # Unroutable exchange
        unroutable_exchange = await cls.channel.declare_exchange(
            UNROUTABLE_EXCHANGE,
            ExchangeType.FANOUT,
            durable=True,
        )

        # Unroutable queue
        unroutable_queue = await cls.channel.declare_queue(
            UNROUTABLE_QUEUE,
            durable=True,
        )
        await unroutable_queue.bind(unroutable_exchange)

        # Event exchange topic + alterante echange
        cls.events_exchange = await cls.channel.declare_exchange(
            EVENTS_EXCHANGE,
            ExchangeType.TOPIC,
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

        # Retry queues
        retry_5s = await cls.channel.declare_queue(
            RETRY_5S_QUEUE,
            durable=True,
            arguments={
                "x-message-ttl": 5_000,
                "x-dead-letter-exchange": EVENTS_EXCHANGE,
            }
        )
        await retry_5s.bind(cls.retry_exchange, routing_key=RETRY_5S_KEY)

        retry_30s = await cls.channel.declare_queue(
            RETRY_30S_QUEUE,
            durable=True,
            arguments={
                "x-message-ttl": 30_000,
                "x-dead-letter-exchange": EVENTS_EXCHANGE,
            }
        )
        await retry_30s.bind(cls.retry_exchange, routing_key=RETRY_30S_KEY)


    @classmethod
    async def close(cls) -> None:
        if cls.connection and not cls.connection.is_closed:
            await cls.connection.close()

        cls.connection = None
        cls.channel = None
        cls.events_exchange = None
        cls.dlx = None
        cls.retry_exchange = None


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
    async def publish_event(
        cls,
        routing_key: str,
        data: dict[str, Any],
        headers: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Publish domain event to topic exchange with routing_key like:
        - product.created
        - product.updated
        - price.updated
        """
        if cls.events_exchange is None:
            await cls.connect()
        assert cls.events_exchange is not None

        envelope = EventEnvelope(
            event_type=routing_key,
            producer = PRODUCER_NAME,
            data=data
        )

        msg = cls._make_message(envelope.model_dump(mode="json"), headers=headers)
        await cls.events_exchange.publish(msg, routing_key=routing_key)
        return envelope


    @classmethod
    async def publish_retry(
        cls,
        retry_key: str,
        original_routing_key: str,
        payload: dict[str, Any],
        headers: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Retry publish:
        - message goes to retry queue by retry_key (retry.5s / retry.30s)
        - after TTL it dead-letters back to EVENTS_EXCHANGE
        - IMPORTANT: we must preserve original routing key.
          RabbitMQ DLX supports per-message routing key via header 'x-death' is read-only,
          so we store it ourselves and consumer will re-publish correctly if needed.
        """
        if cls.retry_exchange is None:
            await cls.connect()
        assert cls.retry_exchange is not None

        new_headers = dict(headers or {})
        new_headers["x-original-routing-key"] = original_routing_key

        msg = cls._make_message(payload, headers=new_headers)
        await cls.retry_exchange.publish(msg, routing_key=retry_key)
