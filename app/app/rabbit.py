import aio_pika
import asyncio


RABBITMQ_URL = "amqp://user:password@rabbitmq:5672/"
EXCANGE_NAME = "price_excange"
QUEUE_NAME = "price_updates"
ROUTING_KEY = "price.update"


class RabbitMq:
    connection: aio_pika.RobustConnection | None = None
    channel: aio_pika.RobustChannel | None = None
    exchange: aio_pika.Exchange | None = None

    @classmethod
    async def connect(cls):
        cls.connection = await aio_pika.connect_robust(RABBITMQ_URL)
        cls.channel = await cls.connection.channel()
        cls.exchange = await cls.channel.declare_exchange(
            EXCANGE_NAME,
            aio_pika.ExchangeType.DIRECT,
            durable=True
        )

        queue = await cls.channel.declare_queue(
            QUEUE_NAME,
            durable=True,
        )
        await queue.bind(cls.exchange, routing_key=ROUTING_KEY)

    @classmethod
    async def publish_price_update(cls, message: str):
        if cls.exchange is None:
            await cls.connect()

        await cls.exchange.publish(
            aio_pika.Message(body=message.encode()),
            routing_key="price.update"
        )

    @classmethod
    async def close(cls) -> None:
        if cls.connection and not cls.connection.is_closed:
            await cls.connection.close()
