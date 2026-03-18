import aio_pika
import asyncio


RABBITMQ_URL = "amqp://user:password@rabbitmq:5672/"
EXCANGE_NAME = "price_excange"


class RabbitMq:
    connection = None
    channel = None
    excange = None

    @classmethod
    async def connect(cls):
        cls.connection = await aio_pika.connect_robust(RABBITMQ_URL)
        cls.channel = await cls.connection.channel()
        cls.excange = await cls.channel.declare_exchange(EXCANGE_NAME,
                                                        aio_pika.ExchangeType.DIRECT)

    @classmethod
    async def publish_price_update(cls, message: str):
        if cls.excange is None:
            await cls.connect()
        await cls.excange.publish(
            aio_pika.Message(body=message.encode()),
            routing_key="price.update"
        )
