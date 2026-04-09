from contextlib import asynccontextmanager
import json
from uuid import uuid4

from aio_pika import Message
from fastapi import Body, FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from . import models, schemas, crud, database, rabbit, models_events
from fastapi.middleware.cors import CORSMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with database.engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)
    await rabbit.RabbitMq.connect()
    yield
    await rabbit.RabbitMq.connection.close()
    await database.engine.dispose()


app = FastAPI(lifespan=lifespan)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)


@app.post("/products", response_model=schemas.ProductRead)
async def create_product(
    product: schemas.ProductCreate,
    db: AsyncSession = Depends(database.get_session)
):
    db_product = await crud.create_product(db, product)
    await rabbit.RabbitMq.publish_event(
        routing_key="product.created",
        data={
            "product_id": db_product.id,
            "name": db_product.name,
            "price": db_product.price,
        }
    )
    await rabbit.RabbitMq.publish_event(
        routing_key="price.updated",
        data={
            "product_id": db_product.id,
            "new_price": db_product.price,
        },
    )
    return db_product

@app.post("/debug/publish_fail/")
async def publish_fail(payload: dict = Body(default={"force_fail": True})):
    await rabbit.RabbitMq.publish_price_update(payload)
    return {"ok": True}


@app.get("/products/", response_model=list[schemas.ProductRead])
async def list_products(db: AsyncSession = Depends(database.get_session)):
    return await crud.get_products(db)


@app.post("/debug/publish_unroutable/")
async def publish_unroutable():
    payload = {"event": "test.unroutable"}
    body = json.dumps(payload).encode("utf-8")
    msg = Message(body=body, content_type="application/json")

    # Публикуем напрямую в exchange с неправильным ключом
    await rabbit.RabbitMq.exchange.publish(msg, routing_key="price.update.WRONG")
    return {"ok": True}


@app.post("/debug/publish_duplicate_price_updated")
async def publish_duplicate_price_updated(product_id: int = Body(1), new_price: float = Body(123.45)):
    event_id = str(uuid4())

    envelope = {
        "event_id": event_id,
        "event_type": "price.updated",
        "occurred_at": "2026-04-09T00:00:00Z",
        "producer": "catalog-service",
        "schema_version": 1,
        "data": {"product_id": product_id, "new_price": new_price},
    }

    # публикуем два раза ОДИН И ТОТ ЖЕ event_id
    await rabbit.RabbitMq.publish_event("price.updated", envelope)
    await rabbit.RabbitMq.publish_event("price.updated", envelope)

    return {"published_event_id": event_id, "count": 2}