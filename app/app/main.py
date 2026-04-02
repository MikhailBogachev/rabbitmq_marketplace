from contextlib import asynccontextmanager
import json

from aio_pika import Message
from fastapi import Body, FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from . import models, schemas, crud, database, rabbit
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
    await rabbit.RabbitMq.publish_price_update(
        {
            "event": "product.created",
            "product_id": db_product.id,
            "price": db_product.price,
            "message": f"Обновленная цена товара {db_product.id}: {db_product.price}",
            "force_fail": False,
        }
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
