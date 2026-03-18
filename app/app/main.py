from contextlib import asynccontextmanager

import asyncio
from fastapi import FastAPI, Depends
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
        f"Обновленная цена товара {db_product.id}: {db_product.price}"
    )
    return db_product


@app.get("/products/", response_model=list[schemas.ProductRead])
async def list_products(db: AsyncSession = Depends(database.get_session)):
    return await crud.get_products(db)
