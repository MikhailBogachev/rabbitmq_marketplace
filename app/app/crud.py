from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from . import models, schemas


async def create_product(
    db: AsyncSession,
    product: schemas.ProductCreate
):
    db_product = models.Product(
        name=product.name,
        description=product.description,
        price=product.price
    )
    db.add(db_product)
    await db.commit()
    await db.refresh(db_product)
    return db_product


async def get_products(db: AsyncSession):
    result = await db.execute(select(models.Product))
    return result.scalars().all()
