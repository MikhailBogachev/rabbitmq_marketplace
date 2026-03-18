from pydantic import BaseModel


class ProductCreate(BaseModel):
    name: str
    description: str | None = None
    price: float


class ProductRead(ProductCreate):
    id: int

    class Config:
        from_attributes = True
