from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base


DATABASE_URL = "postgresql+asyncpg://postgres:password@db:5432/postgres"

engine = create_async_engine(DATABASE_URL, echo=True)
AsyncSessionLocal = sessionmaker(engine,
                                class_=AsyncSession,
                                expire_on_commit=False
)
Base = declarative_base()


async def get_session():
    async with AsyncSessionLocal() as session:
        yield session
