from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base


DATABASE_URL = "postgresql+asyncpg://postgres:password@db:5432/postgres"

engine = create_async_engine(DATABASE_URL, echo=True)
AsyncSessionlocal = sessionmaker(engine,
                                class_=AsyncSession,
                                expire_on_commit=False
)
Base = declarative_base()


async def get_session():
    async with AsyncSessionlocal() as session:
        yield session
