from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from config.config import DATABASE_URL
from models.base import Base

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)

Session = async_sessionmaker(
    bind=engine, class_=AsyncSession, expire_on_commit=False
)


async def create_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
