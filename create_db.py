import asyncio

from database.database import create_db

if __name__ == "__main__":
    asyncio.run(create_db())
