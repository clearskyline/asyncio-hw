import os
from dotenv import load_dotenv

load_dotenv()

PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_USER = os.getenv("PG_USER")
PG_DB = os.getenv("PG_DB")

POSTGRES_DSN = f'postgresql+asyncpg://{PG_USER}:{PG_PASSWORD}@localhost:5431/{PG_DB}'
