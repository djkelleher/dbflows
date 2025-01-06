from functools import cache
from typing import Any, Dict, List

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from .utils import compile_statement, get_connection_pool, logger


class PgPoolConn:
    @classmethod
    async def create(cls, pg_url: str, max_conn: int = 10):
        self = cls()
        self.pool = await get_connection_pool(pg_url, max_conn=max_conn)
        return self

    async def execute(self, query: Any) -> List[Any]:
        async with self.pool.acquire() as conn:
            return await conn.execute(compile_statement(query))

    async def fetch(self, query: sa.Select) -> List[Any]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(compile_statement(query))

    async def fetchrow(self, query: sa.Select) -> List[Any]:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(compile_statement(query))

    async def fetchval(self, query: sa.Select) -> List[Any]:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(compile_statement(query))

    async def close(self):
        logger.info("Closing asyncpg connection pool.")
        await self.pool.close()

@cache
def cached_sa_conn(pg_url: str):
    return create_async_engine(
        pg_url,
        pool_size=20,       # Allow up to 20 persistent connections
        max_overflow=0,    # Allow up to 10 additional connections temporarily
        pool_timeout=150,    # Wait up to 150 seconds for a connection
        echo=True           # Enable SQLAlchemy logging
    )

class PgConn:
    def __init__(self, pg_url: str, use_cached_engine: bool = True):
        self.engine = cached_sa_conn(pg_url) if use_cached_engine else create_async_engine(pg_url)

    async def execute(self, query: Any) -> List[Any]:
        async with self.engine.begin() as conn:
            return await conn.execute(query)

    async def fetch(self, query: sa.Select) -> List[Any]:
        async with self.engine.begin() as conn:
            return (await conn.execute(query)).fetchall()

    async def fetch_dicts(self, query: sa.Select) -> List[Dict[str, Any]]:
        async with self.engine.begin() as conn:
            rows = (await conn.execute(query)).fetchall()
        return [dict(row._mapping) for row in rows]

    async def fetchrow(self, query: sa.Select) -> List[Any]:
        async with self.engine.begin() as conn:
            return (await conn.execute(query)).fetchone()

    async def fetchrow_dict(self, query: sa.Select) -> Dict[str, Any]:
        async with self.engine.begin() as conn:
            row = (await conn.execute(query)).fetchone()
        if row:
            return dict(row._mapping)

    async def fetchval(self, query: sa.Select) -> List[Any]:
        async with self.engine.begin() as conn:
            fetched_value = (await conn.execute(query)).scalar()
        return fetched_value

    async def close(self):
        logger.info("Closing SQLAlchemy engine.")
        await self.engine.dispose()
