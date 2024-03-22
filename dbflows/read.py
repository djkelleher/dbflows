from typing import Any, List

import pandas as pd
import sqlalchemy as sa


async def fetchall(pg_engine: AsyncEngine, query: sa.Select) -> List[Any]:
    async with pg_engine.begin() as conn:
        res = await conn.execute(query)
        return list(res.fetchall())


async def fetchone(pg_engine: AsyncEngine, query: sa.Select) -> List[Any]:
    async with pg_engine.begin() as conn:
        res = await conn.execute(query)
        return res.fetchone()


async def scalars(pg_engine: AsyncEngine, query: sa.Select) -> List[Any]:
    async with pg_engine.begin() as conn:
        res = await conn.execute(query)
        return list(res.scalars())


async def scalar(pg_engine: AsyncEngine, query: sa.Select) -> Any:
    async with pg_engine.begin() as conn:
        res = await conn.execute(query)
        return res.scalar()


async def df_from_query(pg_engine: AsyncEngine, query: sa.Select) -> pd.DataFrame:
    async with pg_engine.begin() as conn:
        return pd.read_sql_query(query, conn)
