import time
from contextlib import asynccontextmanager
from functools import wraps
from typing import Any, Awaitable, Callable, List, ParamSpec, Tuple

import oracledb
from fastapi import FastAPI
from pydantic import BaseModel
import os
from dotenv import load_dotenv

load_dotenv()

pool = None
oracledb.defaults.fetch_lobs = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    try:
        pool = oracledb.create_pool_async(
            user=os.getenv("USER"),
            password=os.getenv("PASSWORD"),
            dsn=os.getenv("DSN"),
        )
    except Exception as e:
        raise Exception(f"Failed to create database connection pool: {e}")

    yield

    if pool:
        await pool.close(force=True)


app = FastAPI(lifespan=lifespan, root_path=os.getenv("ROOT_PATH", ""))


class Project(BaseModel):
    id: str
    road_name: str
    description: str
    county: str
    limits: str
    aqcode: str
    mrp: str
    municipalities: str
    category: str
    type: str

    @classmethod
    def from_tuple(cls, data: Tuple):
        return cls(
            id=str(data[0]),
            road_name=str(data[1]),
            description=str(data[2]),
            county=str(data[3]),
            limits=str(data[4]),
            aqcode=str(data[5]),
            mrp=str(data[6]),
            municipalities=str(data[7]),
            category=str(data[8]),
            type="Other",
        )


class Comment(BaseModel):
    comment_id: str
    name: str
    email: str
    county: str
    comment_text: str
    submitdate: str
    mpms: str

    @classmethod
    def from_tuple(cls, data: Tuple):
        return cls(
            comment_id=str(data[0]),
            name=str(data[1]),
            email=str(data[2]),
            county=str(data[3]),
            comment_text=str(data[4]),
            submitdate=str(data[5]),
            mpms=str(data[6]),
        )


class Response(BaseModel):
    message: str | None
    data: List[object]


T = ParamSpec("T")


def async_timer(func: Callable[T, Awaitable[Any]]) -> Callable[T, Awaitable[Response]]:
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        result = await func(*args, **kwargs)
        end_time = time.time()
        return Response(data=result, message=f"{end_time - start_time:.4f} seconds")

    return wrapper


@async_timer
async def fetch_projects():
    if pool is None:
        raise Exception("Database connection pool is not initialized.")
    try:
        async with pool.acquire() as connection:
            with connection.cursor() as cursor:
                await cursor.execute(
                    'SELECT dbnum id, "Project Name" roadname, descript description, county, section limits, aqcode, lrpid mrp, mcds Municipalities, "Type" Type FROM master_web WHERE \'Type\' is not null'
                )
                result = await cursor.fetchall()
                if not result:
                    raise Exception("Database query did not return expected result.")
                return list(map(lambda row: Project.from_tuple(row), result))
    except oracledb.DatabaseError as e:
        raise Exception(f"Database connection error: {e}")


@app.get("/", response_model=Response)
async def get_projects():
    result = await fetch_projects()
    return result


@async_timer
async def fetch_comments():
    if pool is None:
        raise Exception("Database connection pool is not initialized.")
    try:
        async with pool.acquire() as connection:
            with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT commentid, commentor name, email, tipcounty county, comments comment_text, submitdate, mpms from comments"
                )
                result = await cursor.fetchall()
                if not result:
                    raise Exception("Database query did not return expected result.")
                return list(map(lambda row: Comment.from_tuple(row), result))
    except oracledb.DatabaseError as e:
        raise Exception(f"Database connection error: {e}")


@app.get("/comments")
async def get_comments():
    result = await fetch_comments()
    return result
