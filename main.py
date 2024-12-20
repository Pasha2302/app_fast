from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import json
import httpx
import redis.asyncio as redis_async
from contextlib import asynccontextmanager

from redis.asyncio import Redis

# Настройки Redis
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_CACHE_TTL = 3600  # Время жизни кэша (1 час)

# URL вашего Django API
# DJANGO_API_URL = "http://127.0.0.1/api/v1/get-data-casino/{name}/"
DJANGO_API_URL = "https://adm.incasinowetrust.com/api/v1/get-data-casino/{id}/"


# Pydantic модель для ответа
class CasinoResponse(BaseModel):
    name: int
    data: dict


# Создание lifespan-контекста
@asynccontextmanager
async def lifespan(_app: FastAPI):
    redis_client = await redis_async.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True)
    print("Redis подключен")
    _app.state.redis_client = redis_client
    yield
    await redis_client.close()
    print("Redis отключен")

# Инициализация FastAPI с lifespan
app = FastAPI(lifespan=lifespan)


# Асинхронная функция для получения данных из Django
async def fetch_data_from_django(pk: int):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(DJANGO_API_URL.format(id=pk), timeout=10.0)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=e.response.status_code,
                detail=f"Error fetching data from Django: {e.response.text}",
            )
        except httpx.RequestError as e:
            raise HTTPException(status_code=500, detail=f"Error fetching data from Django: {str(e)}")


# Эндпоинт для клиента
@app.get("/_api/v1/get-data-casino/{pk}/")
async def get_data_casino(pk: int):
    redis_client: Redis = app.state.redis_client

    # Проверяем данные в Redis
    cached_data = await redis_client.get(f"casino:{pk}")
    if cached_data:
        return json.loads(cached_data)

    # Если данных нет в кэше, загружаем их из Django
    data = await fetch_data_from_django(pk)

    # Сохраняем данные в Redis с установленным временем жизни
    await redis_client.setex(f"casino:{pk}", REDIS_CACHE_TTL, json.dumps(data))

    return data
