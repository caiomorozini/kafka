from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.kafka import producer
from app.entities import Message


@asynccontextmanager
async def lifespan(app: FastAPI):
    await producer.start()
    yield
    await producer.stop()


app = FastAPI(lifespan=lifespan)


@app.post("/send")
async def send_message(payload: Message):
    await producer.send("triagem", payload.model_dump_json())
    return {"message": f"Processo {payload.id} criado com sucesso!"}
