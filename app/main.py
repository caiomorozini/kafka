from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends
from app.kafka import get_producer, KafkaProducer
from app.entities import Message


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.producer = KafkaProducer()
    await app.state.producer.start()
    yield
    await app.state.producer.stop()


app = FastAPI(lifespan=lifespan)


@app.post("/send")
async def send_message(payload: Message, producer: KafkaProducer = Depends(get_producer)):
    await producer.send("triagem", payload.model_dump_json())
    return {"message": f"Processo {payload.id} criado com sucesso!"}
