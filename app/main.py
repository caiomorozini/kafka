from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends
from app.kafka import get_producer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic, NewPartitions

from app.entities import Message

TOPICS = {
    "triagem",
    "triagem-dlq"
}

def ensure_topics(admin: KafkaAdminClient):
    existing = set(admin.list_topics())

    topics_to_create = []

    for name in TOPICS:
        if name not in existing:
            topics_to_create.append(
                NewTopic(
                    name=name,
                    num_partitions=5,
                    replication_factor=1,
                )
            )

    if topics_to_create:
        admin.create_topics(topics_to_create)


@asynccontextmanager
async def lifespan(app: FastAPI):

    admin = KafkaAdminClient(
        bootstrap_servers="localhost:29092",
    )
    ensure_topics(admin)
    app.state.admin_client = admin
    app.state.producer = KafkaProducer()
    await app.state.producer.start()
    yield
    await app.state.producer.stop()


app = FastAPI(lifespan=lifespan)


@app.post("/send")
async def send_message(payload: Message, producer: KafkaProducer = Depends(get_producer)):
    await producer.send("triagem", payload.model_dump_json())
    return {"message": f"Processo {payload.id} criado com sucesso!"}
