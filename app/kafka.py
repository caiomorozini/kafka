from fastapi import FastAPI
from aiokafka import AIOKafkaProducer
import asyncio
from contextlib import asynccontextmanager
from app.entities import Message


class KafkaProducer:
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None

    async def start(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
        await self.producer.start()

    async def stop(self):
        if self.producer:
            await self.producer.stop()

    async def send(self, topic, value):
        if self.producer:
            await self.producer.send_and_wait(topic, value.encode("utf-8"))


producer = KafkaProducer(bootstrap_servers="localhost:29092")
