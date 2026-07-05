from fastapi import Request
from aiokafka import AIOKafkaProducer


class KafkaProducer:
    def __init__(self):
        self._producer: AIOKafkaProducer | None = None

    async def start(self):
        self._producer = AIOKafkaProducer(
            bootstrap_servers="localhost:29092",
        )
        await self._producer.start()

    async def stop(self):
        if self._producer:
            await self._producer.stop()

    async def send(self, topic, value):
        if self._producer is None:
            raise RuntimeError("Producer is not started")

        await self._producer.send_and_wait(topic, value.encode("utf-8"))


def get_producer(request: Request) -> KafkaProducer:
    return request.app.state.producer
