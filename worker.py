from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import json
import asyncio

async def consume_messages():
    consumer = AIOKafkaConsumer(
        "triagem",
        bootstrap_servers="localhost:29092",
        group_id="triagem-group",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers="localhost:29092",
    )

    await consumer.start()
    await producer.start()
    try:
        async for msg in consumer:
            # simulando um processamento com problema
            val = json.loads(msg.value.decode("utf-8"))
            await asyncio.sleep(3)  # Simulando um atraso no processamento
            print(f"produto: {val.get('produto')}")
            if val.get("produto") == 1:
                print(f"Erro ao processar a mensagem: {val}")
                # mandando para a dlq
                await producer.send_and_wait("triagem-dlq", msg.value)
            else:
                print(f"Mensagem processada com sucesso: {val}")
            await consumer.commit()
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    import asyncio
    asyncio.run(consume_messages())
