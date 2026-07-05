from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:29092"
)

producer.send("triagem", b'{"id": 1, "produto": 1, "modalidade": 1}')
producer.flush()