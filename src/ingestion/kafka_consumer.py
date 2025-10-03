from confluent_kafka import Consumer

from load_config import read_client


def energy_consumer():
    """
    Consume up to 50 messages from Kafka topic within 10 second timeout.
    Suitable for Airflow batch jobs.

    Returns:
    +---------------+--------------------+--------------------+
    |            key|               value| ingestion_timestamp|
    +---------------+--------------------+--------------------+
    |BRENT_CRUDE_USD|{"status": "succe...|2025-09-24 14:52:...|
    |        WTI_USD|{"status": "succe...|2025-09-24 14:52:...|
    |NATURAL_GAS_USD|{"status": "succe...|2025-09-24 14:52:...|
    +---------------+--------------------+--------------------+
    """
    topic = "energy_prices"
    config = read_client()
    # config["group.id"] = "airflow-batch-consumer"
    # config["auto.offset.reset"] = "earliest"

    ########### for testing ############
    config["group.id"] = "test-consumer"
    config["auto.offset.reset"] = "earliest"
    config["enable.auto.commit"] = False

    consumer = Consumer(config)
    consumer.subscribe([topic])

    try:
        messages = consumer.consume(num_messages=50, timeout=10.0)
        results = []

        for msg in messages:
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            key = msg.key().decode("utf-8") if msg.key() else None
            value = msg.value().decode("utf-8")
            results.append({"key": key, "value": value})

        consumed_df = spark.createDataFrame(results)        

    finally:
        consumer.close()
    
    return consumed_df
