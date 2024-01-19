from confluent_kafka import Consumer, KafkaError

# Define Kafka consumer configuration
from config import config

## Create Kafka consumer
consumer = Consumer(config)

# Subscribe to one or more topics
consumer.subscribe(['weater_to_email_topic'])  # Replace with your Kafka topic

try:
    while True:
        # Poll for messages
        msg = consumer.poll(timeout=1000)  # Adjust the timeout as needed

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event - not an error
                continue
            else:
                print(f"Error: {msg.error()}")
                break

        # Process the received message
        print(f"Received message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    pass
finally:
    # Close down consumer to commit final offsets.
    consumer.close()