import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from create_tickets import generate_ticket
from create_topic import create_topic_if_not_exists

def send_messag(topic_name):
    """
    Envoie de message dans le topic indiqué
    """

    producer = KafkaProducer(
        bootstrap_servers=["redpanda-0:9092", "redpanda-1:9092", "redpanda-2:9092"],
        value_serializer=lambda m: json.dumps(m).encode('ascii'),
        retries=5
    )
    print("Successfully connected to Redpanda cluster")


    # Création de topic s'il n'existe pas encore
    create_topic_if_not_exists(topic_name)

    def on_success(metadata):
        print(f"Message produced to topic '{metadata.topic}' at offset {metadata.offset}")

    def on_error(e):
        print(f"Error sending message: {e}")

    try:
        # Envoie de données dans le topic de Redpanda, boucle infinie pour produire des messages en continu
        while True:
            try:
                msg = generate_ticket()
                future = producer.send(topic_name, msg)
                future.add_callback(on_success)
                future.add_errback(on_error)
                producer.flush()
                time.sleep(1)  # Attendre 1 seconde entre chaque message
            except KeyboardInterrupt:
                print("Arrêt du producer...")
                break
            except Exception as e:
                print(f"Erreur: {e}")
                continue
    finally:
        if producer:
            producer.flush()
            producer.close()

if __name__ == "__main__":
    # Attendre un peu que les brokers soient prêts
    time.sleep(10)

    topic_name = "client_tickets"
    send_messag(topic_name)