from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

NUM_PARTITIONS = 3
REPLICATION_FACTOR = 3

def create_topic_if_not_exists(topic_name):
    admin_client = KafkaAdminClient(
        bootstrap_servers=["redpanda-0:9092", "redpanda-1:9092", "redpanda-2:9092"],
    )
    
    try:
        topic_list = []
        topic_list.append(NewTopic(
            name=topic_name,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR
        ))
        admin_client.create_topics(topic_list)
        print(f"Topic {topic_name} created successfully")
    except TopicAlreadyExistsError:
        print(f"Topic {topic_name} already exists")
    except Exception as e:
        print(f"Error creating topic: {e}")
    finally:
        admin_client.close()