from config import config
from confluent_kafka.admin import (AdminClient, NewTopic, ConfigResource)


def topic_exists(admin, topic):
    topic_metadata = admin.list_topics(topic=topic).topics
    if topic in topic_metadata:
        print(f'Topic {topic} exist')
        return True
    print(f'Topic {topic} does not exist')
    return False


def create_topic(admin, topic):
    new_topic = NewTopic(topic, num_partitions=3, replication_factor=1)
    result_dict = admin.create_topics([new_topic])
    for topic, future in result_dict.items():
        try:
            future.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))

if __name__ == '__main__':

    # Create Admin client
    admin = AdminClient(config)
    topic_name = 'weater_to_email_topic'
    max_msg_k = 50

    # Create topic if it doesn't exist
    if not topic_exists(admin, topic_name):
        create_topic(admin, topic_name)
        print(f'Topic is created {topic_name}')
