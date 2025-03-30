#!/usr/bin/env python3
import argparse
import json
import logging
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Read the last N messages from a Kafka topic')
    parser.add_argument('--num-messages', type=int, default=10, 
                        help='Number of messages to read')
    parser.add_argument('--timeout', type=int, default=10000,
                        help='Consumer timeout in milliseconds')
    parser.add_argument('--pretty', action='store_true',
                        help='Pretty print JSON messages')
    return parser.parse_args()

def get_topic_partitions(consumer, topic):
    """Get all partitions for a topic."""
    try:
        return consumer.partitions_for_topic(topic)
    except KafkaError as e:
        logger.error(f"Error getting partitions for topic {topic}: {e}")
        return None

def get_last_n_messages(bootstrap_servers, topic, n, timeout_ms):
    """Get the last N messages from a Kafka topic."""
    messages = []
    
    # Create a consumer
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',  # Start from the beginning if we need to
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
        enable_auto_commit=False  # Don't commit offsets
    )
    
    # Get list of partitions for the topic
    partitions = get_topic_partitions(consumer, topic)
    if not partitions:
        logger.error(f"Topic {topic} does not exist or has no partitions")
        return messages
    
    # Create TopicPartition objects for each partition
    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    
    # Assign the consumer to these partitions
    consumer.assign(topic_partitions)
    
    # Get end offsets for each partition
    end_offsets = consumer.end_offsets(topic_partitions)
    
    # Calculate starting offsets to get last N messages
    for tp in topic_partitions:
        # Get the end offset for this partition
        end_offset = end_offsets[tp]
        
        if end_offset == 0:
            # Empty partition
            continue
        
        # Calculate how many messages to get from this partition
        # Start with a proportional distribution of N across partitions
        partition_message_count = min(n, end_offset)
        
        # Set the consumer's position to the calculated offset
        start_offset = max(0, end_offset - partition_message_count)
        consumer.seek(tp, start_offset)
    
    # Collect messages
    collected_messages = []
    for message in consumer:
        collected_messages.append({
            'topic': message.topic,
            'partition': message.partition,
            'offset': message.offset,
            'timestamp': message.timestamp,
            'key': message.key.decode('utf-8') if message.key else None,
            'value': message.value
        })
        
        # Break if we've collected enough messages
        if len(collected_messages) >= n:
            break
    
    # Close the consumer
    consumer.close()
    
    # Sort by offset (to get the most recent messages)
    collected_messages.sort(key=lambda x: x['offset'], reverse=True)
    
    # Return the N most recent messages
    return collected_messages[:n]

def main():
    """Main entry point."""
    args = parse_arguments()
    
    logger.info(f"Fetching the last {args.num_messages} messages from topic 'raw.measurements'")
    
    messages = get_last_n_messages(
        ["localhost:9091", "localhost:9092", "localhost:9093"], 
        "raw.measurements", 
        args.num_messages,
        args.timeout
    )
    
    if not messages:
        logger.info(f"No messages found in topic raw.measurements")
        return
    
    # Display the messages
    logger.info(f"Found {len(messages)} messages")
    
    for i, msg in enumerate(messages, 1):
        print(f"\n--- Message {i} ---")
        print(f"Topic: {msg['topic']}")
        print(f"Partition: {msg['partition']}")
        print(f"Offset: {msg['offset']}")
        print(f"Timestamp: {msg['timestamp']}")
        if msg['key']:
            print(f"Key: {msg['key']}")
        
        # Print the value
        if args.pretty and isinstance(msg['value'], (dict, list)):
            value_str = json.dumps(msg['value'], indent=2)
        else:
            value_str = str(msg['value'])
        
        print(f"Value: {value_str}")

if __name__ == "__main__":
    main()