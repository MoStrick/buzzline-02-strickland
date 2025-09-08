"""
kafka_producer_strickland.py

Produce some streaming buzz strings and send them to a Kafka topic.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger



#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "buzz_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 10))
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Message Generator
#####################################


def generate_messages(producer, topic, interval_secs):
    """
    Generate a stream of buzz messages and send them to a Kafka topic.

    Args:
        producer (KafkaProducer): The Kafka producer instance.
        topic (str): The Kafka topic to send messages to.
        interval_secs (int): Time in seconds between sending messages.

    """
    string_list: list = [
        "This doesn’t make any sense",
        "I don’t get it",
        "This is so confusing",
        "I give up",
        "I can’t do this",
        "Wait, what just happened?",
        "I thought I understood, but I don’t",
        "This problem is impossible",
        "Why is this so hard?",
        "Nothing is clicking",
        "I need help",
        "Can you explain this again?",
        "I’m totally lost",
        "This is frustrating",
        "My brain hurts",
        "I’m stuck",
        "I don’t know where to start",
        "I keep messing up",
        "I hate this problem",
        "I’ve tried everything",
        "This is taking forever",
        "Why can’t I figure this out?",
        "I’m going in circles",
        "I keep getting the wrong answer",
        "This is the worst",
        "I don’t like math",
        "I can’t focus",
        "I wish I was done",
        "This is boring",
        "How many problems are left?",
        "Why do we even need to learn this?",
        "This is dragging on",
        "I’d rather be doing anything else",
        "This is so repetitive",
        "I’m tired of this",
        "Can we just stop?",
        "I’m over it",
        "Does this even matter?",
        "This is too much",
        "I can’t believe this is homework",
        "I’ll never use this in real life",
        "This was tough at first but now I think I’ve got it",
        "Oh! Now it makes sense",
        "Wait, I get it now",
        "This is actually kind of fun",
        "I solved it!",
        "I can’t believe I did it!",
        "That wasn’t so bad",
        "Yes! I got it right",
        "I’m on a roll",
        "This is clicking now",
        "This isn’t as hard as I thought",
        "Okay, I think I understand",
        "I’m getting the hang of this",
        "I can do this!",
        "I finally figured it out",
        "I feel smart",
        "This problem makes sense now",
        "I knew I could do it",
        "This is easier than it looked",
        "I like this kind of problem",
        "I think I’m improving",
        "I got the same answer twice, so it must be right",
        "Yes, that worked!",
        "This step was the key",
        "It’s actually satisfying when it works",
        "This is kind of cool",
        "I’m proud of myself",
        "I actually like this problem",
        "This isn’t so scary anymore",
        "Okay, I can explain it to someone else now",
        "I’m in the zone",
        "I’m working faster now",
        "I can focus right now",
        "I like figuring this out",
        "This is keeping me busy",
        "I feel productive",
        "I’m solving these one after another",
        "I just need to keep going",
        "I’m almost finished",
        "I can see the pattern now",
        "I’m starting to enjoy this",
        "I’m determined to finish",
        "This is actually interesting",
        "I just need to double-check my steps",
        "I’m noticing my mistakes and fixing them",
        "This problem is like a puzzle",
        "I just found the trick",
        "This part is easy",
        "I see what the teacher meant now",
        "I’m faster than I was before",
        "I’m focused right now",
        "This feels like progress",
        "I’m learning as I go",
        "This is kind of satisfying",
        "I know what to do next",
        "I just need to be careful",
        "I’ve got momentum",
        "I can finish strong",
        "This feels good",
        "I actually get it now",
        "I’m proud of my work",
        "I can’t wait to check my answer",
    ]
    try:
        while True:
            for message in string_list:
                logger.info(f"Generated buzz: {message}")
                producer.send(topic, value=message)
                logger.info(f"Sent message to topic '{topic}': {message}")
                time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error in message generation: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")


#####################################
# Main Function
#####################################


def main():
    """
    Main entry point for this producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams generated buzz message strings to the Kafka topic.
    """
    logger.info("START producer.")
    logger.info("Loading environment variables from .env file...")
    load_dotenv()
    verify_services()

    # fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Create the Kafka producer
    producer = create_kafka_producer()
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    generate_messages(producer, topic, interval_secs)

    logger.info("END producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
