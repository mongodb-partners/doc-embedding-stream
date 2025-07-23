from io import BytesIO
import json
from confluent_kafka import Producer, Consumer
import boto3
import os
import dotenv
import threading
from pymongo import MongoClient
from llama_parse import LlamaParse
from typing import List, Dict
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField

dotenv.load_dotenv()

# === AWS S3 Setup ===
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN")  # Optional, if using temporary credentials
)

bucket_name = os.getenv("S3_BUCKET_NAME")
prefix = os.getenv("S3_PREFIX")

mongo = MongoClient(os.getenv("MONGODB_URI"))
collection = mongo[os.getenv("MONGODB_DB")][os.getenv("MONGODB_COLLECTION")]

def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config


# === List .pdf files from S3 ===
def list_pdf_files(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in response:
        return []
    return [item["Key"] for item in response["Contents"] if item["Key"].endswith(".pdf")]


def produce(topic, config, payload):
  # creates a new producer instance
  producer = Producer(config)

  # produces a sample message
  producer.produce(topic, key = payload["key"], value = payload["content"]["content"])
  # send any outstanding or buffered messages to the Kafka broker
  producer.flush()

    
def consume(topic, config):
  # sets the consumer group ID and offset
  print(f"Consuming from topic: {topic}")
  config["group.id"] = "python-group-1"
  config["auto.offset.reset"] = "earliest"

  # creates a new consumer instance
  consumer = Consumer(config)
  # subscribes to the specified topic
  consumer.subscribe([topic])

  try:
    while True:
      # consumer polls the topic and prints any incoming messages
      msg = consumer.poll(1.0)
      if msg is not None and msg.error() is None:
          try:
            sr_conf = {'url': os.getenv("SCHEMA_REGISTRY_URL"), 
                       'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"}
            schema_registry_client = SchemaRegistryClient(sr_conf)

            avro_deserializer = AvroDeserializer(schema_registry_client)
            # Convert to JSON
            data = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            json_data = json.loads(json.dumps(data))
            result = collection.insert_one(json_data)
            print("Inserted data:", result.inserted_id)

          except Exception as e:
              print(f"Error processing message: {e}")


  except KeyboardInterrupt:
    pass
  finally:
    # closes the consumer connection
    consumer.close()

def send_files_to_kafka(pdf_keys):
    config = read_config()
    topic = os.getenv("KAFKA_PRODUCE_TOPIC")

    for key in pdf_keys:
        print(f"Processing {key}")
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        file_bytes = BytesIO(obj["Body"].read())
        
        # Parse and chunk the PDF
        chunks = parse_and_chunk_pdf(file_bytes, file_name=key)
        
        if not chunks:
            print(f"⚠️ Skipping empty or failed PDF: {key}")
            continue

        # Send each chunk to Kafka
        for chunk in chunks:
            payload = {
                "key": f"{key}_chunk_{chunk['chunk_id']}",
                "content": chunk,
                # "metadata": chunk["metadata"]
            }
            print(f"✅ Sending chunk ")
            produce(topic, config, payload)


def parse_and_chunk_pdf(file_bytes: BytesIO, file_name: str) -> List[Dict[str, str]]:
    """
    Parse a PDF using LlamaParse and split it into chunks with metadata.
    
    Args:
        file_bytes: BytesIO object containing the PDF
        file_name: Name of the PDF file
        chunk_size: Size of each chunk in characters
        chunk_overlap: Number of characters to overlap between chunks
    
    Returns:
        List of dictionaries containing chunked text and metadata
    """
    try:
        # Initialize LlamaParse with your API key
        parser = LlamaParse(
            api_key=os.getenv("LLAMA_PARSE_API_KEY"),
            result_type="json"  # Get JSON output
        )

        # Parse the PDF with file name in extra_info
        parsed_doc = parser.parse(file_bytes, extra_info={"file_name": file_name})
        
        # Create a list of dictionaries with chunks and metadata
        chunked_content = []

        for i, page in enumerate(parsed_doc.pages):
            chunked_content.append({"chunk_id": i, "content": page.text})

        return chunked_content

    except Exception as e:
        print(f"Error parsing PDF: {str(e)}")
        return []



def main():
    # Start consumer in a separate thread
    consumer_thread = threading.Thread(
        target=consume,
        args=(os.getenv("KAFKA_CONSUME_TOPIC"), read_config()),
        daemon=True  # Thread will exit when main program exits
    )
    consumer_thread.start()

    print("🔍 Listing PDF files from S3...")
    pdf_files = list_pdf_files(bucket_name, prefix)
    if not pdf_files:
        print("No .pdf files found.")
    else:
        print(pdf_files)
        print(f"📦 Found {len(pdf_files)} PDF files. Sending to Kafka...")
        send_files_to_kafka(pdf_files)

    # Keep the main thread running to allow the consumer to continue
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("\nShutting down...")

if __name__ == "__main__":
    main()