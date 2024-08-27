import asyncio
import json

from confluent_kafka.schema_registry import SchemaRegistryClient
from pyiceberg.catalog import load_catalog
from schema_registry.serializers import AsyncAvroMessageSerializer
from schema_registry.client import AsyncSchemaRegistryClient
from kafka import KafkaConsumer
from dotenv import find_dotenv, load_dotenv
import os
import pyarrow as pa

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

topic = os.getenv("KAFKA_TOPIC_NAME")
bootstrap_servers = os.getenv("KAFKA_BROKER")
schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL")
schema_registry_subject = f"{topic}-value"

async def get_schema_from_schema_registry(registry_url, registry_subject):
    sr = SchemaRegistryClient({'url': registry_url})
    schema_str = sr.get_latest_version(registry_subject).schema.schema_str
    schema_json = json.loads(schema_str)
    return schema_json

# Load Iceberg Catalog
catalog_name = 'smartux'
hive_catalog = load_catalog(catalog_name)

# # Get or create the Iceberg table
# table_name = "test_create"
# table = hive_catalog.create_table(
#     'smartux.test_create',
#     schema=get_schema_from_schema_registry()
# )


async def consume():
    schema_registry_client = AsyncSchemaRegistryClient(url=schema_registry_url)
    avro_message_serializer = AsyncAvroMessageSerializer(schema_registry_client)

    consumer = KafkaConsumer(topic,
                             bootstrap_servers=bootstrap_servers,
                             auto_offset_reset='earliest',
                             enable_auto_commit=False
                             )
    table = hive_catalog.load_table('dataux.test_create7')
    new_schema = pa.schema([
        ("events", pa.list_(
            pa.struct([
                ("count", pa.int64()),
                ("dow", pa.int64()),
                ("hour", pa.int64()),
                ("key", pa.string()),
                ("segmentation", pa.struct([
                    ("domain", pa.string()),
                    ("height", pa.int64()),
                    pa.field("parent", pa.string(), nullable=True),
                    ("type", pa.string()),
                    ("view", pa.string()),
                    ("width", pa.int64()),
                    pa.field("x", pa.float64(), nullable=True),
                    ("y", pa.float64())
                ])),
                ("timestamp", pa.int64())
            ])
        )),
        ("app_key", pa.string()),
        ("device_id", pa.string()),
        ("sdk_name", pa.string()),
        ("sdk_version", pa.string()),
        ("t", pa.int64()),
        ("timestamp", pa.int64()),
        ("hour", pa.int64()),
        ("dow", pa.int64()),
        ("screen_size_type", pa.string()),
        ("_id", pa.string())
    ])
    for message in consumer:
        try:
            decoded_message = await avro_message_serializer.decode_message(message=message.value)
            table.append(pa.Table.from_pylist([decoded_message['data']], new_schema))
        except Exception as e:
            print(f"Failed to decode message: {e}")

if __name__ == "__main__":
    # asyncio.run(get_schema_from_schema_registry(schema_registry_url, schema_registry_subject))
    asyncio.run(consume())
