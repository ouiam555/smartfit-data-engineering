import io
from zoneinfo import ZoneInfo
import json
import time

from datetime import datetime, timezone
from confluent_kafka import Consumer
from minio import Minio


BATCH_SIZE = 50
BUCKET_NAME = "bronze"
TOPIC_NAME = "smartfit_raw"

IDLE_TIMEOUT_SECONDS = 30


consumer = Consumer({
    "bootstrap.servers": os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
),
    "group.id": "smartfit-bronze-v1",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})


consumer.subscribe([
    TOPIC_NAME
])


mminio_client = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
    secure=False,
)


batch = []

last_message_time = time.time()


def save_batch(records):

    if not records:
        return

    now = datetime.now(
    ZoneInfo("Africa/Casablanca")
)

    object_name = (
        f"smartfit/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"batch_{now.strftime('%H%M%S_%f')}.jsonl"
    )

    jsonl = "\n".join(
        json.dumps(record, ensure_ascii=False)
        for record in records
    )

    data = jsonl.encode("utf-8")

    minio_client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        data=io.BytesIO(data),
        length=len(data),
        content_type="application/x-ndjson",
    )

    print(
        f"Saved {len(records)} records → "
        f"{BUCKET_NAME}/{object_name}"
    )


try:

    while True:

        message = consumer.poll(1.0)

        if message is None:

            idle_time = (
                time.time()
                - last_message_time
            )

            if idle_time >= IDLE_TIMEOUT_SECONDS:

                print(
                    f"No messages for "
                    f"{IDLE_TIMEOUT_SECONDS} seconds."
                )

                print(
                    "Consumer finished successfully."
                )

                break

            continue


        if message.error():

            print(
                f"Kafka error: "
                f"{message.error()}"
            )

            continue


        last_message_time = time.time()


        raw_message = (
            message
            .value()
            .decode("utf-8")
        )


        record = json.loads(
            raw_message
        )


        batch.append(
            record
        )


        if len(batch) >= BATCH_SIZE:

            save_batch(
                batch
            )

            consumer.commit(
                asynchronous=False
            )

            batch.clear()


except KeyboardInterrupt:

    print(
        "\nStopping consumer..."
    )


finally:

    if batch:

        save_batch(
            batch
        )

        consumer.commit(
            asynchronous=False
        )

    consumer.close()

    print(
        "Kafka consumer closed."
    )