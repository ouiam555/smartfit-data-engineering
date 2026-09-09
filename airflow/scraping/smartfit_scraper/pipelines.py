import json
from confluent_kafka import Producer


class KafkaPipeline:

    def open_spider(self, spider):
        self.producer = Producer({
            "bootstrap.servers": "localhost:9092"
        })

        self.topic = "smartfit_raw"

    def process_item(self, item, spider):

        message = json.dumps(
            dict(item),
            ensure_ascii=False
        )

        self.producer.produce(
            topic=self.topic,
            value=message.encode("utf-8")
        )

        self.producer.poll(0)

        return item

    def close_spider(self, spider):
        self.producer.flush()