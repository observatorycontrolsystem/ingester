import logging
import json
import tasks
import os
from logging.config import dictConfig
from kombu.mixins import ConsumerMixin
from kombu import Connection, Queue

try:
    config = json.loads(open('log_conf.json').read())
    dictConfig(config)
except:
    logging.basicConfig()
logger = logging.getLogger('ingester')


class Listener(ConsumerMixin):
    def __init__(self, api_root, s3_bucket, queue_name, broker):
        self.api_root = api_root
        self.s3_bucket = s3_bucket
        self.queue_name = queue_name
        self.broker = broker

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.queue],
                         callbacks=[self.on_message])]

    def on_message(self, body, message):
        logger.info('sending task {}'.format(body))
        tasks.do_ingest.delay(body, self.api_root, self.s3_bucket)
        message.ack()  # acknowledge to the sender we got this message (it can be popped)


if __name__ == '__main__':
    listener = Listener(
        os.getenv('API_ROOT', 'http://localhost'),
        os.getenv('S3_BUCKET', 'lcogtarchive'),
        os.getenv('INGEST_QUEUE', 'ingest_queue'),
        os.getenv('QUEUE_BROKER', 'memory://localhost')
    )

    with Connection(listener.broker) as connection:
        listener.connection = connection
        listener.queue = Queue(listener.queue_name)
        listener.run()
