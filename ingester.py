import os
from logging.config import dictConfig
import logging
import json
from kombu.mixins import ConsumerMixin
from kombu import Connection, Queue
import tasks

try:
    config = json.loads(open('log_conf.json').read())
    dictConfig(config)
except:
    logging.basicConfig()
logger = logging.getLogger('ingester')


class Ingester(ConsumerMixin):
    def __init__(self):
        self.api_root = os.getenv('API_ROOT', 'http://localhost/')
        self.s3_bucket = os.getenv('S3_BUCKET', 'lcogt-archive')
        self.queue_name = os.getenv('QUEUE_NAME', 'ingest_queue')
        self.connect_str = os.getenv('CONNECT_STR', 'memory://localhost')

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.queue],
                         callbacks=[self.on_message])]

    def on_message(self, body, message):
        """
        This method is called when a message arrives on the message queue
        specifed by self.queue_name  and this instance of ingester is being run
        by the .run() method (see main below).
        On a message, we will add a call to ingest_file to the task queue (which is
        differnet from self.queue). Celery will then handle the execution of that
        task, most likely with a number of different workers asynchronusly.
        """
        logger.info('sending task', body)
        tasks.ingest_file.delay(body['path'])
        message.ack()  # acknowledge to the sender we got this message (it can be popped)

    def ingest(self, path):
        """
        This method does the actual ingesting of a file, and is called by
        celery from the task queue. It can be run by any worker.
        When this method is called, self is NOT the instance of Ingester
        being run by main, it is a new instance instantiated by celery
        in tasks.py
        """
        print('ingested', path)

if __name__ == '__main__':
    """
    If this module is run as main we will set up a single Ingester
    in consumer mode that listens for incoming messages on the ingest_queue.
    It then creates ingest tasks which will crate other instances of Ingester
    (that don't listen on the ingest queue)
    """
    ingester = Ingester()
    with Connection(ingester.connect_str) as connection:
        ingester.connection = connection
        ingester.queue = Queue(ingester.queue_name)
        ingester.run()
