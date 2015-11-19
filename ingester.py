import logging
import json
import tasks
import argparse
import sys
from logging.config import dictConfig
from kombu.mixins import ConsumerMixin
from kombu import Connection, Queue

try:
    config = json.loads(open('log_conf.json').read())
    dictConfig(config)
except:
    logging.basicConfig()
logger = logging.getLogger('ingester')


class Ingester(ConsumerMixin):
    def __init__(self, api_root, s3_bucket, queue_name, broker):
        self.api_root = api_root
        self.s3_bucket = s3_bucket
        self.queue_name = queue_name
        self.broker = broker

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
        tasks.ingest_pack.delay(body)
        message.ack()  # acknowledge to the sender we got this message (it can be popped)

    def ingest(self, pack):
        """
        This method does the actual ingesting of a file, and is called by
        celery from the task queue. It can be run by any worker.
        When this method is called, self is NOT the instance of Ingester
        being run by main, it is a new instance instantiated by celery
        in tasks.py
        """
        print('ingested', pack)

if __name__ == '__main__':
    """
    If this module is run as main we will set up a single Ingester
    in consumer mode that listens for incoming messages on the ingest_queue.
    It then creates ingest tasks which will crate other instances of Ingester
    (that don't listen on the ingest queue)
    """
    parser = argparse.ArgumentParser(
        description='Ingest files from a queue and upload to the archive'
    )

    parser.add_argument(
        '--config',
        default='config.json',
        help='Configuration file to use'
    )

    args = parser.parse_args()

    try:
        config = json.loads(open(args.config).read())
    except FileNotFoundError as err:
        logger.fatal(err)
        sys.exit(1)
    except:
        logger.fatal('Error parsing configuration')
        sys.exit(1)
    try:
        ingester = Ingester(
            config['api_root'],
            config['s3_bucket'],
            config['queue_name'],
            config['broker']
        )
    except KeyError as err:
        logger.fatal('Config file missing value {}'.format(err))
        sys.exit(1)

    with Connection(ingester.broker) as connection:
        ingester.connection = connection
        ingester.queue = Queue(ingester.queue_name)
        ingester.run()
