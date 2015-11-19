import logging
import json
import tasks
import argparse
import sys
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
        listener = Listener(
            config.get('api_root', 'http://localhost'),
            config.get('s3_bucket', 'lcogtarchive'),
            config.get('queue_name', os.getenv('INGEST_QUEUE', 'ingest_queue')),
            config.get('broker', os.getenv('QUEUE_BROKER', 'memory://localhost'))
        )
    except KeyError as err:
        logger.fatal('Config file missing value {}'.format(err))
        sys.exit(1)

    with Connection(listener.broker) as connection:
        listener.connection = connection
        listener.queue = Queue(listener.queue_name)
        listener.run()
