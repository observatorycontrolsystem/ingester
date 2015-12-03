import logging
import json
import tasks
import sys
import settings
from logging.config import dictConfig
from kombu.mixins import ConsumerMixin
from kombu import Connection, Queue

try:
    config = json.loads(open('log_conf.json').read())
    dictConfig(config)
except:
    logging.basicConfig()
    logging.warn('Falling back to basic logger')
logger = logging.getLogger('ingester')


class Listener(ConsumerMixin):
    def __init__(self, queue_name, broker_url):
        self.queue_name = queue_name
        self.broker_url = broker_url

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.queue],
                         callbacks=[self.on_message])]

    def on_message(self, body, message):
        logger.info('sending task {}'.format(body))
        tasks.do_ingest.delay(body)
        message.ack()  # acknowledge to the sender we got this message (it can be popped)


if __name__ == '__main__':
    logger.info('starting listener')
    listener = Listener(
        settings.QUEUE_NAME,
        settings.BROKER_URL
    )

    with Connection(listener.broker_url) as connection:
        listener.connection = connection
        listener.queue = Queue(listener.queue_name)
        try:
            listener.run()
        except KeyboardInterrupt:
            logger.info('Shutting down...')
            sys.exit(0)
