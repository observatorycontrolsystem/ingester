#!/bin/env python
import tasks
import sys
import settings
import logging
from kombu.mixins import ConsumerMixin
from kombu import Connection, Queue

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
        tasks.do_ingest.delay(
            body,
            settings.BUCKET
        )
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
