#!/bin/env python
import tasks
import sys
import settings
import logging
from kombu.mixins import ConsumerMixin
from kombu import Connection, Queue, Exchange

logger = logging.getLogger('ingester')

crawl_exchange = Exchange('fits_files', type='fanout')


def filter_path(path):
    if path and 'fits' in path and all([chars not in path for chars in settings.DISALLOWED_CHARS]):
        return True
    return False


class Listener(ConsumerMixin):
    def __init__(self, broker_url):
        self.broker_url = broker_url

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.queue],
                         callbacks=[self.on_message])]

    def on_message(self, body, message):
        path = body.get('path')
        if filter_path(path):
            logger.info('sending task {}'.format(path))
            tasks.do_ingest.delay(
                path=path,
                bucket=settings.BUCKET,
                api_root=settings.API_ROOT,
                auth_token=settings.AUTH_TOKEN,
                broker_url=settings.BROKER_URL,
                required_headers=settings.REQUIRED_HEADERS,
                blacklist_headers=settings.HEADER_BLACKLIST
            )
        else:
            logger.info('ignoring {}'.format(path))
        message.ack()  # acknowledge to the sender we got this message (it can be popped)


if __name__ == '__main__':
    logger.info('starting listener')
    listener = Listener(
        settings.BROKER_URL
    )

    with Connection(listener.broker_url) as connection:
        listener.connection = connection
        listener.queue = Queue('archive_ingest', crawl_exchange)
        try:
            listener.run()
        except KeyboardInterrupt:
            logger.info('Shutting down...')
            sys.exit(0)
