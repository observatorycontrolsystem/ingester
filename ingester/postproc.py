from kombu.connection import Connection
from kombu import Exchange, Queue

processed_exchange = Exchange('archived_fits', type='fanout')
producer_queue = Queue('', processed_exchange, exclusive=True)


class PostProcService(object):
    def __init__(self, broker_url):
        self.broker_url = broker_url

    def post_to_archived_queue(self, fits_dict):
        with Connection(self.broker_url) as conn:
            queue = conn.SimpleQueue(producer_queue)
            queue.put(fits_dict)
            queue.close()
