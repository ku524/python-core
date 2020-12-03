from datetime import datetime
import socket
import logging
from confluent_kafka import Producer
from spaceone.core import pygrpc
from spaceone.core import utils
from spaceone.core.error import *
from spaceone.core.transaction import Transaction
from spaceone.api.core.v1 import handler_pb2

_STATE = ['STARTED', 'IN-PROGRESS', 'SUCCESS', 'FAILURE']
_LOGGER = logging.getLogger(__name__)

class EventGRPCHandler(object):

    def __init__(self, config):
        self._validate(config)
        self.uri_info = utils.parse_grpc_uri(config['uri'])

    def _validate(self, config):
        pass

    def notify(self, transaction: Transaction, state: str, message: dict):
        if state in _STATE:
            grpc_method = pygrpc.get_grpc_method(self.uri_info)
            grpc_method({
                'service': transaction.service,
                'resource': transaction.resource,
                'verb': transaction.verb,
                'state': state,
                'message': message
            })

class EventKafkaStreamHandler(object):
    def __init__(self, config):
        self._validate(config)
        self.producer = Producer({'bootstrap.servers': config['endpoint']})
        self.topic = config['topic']
        self.debug = config.get('debug', False)
        self.timeout = config.get('timeout', 3)

    def __del__(self):
        self.producer.flush(timeout=self.timeout)

    def _validate(self, config):
        if config.get('endpoint') is None:
            raise ERROR_HANDLER_CONFIGURATION(handler_name=self.__class__.__name__, reason='endpoint is not set.')
        if config.get('topic') is None:
            raise ERROR_HANDLER_CONFIGURATION(handler_name=self.__class__.__name__, reason='topic is not set.')

    def notify(self, transaction: Transaction, state: str, message: dict):
        if state in _STATE:
            kafka_message = str({
                'timestamp': datetime.utcnow().isoformat(),
                'transaction_id': transaction.get_meta('transaction_id'),
                'hostname': socket.gethostname(),
                'service': transaction.service,
                'resource': transaction.resource,
                'verb': transaction.verb,
                'state': state,
                'message': {} if state == 'SUCCESS' else message
            })
            if self.debug:
                self.producer.produce(self.topic, kafka_message, callback=self._response_callback)
            else:
                self.producer.produce(self.topic, kafka_message)

    @staticmethod
    def _response_callback(err, msg):
        if err:
            _LOGGER.debug(f'Message delivery failed. (reason = {err})')
        else:
            _LOGGER.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
