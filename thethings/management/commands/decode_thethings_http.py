import base64
import json
import logging

import pytz
from dateutil.parser import parse
from django.conf import settings
from influxdb.exceptions import InfluxDBClientError

from broker.management.commands import RabbitCommand

from broker.utils import (
    create_dataline, create_parsed_data_message,
    data_pack, data_unpack,
    decode_json_body, get_datalogger, decode_payload,
    create_routing_key, send_message
)

logger = logging.getLogger('thethings')


def parse_thethings_request(serialised_request, data):
    devid = data['hardware_serial']
    metadata = data['metadata']
    payload_base64 = data['payload_raw']
    logging.debug(json.dumps(data, indent=1))
    datalogger, created = get_datalogger(devid=devid, update_activity=False)
    timestamp = parse(metadata['time'])
    timestamp = timestamp.astimezone(pytz.UTC)
    payload_hex = base64.decodebytes(bytes(payload_base64, 'utf8')).hex()
    rssi = metadata['gateways'][0]['rssi']
    port = data['port']
    logging.info(f'Decode Thethings: time={timestamp}, payload_hex={payload_hex}, rssi={rssi}')
    payload = decode_payload(datalogger, payload_hex, port)
    payload['rssi'] = rssi
    logging.debug(payload)
    # RabbitMQ part
    key = create_routing_key('thethings', devid)
    dataline = create_dataline(timestamp, payload)
    datalines = [dataline]
    message = create_parsed_data_message(devid, datalines=datalines)
    packed_message = data_pack(message)
    logger.debug(f'exchange={settings.PARSED_DATA_EXCHANGE} key={key}  packed_message={packed_message}')
    send_message(settings.PARSED_DATA_EXCHANGE, key, packed_message)
    return True


def consumer_callback(channel, method, properties, body, options=None):
    serialised_request = data_unpack(body)
    ok, data = decode_json_body(serialised_request['request.body'])
    if 'hardware_serial' in data:
        parse_thethings_request(serialised_request, data)
    else:
        logger.warning(f'"hardware_serial"" was not found in data.')
    logger.debug(json.dumps(data, indent=2))
    channel.basic_ack(method.delivery_tag)


class Command(RabbitCommand):
    help = 'Decode thethings'

    def add_arguments(self, parser):
        parser.add_argument('--prefix', type=str,
                            help='queue and routing_key prefix, overrides settings.ROUTING_KEY_PREFIX')
        super().add_arguments(parser)

    def handle(self, *args, **options):
        logger.info(f'Start handling {__name__}')
        name = 'thethings'
        # FIXME: constructing options should be in a function in broker.utils
        if options["prefix"] is None:
            prefix = settings.RABBITMQ["ROUTING_KEY_PREFIX"]
        else:
            prefix = options["prefix"]
        options['exchange'] = settings.RAW_HTTP_EXCHANGE
        options['routing_key'] = f'{prefix}.{name}.#'
        options['queue'] = f'{prefix}_decode_{name}_http_queue'
        options['consumer_callback'] = consumer_callback
        super().handle(*args, **options)
