# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 UCLouvain.
#
# Invenio-Chamo-Harvester is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""API for harvesting records."""

from __future__ import absolute_import, print_function

import base64
from contextlib import contextmanager
from copy import deepcopy

import click
import pytz
import requests
from celery import current_app as current_celery_app
from dojson.contrib.marc21.utils import create_record
from flask import current_app
from kombu import Producer as KombuProducer
from kombu.compat import Consumer
from lxml import etree

from .dojson.contrib.marc21 import marc21

XMLParser = etree.XMLParser(remove_blank_text=True, recover=True,
                            resolve_entities=False)


class ChamoHarvesterProducer(KombuProducer):
    """Producer validating published messages.

    For more information visit :class:`kombu:kombu.Producer`.
    """

    def publish(self, data, **kwargs):
        """Validate operation type."""
        assert data.get('op') in {'harvest', 'create', 'delete', 'update'}
        return super(ChamoHarvesterProducer, self).publish(data, **kwargs)


class ChamoRecordHarvester(object):
    """Provide an interface for harvesting Virtua records in Rero-ils."""

    def __init__(self, exchange=None, queue=None,
                 routing_key=None):
        """Initialize indexer.

        :param exchange: A :class:`kombu.Exchange` instance for message queue.
        :param queue: A :class:`kombu.Queue` instance for message queue.
        :param routing_key: Routing key for message queue.
        """
        self._exchange = exchange
        self._queue = queue
        self._routing_key = routing_key

    @property
    def mq_queue(self):
        """Message Queue queue.

        :returns: The Message Queue queue.
        """
        return self._queue or current_app.config['CHAMO_HARVESTER_MQ_QUEUE']

    @property
    def mq_exchange(self):
        """Message Queue exchange.

        :returns: The Message Queue exchange.
        """
        return self._exchange or current_app.config[
            'CHAMO_HARVESTER_MQ_EXCHANGE']

    @property
    def mq_routing_key(self):
        """Message Queue routing key.

        :returns: The Message Queue routing key.
        """
        return (self._routing_key or
                current_app.config['CHAMO_HARVESTER_MQ_ROUTING_KEY'])

    def harvest(self, record_id, arguments=None, **kwargs):
        """Harvest a record.

        :param record_id: id of the record.
        """
        return None

    def bulk_to_harvest(self, record_id_iterator):
        """Bulk harvest records.

        :param record_id_iterator: Iterator yielding record ID.
        """
        self._bulk_op(record_id_iterator,
                      'harvest',
                      current_app.config['CHAMO_HARVESTER_CHAMO_BASE_URL'])

    def process_bulk_queue(self):
        """Process bulk harvesting queue."""
        from .tasks import bulk_records
        count = 0
        with current_celery_app.pool.acquire(block=True) as conn:
            try:
                consumer = Consumer(
                    connection=conn,
                    queue=self.mq_queue.name,
                    exchange=self.mq_exchange.name,
                    routing_key=self.mq_routing_key,
                )

                count = bulk_records(
                    self._actionsiter(consumer.iterqueue())
                )
                consumer.close()
            except Exception as e:
                click.secho(
                    'Harvester Bulk queue Error: {e}'.format(e=e),
                    fg='red'
                )
        return count

    @contextmanager
    def create_producer(self):
        """Context manager that yields an instance of ``Producer``."""
        with current_celery_app.pool.acquire(block=True) as conn:
            yield ChamoHarvesterProducer(
                conn,
                exchange=self.mq_exchange,
                routing_key=self.mq_routing_key,
                auto_declare=True,
            )

    def _bulk_op(self, record_id_iterator, op_type, url):
        """Harvest record in Rero-ILS asynchronously.

        :param record_id_iterator: Iterator that yields record UUIDs.
        :param op_type: Indexing operation (one of ``harvest``,
            ``delete`` or ``update``).
        """
        with self.create_producer() as producer:
            for rec in record_id_iterator:
                producer.publish(dict(
                    id=str(rec),
                    uri='{base_url}/invenio/bib/{id}'.format(base_url=url,
                                                             id=str(rec)),
                    op=op_type
                ))

    def _actionsiter(self, message_iterator):
        """Iterate bulk actions.

        :param message_iterator: Iterator yielding messages from a queue.
        """
        for message in message_iterator:
            payload = message.decode()
            try:
                yield self._harvest_action(payload)
                message.ack()
            except Exception:
                message.reject()
                current_app.logger.error(
                    "Failed to harvest record {0}".format(payload.get('id')),
                    exc_info=True)

    def _harvest_action(self, payload):
        """Bulk index action.

        :param payload: Decoded message body.
        :returns: Dictionary defining an Elasticsearch bulk 'index' action.
        """
        
        record = ChamoBibRecord.get_record_by_uri(payload['uri'])     
        
        data = self._prepare_record(record)     
        
        action = {
            '_op_type': 'harvest',
            '_id': str(payload['id']),
            'frbr': record.isFrbr,
            'document': data.get('document'),
            'items': data.get('items'),
            'holdings': data.get('holdings')
        }
        return action

    @staticmethod
    def _prepare_record(record):
        """Prepare record data for indexing.

        :param record: The record to prepare.
        :returns: The record metadata.
        """
        rec = create_record(record.xml)
        rec = marc21.do(rec)

        data = {
            'document': rec,
            'items': record.items,
            'holdings': record.holdings
        }
        return data


class BulkChamoRecordHarvester(ChamoRecordHarvester):
    """Provide an interface to retrieve id from chamo rest API."""

    def harvest(self, records):
        """Harvest a record.

        :param record: Record instance.
        """
        self.bulk_to_harvest(records)

    def harvest_by_id(self, record_id):
        """Harvest a record.

        :param record: Record instance.
        """
        self.bulk_to_harvest([record_id])


class ChamoBibRecord(object):
    """Chamo bibliographic record from an API Rest."""

    def __init__(self, data):
        """Initialize instance."""
        self.data = data

    @property
    def isFrbr(self):
        """The linked items of bibliographic record."""
        return True if self.data.get('frbrType') is None else False

    @property
    def raw(self):
        """Return pure Python dictionary with record metadata."""
        return deepcopy(dict(self.data))

    @property
    def xml(self):
        """The bibliographic record as parsed XML."""
        xml = base64.b64decode(self.data.get('marcXmlData', {}).get('raw', {}))
        return etree.XML(xml, parser=XMLParser)

    @property
    def document(self):
        """Do json converted bibliographic record."""
        rec = create_record(self.xml)
        return marc21.do(rec)

    @property
    def items(self):
        """The linked items of bibliographic record."""
        return self.data.get('items') or []

    @property
    def holdings(self):
        """The linked holdings of bibliographic record."""
        return self.data.get('holdings') or []

    @classmethod
    def get_record_by_uri(self, uri):
        """Get chamo record by uri value."""
        try:
            request = requests.get(uri, auth=(
                current_app.config['CHAMO_HARVESTER_CHAMO_USER'],
                current_app.config['CHAMO_HARVESTER_CHAMO_PASSWORD']))
            return self(request.json())
        except Exception as e:
            click.secho(
                'Get ressource Error: {e}'.format(e=e),
                fg='red'
            )
            return None
    
    @classmethod
    def get_record_by_id(self, id):
        """Get chamo record by id value."""
        try:
            uri = uri='{base_url}/invenio/bib/{id}'.format(
                base_url=current_app.config['CHAMO_HARVESTER_CHAMO_BASE_URL'],
                id=str(id))
            
            request = requests.get(uri, auth=(
                current_app.config['CHAMO_HARVESTER_CHAMO_USER'],
                current_app.config['CHAMO_HARVESTER_CHAMO_PASSWORD']))
            return self(request.json())
        except Exception as e:
            click.secho(
                'Get ressource Error: {e}'.format(e=e),
                fg='red'
            )
            return None

    def dumps(self, **kwargs):
        """Return pure Python dictionary with record metadata."""
        return {
            'document': self.document,
            'holdings': self.holdings,
            'items': self.items
        }
