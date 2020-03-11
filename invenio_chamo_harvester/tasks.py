# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Invenio.
# Copyright (C) 2019 UCLouvain.
#
# Invenio-Chamo-Harvester is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""Celery tasks to harvest records from Chamo Rest API."""

from __future__ import absolute_import, print_function

import time
from datetime import datetime
import sys

import click
import requests
from celery import shared_task
from flask import current_app
from invenio_db import db
from invenio_indexer.api import RecordIndexer
from invenio_jsonschemas import current_jsonschemas
from rero_ils.modules.documents.api import Document, DocumentsSearch
from rero_ils.modules.documents.models import DocumentIdentifier
from rero_ils.modules.holdings.api import Holding
from rero_ils.modules.items.api import Item
from .utils import get_max_record_pid

from .api import ChamoRecordHarvester
from .utils import extract_records_id, map_item_type, map_locations


@shared_task(ignore_result=True)
def process_bulk_queue(version_type=None):
    """Process bulk harvesting queue.

    :param str version_type: Elasticsearch version type.
    Note: You can start multiple versions of this task.
    """
    ChamoRecordHarvester().process_bulk_queue()


@shared_task(ignore_result=True)
def queue_records_to_harvest(size=1000, next_id=None, modified_since=None,
                             verbose=False):
    """Queue records to harvest from Chamo Rest API."""
    uri = '{base_url}/{route}?all=true&batchSize={size}'.format(
        base_url=current_app.config['CHAMO_HARVESTER_CHAMO_BASE_URL'],
        route='bibs',
        size=size)
    if next_id:
        uri += '&next={next_id}'.format(next_id=next_id)
    if verbose:
        click.echo('Get records from {uri}'.format(uri=uri))

    try:
        count = 0
        request = requests.get(uri, auth=(
            current_app.config['CHAMO_HARVESTER_CHAMO_USER'],
            current_app.config['CHAMO_HARVESTER_CHAMO_PASSWORD']))
        data = request.json()

        next = data.get('next', {})
        while next:
            records = extract_records_id(data)
            if verbose:
                click.echo('List records :  {records}'.format(records=records))
            ChamoRecordHarvester().bulk_to_harvest(records)
            count += len(records)

            request = requests.get(next, auth=(
                current_app.config['CHAMO_HARVESTER_CHAMO_USER'],
                current_app.config['CHAMO_HARVESTER_CHAMO_PASSWORD']))
            data = request.json()
            next = data.get('next', None)
        return count
    except Exception as e:
        click.secho(
            'Harvesting API Error: {e}'.format(e=e),
            fg='red'
        )
        return 0, uri, []


@shared_task(ignore_result=True)
def harvest_record(record_uuid):
    """Index a single record.

    :param record_uuid: The record UUID.
    """
    ChamoRecordHarvester().harvest_by_id(record_uuid)


@shared_task(ignore_result=True)
def delete_record(record_uuid):
    """Delete a single record.

    :param record_uuid: The record UUID.
    """
    ChamoRecordHarvester().delete_by_id(record_uuid)


@shared_task(ignore_result=True)
def bulk_records(records):
    """Records creation."""
    bulk_size = current_app.config['CHAMO_HARVESTER_BULK_SIZE']
    current_app.logger.info('harverster bulk size : {size}'.format(
        size=bulk_size))
    n_updated = 0
    n_rejected = 0
    n_created = 0
    record_schema = current_jsonschemas.path_to_url('documents/document-v0.0.1.json')
    item_schema = current_jsonschemas.path_to_url('items/item-v0.0.1.json')
    holding_schema = current_jsonschemas.path_to_url('holdings/holding-v0.0.1.json')
    host_url = current_app.config.get('RERO_ILS_APP_BASE_URL')
    url_api = '{host}/api/{doc_type}/{pid}'
    record_id_iterator = []
    item_id_iterator = []
    holding_id_iterator = []
    indexer = RecordIndexer()
    start_time = datetime.now()
    celery_kwargs = {
        'kwargs': {
        }
    }
    for record in records:
        try:
            if record.get('frbr', False):
                document = record.get('document', {})
                """
                # check if already in Rero-ILS
                pid = None

                for identifier in document.get('identifiedBy') :
                    if identifier.get('source') == 'VIRTUA' :
                        bibid = identifier.get('value')
                        query = DocumentsSearch().filter(
                            'term',
                            identifiedBy__value=bibid
                        ).source(includes=['pid'])
                        try:
                            pid = [r.pid for r in query.scan()].pop()
                        except IndexError:
                            pid = None
                if pid:
                    # update the record
                    # Do nothing for the moment
                    continue
                else:
                    """
                document['$schema'] = record_schema

                created_time = datetime.now()
                current_app.logger.info('create document')
                document = Document.create(
                    document,
                    dbcommit=False,
                    reindex=False
                )
                record_id_iterator.append(document.id)
                db.session.add(DocumentIdentifier(recid=document.get('pid')))
                uri_documents = url_api.format(host=host_url,
                                               doc_type='documents',
                                               pid=document.get('pid'))
                
                map_holdings = {}
                for holding in record.get('holdings'):
                    holding['$schema'] = holding_schema
                    holding['document'] = {
                        '$ref': uri_documents
                        }
                    holding['circulation_category'] = {
                        '$ref': map_item_type(str(holding.get('circulation_category')))
                        }
                    holding['location'] = {
                        '$ref': map_locations(str(holding.get('location')))
                        }
                    
                    created_time = datetime.now()

                    result = Holding.create(
                        holding,
                        dbcommit=False,
                        reindex=False
                    )
                    
                    map_holdings.update({
                            '{location}#{cica}'.format(
                                location = holding.get('location'),
                                cica = holding.get('circulation_category')) : result.get('pid')
                        }
                    )
                    holding_id_iterator.append(result.id)
                
                for item in record.get('items'):
                    item['$schema'] = item_schema
                    item['document'] = {
                        '$ref': uri_documents
                        }
                    item['item_type'] = {
                        '$ref': map_item_type(str(item.get('item_type')))
                        }
                    item['location'] = {
                        '$ref': map_locations(str(item.get('location')))
                        }

                    holding_pid = map_holdings.get(
                        '{location}#{cica}'.format(
                            location = item.get('location'),
                            cica = item.get('item_type')))
                    if holding_pid is None:
                        click.secho('holding pid is None for record : {id} '.format(
                            id=document.pid
                        ), fg='red')
                        click.secho('holding map : {map}.'.format(
                            map=map_holdings), fg='white')
                        click.secho('item to map : {location}#{cica}'.format(
                            location = item.get('location'),
                            cica = item.get('item_type')), fg='yellow')
                    
                    item['holding'] = {
                        '$ref': url_api.format(host=host_url,
                                    doc_type='holdings',
                                    pid=holding_pid)
                        }

                    result = Item.create(
                        item,
                        dbcommit=False,
                        reindex=False
                    )
                    # item_pids.append(result.pid)
                    item_id_iterator.append(result.id)
                n_created += 1

            if n_created % bulk_size == 0:
                execution_time = datetime.now() - start_time
                click.secho('{nb} created records in {execution_time}.'
                            .format(nb=len(record_id_iterator),
                                    execution_time=execution_time),
                            fg='white')
                start_time = datetime.now()

                db.session.commit()

                execution_time = datetime.now() - start_time
                
                current_app.logger.info('{nb} commited records in {execution_time}.'
                            .format(nb=len(record_id_iterator),
                                    execution_time=execution_time))
                
                click.secho('{nb} commited records in {execution_time}.'
                            .format(nb=len(record_id_iterator),
                                    execution_time=execution_time),
                            fg='white')
                start_time = datetime.now()
                click.secho('sending {n} holdings to indexer queue.'
                            .format(n=len(holding_id_iterator)), fg='white')
                indexer.bulk_index(holding_id_iterator)
                click.secho('process queue...', fg='yellow')
                
                indexer.process_bulk_queue()

                click.secho('sending {n} items to indexer queue.'
                            .format(n=len(item_id_iterator)), fg='white')
                indexer.bulk_index(item_id_iterator)
                click.secho('process queue...', fg='yellow')
                
                indexer.process_bulk_queue()

                click.secho('sending {n} documents to indexer queue.'
                            .format(n=len(record_id_iterator)), fg='white')
                indexer.bulk_index(record_id_iterator)
                click.secho('process queue...', fg='yellow')
                
                indexer.process_bulk_queue()

                execution_time = datetime.now() - start_time
                click.secho('indexing records process in {execution_time}.'
                            .format(execution_time=execution_time),
                            fg='white')
                click.secho('processing next batch records.', fg='green')

                record_id_iterator.clear()
                holding_id_iterator.clear()
                item_id_iterator.clear()
                start_time = datetime.now()
        
        except Exception as e:
            n_rejected += 1
            current_app.logger.error('Error processing record [{id}] : {e}'
                        .format(
                            id=str(record.get('_id')).strip(), 
                            e=str(e)))
    try:
        start_time = datetime.now()
        db.session.commit()
        execution_time = datetime.now() - start_time
        current_app.logger.info('{nb} commited records in {execution_time}.'
                    .format(nb=len(record_id_iterator),
                            execution_time=execution_time))
        
        indexer.bulk_index(holding_id_iterator)
        indexer.process_bulk_queue()
        indexer.bulk_index(item_id_iterator)
        indexer.process_bulk_queue()
        indexer.bulk_index(record_id_iterator)
        indexer.process_bulk_queue()
    except Exception as e:
        current_app.logger.error(e)

    max_recid = get_max_record_pid('doc')
    DocumentIdentifier._set_sequence(max_recid)
    db.session.commit()

    return n_created
