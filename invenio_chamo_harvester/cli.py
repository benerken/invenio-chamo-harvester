# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 UCLouvain.
#
# Invenio-Chamo-Harvester is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""CLI for Chamo Harvester."""

from __future__ import absolute_import, print_function

import click
import yaml
from celery.messaging import establish_connection
from flask import current_app
from flask.cli import with_appcontext
from invenio_chamo_harvester.api import ChamoRecordHarvester, ChamoBibRecord
from invenio_chamo_harvester.tasks import (process_bulk_queue,
                                           queue_records_to_harvest,
                                           bulk_record)
from invenio_chamo_harvester.utils import get_max_record_pid
from invenio_pidstore.models import PersistentIdentifier, PIDStatus, RecordIdentifier
from rero_ils.modules.documents.api import Document
from rero_ils.modules.documents.models import DocumentIdentifier
from sqlalchemy import func
from invenio_db import db

def abort_if_false(ctx, param, value):
    """Abort command is value is False."""
    if not value:
        ctx.abort()


@click.group()
def chamo():
    """Fixtures management commands."""


@chamo.command("harvest")
@click.option('-s', '--size', type=int, default=1000)
@click.option('-n', '--next-id', type=int, default=0)
@click.option('-m', '--modified-since', default=None)
@click.option('-v', '--verbose', is_flag=True, default=False)
@click.option('--yes-i-know', is_flag=True, callback=abort_if_false,
              expose_value=False,
              prompt='Do you really want to harvest all records?')
@click.option('-f', '--file', type=click.File('r'), default=None)
@with_appcontext
def harvest_chamo(size, next_id, modified_since, verbose, file):
    """Harvest all records."""

    try:
        count = 0
        if file:
            click.secho('Reading records file to harvesting queue ...', fg='green')
            records = []
            for pid in file:
                records.append(pid)
            ChamoRecordHarvester().bulk_to_harvest(records)
            count=len(records)
        else :
            click.secho('Sending records to harvesting queue ...', fg='green')
            count = queue_records_to_harvest(
                next_id=next_id,
                modified_since=modified_since,
                size=size)
        click.secho(
            'Records queued: {count}'.format(count=count),
            fg='blue'
        )
        click.secho('Execute "run" command to process the queue!',
                    fg='red')
    except Exception as e:
        click.secho(
            'Harvesting Error: {e}'.format(e=e),
            fg='red'
        )

@chamo.command("run")
@click.option('--delayed', '-d', is_flag=True,
              help='Run harvesting in background.')
@click.option('--concurrency', '-c', default=1, type=int,
              help='Number of concurrent harvesting tasks to start.')
@with_appcontext
def run(delayed, concurrency):
    """Run bulk record harvesting."""
    if delayed:
        celery_kwargs = {
            'kwargs': {
            }
        }
        click.secho(
            'Starting {0} tasks for harvesting records...'.format(concurrency),
            fg='green')
        for c in range(0, concurrency):
            process_bulk_queue.apply_async(**celery_kwargs)
    else:
        click.secho('Retrieve queued records...', fg='green')
        ChamoRecordHarvester().process_bulk_queue()

@chamo.command("record")
@click.option('--bibid', '-i', default=0, type=int,
              help='BIBID of the record.')
@with_appcontext
def record(bibid):
    """Run transform to invenio record."""
    if bibid>0:
        print(bulk_record(ChamoBibRecord.get_record_by_id(bibid)))


@chamo.command("max_id")
@click.option('--with-deleted', '-d', is_flag=True,
              help='With deleted record.')
@with_appcontext
def max_id(with_deleted):
    """Get max record identifier."""
    print(get_max_record_pid('doc'))


@chamo.group(chain=True)
def queue():
    """Manage harvester queue."""


@queue.resultcallback()
@with_appcontext
def process_actions(actions):
    """Process queue actions."""
    queue = current_app.config['CHAMO_HARVESTER_MQ_QUEUE']
    with establish_connection() as c:
        q = queue(c)
        for action in actions:
            q = action(q)


@queue.command('init')
def init_queue():
    """Initialize harvester queue."""
    def action(queue):
        queue.declare()
        click.secho('Harvester queue has been initialized.', fg='green')
        return queue
    return action


@queue.command('purge')
def purge_queue():
    """Purge indexing queue."""
    def action(queue):
        queue.purge()
        click.secho('Harvester queue has been purged.', fg='green')
        return queue
    return action


@queue.command('delete')
def delete_queue():
    """Delete indexing queue."""
    def action(queue):
        queue.delete()
        click.secho('Indexing queue has been deleted.', fg='green')
        return queue
    return action
