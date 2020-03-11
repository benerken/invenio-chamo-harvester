# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 UCLouvain.
#
# Invenio-Chamo-Harvester is free software; you can redistribute it and/or
# modify it under the terms of the MIT License; see LICENSE file for more
# details.

"""Invenio module for collecting records using Chamo REST API."""

from __future__ import absolute_import, print_function

from kombu import Exchange, Queue

CHAMO_HARVESTER_CHAMO_BASE_URL = "http://localhost:8080/rest"
CHAMO_HARVESTER_CHAMO_USER = ""
CHAMO_HARVESTER_CHAMO_PASSWORD = ""
CHAMO_HARVESTER_BULK_SIZE = 1000

CHAMO_HARVESTER_DEFAULT_DOC_TYPE = "doc"
"""Default doc_type to use if no schema is defined."""

CHAMO_HARVESTER_MQ_EXCHANGE = Exchange('chamo_harvester', type='direct')
"""Default exchange for message queue."""

CHAMO_HARVESTER_MQ_QUEUE = Queue(
    'chamo_harvester',
    exchange=CHAMO_HARVESTER_MQ_EXCHANGE,
    routing_key='chamo_harvester')
"""Default queue for message queue."""

CHAMO_HARVESTER_MQ_ROUTING_KEY = 'chamo_harvester'
"""Default routing key for message queue."""

CHAMO_HARVESTER_BULK_REQUEST_TIMEOUT = 10
"""Request timeout to use in Bulk indexing."""

CHAMO_HARVESTER_RECORD_TO_HARVEST = \
    'invenio_chamo-harvester.utils.default_record_to_harvest'
"""Provide an implemetation of record_to_harvest function"""

CHAMO_HARVESTER_BEFORE_CREATION_HOOKS = []
"""List of automatically connected hooks (function or importable string)."""
