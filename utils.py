#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import namedtuple

Tender = namedtuple(
    'Tender', [
        "entity_id", "entity_name", "proc_type", "status", "title", "uaid",
        "id", "price", "price_uah", "currency", "vat", "date", "procedure_name",
        "status_name"])