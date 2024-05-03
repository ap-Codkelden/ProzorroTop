#!/usr/bin/env python
# -*- coding: utf-8 -*-


import csv
import re
import zipfile
from collections import namedtuple
from typing import List
from pathlib import Path
from zoneinfo import ZoneInfo
from datetime import datetime


Tender = namedtuple(
    'Tender', [
        "entity_id", "entity_name", "proc_type", "status", "title", "uaid",
        "id", "price", "price_uah", "currency", "vat", "date", "procedure_name",
        "status_name"])


def mk_offset_param(date_obj: datetime) -> str:
    """
    Makes an offset parameter for the API
    """
    kyiv_zone = ZoneInfo("Europe/Kyiv")
    date_obj_offset = date_obj.astimezone(kyiv_zone)
    iso_string = date_obj_offset.isoformat()
    return iso_string


def make_csv_datafile(data: List, filedate=None):
    backup_dir = Path.cwd().joinpath('archive')
    Path(backup_dir).mkdir(parents=True, exist_ok=True)
    if not data:
        return 1
    header = ['entity_id', 'entity_name', 'proc_type', 'status', 'title',
              'uaid', 'id', 'price', 'price_uah', 'currency', 'vat', 'date',
              'procedure_name', 'status_name']
    csv_file_name = f"{filedate.replace('-', '')}_data.csv"
    zip_file_name = Path.cwd().joinpath('archive', f"{csv_file_name}.zip")
    with open(csv_file_name, "w") as f:
        writer = csv.writer(f, delimiter='\t', quoting=2)
        writer.writerow(header)
        writer.writerows(data)
    try:
        with zipfile.ZipFile(zip_file_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(csv_file_name)
    except Exception as e:
        pass
    else:
        Path(csv_file_name).unlink()
    return 0


def seconds_to_hms(seconds):
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    remaining_seconds = int(seconds % 60)
    return hours, minutes, remaining_seconds


def text_clean(string: str, typostrofe=False) -> str:
    # re.sub(pattern, repl, string, count=0, flags=0)
    k = string.replace('\xa0', ' ') \
        .replace('"', '”') \
        .replace('`', "'")
    k = k.replace("'", '’')
    k = re.sub("&nbsp;?", "", k)
    k = re.sub(r"\s+", " ", k)
    return k.strip()
