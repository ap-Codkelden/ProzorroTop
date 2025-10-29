#!/usr/bin/env python
# -*- coding: utf-8 -*-


import csv
import re
import zipfile
import locale
from typing import List, Optional, Any, Tuple
from pathlib import Path
from zoneinfo import ZoneInfo
from datetime import datetime
from dataclasses import dataclass


BULLET = "\U0001F518"
NOBR = "\U000000A0"
BOX = "\U0001F4E6"

@dataclass
class Tender:
    entity_id: Optional[str] = None
    entity_name: Optional[str] = None
    proc_type: Optional[str] = None
    status: Optional[str] = None
    title: Optional[str] = None
    uaid: Optional[str] = None
    id: Optional[str] = None
    price: Optional[float] = None
    price_uah: Optional[float] = None
    currency: Optional[str] = None
    vat: Optional[bool] = None
    date: Optional[datetime] = None
    procedure_name: Optional[str] = None
    status_name: Optional[str] = None

    @classmethod
    def from_tuple(cls, row: Tuple[Any, ...]) -> "Tender":
        fields = list(cls.__dataclass_fields__.keys())
        data = {k: v for k, v in zip(fields, row)}
        return cls(**data)


def mk_offset_param(date_obj: datetime) -> str:
    """
    Makes an offset parameter for the API
    """
    kyiv_zone = ZoneInfo("Europe/Kyiv")
    date_obj_offset = date_obj.astimezone(kyiv_zone)
    iso_string = date_obj_offset.isoformat()
    return iso_string


def beautify_number(n, suffix='грн.'):
    if not n or n is None:
        return ""
    for unit in ('', 'тис.', 'млн', 'млрд', 'трлн'):
        if abs(n) < 1000.0:
            n = locale.format_string("%3.1f", n)
            return f"{n} {unit} {suffix}"
        n /= 1000.0
    n = locale.format_string("%.1f", n)
    return f"{n} квдрлн {suffix}"


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
        with zipfile.ZipFile(
            zip_file_name, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.write(csv_file_name)
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


def text_clean(string: str, typostrofe=False, translit=False,
                   uk2en=False) -> Optional[str]:
    en_letters = "aeiopcxBKMHTyAEIOPCX"
    uk_letters = "аеіорсхВКМНТуАЕІОРСХ"
    if not uk2en:
        order = en_letters, uk_letters
    else:
        order = uk_letters, en_letters
    transpose = str.maketrans(*order)
    k = string.replace('\xa0', ' ') \
        .replace('"', '”') \
        .replace('`', "'")
    if typostrofe:
        k = k.replace("'", '’')
    k = re.sub("&nbsp;?", "", k)
    k = re.sub(r"\s+", " ", k)
    if translit:
        k = k.translate(transpose)
    return k.strip()
