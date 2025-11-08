#!/usr/bin/env python
# -*- coding: utf-8 -*-

# TODO:
# зберігати схему, щоб виводити посилання на Clarity або ні
# CPV
# Rewrite as a thread


import logging
import pickle
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import ArrowInvalid
import time
from datetime import (
    datetime,
    timedelta,
    timezone)
from typing import Optional, Dict

import requests
import urllib3

from currency import EXCHANGE
from classifiers import PROCDICT, STATUSDICT
from tqdm import tqdm
from requests.exceptions import (
    HTTPError,
    RequestException)
from urllib3.exceptions import InsecureRequestWarning
from utils import (
    YESTERDAY,
    DUCKDB_NAME,
    START_DATE,
    KYIV_ZONE,
    text_clean,
    seconds_to_hms,
    mk_offset_param)
    
urllib3.disable_warnings(InsecureRequestWarning)


SLEEP = 0.24
API_URL = "https://api.openprocurement.org"
API_PATH = "/api/0/tenders"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}

logging.basicConfig(
    filename='download.log',
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
rootLogger = logging.getLogger()
fileHandler = logging.FileHandler('download.log')
rootLogger.addHandler(fileHandler)
consoleHandler = logging.StreamHandler()
rootLogger.addHandler(consoleHandler)

s = requests.Session()
s.headers.update(HEADERS)

tender_schema = pa.schema([
    ("entity_id", pa.string()),
    ("entity_name", pa.string()),
    ("proc_type", pa.string()),
    ("status", pa.string()),
    ('clarif_until', pa.string()),
    ("title", pa.string()),
    ("uaid", pa.string()),
    ("id", pa.string()),
    ("price", pa.float64()),
    ("price_uah", pa.float64()),
    ("currency", pa.string()),
    ("vat", pa.bool_()),
    ("date", pa.string())
    ])

proc_schema = pa.schema([
    ("procedure", pa.string()),
    ("procedure_name", pa.string()),
    ])

status_schema = pa.schema([
    ("status", pa.string()),
    ("status_name", pa.string()),
    ])

# ======================================================================
#   Functions
# ======================================================================

def classifier_to_table(data, schema: pa.schema):
    columns = list(zip(*data))
    arrays = [pa.array(col, type=f.type) for col, f in zip(columns, schema)]
    table = pa.Table.from_arrays(arrays, schema=schema)
    return table


def get_procurement(tid_: str) -> Optional[dict]:
    try:
        res_ = s.get(f"{API_URL}{API_PATH}/{tid_}")
        res_.raise_for_status()
        res_json = res_.json()
        return res_json.get('data')
    except (requests.RequestException, ValueError, KeyError) as e:
        logging.error(f"Error fetching procurement {tid_}: {e}")
        return


def get_tender_date(data: dict) -> datetime:
    # Let tdate_ be a date in future
    tdate_ = datetime(year=2199, month=1, day=1).astimezone()
    if (m := data.get('enquiryPeriod')) is not None:
        tdate_ = datetime.fromisoformat(m['startDate'])
    elif (m := data.get('data')) is not None:
        tdate_ = datetime.fromisoformat(m['date'])
    elif (m := data.get('tenderID')) is not None:
        tender_date_from_id = m[3:13]
        tdate_ = datetime.fromisoformat(tender_date_from_id).astimezone(KYIV_ZONE)
    elif (docs := data.get("documents")) is not None:
        for doc in docs:
            dt_published, dt_modified = doc["datePublished"], doc["dateModified"]
            docs_none_list = [x is None for x in (dt_published, dt_modified)]
            if all(docs_none_list):
                continue
            dates_compare = (
                tdate_,
                *(datetime.fromisoformat(d).astimezone(KYIV_ZONE) for d in (
                        dt_published,
                        dt_modified)),)
            tdate_ = min(dates_compare)
    return tdate_


def get_tender_info(tndr_data, currency_dict=EXCHANGE) -> Dict:
    try:
        result = {}
        mt = tndr_data['procurementMethodType']
        result["proc_type"] = mt
        # Entity construction
        entity = tndr_data['procuringEntity']
        result["entity_name"] = text_clean(entity["name"])
        result["entity_id"] = entity["identifier"]["id"]
        result['id'], result['uaid'] = tndr_data['id'], tndr_data['tenderID']
        result['status'] = result["proc_type"] + "." + tndr_data['status']
        result["title"] = text_clean(tndr_data['title'])
        if (ep := tndr_data.get('enquiryPeriod')) is not None:
            enquiry_period = ep.get('clarificationsUntil')
            result['clarifications_until'] = enquiry_period
        if (value_data := tndr_data.get('value')) is not None:
            result["price"] = value_data.get("amount")
            currency = value_data.get("currency")
            result["currency"] = currency
            # f'{"бе" if tndr_data['value']["valueAddedTaxIncluded"] else ""}з ПДВ'
            result["vat"] = None if (vat := value_data.get("valueAddedTaxIncluded")) is None else vat
            if currency == "UAH":
                result['price_uah'] = result["price"]
            else:
                result['price_uah'] = round(result['price'] * currency_dict[currency], 2)
        else:
            result['vat'] = result["price"] = result['currency'] = result['price_uah'] = None
    except Exception as e1:
        logging.error(tndr_data['id'])
        logging.critical(e1)
        raise
    else:
        return result


def count_tenders_records(con):
    COUNT_QRY = "SELECT COUNT(*) FROM tenders;"
    return con.sql(COUNT_QRY).fetchone()[0]


def duckdb_insert(data, database=DUCKDB_NAME,
                  store_parquet=False):
    try:
        table = pa.Table.from_pylist(data, schema=tender_schema)
        with duckdb.connect(database=database) as con:
            pre_count = count_tenders_records(con)

            con.register("tenders_data", table)
            con.sql("INSERT INTO tenders "
                    "SELECT * FROM tenders_data "
                    "ON CONFLICT (id) DO NOTHING;")
            
            post_count = count_tenders_records(con)
            inserted = post_count - pre_count
            con.commit()
    except ArrowInvalid as e:
        err_file = "errdump.txt"
        logging.error(f"Arrow error, investigate {err_file}")
        with open(err_file, "w", encoding="utf-8") as f:
            f.write(str(data))
        raise
    except Exception as e_:
        logging.exception(e_)
        inserted = 0
    else:
        if store_parquet:
            pq.write_table(table, "output.parquet")
        inserted = len(data)
    return inserted

logging.info("Database creation start")

duckdb_create_string = """
CREATE TABLE IF NOT EXISTS tenders (
    entity_id VARCHAR,
    entity_name VARCHAR,
    proc_type VARCHAR,
    status VARCHAR,
    clarif_until VARCHAR,
    title VARCHAR,
    uaid VARCHAR,
    id VARCHAR PRIMARY KEY,
    price DECIMAL(12,2),
    price_uah DECIMAL(12,2),
    currency VARCHAR,
    vat BOOLEAN,
    date DATE);"""

index_statements = [
    "CREATE INDEX IF NOT EXISTS idx_tenders_date ON tenders(date);",
    "CREATE INDEX IF NOT EXISTS idx_tenders_entity ON tenders(entity_id);",
    "CREATE INDEX IF NOT EXISTS idx_tenders_proc ON tenders(proc_type);"
]

with duckdb.connect(DUCKDB_NAME) as con:
    con.sql(duckdb_create_string)
    for idx_sql in index_statements:
        con.sql(idx_sql)

    con.register("procedure_table", classifier_to_table(
        PROCDICT, proc_schema))
    con.sql("CREATE OR REPLACE TABLE procdict AS " \
            "SELECT * FROM procedure_table;")

    con.register("status_table", classifier_to_table(
        STATUSDICT, status_schema))
    
    con.sql("CREATE OR REPLACE TABLE statusdict (" \
            "status INTEGER PRIMARY KEY, status_name " \
            "TEXT NOT NULL);")
    con.sql("INSERT INTO statusdict " \
            "SELECT * FROM status_table;")
    con.commit()

logging.info("DuckDB Database creation end")

stop_date = datetime.fromisoformat(START_DATE) + timedelta(hours=24)
STOP_DATE = datetime.combine(
    YESTERDAY, datetime.min.time()).replace(tzinfo=KYIV_ZONE)
stop_date = stop_date.astimezone(KYIV_ZONE)

logging.info(f"Startdate is: {START_DATE}; "
             f"Stopdate is: {stop_date.date().isoformat()}")

offset = mk_offset_param(YESTERDAY)
counter = 0
stop = False
tenders_list = []
start_time = time.time()
logging.info("IDs harvesting has begun")
# print(API_URL + API_PATH)

pbar = tqdm(total=None, desc="Fetching items x 100")


while not stop:
    try:
        res = s.get(
            API_URL + API_PATH,
            params={"offset": offset},
            verify=False  # cert='pem_cert.crt', # key=pem-chain.pem
            )
        res.raise_for_status()
        json_data = res.json()
        data_ = json_data['data']
        tenders_list.extend(data_)

        last_date = datetime.fromisoformat(data_[-1]['dateModified'])

        counter += 1
        pbar.update(len(data_))

        if last_date >= stop_date:
            stop = True
            logging.info(f'StopDate {stop_date} is reached')
            logging.info(f"Fetched total: {len(tenders_list)}")
        offset = json_data["next_page"]["offset"]
        time.sleep(SLEEP)
    except (HTTPError, RequestException) as err:
        logging.error(f"Request error occurred: {err}")
        raise
    except Exception as e:
        raise

pbar.close()

end_time = round(time.time() - start_time, 2)
logging.info(f"IDs harvesting complete.\n{len(tenders_list)} items "
             f"have been harvested within {end_time} s.")

yesterday_date = datetime.combine(
    YESTERDAY, datetime.min.time(),
    tzinfo=timezone.utc).astimezone(KYIV_ZONE)
fresh = []
tender_box = []
counter = 0
logging.info("Freshing has begun")
start_time = time.time()

for t in tqdm(tenders_list):
    tid = t['id']
    procurement_data = get_procurement(tid)  # Dict
    if procurement_data is None:
        continue
    counter += 1
    time.sleep(SLEEP)
    tdate = get_tender_date(procurement_data)
    if tdate < yesterday_date:
        continue
    tdate = tdate.date().isoformat()
    p = tid, tdate
    fresh.append(p)
    tender_info = get_tender_info(procurement_data)
    tender_info['date'] = tdate
    tender_box.append(tender_info)
    if counter % 500 == 0:
        try:
            ins: int = duckdb_insert(tender_box)
            if ins == 0:
                raise ValueError("No data inserted. All data is LOST.")
        except Exception as e:
            logging.error("No data inserted. Data LOST, stopped.")
            raise
        else:
            logging.info(f"checked {counter} / inserted {ins}")
            tender_box = []

duckdb_insert(tender_box)


total_seconds = time.time() - start_time
hours, minutes, seconds = seconds_to_hms(total_seconds)

logging.info(f"Fresh complete. {len(fresh)} items has checked within {hours} hours, "
             f"{minutes} minutes, and {seconds} seconds.")
with open("fresh.pickle", "wb") as f:
    pickle.dump(fresh, f)
