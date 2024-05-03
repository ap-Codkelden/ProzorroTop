#!/usr/bin/env python
# -*- coding: utf-8 -*-

# TODO:
# зберігати схему, щоб виводити посилання на Clarity або ні
# CPV
# Rewrite as a thread
# use duckdb?


import logging
import pickle
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Optional

import requests
import ujson
import urllib3

from currency import EXCHANGE
from zoneinfo import ZoneInfo
from urllib3.exceptions import InsecureRequestWarning
from utils import Tender, make_csv_datafile, mk_offset_param, seconds_to_hms,\
    text_clean

urllib3.disable_warnings(InsecureRequestWarning)
requests.models.json = ujson

SLEEP = 0.24
API_URL = "https://api.openprocurement.org"
API_PATH = "/api/0/tenders"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
TODAY = datetime.today()
TODAY = TODAY.replace(hour=0, minute=0, second=0, microsecond=0)
kyiv_zone = ZoneInfo("Europe/Kyiv")
YESTERDAY = TODAY - timedelta(hours=24)
START_DATE = YESTERDAY.date().isoformat()

LIMIT = 6
DATABASE_NAME = "procurements.db"

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


# ======================================================================
#   Functions
# ======================================================================


def get_rocurement(tid_: str) -> Optional[dict]:
    res_ = s.get(API_URL + API_PATH + f"/{tid_}")
    if res_.status_code != 200:
        print(res_.url)
        print("Can't load procurement data")
        return
    data = res_.json()['data']
    return data


def get_tender_date(data: dict) -> datetime:
    # Let tdate_ be a date in future
    tdate_ = datetime(year=2199, month=1, day=1).astimezone(kyiv_zone)
    if (m := data.get('enquiryPeriod')) is not None:
        tdate_ = datetime.fromisoformat(m['startDate'])
    elif (m := data.get('data')) is not None:
        tdate_ = datetime.fromisoformat(m['date'])
    elif (m := data.get('tenderID')) is not None:
        tender_date_from_id = m[3:13]
        tdate_ = datetime.fromisoformat(tender_date_from_id).astimezone(kyiv_zone)
    elif (docs := data.get("documents")) is not None:
        for doc in docs:
            dt_published, dt_modified = doc["datePublished"], doc["dateModified"]
            docs_none_list = [x is None for x in (dt_published, dt_modified)]
            if all(docs_none_list):
                continue
            dates_compare = (tdate_,
                             *(datetime.fromisoformat(d).astimezone(kyiv_zone) for d in (dt_published, dt_modified)),)
            tdate_ = min(dates_compare)
    return tdate_


def get_tender_info(tndr_data, currency_dict=EXCHANGE):
    try:
        result = {}
        mt = tndr_data['procurementMethodType']
        result["proc_type"] = mt
        # Entity
        entity = tndr_data['procuringEntity']
        result["entity_name"] = entity["name"]
        result["entity_id"] = entity["identifier"]["id"]

        result['id'], result['uaid'] = tid, tndr_data['tenderID']
        result['status'] = result["proc_type"] + "." + tndr_data['status']
        result["title"] = text_clean(tndr_data['title'])
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
        logging.error(tid)
        logging.critical(e1)
        raise
    else:
        return result


def db_insert(cursor, data):
    try:
        cursor.executemany(
            "INSERT INTO tenders ('entity_id', 'entity_name', 'proc_type', "
            "                     'status', 'title', 'uaid', 'id', 'price', "
            "'price_uah', 'currency', 'vat', 'date') "
            "VALUES (:entity_id, :entity_name, :proc_type, :status, :title,"
            ":uaid, :id, :price, :price_uah, :currency, :vat, :date)", data)
    except Exception as e_:
        logging.exception(e_)
        inserted = 0
    else:
        inserted = len(data)
    return inserted


logging.info("Database creation start")
db = sqlite3.connect(DATABASE_NAME)
cur = db.cursor()

cur.execute('CREATE TABLE IF NOT EXISTS tenders ('
            'entity_id text, entity_name text, '
            'proc_type text, status text, '
            "title text, uaid text, id text, "
            "price real, price_uah real, "
            "currency text, vat integer, date text);")

with open('procdist.sql') as f:
    script = f.read()
    cur.executescript(script)

with open('statusdist.sql') as f:
    script = f.read()
    cur.executescript(script)

db.commit()
logging.info("Database creation end")

stop_date = datetime.fromisoformat(START_DATE) + timedelta(hours=24)
stop_date = stop_date.astimezone(kyiv_zone)

logging.info(f"Startdate is: {START_DATE}; "
             f"Stopdate is: {stop_date.date().isoformat()}")

offset = mk_offset_param(YESTERDAY)
counter = 0
stop = False
tenders_list = []
start_time = time.time()
logging.info("IDs harvesting has begun")
# print(API_URL + API_PATH)
while not stop:
    res = s.get(API_URL + API_PATH,
                params={"offset": offset},
                verify=False  # cert='pem_cert.crt', # key=pem-chain.pem
                )

    if res.status_code != 200:
        print("Cannot reach API")
        break
    data_ = res.json()['data']
    tenders_list.extend(data_)
    try:
        last_date = datetime.fromisoformat(data_[-1]['dateModified'])
    except Exception as e:
        raise
    counter += 1
    if counter % 10 == 0:
        logging.info(f"Fetched {counter}")
    if last_date >= stop_date:
        stop = True
        # print("Stop")
    offset = res.json()["next_page"]["offset"]
    time.sleep(SLEEP)

end_time = round(time.time() - start_time, 2)
logging.info(f"Id harvesting complete. {len(tenders_list)} items has harvest "
             f"within {end_time} s.")

k = YESTERDAY.astimezone(kyiv_zone)
fresh = []
tender_box = []
counter = 0

logging.info("Freshing has begun")
start_time = time.time()
for t in tenders_list:
    tid = t['id']
    procurement_data = get_rocurement(tid)  # Dict
    counter += 1
    time.sleep(SLEEP)
    tdate = get_tender_date(procurement_data)
    if tdate < k:
        continue
    tdate = tdate.date().isoformat()
    p = tid, tdate
    fresh.append(p)
    tender_info = get_tender_info(procurement_data)
    tender_info['date'] = tdate
    tender_box.append(tender_info)
    if counter % 500 == 0:
        try:
            ins = db_insert(cur, tender_box)
            if ins == 0:
                raise ValueError("No data inserted. All the data is LOST.")
        except Exception as e:
            logging.error("No data inserted. Data LOST, stopped.")
            raise
        else:
            logging.info(f"checked / inserted {counter} / {ins}")
            db.commit()
            tender_box = []
db_insert(cur, tender_box)
db.commit()

total_seconds = time.time() - start_time
hours, minutes, seconds = seconds_to_hms(total_seconds)

logging.info(f"Fresh complete. {len(fresh)} items has checked within {hours} hours, "
             f"{minutes} minutes, and {seconds} seconds.")
with open("fresh.pickle", "wb") as f:
    pickle.dump(fresh, f)

# Інформація для БОТА
logging.info(f"Perform Database Query")
qry_template = """SELECT tenders.*
, procdist.procedure_name, statusdist.status_name
from tenders
LEFT JOIN procdist
on tenders.proc_type = procdist.procedure
LEFT JOIN statusdist
on tenders.status = statusdist.status """ \
               f'where date= "{START_DATE}"' \
               f'order by price_uah desc;'

qry_box = None
tender_info = None
try:
    cur.execute(qry_template)
    qry_box = cur.fetchall()
    if not qry_box:
        raise ValueError("Empty query result")
except (ValueError, Exception) as e:
    # logging.error("Error to fetch procurement chart's top")
    logging.error(e)
else:
    logging.info(f"Database Query Done")
    tenders_info = [r for r in map(Tender._make, qry_box[:LIMIT])]
    with open("tenders_.pickle", "wb") as f:
        pickle.dump(tenders_info, f)

try:
    make_csv_datafile(qry_box, filedate=START_DATE)
except Exception as e:
    logging.error(e)
finally:
    db.close()
