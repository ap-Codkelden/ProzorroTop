#!/usr/bin/env python
# -*- coding: utf-8 -*-

# TODO:
# зберігати схему, щоб виводити посилання на клеріті або ні
# CPV

import os
import requests
import sqlite3
import time
from datetime import datetime, timedelta
from pytz import timezone
import logging
import pickle
from collections import namedtuple
from currency import get_exchange, read_exchange, EXCHANGE
import ujson
from utils import Tender
requests.models.json = ujson

SLEEP = 0.27
API_URL = "https://api.openprocurement.org"
API_PATH = "/api/2.5/tenders"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",}
TODAY = datetime.today()
LIMIT = 6

kyiv = timezone("Europe/Kiev")
yesterday = datetime.today() - timedelta(hours=24)
START_DATE = yesterday.date().isoformat()
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

'''
   ФУНКЦІЇ
'''

def get_rocurement(tid_):
    res_ = s.get(API_URL + API_PATH + f"/{tid_}")
    if res_.status_code != 200:
        print(res_.url)
        print("Can't load procurement data")
        return
    data = res_.json()['data']
    return data


def get_tender_date(data):
    tdate_ = None
    if (m:=data.get('enquiryPeriod')) is not None:
        tdate_ = datetime.fromisoformat(m['startDate']).astimezone(kyiv)
    elif (m:=data.get('data')) is not None:
        tdate_ = datetime.fromisoformat(m['date']).astimezone(kyiv)
    elif (m:=data.get('tenderID')) is not None:
        tender_id_parts = m.split("-")
        tdate_ = "-".join([
            tender_id_parts[1], tender_id_parts[2],
            tender_id_parts[3]])
        tdate_ = datetime.fromisoformat(tdate_).astimezone(kyiv)
    elif (docs:=data.get("documents")) is not None:
        for doc in docs:
            if all((x is None for x in (
                    doc["datePublished"], doc["dateModified"]))):
                continue
            if tdate_ is None:
                dates_compare = (datetime.fromisoformat(
                    doc["datePublished"]).astimezone(kyiv),
                                 datetime.fromisoformat(
                                     doc["dateModified"]).astimezone(kyiv))
            else:
                dates_compare = (tdate_,
                                 datetime.fromisoformat(
                                     doc["datePublished"]).astimezone(kyiv),
                                 datetime.fromisoformat(
                                     doc["dateModified"]).astimezone(kyiv))
            tdate_ = min(dates_compare)
    return tdate_


def get_tender_info(data_, currency_dict=EXCHANGE):
    try:
        res = {}
        # print(data_['status'])
        mt = data_['procurementMethodType']
        res["proc_type"] = mt
        # -------------------------
        # Entity
        entity = data_['procuringEntity']
        res["entity_name"] = entity["name"]
        res["entity_id"] = entity["identifier"]["id"]
        # -------------------------
        res['id'], res['uaid'] = tid, data_['tenderID']
        res['status'] = res["proc_type"] + "." + data_['status']
        res["title"] = data_['title']
        if (value_data:=data_.get('value')) is not None:
            res["price"] = value_data.get("amount")
            currency = value_data.get("currency")
            res["currency"] = currency
            if (vat:=value_data.get("valueAddedTaxIncluded")) is not None:
                # f'{"бе" if data_['value']["valueAddedTaxIncluded"] else ""}з ПДВ'
                res["vat"] = 1 if vat else 0
            else:
                res["vat"] = vat
            if currency == "UAH":
                res['price_uah'] = res["price"]
            else:
                res['price_uah'] = round(
                    res["price"] * currency_dict[currency], 2)
        else:
            res["vat"] = res["price"] = res["currency"] = res['price_uah'] = None
    except:
        print(tid)
        raise
    else:
        return res


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

#-----------------------------------------------------------------------------

logging.info("Database creation start")
db = sqlite3.connect(DATABASE_NAME)
cur = db.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS tenders ("
            'entity_id text, '
            'entity_name text, '
             "proc_type text, "
            "status text, "
            "title text, "
            "uaid text, id text, "
            "price real, "
            "price_uah real, "
            "currency text, "
            "vat integer, date text);")

with open('procdist.sql') as f:
    script = f.read()
    cur.executescript(script)

with open('statusdist.sql') as f:
    script = f.read()
    cur.executescript(script)

db.commit()
# db.close()
logging.info("Database creation end")


stop_date = datetime.fromisoformat(START_DATE) + timedelta(hours=24)
stop_date = stop_date.astimezone(kyiv)

logging.info(f"Startdate is: {START_DATE}; "
    f"Stopdate is: {stop_date.date().isoformat()}")

offset = f"{START_DATE}T00:00:00.000000+03:00"
counter = 0
stop = False
tenders_list = []
start_time = time.time()
logging.info("Id harvesting has begun")
while not stop:
    res = s.get(API_URL + API_PATH,
               params={"offset": offset})
    if res.status_code != 200:
        print("Can't load API")
        break
    data_ = res.json()['data']
    tenders_list.extend(data_)
    try:
        last_date = datetime.fromisoformat(data_[-1]['dateModified'])
    except:
        raise
    counter += 1
    if counter % 100 == 0:
        logging.info(f"Fetched {counter}")
    if last_date >= stop_date:
        stop = True
        # print("Stop")
    offset = res.json()["next_page"]["offset"]
    time.sleep(SLEEP)

end_time = round(time.time() - start_time, 2)
logging.info(f"Id harvesting complete. {len(tenders_list)} items has harvest "
    f"within {end_time} s.")

k = datetime.fromisoformat(START_DATE).astimezone(kyiv)
fresh = []
tender_box = []
counter = 0

logging.info("Freshing has begun")
start_time = time.time()
for t in tenders_list:
    tid = t['id']
    procurement_data = get_rocurement(tid)
    counter+=1
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
                raise ValueError("No data inserted. Data LOST.")
        except:
            logging.error("No data inserted. Data LOST, stopped.")
            raise
        else:
            logging.info(f"{counter} / {ins} checked / inserted")
            db.commit()
            tender_box = []
db_insert(cur, tender_box)
db.commit()

end_time = round(time.time() - start_time, 2)
logging.info(f"Freshing complete. {len(fresh)} items has checked within {end_time} s.")
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
on tenders.status = statusdist.status """\
f'where date= "{START_DATE}"'\
f'order by price_uah desc limit {LIMIT};'

qry_box = []
try:
    cur.execute(qry_template)
    qry_box = cur.fetchall()
except Exception as e:
    logging.error("Error TOP get")
    logging.error(e)
    tenders_info = qry_box
else:
    logging.info(f"Database Query Done")
    tenders_info = [r for r in map(Tender._make, qry_box)]
finally:
    db.close()

with open("tenders_.pickle", "wb") as f:
    pickle.dump(tenders_info, f)
