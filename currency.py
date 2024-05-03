#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
from datetime import datetime, timedelta
import logging
import json


HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}

logging.basicConfig(
    filename='download.log',
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
rootLogger = logging.getLogger('download')
fileHandler = logging.FileHandler('download.log')
rootLogger.addHandler(fileHandler)
consoleHandler = logging.StreamHandler()
rootLogger.addHandler(consoleHandler)


s = requests.Session()
s.headers.update(HEADERS)
yesterday = datetime.today() - timedelta(hours=24)
START_DATE = yesterday.date().isoformat()

def get_exchange(currency, date):
    exdate = (datetime.fromisoformat(date) - timedelta(hours=24)).strftime("%Y%m%d")
    res = s.get("https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange",
                params={"valcode":currency, "date": exdate,
                       "json": True})
    if res.status_code != 200:
        print(":(")
        return
    k = res.json()
    return k[0]['rate']


def read_exchange():
    k = {"UAH": 1, "date": START_DATE}
    try:
        u = get_exchange("USD", START_DATE)
        e = get_exchange("EUR", START_DATE)
        r = get_exchange("RUB", START_DATE)
        g = get_exchange("GBP", START_DATE)
        if any([x is None for x in (u, e, r, g)]):
            raise ValueError
    except:
        with open("exchange.json") as f:
            k = json.load(f)
        logging.warning('CURRENCY: Cannot fetch actual exchange rate, restored.')
        # raise
    else:
        k["EUR"] = e
        k["USD"] = u
        k['RUB'] = r
        k['GBP'] = g
        with open("exchange.json", "w") as f:
            json.dump(k, f)
        logging.info('Exchange rates saved.')
    finally:
        logging.info('Exchange rate updated.')
    return k


EXCHANGE = read_exchange()
