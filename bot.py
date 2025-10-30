#!/usr/bin/env python
# -*- coding: utf-8 -*-


import locale
import logging
import pickle
import duckdb
import telebot
import sys
import time
from pathlib import Path
from typing import List

from config import TOKEN, CHANNEL
from utils import (
    DUCKDB_NAME, BULLET, NOBR, BOX, LIMIT,
    Tender, START_DATE, make_csv_datafile,
    beautify_number)


logging.basicConfig(
    filename='bot.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

try:
    locale.setlocale(locale.LC_TIME, "uk_UA.UTF-8")
    locale.setlocale(locale.LC_NUMERIC, "uk_UA.UTF-8")
    LOC_DATE = "%d %B %Y"
except locale.Error:
    logging.warning("Locale is not supported")
    LOC_DATE = r"%d.%m.%Y"


bot = telebot.TeleBot(TOKEN)
already_sent = False
current_dt = sent_dt = None
current_dt_file = Path("./current_dt.txt")


def send_messages(message_box: List[str]):
    try:
        for msg in message_box:
            bot.send_message(
                CHANNEL, msg, parse_mode='HTML',
                disable_web_page_preview=True)
            time.sleep(.64)
    except Exception:
        logging.error("Portion does not sent")
        raise
    else:
        logging.info("Successfully sent")
        current_dt_file.write_text(f"{current_dt}")

# Інформація для БОТА
logging.info(f"Perform Database Query")

qry_template = (
    "SELECT tenders.*"
    "  , procdict.procedure_name, statusdict.status_name"
    "  from tenders"
    "  LEFT JOIN procdict"
    "  on tenders.proc_type = procdict.procedure"
    "  LEFT JOIN statusdict"
    "  on tenders.status = statusdict.status"
    f"  where date= '{START_DATE}'"
    "  order by price_uah desc;")

qry_box: list[tuple] = []

try:
    with duckdb.connect(DUCKDB_NAME) as con:
        qry_box = con.sql(qry_template).fetchall()

    if not qry_box:
        raise ValueError("Empty query result")
except (ValueError, Exception) as e:
    logging.error("Error to fetch procurement chart's top")
    logging.error(e)
else:
    logging.info(f"Database Query Done")
    tenders_info = [Tender.from_tuple(r) for r in qry_box[:LIMIT]]
    with open("qry_res.pickle", "wb") as f:
        pickle.dump(qry_box, f)
    
with open("tenders_.pickle", "wb") as f:
    pickle.dump(tenders_info, f) # type: ignore

try:
    make_csv_datafile(qry_box, filedate=START_DATE)
except Exception as e:
    logging.error(e)

try:
    with open("tenders_.pickle", "rb") as f:
        top_tenders = pickle.load(f)
    if not top_tenders:
        raise ValueError
except (AttributeError, ValueError):
    logging.critical("Can't load top: Empty TOP")
    sys.exit(1)
except FileNotFoundError:
    logging.critical("TOP File is missing")
    sys.exit(1)

if current_dt_file.is_file():
    sent_dt = current_dt_file.read_text()
else:
    current_dt_file.write_text("1970-01-01")

first_msg = True
message_box: list[str] = []
msg_portion = []
for m in top_tenders:
    msg = f'{BULLET}{NOBR}<b>{beautify_number(m.price_uah)}</b>{NOBR}'\
      f'— {m.entity_name.strip()} (<a href="https://clarity-project.info/edr/'\
      f'{int(m.entity_id)}">{m.entity_id}</a>)\n'\
      f'<a href="https://prozorro.gov.ua/tender/{m.uaid}">'\
      f'{m.title[:120] + "…"}</a>\nПроцедура: <em>{m.procedure_name}</em>'
    if first_msg:
        current_dt = m.date
        if current_dt == sent_dt:
            logging.error("Already sent")
            already_sent = True
        msg = f'За {current_dt.strftime(LOC_DATE)} було створено '\
              'такий топ закупівель:\n\n' + msg
        first_msg = False
    cur_portion = "\n\n".join(msg_portion)
    len_cur_portion = len(cur_portion)

    if len_cur_portion + len(msg) + 2 > 4096:
        message_box.append(cur_portion)
        msg_portion = [msg]
    else:
        msg_portion.append(msg)
message_box.append("\n\n".join(msg_portion))

# Add link to archive
archive_advertise = (f'\n\n<a href="https://zbs.dp.ua/moneydog">{BOX}'
                    'Архів останніх закупівель</a>')
if message_box:
    last_msg_lehgth = len(message_box[-1])
    if last_msg_lehgth < 4026:
        new_msg = message_box[-1] + archive_advertise
        message_box[-1] = new_msg
    else:
        message_box.append(archive_advertise)

if already_sent: sys.exit()
send_messages(message_box)
