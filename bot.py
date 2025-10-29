#!/usr/bin/env python
# -*- coding: utf-8 -*-


import locale
import logging
import pickle
import telebot
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import Optional, List

from config import TOKEN, CHANNEL
from utils import BULLET, NOBR, BOX

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
first_msg = True
already_sent = False
current_dt = sent_dt = None
current_dt_file = Path("./current_dt.txt")


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
