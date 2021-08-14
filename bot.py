#!/usr/bin/env python
# -*- coding: utf-8 -*-


import locale
import logging
import pickle
import telebot
import time
import sys
from config import TOKEN, CHANNEL
from datetime import datetime
from utils import Tender

try:
    locale.setlocale(locale.LC_TIME, "uk_UA.UTF-8")
    locale.setlocale(locale.LC_NUMERIC, "uk_UA.UTF-8")
    LOC_DATE = "%d %B %Y"
except locale.Error:
    logging.warning("Locale is not supported")
    LOC_DATE = "%d.%m.%Y"

BULLET = "\U0001F518"
NOBR = "\U000000A0"


def beautify_number(n, suffix='грн.'):
    if not n or n is None:
        return ""
    for unit in ('', 'тис.', 'млн', 'млрд', 'трлн'):
        if abs(n) < 1000.0:
            n = locale.format_string("%3.1f", n)
            return "%s %s %s" % (n, unit, suffix)
        n /= 1000.0
    n = locale.format_string("%.1f", n)
    return "%s %s %s" % (n, 'квдрлн', suffix)


logging.basicConfig(
    filename='bot.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

try:
    with open("tenders_.pickle", "rb") as f:
        top_tenders = pickle.load(f)
except (AttributeError, FileNotFoundError):
    logging.critical("Can't load top")
    # raise
    sys.exit(1)

if not top_tenders:
    logging.warning("Empty TOP")
    sys.exit(1)

top_tenders = top_tenders[:5]
print(len(top_tenders))

bot = telebot.TeleBot(TOKEN)
message_box = []
msg_portion = []
first_msg = True

for m in top_tenders:
    # print(m)
    msg = f'{BULLET}{NOBR}<b>{beautify_number(m.price_uah)}</b>{NOBR}'\
      f'— {m.entity_name.strip()} (<a href="https://clarity-project.info/edr/'\
      f'{int(m.entity_id)}">{m.entity_id}</a>)\n'\
      f'<a href="https://prozorro.gov.ua/tender/{m.uaid}">'\
      f'{m.title[:120] + "…"}</a>\nПроцедура: <em>{m.procedure_name}</em>'
    if first_msg:
        msg = f'За {datetime.fromisoformat(m.date).strftime(LOC_DATE)} '\
              'було створено такий топ закупівель:\n\n' + msg
        first_msg = False
    cur_portion = "\n\n".join(msg_portion)
    len_cur_portion = len(cur_portion)
    print(len_cur_portion)
    if len_cur_portion + len(msg) + 2 > 4096:
        message_box.append(cur_portion)
        msg_portion = [msg]
    else:
        msg_portion.append(msg)

message_box.append("\n\n".join(msg_portion))
print(len(message_box[0]))

for msg in message_box:
    try:
        bot.send_message(CHANNEL,
                         msg, disable_web_page_preview=True,
                         parse_mode='HTML')
    except:
        logging.error("Portion NOT sent")
    else:
        logging.info("Portion successfully sent")
    time.sleep(.5)
    # pass
