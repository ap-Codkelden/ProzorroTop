#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import locale
import logging
import duckdb
import sys
from datetime import datetime
from pathlib import Path
from typing import List

import discord

from config import DISCORD_TOKEN, DISCORD_CHANNEL_ID
from utils import (
    DUCKDB_NAME, BULLET, LIMIT,
    Tender, START_DATE, beautify_number
)


logging.basicConfig(
    filename="discobot.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


try:
    locale.setlocale(locale.LC_TIME, "uk_UA.UTF-8")
    LOC_DATE = "%d %B %Y"
except locale.Error:
    LOC_DATE = "%d.%m.%Y"


qry_template = (
    "SELECT tenders.*"
    " , procdict.procedure_name, statusdict.status_name"
    " FROM tenders"
    " LEFT JOIN procdict"
    " ON tenders.proc_type = procdict.procedure"
    " LEFT JOIN statusdict"
    " ON tenders.status = statusdict.status"
    f" WHERE date = '{START_DATE}'"
    " ORDER BY price_uah DESC;"
)

try:
    with duckdb.connect(DUCKDB_NAME) as con:
        qry_box = con.sql(qry_template).fetchall()

    if not qry_box:
        raise ValueError("Empty query result")

except Exception as e:
    logging.error(e)
    sys.exit(1)

tenders_info = [Tender.from_tuple(r) for r in qry_box[:LIMIT]]

if not tenders_info:
    logging.error("No tenders after LIMIT")
    sys.exit(1)

# Markdown

def build_line(m: Tender) -> str:
    return (
        f"{BULLET} **{beautify_number(m.price_uah)}**"
        f" — {m.entity_name.strip()} "
        f"([{m.entity_id}](https://clarity-project.info/edr/{int(m.entity_id)}))\n"
        f"[{m.title[:120]}…](https://prozorro.gov.ua/tender/{m.uaid})\n"
        f"*Процедура:* _{m.procedure_name}_"
    )

lines = [build_line(m) for m in tenders_info]
report_date = tenders_info[0].date


def chunk_lines(text_lines: List[str], limit: int = 4096) -> List[str]:
    chunks = []
    buffer = ""

    for part in text_lines:
        candidate = (buffer + "\n\n" + part) if buffer else part
        if len(candidate) > limit:
            chunks.append(buffer)
            buffer = part
        else:
            buffer = candidate

    if buffer:
        chunks.append(buffer)

    return chunks


message_box = chunk_lines(lines)

async def send_to_discord():

    intents = discord.Intents.default()
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        logging.info("Connected to Discord")

        channel = client.get_channel(DISCORD_CHANNEL_ID)

        if not channel:
            logging.error("Channel not found")
            await client.close()
            return

        for i, msg in enumerate(message_box):

            embed = discord.Embed(
                title=f"📊 Топ закупівель — {report_date.strftime(LOC_DATE)}"
                if i == 0 else None,
                description=msg,
                color=0x1F8BFF
            )

            # Технічний час постингу
            embed.timestamp = datetime.utcnow()

            # Footer
            embed.set_footer(
                text="Prozorro Watchdog Bot"
            )

            await channel.send(embed=embed)
            await asyncio.sleep(0.8)

        await client.close()


if __name__ == "__main__":
    asyncio.run(send_to_discord())
