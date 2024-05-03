## Prozorro Watchdog

### The flow

The Moneybot's work involves two acts. The first collects the tenders'
data and stores it in a file, and the second converts the data into
messages and sends them to a particular Telegram channel.

### Collection

The `get_procurements.py` script should be executed to start the data
collection process. During its work, the `procurements.db` SQLite
database will be populated with data about the procurements that were
quoted last day,  and the file `tenders_.pickle` will be also created.
There are only top N procurements by amount stored in this file; the N
number is set by `LIMIT` variable of the `get_procurements.py` (6 by
default).

The `procurements.db` SQLite contains all the data about quoted
procurements. If its size does not fit your capabilities, you can
safely delete it.

At the end of the data collection process, a zipped CSV file named
`yyyy-mm-dd_procdata.csv.zip` will be created in the `archive`
directory (this directory is created in case it does not exist). You
can set up your webserver to make this directory accessible from the
Web.

### Posting

Once a day (via `cron`), the second script, `bot.py` takes the data from
the `tenders_.pickle` and sends them as formatted messages into a
Telegram channel. The channel's ID is stored in the `config.py` file
along with the access token. You should create your own config by
editing `config.skel.py`.

The messages are organized into portions to fit the message length
restriction in Telegram (4096 characters, so be careful with compound
emojis). At the end of the last message, the link to the `archive`
directory is added.

### Currency exchange rates

To convert the prices in other currencies into Ukrainian Hryvnia, we use
the exchange rates from the Ukrainian central bank body. The
corresponding code is stored in the `currency.py` file.

If, for some reason, yesterday's exchange rates are not available, a
fallback to the previously obtained rates is performed.

### Miscellaneous

Because there are many procurements quoting on particular days, we
recommend running `get_procurements.py` and `bot.py` with several hours
gap.  