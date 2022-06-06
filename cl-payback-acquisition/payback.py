import datetime
from calendar import monthrange
import numpy as np
import pandas as pd
import pytz
from os import path, chdir

from sqlalchemy import null
chdir(path.join('cl-payback-acquisition'))
from modules.sql import dwh
from modules.snowflake_connector import sn_dwh

santiago_tz = pytz.timezone('America/Santiago')


date = (datetime.datetime.now(tz=santiago_tz).replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
month = (datetime.datetime.now(tz=santiago_tz).replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d")

query_name = 'budget'
budget = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
    _date = date
    )

budget['acq_channel_level_2'] = np.where(budget['acq_channel_level_1'] != 'Digital',budget['acq_channel_level_1'],budget['acq_channel_level_2'])