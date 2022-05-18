import datetime
import numpy as np
import pandas as pd
import pytz
from os import path, chdir
chdir(path.join('cl-payback-acquisition'))
from modules.sql import dwh
from modules.snowflake_connector import sn_dwh

query_name= 'crs'
crs = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'select_{query_name}.sql')
    )