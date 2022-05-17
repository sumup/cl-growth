import datetime
import numpy as np
import pandas as pd
import pytz
from ..modules.sql import dwh
from os import path
from ..modules.snowflake_connector import sn_dwh

query_name= 'crs'
crs = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'select_{query_name}.sql')
    )