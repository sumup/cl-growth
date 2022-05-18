import datetime
import numpy as np
import pandas as pd
import pytz
from os import path, chdir
chdir(path.join('cl-payback-acquisition'))
from modules.sql import dwh
from modules.snowflake_connector import sn_dwh


query_name = 'partners_facebook_campaigns_data'
partners_campaigns = sn_dwh(role='ACQUISITION_ANALYST_CL').cursor_to_pandas(
    filename=path.join('querys', 'snowflake', f'select_{query_name}.sql')
)
partners_campaigns.columns= partners_campaigns.columns.str.lower()

query_name = 'om_facebook_campaigns_data'
online_campaigns = sn_dwh(role='ACQUISITION_ANALYST_CL').cursor_to_pandas(
    filename=path.join('querys', 'snowflake', f'select_{query_name}.sql')
)
online_campaigns.columns = online_campaigns.columns.str.lower()