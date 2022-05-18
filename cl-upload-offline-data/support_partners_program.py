import datetime
import numpy as np
import pandas as pd
import pytz
from os import path, chdir
chdir(path.join('cl-upload-offline-data'))
from modules.sql import dwh
from modules.snowflake_connector import sn_dwh

query_name = 'card_tokens'
card_tokens = sn_dwh(role='ACQUISITION_ANALYST_CL').cursor_to_pandas(
    filename=path.join('querys', 'snowflake', f'select_{query_name}.sql')
)
card_tokens.columns= card_tokens.columns.str.lower()

query_name= 'partners_support'
delete_retail = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'delete_{query_name}.sql'),
    )

dwh().pandas_to_dwh(
        dataframe=card_tokens,
        schema_name='analyst_acquisition_cl',
        table_name='partners_support',
        if_exists='append'
    )