import datetime
import numpy as np
import pandas as pd
import pytz
from os import path, chdir
chdir(path.join('cl-upload-offline-data'))
from modules.sql import dwh
from modules.snowflake_connector import sn_dwh

query_name = 'influencers_vouchers'
raf_influencers = sn_dwh(role='ACQUISITION_ANALYST_CL').cursor_to_pandas(
    filename=path.join('querys', 'snowflake', f'select_{query_name}.sql')
)
raf_influencers.columns= raf_influencers.columns.str.lower()

query_name= 'all_raf_influencers'
delete_retail = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'delete_{query_name}.sql'),
    )

dwh().pandas_to_dwh(
        dataframe=raf_influencers,
        schema_name='analyst_acquisition_cl',
        table_name='raf_influencers',
        if_exists='append'
    )