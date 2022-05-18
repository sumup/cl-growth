import datetime
import numpy as np
import pandas as pd
import pytz
from os import path, chdir
chdir(path.join('cl-upload-offline-data'))
from modules.sql import dwh
from modules.snowflake_connector import sn_dwh

query_name = 'offline_serial_numbers'
serial_numbers = sn_dwh(role='ACQUISITION_ANALYST_CL').cursor_to_pandas(
    filename=path.join('querys', 'snowflake', f'select_{query_name}.sql')
)
serial_numbers.columns= serial_numbers.columns.str.lower()

query_name= 'all_retail'
delete_retail = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'delete_{query_name}.sql'),
    )

dwh().pandas_to_dwh(
        dataframe=serial_numbers,
        schema_name='analyst_acquisition_cl',
        table_name='retail_punto_serial_numbers_prod',
        if_exists='append'
    )