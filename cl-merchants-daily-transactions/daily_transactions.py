import datetime
import numpy as np
import pandas as pd
import pytz
from ..modules.sql import dwh
from os import path
from ..modules.snowflake_connector import sn_dwh


start_date = '2016-01-01 00:00:00'  
end_date = '2022-05-18 00:00:00'

#end_date = datetime.datetime.now()

##################################################################################
# Stage 0: Parameters
##################################################################################

prod = False

if not prod:

    start_date_dt = datetime.datetime.fromisoformat(start_date)
    end_date_dt = datetime.datetime.fromisoformat(end_date)
    delta = end_date_dt - start_date_dt   # as timedelta

    days = []
    for i in range(0, delta.days):
        days.append([(start_date_dt + datetime.timedelta(days=i)).strftime("%Y-%m-%d 00:00:00"), (start_date_dt + datetime.timedelta(days=i+1)).strftime("%Y-%m-%d 00:00:00")])

    for start_date, end_date in days:
            print(f'Running from {start_date} to {end_date}')
            query_name = 'merchants_daily_transactions.sql'
            extracted_data = dwh().dwh_to_pandas(
                filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
                _start_date = start_date,
                _end_date = end_date)

            dwh().pandas_to_dwh(
                        dataframe=extracted_data,
                        schema_name='analyst_acquisition_cl',
                        table_name='merchants_daily_transactions',
                        if_exists='append'
                    )
if prod:

    santiago_tz = pytz.timezone('America/Santiago')

    start_date = (datetime.datetime.now(tz=santiago_tz) - datetime.timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    end_date = datetime.datetime.now(tz=santiago_tz).strftime("%Y-%m-%d 00:00:00")

    extracted_data = dwh().dwh_to_pandas(
                filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
                _start_date = start_date,
                _end_date = end_date)

    dwh().pandas_to_dwh(
                dataframe=extracted_data,
                schema_name='analyst_acquisition_cl',
                table_name='merchants_daily_transactions',
                if_exists='append'
            )