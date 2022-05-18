import datetime
import numpy as np
import pandas as pd
import requests
import time
from os import path, chdir
chdir(path.join('cl-refresh-mv'))
from modules.sql import dwh
from requests.api import request
from requests.sessions import RequestsCookieJar

##################################################################################
# Stage 0: Update NCRO
##################################################################################

try:
    started_at = datetime.datetime.now()
    query_name = 'partners_mean_tpv'
    result = dwh().run_query(filename=path.join('querys', 'server_querys', f'update_{query_name}.sql'))
    ended_at = datetime.datetime.now()
    minutes = (ended_at - started_at).seconds/60
    refresh_results = pd.DataFrame(columns=['date_refresh', 'result', 'reason_failed', 'started_at', 'ended_at', 'execution_time_minutes'])
    data = {
        'date_refresh': datetime.datetime.now().date().strftime('%Y-%m-%d')
        ,'result': 'partners_mean_tpv_successful'
        ,'reason_failed': np.nan
        ,'started_at': started_at
        ,'ended_at': ended_at
        ,'execution_time_minutes': minutes
    }
    refresh_results = refresh_results.append(data, ignore_index=True)
    print(refresh_results)

except Exception as e:
    refresh_results = pd.DataFrame(columns=['date_refresh', 'result', 'reason_failed', 'started_at', 'ended_at', 'execution_time_minutes'])
    data = {
        'date_refresh': datetime.datetime.now().date().strftime('%Y-%m-%d')
        ,'result': 'partners_mean_tpv_successful_failed'
        ,'reason_failed': str(e)
        ,'started_at': np.nan
        ,'ended_at': np.nan
        ,'execution_time_minutes': np.nan
    }
    refresh_results = refresh_results.append(data, ignore_index=True)

dwh().pandas_to_dwh(
    dataframe=refresh_results,
    schema_name='analyst_acquisition_cl',
    table_name='refresh_ncro_results',
    if_exists='append'
)
