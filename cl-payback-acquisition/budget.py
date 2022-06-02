import datetime
from calendar import monthrange
import numpy as np
import pandas as pd
import pytz
from os import path, chdir
chdir(path.join('cl-payback-acquisition'))
from modules.sql import dwh
from modules.snowflake_connector import sn_dwh

start_date = (datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
end_date = (datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)).strftime("%Y-%m-%d 00:00:00")

print(f'Running from {start_date} to {end_date}')
#################################################################################
################################ Digital Budget #################################
#################################################################################

query_name = 'om_campaigns_data'
online_campaigns = sn_dwh(role='ACQUISITION_ANALYST_CL').cursor_to_pandas(
    filename=path.join('querys', 'snowflake', f'select_{query_name}.sql'),
            _start_date = start_date,
            _end_date = end_date
)

online_campaigns.columns = online_campaigns.columns.str.lower()

online_campaigns = online_campaigns.drop(['total_purchases'], axis=1)

dwh().pandas_to_dwh(
        dataframe=online_campaigns,
        schema_name='analyst_acquisition_cl',
        table_name='growth_acquisition_budget',
        if_exists='append'
    )

#################################################################################
################################## RaF Budget ###################################
#################################################################################

query_name = 'raf_budget'
raf_budget = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
    _start_date = start_date,
    _end_date = end_date)

raf_budget_bonus = raf_budget.copy()
raf_budget['acq_channel_level_1'] = 'RaF'
raf_budget = raf_budget.drop(['raf_bonus_rewards'], axis=1)
raf_budget['acq_channel_level_2'] = np.where(raf_budget['raf_regular_rewards'].isnull(),np.nan, 'Regular')

raf_budget_bonus = raf_budget_bonus.drop(['raf_regular_rewards'], axis=1)
raf_budget_bonus['acq_channel_level_1'] = 'RaF'
raf_budget_bonus['acq_channel_level_2'] = np.where(raf_budget_bonus['raf_bonus_rewards'].isnull(),np.nan, 'Bonus')

raf_budget.rename(
columns = {
    'raf_regular_rewards':'amount_spent'
},
inplace = True
)

raf_budget_bonus.rename(
columns = {
    'raf_bonus_rewards':'amount_spent'
},
inplace = True
)

if raf_budget['acq_channel_level_2'].notnull():
    
    dwh().pandas_to_dwh(
        dataframe=raf_budget,
        schema_name='analyst_acquisition_cl',
        table_name='growth_acquisition_budget',
        if_exists='append'
    )

if raf_budget['acq_channel_level_2'].notnull():
    
    dwh().pandas_to_dwh(
        dataframe=raf_budget,
        schema_name='analyst_acquisition_cl',
        table_name='growth_acquisition_budget',
        if_exists='append'
    )




#################################################################################
################################ Partners Budget ################################
#################################################################################


query_name = 'partners_facebook_campaigns_data'
partners_campaigns = sn_dwh(role='ACQUISITION_ANALYST_CL').cursor_to_pandas(
    filename=path.join('querys', 'snowflake', f'select_{query_name}.sql'),
            _start_date = start_date,
            _end_date = end_date
)
partners_campaigns.columns= partners_campaigns.columns.str.lower()

partners_campaigns['acq_channel_level_1'] = 'Partners'
dwh().pandas_to_dwh(
        dataframe=partners_campaigns,
        schema_name='analyst_acquisition_cl',
        table_name='growth_acquisition_budget',
        if_exists='append'
    )

query_name = 'partners_bonus'
partners_budget = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
    _start_date = start_date,
    _end_date = end_date)

dwh().pandas_to_dwh(
        dataframe=partners_budget,
        schema_name='analyst_acquisition_cl',
        table_name='growth_acquisition_budget',
        if_exists='append'
    )


#################################################################################
################################# Google Sheets Budget ##################################
#################################################################################

first_month_day = (datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d")
query_name = 'external_budget'
external_budget = sn_dwh(role='ACQUISITION_ANALYST_CL').cursor_to_pandas(
    filename=path.join('querys', 'snowflake', f'select_{query_name}.sql'),
            _date = first_month_day,
)


year = (datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)).year
month = (datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)).month
num_days = monthrange(year, month)[1]

external_budget['amount_spent'] = external_budget['amount_spent']/num_days

external_budget['date'] = start_date
external_budget['acq_channel_level_2'] = 'Others'

dwh().pandas_to_dwh(
        dataframe=external_budget,
        schema_name='analyst_acquisition_cl',
        table_name='growth_acquisition_budget',
        if_exists='append'
    )
