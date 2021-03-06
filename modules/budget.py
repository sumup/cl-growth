import datetime
from calendar import monthrange
import numpy as np
import pandas as pd
import pytz
from os import path, chdir

from sqlalchemy import null
from modules.sql import dwh
from modules.snowflake_connector import sn_dwh

def main(start_date):
#################################################################################
################################ Digital Budget #################################
#################################################################################

    query_name = 'om_campaigns_data'
    online_campaigns = sn_dwh(role='ACQUISITION_ANALYST_CL').cursor_to_pandas(
        filename=path.join('querys', 'snowflake', f'select_{query_name}.sql'),
                _start_date = start_date
    )

    online_campaigns.columns = online_campaigns.columns.str.lower()

    online_campaigns = online_campaigns.drop(['total_purchases'], axis=1)
    online_campaigns['acq_channel_level_1'] = 'Digital'
    online_campaigns['acq_channel_level_1'] = np.where(online_campaigns['acq_channel_level_3'].str.contains('Small MX'),'Small Mx',online_campaigns['acq_channel_level_1'])

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
        _start_date = start_date)

    raf_budget_bonus = raf_budget.copy()
    raf_budget['acq_channel_level_1'] = 'RaF'
    raf_budget = raf_budget.drop(['raf_bonus_rewards'], axis=1)
    raf_budget['acq_channel_level_2'] = np.where(pd.isnull(raf_budget['raf_regular_rewards']),None, 'Regular')

    raf_budget_bonus = raf_budget_bonus.drop(['raf_regular_rewards'], axis=1)
    raf_budget_bonus['acq_channel_level_1'] = 'RaF'
    raf_budget_bonus['acq_channel_level_2'] = np.where(pd.isnull(raf_budget_bonus['raf_bonus_rewards']),None, 'Bonus')

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

    raf_budget = pd.concat([raf_budget,raf_budget_bonus])

    raf_budget.dropna(subset = ["acq_channel_level_2"], inplace=True)
    raf_budget['acq_channel_level_3'] = None

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
                _start_date = start_date
    )
    partners_campaigns.columns= partners_campaigns.columns.str.lower()

    partners_campaigns['acq_channel_level_1'] = 'Partners'
    partners_campaigns['acq_channel_level_3'] = None
    dwh().pandas_to_dwh(
            dataframe=partners_campaigns,
            schema_name='analyst_acquisition_cl',
            table_name='growth_acquisition_budget',
            if_exists='append'
        )

    query_name = 'partners_bonus'
    partners_budget = dwh().dwh_to_pandas(
        filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
        _start_date = start_date)
    partners_budget['acq_channel_level_3'] = None
    dwh().pandas_to_dwh(
            dataframe=partners_budget,
            schema_name='analyst_acquisition_cl',
            table_name='growth_acquisition_budget',
            if_exists='append'
        )


    #################################################################################
    ################################# Google Sheets Budget ##################################
    #################################################################################

    first_month_day = datetime.datetime.strptime(start_date,"%Y-%m-%d").replace(day=1).strftime("%Y-%m-%d")
    query_name = 'external_budget'
    external_budget = sn_dwh(role='ACQUISITION_ANALYST_CL').cursor_to_pandas(
        filename=path.join('querys', 'snowflake', f'select_{query_name}.sql'),
                _date = first_month_day,
    )

    external_budget.columns= external_budget.columns.str.lower()


    year = (datetime.datetime.strptime(start_date,"%Y-%m-%d")).year
    month = (datetime.datetime.strptime(start_date,"%Y-%m-%d")).month
    num_days = monthrange(year, month)[1]

    external_budget['amount_spent'] = external_budget['amount_spent']/num_days

    external_budget['date'] = start_date
    external_budget['acq_channel_level_2'] = 'Others'
    external_budget['acq_channel_level_3'] = None
    external_budget = external_budget.drop(['month'], axis=1)


    dwh().pandas_to_dwh(
            dataframe=external_budget,
            schema_name='analyst_acquisition_cl',
            table_name='growth_acquisition_budget',
            if_exists='append'
        )

if __name__ == "__main__":
    print("Run here only if testing")
