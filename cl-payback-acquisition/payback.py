import datetime
from calendar import monthrange
import numpy as np
import pandas as pd
import pytz
from os import path, chdir

from sqlalchemy import null
chdir(path.join('cl-payback-acquisition'))
from modules.sql import dwh
from modules.snowflake_connector import sn_dwh

santiago_tz = pytz.timezone('America/Santiago')


date = (datetime.datetime.now(tz=santiago_tz).replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
month = (datetime.datetime.now(tz=santiago_tz).replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d")

query_name = 'budget'
budget = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
    _date = date
    )

budget['acq_channel_level_2'] = np.where(budget['acq_channel_level_1'] != 'Digital',budget['acq_channel_level_1'],budget['acq_channel_level_2'])
budget = budget.groupby(['date','acq_channel_level_1','acq_channel_level_2','cr_type']).sum()['total_amount_spent'].reset_index()

query_name = 'sales'
sales = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
    _date = date
    )
sales = sales.drop(sales[(sales.acq_channel_level_1 != 'PARTNERS') & (sales.acq_channel_level_1 != 'PUNTO') & (sales.acq_channel_level_1 != 'RAF') & (sales.acq_channel_level_1 != 'DIGITAL')].index)
sales.acq_channel_level_1 = sales.acq_channel_level_1.str.title()
sales['acq_channel_level_1'] = np.where(sales['acq_channel_level_1'] == 'Raf','RaF',sales.acq_channel_level_1)
sales['acq_channel_level_2'] = np.where(sales['acq_channel_level_1'] != 'Digital',sales['acq_channel_level_1'],sales['acq_channel_level_2'])
ratio = sales.groupby(['date','acq_channel_level_1','acq_channel_level_2']).sum()['ncro'].reset_index()

ratio = ratio.merge(sales.groupby(['date','acq_channel_level_1','acq_channel_level_2','cr_type']).sum()['ncro'].reset_index(),on=['date','acq_channel_level_1','acq_channel_level_2'],how='left',suffixes=['_total','_cr_type'])
ratio['card_reader_ratio'] = ratio['ncro_cr_type']/ratio['ncro_total']

sales = sales.merge(ratio,how='left', on=['date','acq_channel_level_1','acq_channel_level_2','cr_type'])
sales = sales.drop(['ncro_total','ncro_cr_type'],axis=1)

sales = sales.merge(budget,how='outer',on = ['date','acq_channel_level_1','acq_channel_level_2'])