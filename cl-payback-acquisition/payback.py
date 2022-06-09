import datetime
from calendar import monthrange
from email.headerregistry import DateHeader
import numpy as np
import pandas as pd
import pytz
from os import path, chdir

from sqlalchemy import null
chdir(path.join('cl-payback-acquisition'))
from modules.sql import dwh
from modules.snowflake_connector import sn_dwh
import budget
import crs_ncro

santiago_tz = pytz.timezone('America/Santiago')


date = (datetime.datetime.now(tz=santiago_tz).replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
month = (datetime.datetime.now(tz=santiago_tz).replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d")
partners_date = (datetime.datetime.now(tz=santiago_tz).replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=31)).strftime("%Y-%m-%d")


budget.main(date)
crs_ncro.main(date)


query_name = 'budget'
budget_table = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
    _date = date
    )

budget_table['acq_channel_level_2'] = np.where(budget_table['acq_channel_level_1'] != 'Digital',budget_table['acq_channel_level_1'],budget_table['acq_channel_level_2'])
budget_table = budget_table.groupby(['date','acq_channel_level_1','acq_channel_level_2']).sum()['total_amount_spent'].reset_index()

query_name = 'sales'
sales = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
    _date = date
    )
sales = sales.drop(sales[(sales.acq_channel_level_1 == 'PARTNERS')].index)
sales.acq_channel_level_1 = sales.acq_channel_level_1.str.title()

query_name = 'sales_partners'
sales_partners = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
    _date = partners_date
    )
sales_partners.acq_channel_level_1 = sales_partners.acq_channel_level_1.str.title()
sales_partners.date = date

sales = pd.concat([sales,sales_partners])

sales['acq_channel_level_1'] = np.where(sales['acq_channel_level_1'] == 'Raf','RaF',sales.acq_channel_level_1)
sales['acq_channel_level_2'] = np.where(sales['acq_channel_level_1'] != 'Digital',sales['acq_channel_level_1'],sales['acq_channel_level_2'])

sales.rename(
        columns = {
            'crs': 'crs_payback',
            'ncro': 'ncro_payback',
            'weighted_price': 'weighted_price_payback'
        },
        inplace = True
    )

query_name = 'sales'
real_sales = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
    _date = date
    )

real_sales.acq_channel_level_1 = real_sales.acq_channel_level_1.str.title()


real_sales['acq_channel_level_1'] = np.where(real_sales['acq_channel_level_1'] == 'Raf','RaF',real_sales.acq_channel_level_1)
real_sales['acq_channel_level_2'] = np.where(real_sales['acq_channel_level_1'] != 'Digital',real_sales['acq_channel_level_1'],real_sales['acq_channel_level_2'])

sales.date = date
real_sales.date = date
sales = sales.merge(real_sales,how='outer',on=['date','acq_channel_level_1','acq_channel_level_2','cr_type'])

ratio = sales.groupby(['date','acq_channel_level_1','acq_channel_level_2']).sum()['ncro_payback'].reset_index()

ratio = ratio.merge(sales.groupby(['date','acq_channel_level_1','acq_channel_level_2','cr_type']).sum()['ncro_payback'].reset_index(),on=['date','acq_channel_level_1','acq_channel_level_2'],how='left',suffixes=['_total','_cr_type'])
ratio['card_reader_ratio'] = ratio['ncro_payback_cr_type']/ratio['ncro_payback_total']

sales = sales.merge(ratio,how='left', on=['date','acq_channel_level_1','acq_channel_level_2','cr_type'])
sales = sales.drop(['ncro_payback_total','ncro_payback_cr_type'],axis=1)

sales.date = date
budget_table.date = date

sales = sales.merge(budget_table,how='outer',on = ['date','acq_channel_level_1','acq_channel_level_2'])

sales['proportional_budget'] = sales.card_reader_ratio * sales.total_amount_spent
sales = sales.drop(['card_reader_ratio','total_amount_spent'],axis=1)

query_name = 'unit_economics'
unit_economics = sn_dwh(role='ACQUISITION_ANALYST_CL').cursor_to_pandas(
    filename=path.join('querys', 'snowflake', f'select_{query_name}.sql'),
            _month = month,
)
unit_economics.columns= unit_economics.columns.str.lower()


punto_solo = [date,'Punto','SOLO']
raf_solo = [date,'RaF','SOLO']
partners_solo = [date,'Partners','SOLO']
digital_solo = [date, 'Digital','SOLO']
vaps_solo = [date, 'Vap','SOLO']
retail_solo = [date, 'Retail','SOLO']
others_solo = [date, 'Other','SOLO']


punto_air = [date,'Punto','AIR']
raf_air = [date,'RaF','AIR']
partners_air =  [date,'Partners','AIR']
digital_air = [date, 'Digital','AIR']
vaps_air = [date, 'Vap','AIR']
retail_air = [date, 'Retail','AIR']
others_air = [date, 'Other','AIR']

for index, row in unit_economics.iterrows():
    if row.acq_channel_level_1 == 'Punto':
        if row.metric == 'Avg Monthly Net Rev / nCRO Air':
            punto_air.append(row.value)
        elif row.metric == 'Avg Monthly Net Rev / nCRO SOLO':
            punto_solo.append(row.value)
        elif row.metric =='CRS Net Landed Cost Air':
            punto_air.append(row.value)
        elif row.metric =='CRS Net Landed Cost SOLO':
            punto_solo.append(row.value)
    elif row.acq_channel_level_1 == 'Digital':
        if row.metric == 'Avg Monthly Net Rev / nCRO Air':
            digital_air.append(row.value)
        elif row.metric == 'Avg Monthly Net Rev / nCRO SOLO':
            digital_solo.append(row.value)
        elif row.metric =='CRS Net Landed Cost Air':
            digital_air.append(row.value)
        elif row.metric =='CRS Net Landed Cost SOLO':
            digital_solo.append(row.value)
    elif row.acq_channel_level_1 == 'RaF':
        if row.metric == 'Avg Monthly Net Rev / nCRO Air':
            raf_air.append(row.value)
        elif row.metric == 'Avg Monthly Net Rev / nCRO SOLO':
            raf_solo.append(row.value)
        elif row.metric =='CRS Net Landed Cost Air':
            raf_air.append(row.value)
        elif row.metric =='CRS Net Landed Cost SOLO':
            raf_solo.append(row.value)
    elif row.acq_channel_level_1 == 'Partners':
        if row.metric == 'Avg Monthly Net Rev / nCRO Air':
            partners_air.append(row.value)
        elif row.metric == 'Avg Monthly Net Rev / nCRO SOLO':
            partners_solo.append(row.value)
        elif row.metric =='CRS Net Landed Cost Air':
            partners_air.append(row.value)
        elif row.metric =='CRS Net Landed Cost SOLO':
            partners_solo.append(row.value)
    elif row.acq_channel_level_1 == 'Retail':
        if row.metric == 'Avg Monthly Net Rev / nCRO Air':
            retail_air.append(row.value)
        elif row.metric == 'Avg Monthly Net Rev / nCRO SOLO':
            retail_solo.append(row.value)
        elif row.metric =='CRS Net Landed Cost Air':
            retail_air.append(row.value)
        elif row.metric =='CRS Net Landed Cost SOLO':
            retail_solo.append(row.value)
    elif row.acq_channel_level_1 == 'Vaps':
        if row.metric == 'Avg Monthly Net Rev / nCRO Air':
            vaps_air.append(row.value)
        elif row.metric == 'Avg Monthly Net Rev / nCRO SOLO':
            vaps_solo.append(row.value)
        elif row.metric =='CRS Net Landed Cost Air':
            vaps_air.append(row.value)
        elif row.metric =='CRS Net Landed Cost SOLO':
            vaps_solo.append(row.value)
    elif row.acq_channel_level_1 == 'Others':
        if row.metric == 'Avg Monthly Net Rev / nCRO Air':
            others_air.append(row.value)
        elif row.metric == 'Avg Monthly Net Rev / nCRO SOLO':
            others_solo.append(row.value)
        elif row.metric =='CRS Net Landed Cost Air':
            others_air.append(row.value)
        elif row.metric =='CRS Net Landed Cost SOLO':
            others_solo.append(row.value)

rue = [punto_solo,punto_air,raf_solo,raf_air,partners_solo,partners_air,digital_solo,digital_air,retail_solo,retail_air,vaps_solo,vaps_air,others_solo,others_air]
real_unit_economics = pd.DataFrame(rue,columns=['date','acq_channel_level_1','cr_type','avg_monthly_net_rev_ncro','crs_net_landed_cost'])

sales = sales.merge(real_unit_economics, how='left', on= ['date','acq_channel_level_1','cr_type'])

sales['crs_payback'] = sales['crs_payback'].fillna(0)
sales['ncro_payback'] = sales['ncro_payback'].fillna(0)
sales['weighted_price_payback'] = sales['weighted_price_payback'].fillna(0)
sales['crs'] = sales['crs'].fillna(0)
sales['ncro'] = sales['ncro'].fillna(0)
sales['weighted_price'] = sales['weighted_price'].fillna(0)
sales['proportional_budget'] = sales['proportional_budget'].fillna(0)

dwh().pandas_to_dwh(
        dataframe=sales,
        schema_name='analyst_acquisition_cl',
        table_name='growth_forecast_data',
        if_exists='append'
    )