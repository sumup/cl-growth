import datetime
import numpy as np
import pandas as pd
import pytz
from os import path, chdir
chdir(path.join('cl-payback-acquisition'))
import modules.budget
import modules.crs_ncro
from modules.sql import dwh
from modules.snowflake_connector import sn_dwh


santiago_tz = pytz.timezone('America/Santiago')

start_date = (datetime.datetime.now(tz=santiago_tz).replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=31)).strftime("%Y-%m-%d 00:00:00")
end_date = (datetime.datetime.now(tz=santiago_tz).replace(hour=0, minute=0, second=0, microsecond=0)).strftime("%Y-%m-%d 00:00:00")

start_date_dt = datetime.datetime.fromisoformat(start_date)
end_date_dt = datetime.datetime.fromisoformat(end_date)
delta = end_date_dt - start_date_dt   # as timedelta

days = []
for i in range(0, delta.days):
    days.append((start_date_dt + datetime.timedelta(days=i)).strftime("%Y-%m-%d"))

start_date_str = days[0]

query_name = 'budget_data'
delete_budget = dwh().dwh_to_pandas(
filename=path.join('querys', 'server_querys', f'delete_{query_name}.sql'),
_date = start_date_str
)
for day in days:
    budget.main(day)

query_name = 'last_saved_shipping_date'
saved = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
    )

query_name = 'last_shipping_date'
upload = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
    )

saved_date = saved.values[0][0]
uploaded_date = upload.values[0][0]

if saved_date < uploaded_date:
    dwh().pandas_to_dwh(
        dataframe=upload,
        schema_name='analyst_acquisition_cl',
        table_name='logistic_last_update',
        if_exists='append'
    )
    query_name = 'forecast_data'
    delete_forecast = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'delete_{query_name}.sql'),
    _date = saved_date.strftime("%Y-%m-%d")
    )
    query_name = 'crs_data'
    delete_crs = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'delete_{query_name}.sql'),
    _date = saved_date.strftime("%Y-%m-%d")
    )
    start_date_dt = datetime.datetime.combine(saved_date, datetime.datetime.min.time()) 
    end_date_dt = datetime.datetime.fromisoformat(days[-1])
    delta = end_date_dt - start_date_dt
    days_log = []
    for i in range(0, (delta.days  + 1)):
        days_log.append((start_date_dt + datetime.timedelta(days=i)).strftime("%Y-%m-%d"))
else:
    days_log = [days[-1]]

if len(days) > len(days_log):
    days_final = days
    query_name = 'forecast_data'
    delete_forecast = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'delete_{query_name}.sql'),
    _date = start_date_str
    )
    query_name = 'crs_data'
    delete_crs = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'delete_{query_name}.sql'),
    _date = start_date_str
    )
else:
    days_final = days_log

for day in days_final:
    datetime_day = datetime.datetime.strptime(day,"%Y-%m-%d")
    month = datetime_day.replace(day=1).strftime("%Y-%m-%d")
    partners_date = (datetime_day - datetime.timedelta(days=31)).strftime("%Y-%m-%d")


    crs_ncro.main(day)


    query_name = 'budget'
    budget_table = dwh().dwh_to_pandas(
        filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
        _date = day
        )

    budget_table['acq_channel_level_2'] = np.where(budget_table['acq_channel_level_1'] != 'Digital',budget_table['acq_channel_level_1'],budget_table['acq_channel_level_2'])
    budget_table = budget_table.groupby(['date','acq_channel_level_1','acq_channel_level_2']).sum()['total_amount_spent'].reset_index()

    query_name = 'sales'
    sales = dwh().dwh_to_pandas(
        filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
        _date = day
        )
    sales = sales.drop(sales[(sales.acq_channel_level_1 == 'PARTNERS')].index)
    sales.acq_channel_level_1 = sales.acq_channel_level_1.str.title()

    query_name = 'sales_partners'
    sales_partners = dwh().dwh_to_pandas(
        filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
        _date = partners_date
        )
    sales_partners.acq_channel_level_1 = sales_partners.acq_channel_level_1.str.title()
    sales_partners.date = day

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
        _date = day
        )

    real_sales.acq_channel_level_1 = real_sales.acq_channel_level_1.str.title()


    real_sales['acq_channel_level_1'] = np.where(real_sales['acq_channel_level_1'] == 'Raf','RaF',real_sales.acq_channel_level_1)
    real_sales['acq_channel_level_2'] = np.where(real_sales['acq_channel_level_1'] != 'Digital',real_sales['acq_channel_level_1'],real_sales['acq_channel_level_2'])

    sales.date = day
    real_sales.date = day
    sales = sales.merge(real_sales,how='outer',on=['date','acq_channel_level_1','acq_channel_level_2','cr_type'])

    ratio = sales.groupby(['date','acq_channel_level_1','acq_channel_level_2']).sum()['ncro_payback'].reset_index()

    ratio = ratio.merge(sales.groupby(['date','acq_channel_level_1','acq_channel_level_2','cr_type']).sum()['ncro_payback'].reset_index(),on=['date','acq_channel_level_1','acq_channel_level_2'],how='left',suffixes=['_total','_cr_type'])
    ratio['card_reader_ratio'] = ratio['ncro_payback_cr_type']/ratio['ncro_payback_total']

    sales = sales.merge(ratio,how='left', on=['date','acq_channel_level_1','acq_channel_level_2','cr_type'])
    sales = sales.drop(['ncro_payback_total','ncro_payback_cr_type'],axis=1)

    sales.date = day
    budget_table.date = day

    sales = sales.merge(budget_table,how='outer',on = ['date','acq_channel_level_1','acq_channel_level_2'])

    sales['proportional_budget'] = sales.card_reader_ratio * sales.total_amount_spent
    sales = sales.drop(['card_reader_ratio','total_amount_spent'],axis=1)

    query_name = 'unit_economics'
    unit_economics = sn_dwh(role='ACQUISITION_ANALYST_CL').cursor_to_pandas(
        filename=path.join('querys', 'snowflake', f'select_{query_name}.sql'),
                _month = month,
    )
    unit_economics.columns= unit_economics.columns.str.lower()


    punto_solo = [day,'Punto','SOLO']
    raf_solo = [day,'RaF','SOLO']
    partners_solo = [day,'Partners','SOLO']
    digital_solo = [day, 'Digital','SOLO']
    vaps_solo = [day, 'Vap','SOLO']
    retail_solo = [day, 'Retail','SOLO']
    others_solo = [day, 'Other','SOLO']
    smx_solo = [day, 'Small Mx','SOLO']


    punto_air = [day,'Punto','AIR']
    raf_air = [day,'RaF','AIR']
    partners_air =  [day,'Partners','AIR']
    digital_air = [day, 'Digital','AIR']
    vaps_air = [day, 'Vap','AIR']
    retail_air = [day, 'Retail','AIR']
    others_air = [day, 'Other','AIR']
    smx_air = [day, 'Small Mx','AIR']

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
        elif row.acq_channel_level_1 == 'Small Mx':
            if row.metric == 'Avg Monthly Net Rev / nCRO Air':
                smx_air.append(row.value)
            elif row.metric == 'Avg Monthly Net Rev / nCRO SOLO':
                smx_solo.append(row.value)
            elif row.metric =='CRS Net Landed Cost Air':
                smx_air.append(row.value)
            elif row.metric =='CRS Net Landed Cost SOLO':
                smx_solo.append(row.value)

    rue = [punto_solo,punto_air,raf_solo,raf_air,partners_solo,partners_air,digital_solo,digital_air,retail_solo,retail_air,vaps_solo,vaps_air,others_solo,others_air,smx_solo,smx_air]
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
        
    query_name = 'wrong_budget'
    fix = dwh().dwh_to_pandas(
        filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
        _date = day
        ) 
    for index, row in fix.iterrows():
        query_name = 'wrong_budget'
        delete_budget = dwh().dwh_to_pandas(
        filename=path.join('querys', 'server_querys', f'delete_{query_name}.sql'),
        _date = day,
        _acq_channel_level_1 = row.acq_channel_level_1
        )

    dwh().pandas_to_dwh(
            dataframe=fix,
            schema_name='analyst_acquisition_cl',
            table_name='growth_forecast_data',
            if_exists='append'
        )
