import datetime
import numpy as np
import pandas as pd
import pytz
from sqlalchemy import true
from os import path, chdir
from modules.sql import dwh
from modules.snowflake_connector import sn_dwh

def main(start_date):
    query_name= 'crs'
    crs = dwh().dwh_to_pandas(
        filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
        _start_date = start_date
        )

    crs['cr_type'] = np.where(crs['cr_type'] == 'BUNDLE', 'AIR', crs['cr_type'])

    crs['total_price'] = crs['qty'] * crs['original_price']
    new_crs = crs.groupby(['date','channel','cr_type']).sum(['qty','total_price']).reset_index()
    new_crs = new_crs.drop(['original_price'], axis=1)
    new_crs['weighted_price'] = new_crs['total_price'] / new_crs['qty']
    new_crs['weighted_price'] = new_crs['weighted_price']/1.19

    query_name= 'ncro'
    ncro = dwh().dwh_to_pandas(
        filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
        _start_date = start_date
        )


    query_name = 'om_campaigns_data'
    online_campaigns = sn_dwh(role='ACQUISITION_ANALYST_CL').cursor_to_pandas(
        filename=path.join('querys', 'snowflake', f'select_{query_name}.sql'),
        _start_date = start_date
    )

    online_campaigns.columns = online_campaigns.columns.str.lower()
    ratio = online_campaigns.groupby(['date','acq_channel_level_2']).sum()['total_purchases'].reset_index()

    ratio = ratio.merge(online_campaigns.groupby(['date']).sum()['total_purchases'].reset_index(),on='date',how='left',suffixes=['_channel','_total'])
    ratio['channel_ratio'] = ratio['total_purchases_channel']/ratio['total_purchases_total']
    ratio['channel'] = 'DIGITAL'

    new_crs  = new_crs.merge(ncro,how='outer',left_on=['date','channel','cr_type'],right_on=['date','channel','cr_type'])


    new_crs = new_crs.merge(ratio,how='left',left_on=['date','channel'],right_on=['date','channel'])

    new_crs['qty'] = np.where(new_crs['channel'] == 'DIGITAL',np.where(new_crs['channel_ratio'].isnull(),new_crs['qty'], round(new_crs['qty'] * new_crs['channel_ratio'])),new_crs['qty'])
    new_crs['qty_ncro'] = np.where(new_crs['channel'] == 'DIGITAL',np.where(new_crs['channel_ratio'].isnull(),new_crs['qty_ncro'], round(new_crs['qty_ncro'] * new_crs['channel_ratio'])),new_crs['qty_ncro'])
    new_crs = new_crs.drop(['total_price','total_purchases_channel','total_purchases_total','channel_ratio'], axis=1)

    new_crs.rename(
    columns = {
        'channel':'acq_channel_level_1',
        'qty': 'crs',
        'qty_ncro': 'ncro'
    },
    inplace = True
    )

    dwh().pandas_to_dwh(
            dataframe=new_crs,
            schema_name='analyst_acquisition_cl',
            table_name='growth_sales',
            if_exists='append'
        )
if __name__ == "__main__":
    print("Run here only if testing")