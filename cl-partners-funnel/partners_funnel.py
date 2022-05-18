import datetime
import numpy as np
import pandas as pd
import pytz
import requests
import time
from os import path, chdir
chdir(path.join('cl-partners-funnel'))
from modules.sql import dwh
from modules.snowflake_connector import sn_dwh
from requests.api import request
from requests.sessions import RequestsCookieJar


funnel = pd.DataFrame(columns=['partner_id','email','cohort','status_date','status','lead_source'])
######################################################
########### Stage 1: Get Leads Information ###########
######################################################

query_name = 'partners_leads'
chile_leads = sn_dwh(role='ACQUISITION_ANALYST_CL').cursor_to_pandas(
    filename=path.join('querys', 'snowflake', f'select_{query_name}.sql')
)
chile_leads.columns= chile_leads.columns.str.lower()

query_name = 'partners_leads_dashboard'
chile_leads_dashboard = dwh().dwh_to_pandas(
        filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
        db_uri = 'postgresql+psycopg2://raimundo_cavada:Rcavada_2022@188.166.10.10:5432/sumup')

query_name = 'all_dashboard_leads'
delete_all_dashboard = dwh().run_query(
    filename=path.join('querys', 'server_querys', f'delete_{query_name}.sql'),)

dwh().pandas_to_dwh(
        dataframe=chile_leads_dashboard,
        schema_name='analyst_acquisition_cl',
        table_name='partners_leads_dashboard',
        if_exists='append'
    )

query_name ='partners_leads_dashboard_complete'
chile_leads_dashboard_complete = dwh().dwh_to_pandas(
        filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),)
chile_leads = chile_leads.append(chile_leads_dashboard_complete)

query_name = 'partners_ncro'
partners_ncro = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
    )
chile_leads = pd.merge(chile_leads,partners_ncro,how='left',left_on='partner_id',right_on='partner_id')
for index,record in chile_leads.iterrows():
    try: 
        partner_id = int(record['partner_id'])
    except:
        partner_id = record['partner_id']
    aux_df= pd.DataFrame(data=[[partner_id,record['email'],record['form_date'],record['form_date'],'lead',record['lead_source']]],columns=['partner_id','email','cohort','status_date','status','lead_source'])
    funnel = funnel.append(aux_df)
    if type(record['training_date']) in (datetime.date, pd.Timestamp):
        aux_df= pd.DataFrame(data=[[partner_id,record['email'],record['form_date'],record['training_date'],'trained',record['lead_source']]],columns=['partner_id','email','cohort','status_date','status','lead_source'])
        funnel = funnel.append(aux_df)
    if record['payment_date'] != None:
        aux_df= pd.DataFrame(data=[[partner_id,record['email'],record['form_date'],record['payment_date'],'ordered',record['lead_source']]],columns=['partner_id','email','cohort','status_date','status','lead_source'])
        funnel = funnel.append(aux_df)
    if type(record['min']) is datetime.date:
        aux_df= pd.DataFrame(data=[[partner_id,record['email'],record['form_date'],record['min'],'live',record['lead_source']]],columns=['partner_id','email','cohort','status_date','status','lead_source'])
        funnel = funnel.append(aux_df)
        aux_df= pd.DataFrame(data=[[partner_id,record['email'],record['form_date'],record['min'],'end',record['lead_source']]],columns=['partner_id','email','cohort','status_date','status','lead_source'])
        funnel = funnel.append(aux_df)

query_name = 'all_funnel'
delete_funnel = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'delete_{query_name}.sql'),
    )

dwh().pandas_to_dwh(
        dataframe=funnel,
        schema_name='analyst_acquisition_cl',
        table_name='partners_funnel',
        if_exists='append'
    )

################################################################
############# Stage 2: Upload Partners Information #############
################################################################

query_name = 'all_partners_information'
partners_information = dwh().dwh_to_pandas(
        filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
        db_uri = 'postgresql+psycopg2://raimundo_cavada:Rcavada_2022@188.166.10.10:5432/sumup')

query_name = 'all_partners_information'
delete_pi = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'delete_{query_name}.sql'),
    )

dwh().pandas_to_dwh(
        dataframe=partners_information,
        schema_name='analyst_acquisition_cl',
        table_name='partners_information',
        if_exists='append'
    )

########################################################
######## Stage 3: Upload Partners Vouchers #############
########################################################

query_name = 'all_partners_vouchers'
partners_vouchers = dwh().dwh_to_pandas(
        filename=path.join('querys', 'server_querys', f'select_{query_name}.sql'),
        db_uri = 'postgresql+psycopg2://raimundo_cavada:Rcavada_2022@188.166.10.10:5432/sumup')

query_name = 'all_partners_vouchers'
delete_pi = dwh().dwh_to_pandas(
    filename=path.join('querys', 'server_querys', f'delete_{query_name}.sql'),
    )

dwh().pandas_to_dwh(
        dataframe=partners_vouchers,
        schema_name='analyst_acquisition_cl',
        table_name='partners_vouchers',
        if_exists='append'
    )