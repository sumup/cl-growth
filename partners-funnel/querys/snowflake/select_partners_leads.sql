SELECT 
u.merchant_id as partner_id,
l.email as email,
l.created_date::date as form_date,
l.training_date_c as training_date,
l.lead_source as lead_source,
min(po.payment_date)::date as payment_date
from SRC_SALESFORCE.LEAD as l
left join "SUMUP_DWH_PROD"."SRC_PAYMENT"."USERS" as u on lower(u.email) = lower(l.email)
left join "SUMUP_DWH_PROD"."ANALYST_ACQUISITION_CL"."PARTNERS_ORDERS" as po on po.merchant_id = u.merchant_id
where l.LEADS_SOURCE_WEBSITE_C = 'PAR-es-CL'
and l.status != 'Rejected'
group by 1,2,3,4,5