select
date::date as date,
channel_chile as acq_channel_level_2,
campaign as acq_channel_level_3,
sum(cost) as amount_spent,
sum(purchases_chile) as total_purchases
from "SHARED_FUNNEL_SUMUP__LM3JD3KWKTSKJUJGEZZ"."FUNNEL__LM3JD3KWKTSKJUJGEZZ"."ONLINE_DATA_CHILE"
where date = '_start_date'
group by 1,2,3