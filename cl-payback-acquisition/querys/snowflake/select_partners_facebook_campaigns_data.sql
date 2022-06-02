select 
date,
CAMPAIGN_name__FACEBOOK_ADS as acq_channel_level_2,
sum(AMOUNT_SPENT__FACEBOOK_ADS) as amount_spent, 
sum(purchases__FACEBOOK_ADS) as purchases
from "SHARED_FUNNEL_SUMUP__LM3JD3KWKTSKJUJGEZZ"."FUNNEL__LM3JD3KWKTSKJUJGEZZ"."PARTNERS_FACEBOOK_CAMPAIGNS_CHILE"
where date between '_start_date' and '_end_date'
group by 1