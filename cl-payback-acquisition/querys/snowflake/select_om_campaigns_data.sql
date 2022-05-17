select
case when weekiso(date::date)< 10 then concat(yearofweekiso(date::date),'-0',weekiso(date::date))
else
concat(yearofweekiso(date::date),'-',weekiso(date::date))
end as week_date,
channel_chile as om_channel,
campaign,
cost,
purchases_chile
from "SHARED_FUNNEL_SUMUP__LM3JD3KWKTSKJUJGEZZ"."FUNNEL__LM3JD3KWKTSKJUJGEZZ"."ONLINE_DATA_CHILE"