select
pdn.last_day as date,
'Partners' as acq_channel_level_1,
'Bonus' as acq_channel_level_2,
sum(pdn.bonus) as amount_spent
from analyst_acquisition_cl.partners_dashboard_new pdn
where pdn.last_day between '_start_date' and '_end_date'
group by 1,2,3