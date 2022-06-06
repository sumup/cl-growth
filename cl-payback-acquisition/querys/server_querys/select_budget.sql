select 
gab."date",
gab.acq_channel_level_1,
gab.acq_channel_level_2,
sum(gab.amount_spent)
from analyst_acquisition_cl.growth_acquisition_budget gab
where gab."date"  = '_date'
group by 1,2,3