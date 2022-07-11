select gfd."date",
gfd.acq_channel_level_1,
gab.acq_channel_level_2,
gfd.cr_type,
gfd.crs_payback,
gfd.ncro_payback,
gfd.weighted_price_payback,
gfd.crs,
gfd.ncro,
gfd.weighted_price, 
sum(gab.amount_spent) as proportional_budget,
gfd.avg_monthly_net_rev_ncro,
gfd.crs_net_landed_cost
from analyst_acquisition_cl.growth_forecast_data gfd
left join analyst_acquisition_cl.growth_acquisition_budget gab on gab."date" = gfd."date" and gab.acq_channel_level_1 = gfd.acq_channel_level_1 
group by 1,2,3,4,5,6,7,8,9,10,12,13
having sum(gfd.ncro_payback)  = 0
and sum(gab.amount_spent) is not null
and sum(gfd.crs_payback) = 0
and gfd.date = '_date'