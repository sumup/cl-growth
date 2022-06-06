select
gs."date" ,
gs.acq_channel_level_1,
'Partners' as acq_channel_level_2,
gs.cr_type,
gs.crs,
gs.ncro,
gs.weighted_price 
from analyst_acquisition_cl.growth_sales gs
where gs."date"  = '_date'
and gs.gs.acq_channel_level_1 = 'PARTNERS'