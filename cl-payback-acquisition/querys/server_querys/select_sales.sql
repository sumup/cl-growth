select
gs."date" ,
gs.acq_channel_level_1,
gs.acq_channel_level_2,
gs.cr_type,
gs.crs,
gs.ncro,
gs.weighted_price 
from analyst_acquisition_cl.growth_sales gs
where gs."date"  = '_date'