delete from analyst_acquisition_cl.growth_forecast_data gfd
where gfd.date = '_date'
and gfd.acq_channel_level_1 = '_acq_channel_level_1'
returning *