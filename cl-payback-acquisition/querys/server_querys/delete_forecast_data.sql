delete from analyst_acquisition_cl.growth_forecast_data gfd
where gfd.date >= '_date'
returning *