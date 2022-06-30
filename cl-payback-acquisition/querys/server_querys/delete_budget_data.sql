delete from analyst_acquisition_cl.growth_acquisition_budget gab
where gab.date >= '_date'
returning *