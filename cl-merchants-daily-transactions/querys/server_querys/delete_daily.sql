delete from analyst_acquisition_cl.merchants_daily_transactions mdt 
where mdt.day >= timezone('UTC', timezone('America/Santiago', '_start_date'::timestamp))
and mdt.day < timezone('UTC', timezone('America/Santiago', '_end_date'::timestamp))
returning *