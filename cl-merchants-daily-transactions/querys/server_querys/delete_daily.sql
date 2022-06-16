delete from analyst_acquisition_cl.merchants_daily_transactions mdt 
where mdt.day = '_day'
returning *