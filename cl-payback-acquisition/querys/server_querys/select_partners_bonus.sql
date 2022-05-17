select pncro.*, nbt.cro_bonus 
from (select pdp.partner_id, count(distinct pdp.merchant_id) as ncro
from analyst_acquisition_cl.partners_dashboard_prod pdp
where pdp.comercio = 'Nuevo'
and pdp.activation_date between timezone('America/Santiago', date_trunc('month', now())::timestamp) and timezone('America/Santiago', now()::timestamp)
group by 1) as pncro
left join analyst_acquisition_cl.ncros_bonus_table nbt on nbt.quantity_ncro = pncro.ncro
