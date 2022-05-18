select pd.partner_id, min(pd.activation_date)
from analyst_acquisition_cl.partners_dashboard_prod pd
where pd.comercio = 'Nuevo'
group by 1