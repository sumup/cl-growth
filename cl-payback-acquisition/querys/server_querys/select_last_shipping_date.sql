select st.shipping_date 
from "external".shipping_tracking st 
where st.processor in ('CHILE','chile')
and st.shipping_date <= now()
order by st.shipping_date desc 
limit 1