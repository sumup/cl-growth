select llu.shipping_date
from analyst_acquisition_cl.logistic_last_update llu
order by llu.uploaded_at desc
limit 1