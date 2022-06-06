select
month__google_sheets as month,
channel__google_sheets as acq_channel_level_1,
metric__google_sheets as metric,
value__google_sheets as value
from "SHARED_FUNNEL_SUMUP__LM3JD3KWKTSKJUJGEZZ"."FUNNEL__LM3JD3KWKTSKJUJGEZZ"."CL_UNITS_ECONOMICS"
where month__google_sheets = '_month'
and metric__google_sheets != 'Budget'
order by metric__google_sheets