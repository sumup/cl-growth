select
MONTH__GOOGLE_SHEETS as month,
CHANNEL__GOOGLE_SHEETS as acq_channel_level_1,
value__GOOGLE_SHEETS as amount_spent
from "SHARED_FUNNEL_SUMUP__LM3JD3KWKTSKJUJGEZZ"."FUNNEL__LM3JD3KWKTSKJUJGEZZ"."CL_UNITS_ECONOMICS"
where MONTH__GOOGLE_SHEETS = '_date'
and METRIC__GOOGLE_SHEETS = 'Budget'