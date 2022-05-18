select
pi2.email,
pi2.registration_date as form_date,
(case when pi2.third_video_date is null then pi2.first_purchase_intent_date
else pi2.third_video_date
end ) as training_date,
'dashboard' as lead_source
from partners.partners_information pi2
where pi2.email not like '%%@sumup.com'
and pi2.email not like 'vicente.sandoval%%'
and pi2.email not like 'test%%'