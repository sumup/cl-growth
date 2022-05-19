select 
tx.updated_at as day,
tx.merchant_id,
tx.code as serial_number,
tx.card_reader_type,
tx.payment_type,
tx."name" as event_type, 
count(tx.tx_result = '11' or NULL) as total_succesfull_tx,
count(tx.tx_result = '20' or NULL) as total_failed_tx,
sum(case
	when tx.tx_result = '11' then tx.event_amount
	else 0
end) as total_amount_succesfull,
sum(case
	when tx.tx_result = '20' then tx.event_amount
	else 0
end) as total_amount_failed,
sum(case
	when tx.tx_result = '11' then tx.processing_fee
	else 0
end) as total_processing_fee,
sum(case
	when tx.tx_result = '11' then tx.acceleration_fee
	else 0
end) as total_acceleration_fee,
sum(case
	when tx.tx_result = '11' then tx.fixed_fee
	else 0
end) as total_fixed_fee,
sum(case
	when tx.tx_result = '11' then tx.total_fee
	else 0
end) as total_fee
from public.merchants m
, lateral (
select
t.merchant_id ,
case
	when tes.transaction_event_status_id in (10,11,15) 
then -1 * t.amount
else t.amount
end as event_amount,
t.tx_result ,
t.payment_type ,
case
	when tes.transaction_event_status_id in (10,11,15) 
then -1 * te.processing_fee 
else te.processing_fee
end as processing_fee,
case
	when tes.transaction_event_status_id in (10,11,15) 
then -1 * te.acceleration_fee  
else te.acceleration_fee
end as acceleration_fee,
case
	when tes.transaction_event_status_id in (10,11,15) 
then -1 * te.fixed_fee  
else te.fixed_fee
end as fixed_fee,
case
	when tes.transaction_event_status_id in (10,11,15) 
then -1 * t.total_fee
else t.total_fee
end as total_fee,
t.card_reader_id,
cr.code,
crt."name" as card_reader_type,
timezone('America/Santiago', timezone('UTC', tes.updated_at))::date as updated_at,
ts."name" 
from transactions t
left join card_readers cr on cr.id = t.card_reader_id
left join card_reader_types crt on crt.id = cr.card_reader_type_id
left join transaction_events te on te.transaction_id = t.id
left join transaction_event_states tes on tes.transaction_event_id  = te.id 
left join transaction_statuses ts on ts.id = tes.transaction_event_status_id 
where t.merchant_id = m.id
and tes.transaction_event_status_id in (11,15,17,102)
and tes.updated_at >= timezone('UTC', timezone('America/Santiago', '_start_date'::timestamp))
and tes.updated_at < timezone('UTC', timezone('America/Santiago', '_end_date'::timestamp))
and t.tx_result in ('11', '20')
) tx
where m.country_id = 50
group by 1,2,3,4,5,6