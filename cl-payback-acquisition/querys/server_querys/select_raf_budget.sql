/* ############################################################################## */
/* RaF Budget														 			  */
/* ############################################################################## */

with daily_raf_regular_rewards as (
	select
		sq1.paid_date as day
		,sum(sq1.reward) as raf_regular_rewards
	from (
		select
			ri.voucher
			,c.additional_data ->> 'paid_at' as paid_date
			,c.additional_data -> 'products' -> 0 ->> 'product_title' as product
			,case 
				when c.additional_data -> 'products' -> 0 ->> 'product_title' in ('card_reader.air', 'card_reader.air_bundle') then 'Air'
				when c.additional_data -> 'products' -> 0 ->> 'product_title' in ('card_reader.solo_bundle_cradle') then 'SOLO'
				when c.additional_data -> 'products' -> 0 ->> 'product_title' in ('card_reader.pin') then 'PIN'
				else null
				end as reader_type
			,c.additional_data ->> 'shop_order_id' as public_shipping_order_id
			,sou.order_status
			,c.additional_data ->> 'order_new_customer' as order_new_customer
			,u.external_user_id as referrer
			,c.referrer_user_id
			,u2.external_user_id as referral
			--,c.friend_user_id as referred_user_id
			,c.created_at
			,cast(r.amount as decimal)/100 as reward
			,ncro.card_reader_owner_date
			,ncro.crod_source
			,ncro.digital_voucher
			,ncro.salesforce_order_id
			,ncro.channel
		from referrals.conversions c
		left join referrals.users u on c.referrer_user_id = u.id 
		left join referrals.users u2 on c.friend_user_id = u2.id
		left join referrals.rewards r on r.conversion_id = c.id
		left join olap.v_m_dim_merchant vmdm on u2.external_user_id = vmdm.merchant_code
		left join analyst_acquisition_cl.new_card_readers_owners ncro on vmdm.dim_merchant_id = ncro.dim_merchant_id
		left join "external".shipping_orders_united sou on (c.additional_data ->> 'shop_order_id')::bigint = sou.public_shipping_order_id
		left join analyst_acquisition_cl.raf_influencers ri on u.external_user_id = ri.mx_code
		where u.country_id = 31 
		and c.conversion_type = 'PURCHASED'
		and r.reward_type = 'REGULAR'
		and ri.voucher is null
	) sq1
	group by 1	
)
, daily_raf_bonus_rewards as (
	select
		sq1.paid_date as day
		,sum(sq1.reward) as raf_bonus_rewards
	from (
		select
			ri.voucher
			,c.additional_data ->> 'paid_at' as paid_date
			,c.additional_data -> 'products' -> 0 ->> 'product_title' as product
			,case 
				when c.additional_data -> 'products' -> 0 ->> 'product_title' in ('card_reader.air', 'card_reader.air_bundle') then 'Air'
				when c.additional_data -> 'products' -> 0 ->> 'product_title' in ('card_reader.solo_bundle_cradle') then 'SOLO'
				when c.additional_data -> 'products' -> 0 ->> 'product_title' in ('card_reader.pin') then 'PIN'
				else null
				end as reader_type
			,c.additional_data ->> 'shop_order_id' as public_shipping_order_id
			,sou.order_status
			,c.additional_data ->> 'order_new_customer' as order_new_customer
			,u.external_user_id as referrer
			,c.referrer_user_id
			,u2.external_user_id as referral
			--,c.friend_user_id as referred_user_id
			,c.created_at
			,cast(r.amount as decimal)/100 as reward
			,ncro.card_reader_owner_date
			,ncro.crod_source
			,ncro.digital_voucher
			,ncro.salesforce_order_id
			,ncro.channel
		from referrals.conversions c
		left join referrals.users u on c.referrer_user_id = u.id 
		left join referrals.users u2 on c.friend_user_id = u2.id
		left join referrals.rewards r on r.conversion_id = c.id
		left join olap.v_m_dim_merchant vmdm on u2.external_user_id = vmdm.merchant_code
		left join analyst_acquisition_cl.new_card_readers_owners ncro on vmdm.dim_merchant_id = ncro.dim_merchant_id
		left join "external".shipping_orders_united sou on (c.additional_data ->> 'shop_order_id')::bigint = sou.public_shipping_order_id
		left join analyst_acquisition_cl.raf_influencers ri on u.external_user_id = ri.mx_code
		where u.country_id = 31 
		and c.conversion_type = 'PURCHASED'
		and r.reward_type = 'BONUS'
		and ri.voucher is null
	) sq1
	group by 1	
)
select
	drrr.day as date
	,drrr.raf_regular_rewards
	,drbr.raf_bonus_rewards
from daily_raf_regular_rewards drrr
left join daily_raf_bonus_rewards drbr on drrr.day = drbr.day
where drrr.day between '_start_date' and '_end_date'
order by drrr.day desc