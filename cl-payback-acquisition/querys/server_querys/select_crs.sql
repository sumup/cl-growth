select
	--trim(regexp_replace(v.code,'[^[:alpha:]\s]', '', 'g'))  as voucher_name
	--, sop.shipping_order_id as order_id
	to_char(so.payment_date, 'IYYY-IW') as week_date
	--, so.reason as cr_shipped_reason
	, case 
		when v.code ilike 'CL_PARTNERS%%' then 'PARTNERS'
		when rs.is_influencer then 'RAF-I'
		when rs.referrer is not null then 'RAF'
		when v.code ilike 'CL_REF%%' then 'RAF'
		when (p2.name in ('Gifts_influencer','GIFTS_INFLUENCERSOLO')
			or p2.name in ('CL_Influencers') 
			or p2.name in ('CL_Influencers_Solo')) then 'RAF-I'
		when v.code in ('AIR_PHYSICAL_CL', 'BUNDLE_PHYSICAL_CL', 'SOLO_PHYSICAL_CL') then 'PUNTO'
		when v.code in ('COMPRAS_RETAIL_CL', 'CL_RETAIL_COMPRAS_CL','CL_RETAIL_SOLO') then 'RETAIL'
		when v.code in ('CL_VAPS', 'VAPS_CL', 'MVPVAPSQ4') then 'VAP'
		when v.code in ('VENTAS_MASIVAS_CL','CL_PRODEMU') then 'MASSIVE SALES'
		when v.code is null then 'DIGITAL'
		else 'OTHER'
		end as channel
	, case 
		when p.title in ('card_reader.solo_bundle_cradle') then 'SOLO'
		when p.title in ('accessory.air_cradle') then 'CRADLE'
		when p.title in ('card_reader.air_bundle') then 'AIR'
		else 'AIR' 
		end as cr_type
	, sum(sop.quantity) as qty
	, sop.original_price 
from public.shipping_orders so
left join "external".shipping_orders_united sou on sou.public_shipping_order_id = so.id and sou.order_status = 'PAID'
left join public.shipping_orders_products sop on sop.shipping_order_id = so.id 
left join public.products p on p.id = sop.product_id 
left join public.vouchers v on v.id = so.voucher_id 
left join public.promotions p2 on p2.id= v.promotion_id
left join analyst_acquisition_cl.raf_sales rs on rs.referral_mx_id = so.merchant_id and sou.updated_at  = rs.paid_date::date
where so.country_id = 50
and so.status = 'PAID'
and so.reason in ('customer_requested','partner_requested')
and sop.quantity > 0
and p.title <> 'accessory.air_cradle'
group by 1,2,3,5--,4,
order by week_date desc 