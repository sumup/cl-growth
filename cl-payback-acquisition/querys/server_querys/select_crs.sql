select
	--trim(regexp_replace(v.code,'[^[:alpha:]\s]', '', 'g'))  as voucher_name
	--, sop.shipping_order_id as order_id
	so.payment_date as date
	--, so.reason as cr_shipped_reason
	, case 
		when v.code ilike 'CL_PARTNERS%%' then 'PARTNERS'
		when rs.is_influencer then 'RAF'
		when rs.referrer is not null then 'RAF'
		when v.code ilike 'CL_REF%%' then 'RAF'
		when (p2.name in ('Gifts_influencer','GIFTS_INFLUENCERSOLO')
			or p2.name in ('CL_Influencers') 
			or p2.name in ('CL_Influencers_Solo')) then 'RAF'
		when p2.name in ('_PHYSICAL_SALES_CL', 'PHYSICAL_SALES_CL_', 'PHYSICAL_SALES_CL','_PHYSICAL_SALES_CL_2','SOLO_PHYSICAL_CL_STORES') then 'PUNTO'
		when v.code in ('COMPRAS_RETAIL_CL', 'CL_RETAIL_COMPRAS_CL','CL_RETAIL_SOLO','RETAIL_AIR_CASAROYAL_CL','RETAIL_AIR_EASY_CL','RETAIL_SOLO_EASY_CL') then 'RETAIL'
		when v.code in ('CL_VAPS', 'VAPS_CL', 'MVPVAPSQ4') then 'VAP'
		when v.code in ('VENTAS_MASIVAS_CL','CL_PRODEMU') then 'OTHER'
		when v.code in ('CL_SMALLMX_FREE_AIR', 'CL_SMALLMX_FREE_SOLO','CL_INTERNALSALES_AIR', 'CL_INTERNALSALES_SOLO', 'CL_EXTERNALSALES_BST_AIR', 'CL_EXTERNALSALES_GYT_AIR', 'CL_EXTERNAL SALES_BST_SOLO', 'CL_EXTERNAL SALES_GYT_SOLO') THEN 'Small Mx'
		when v.code ilike  'CL_IS_SOLO_SRSOL%%' then 'Small Mx' 
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
	, so.price / so.amount_ordered as original_price
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
and p.title <> 'accessory.solo_printer'
and so.payment_date = '_start_date'
group by 1,2,3,5--,4,
order by date desc 