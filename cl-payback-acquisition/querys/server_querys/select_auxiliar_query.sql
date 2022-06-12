select
ncro.card_reader_owner_date as date,
case
when ncro.channel in ('partners', 'partners_c', 'partners_rcp')then 'PARTNERS'
when ncro.channel in ('digital') then 'DIGITAL'
when ncro.channel in ('raf') then 'RAF'
when ncro.channel in ('raf_influencers') then 'RAF'
when ncro.channel in ('Punto Sumup') then 'PUNTO'
when ncro.channel in ('Retail','Distribuidor') then 'RETAIL'
when ncro.channel in ('Vaps') then 'VAP'
when ncro.channel in ('Ventas Masivas') then 'OTHER'
else 'OTHER'
end as acq_channel_level_1,
case
when ncro.channel in ('partners', 'partners_c', 'partners_rcp')then 'PARTNERS'
when ncro.channel in ('digital') then 'DIGITAL'
when ncro.channel in ('raf') then 'RAF'
when ncro.channel in ('raf_influencers') then 'RAF'
when ncro.channel in ('Punto Sumup') then 'PUNTO'
when ncro.channel in ('Retail','Distribuidor') then 'RETAIL'
when ncro.channel in ('Vaps') then 'VAP'
when ncro.channel in ('Ventas Masivas') then 'OTHER'
else 'OTHER'
end as acq_channel_level_2,
case
when (ncro.card_reader_type is null and ncro.punto_card_reader_type  is null) then 'AIR'
when (ncro.card_reader_type is null) then upper(ncro.punto_card_reader_type)
else upper(ncro.card_reader_type)
end as cr_type,
0 as crs_payback,
count(distinct dim_merchant_id) as ncro_payback,
0 as weighted_price_payback,
0 as crs,
count(distinct dim_merchant_id) as ncro,
0 as weighted_price,
0 as proportional_budget,
0 as avg_monthly_net_rev_ncro,
0 as crs_net_landed_cost
from analyst_acquisition_cl.new_card_readers_owners ncro
where ncro.card_reader_owner_date = '_start_date'
and ncro.channel not in ('First affiliate/influencer order', 'First partner orders', 'Welcome kits')
group by 1,2,3,4