select
ncro.card_reader_owner_date as date,
case
when ncro.channel in ('partners', 'partners_rcp', 'partner_r') then 'PARTNERS'
when ncro.channel in ('digital') then 'DIGITAL'
when ncro.channel in ('raf') then 'RAF'
when ncro.channel in ('raf_influencers') then 'RAF'
when ncro.channel in ('Punto Sumup') then 'PUNTO'
when ncro.channel in ('Retail','Distribuidor') then 'RETAIL'
when ncro.channel in ('Vaps') then 'VAP'
when ncro.channel in ('Ventas Masivas') then 'OTHER'
else 'OTHER'
end as channel,
case
when (ncro.card_reader_type is null and ncro.punto_card_reader_type  is null) then 'AIR'
when (ncro.card_reader_type is null) then upper(ncro.punto_card_reader_type)
else upper(ncro.card_reader_type)
end as cr_type,
count(distinct dim_merchant_id) as qty_ncro
from analyst_acquisition_cl.new_card_readers_owners ncro
where ncro.card_reader_owner_date = '_start_date'
group by 1,2,3