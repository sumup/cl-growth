select
    tx.id as transaction_id
    ,tx.merchant_id
    ,tx.server_time_created_at
    ,timezone('America/Santiago',timezone('UTC', tx.server_time_created_at)) as server_time_created_at_chile
    ,tx.lat
    ,tx.lon
    ,tx.amount
    ,tx.tx_result
    from transactions tx
    left join analyst_acquisition_cl.partners_dashboard_prod pdp on pdp.cr_transaction_id = tx.id 
    where pdp.cr_transaction_id is not null