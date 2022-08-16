SELECT DISTINCT
    wosl.id
    , wosl.category
    , wosl.brand
    , wosl.condition
    , wosl.device
    , wostlst.stock
    , wostlst.ek
    , wostlst.offers_stock_time
    , woslclsd.offers_created_last_seven_days
    , woslclfd.offers_created_last_fourteen_days
    , woslslsd.offers_sold_last_seven_days
    , woslslfd.offers_sold_last_fourteen_days
    , woslsisd.offers_stock_in_seven_days
    , woslsifd.offers_stock_in_fourteen_days
FROM {{ ref('wkfs_offer_stock_list') }} AS wosl
    LEFT JOIN {{ ref('wkfs_offer_stock_list_created_last_seven_days') }} AS woslclsd
        ON wosl.id = woslclsd.id
    LEFT JOIN {{ ref('wkfs_offer_stock_list_created_last_fourteen_days') }} AS woslclfd
        ON wosl.id = woslclfd.id
    LEFT JOIN {{ ref('wkfs_offer_stock_list_sales_last_seven_days') }} AS woslslsd
        ON wosl.id = woslslsd.id
    LEFT JOIN {{ ref('wkfs_offer_stock_list_sales_last_fourteen_days') }} AS woslslfd
        ON wosl.id = woslslfd.id
    LEFT JOIN {{ ref('wkfs_offer_stock_list_stock_in_seven_days') }} AS woslsisd
        ON wosl.id = woslsisd.id
    LEFT JOIN {{ ref('wkfs_offer_stock_list_stock_in_fourteen_days') }} AS woslsifd
        ON wosl.id = woslsifd.id
    LEFT JOIN {{ ref('wkfs_offer_stock_list_stock_time') }} AS wostlst
        ON wosl.id = wostlst.id
WHERE wostlst.stock IS NOT NULL
    AND woslclsd.offers_created_last_seven_days IS NOT NULL
    AND woslclfd.offers_created_last_fourteen_days IS NOT NULL
    AND woslslsd.offers_sold_last_seven_days IS NOT NULL
    AND woslslfd.offers_sold_last_fourteen_days IS NOT NULL
    AND woslsisd.offers_stock_in_seven_days IS NOT NULL
    AND woslsifd.offers_stock_in_fourteen_days IS NOT NULL

