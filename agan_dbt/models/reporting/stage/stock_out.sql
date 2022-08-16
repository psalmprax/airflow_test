select distinct 
	date(ows.stock_out) as date
    ,EXTRACT(year from ows.stock_out) as year
    ,EXTRACT(MONTH FROM ows.stock_out) as month
--    ,concat(extract(year from ows.stock_out)::int,'-',to_char(ows.stock_out,'MM')) as year_month
    ,extract(year from ows.stock_out)::int||'-'||to_char(ows.stock_out,'MM') as year_month
--    ,concat(to_char(ows.stock_out,'MM'),'-',to_char(ows.stock_out,'DD')) as month_date
    ,to_char(ows.stock_out,'MM')||'-'||to_char(ows.stock_out,'DD') as month_date
    ,CASE
        WHEN owo.manufactor_name LIKE '%Apple%' THEN 'Apple'
        WHEN owo.manufactor_name LIKE '%MacBook%' THEN 'Apple'
        WHEN owo.manufactor_name LIKE '%Canon%' THEN 'Canon'
        WHEN owo.manufactor_name LIKE '%Google%' THEN 'Google'
        WHEN owo.manufactor_name LIKE '%Huawei%' THEN 'Huawei'
        WHEN owo.manufactor_name LIKE '%Microsoft%' THEN 'Microsoft'
        WHEN owo.manufactor_name LIKE '%Nikon%' THEN 'Nikon'
        WHEN owo.manufactor_name LIKE '%OnePlus%' THEN 'OnePlus'
        WHEN owo.manufactor_name LIKE '%Samsung%' THEN 'Samsung'
        WHEN owo.manufactor_name LIKE '%Sony%' THEN 'Sony'
        WHEN owo.manufactor_name LIKE '%Xiaomi%' THEN 'Xiaomi'
        ELSE 'other' END as brand
    ,CASE 
        WHEN owo.category IN (
            'Apple Displays', 'Apple MacBooks', 'Apple Macs',
            'Audio & HiFi', 'Blitzgeräte','Camcorder',
            'Digitalkameras','Grafikkarten','Handys','Konsolen',
            'Notebooks','Objektive','Smartwatches','Tablets','Virtual Reality','Zubehoer') THEN owo.category
        ELSE 'Other or new' END as category
    ,CASE 
        WHEN ows.stock_out_channel IN (
            'Amazon','B2B Verkauf','Back Market','Cdiscount','eBay','eBay_ES','eBay_FR','eBay_IT','eBay_Teildefekte','Fnac','Handydortmund','Idealo DE','Mitarbeiter_Verkauf','Notebook12','Rakuten FR','Rakuten GmbH','refurbed','Webshop','Webshop_ES','Webshop_FR','Webshop_IT','Wish') 
            THEN ows.stock_out_channel
        WHEN ows.stock_out_channel IS NULL THEN ows.stock_out_channel
        WHEN ows.stock_out_channel LIKE 'eprice' THEN 'Eprice (IT)'
        WHEN ows.stock_out_channel LIKE 'Rueducommerce' THEN 'RDC (FR)'
        ELSE 'Other Marketplaces: Amazon IT, Amazon FR, Amazon ES, LDLC, Lengow 1, Lengow 2, Lengow 3, Lengow 4' 
        END as channel
    ,owo.condition_id as condition
    ,sum(coalesce(ows.stock_out_price + ows.stock_out_shipping_costs,0.00)) stock_out_price_gross--over (partition by owo.category, ows.stock_out) stock_out_price_gross
    ,SUM(CASE WHEN owt.application_id IN (61, 62, 63, 64, 65)
        THEN ows.stock_out_price - owo.paid_amount + owo.promo_amount + ows.stock_out_shipping_costs
        ELSE ows.stock_out_price - owo.paid_amount + ows.stock_out_shipping_costs
    END) AS marge_2
FROM {{ ref('obj_wkfs_stock') }} AS ows
JOIN {{ ref('obj_wkfs_offer') }} AS owo
    ON ows.offer_id = owo.id
JOIN {{ ref('obj_wkfs_trade') }} AS owt
    ON owt.id = owo.trade_id
WHERE ows.stock_out > '2021-01-01'
--    AND rcosm.stock_out_channel not in (SELECT channels from {{ ref('dim_blacklist') }})
    AND ows.stock_out_channel not in (SELECT channels from {{ ref('dim_blacklist') }})
    AND ows.stock_out_channel is not null
GROUP BY 1,2,3,4,5,6,7,8,9