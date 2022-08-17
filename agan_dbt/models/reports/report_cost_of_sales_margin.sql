WITH original AS
(
SELECT distinct
    ows.id
    , owo.id AS offer_id
    , CASE WHEN ows.stock_out_price > 0
           AND owo.state = 'paid'
           AND ows.stock_out_channel NOT IN ('RE_Kanal', 'other', 'Notebook12')
           AND ows.stock_out_info is NOT null
           AND ows.stock_out_info != ''
           THEN owo.id || '-' || ows.stock_out_info
      END AS ta_ls
    , owo.category
    , owo.device
    , owo.device_id
    , owo.article_number
    , ows.stock_out AS stock_out_at
    , owo.date_paid
    , CASE WHEN owo.converted_amount > 0
           THEN owo.converted_amount
           ELSE owo.accepted_amount
      END AS accepted_amount
    , REPLACE(ROUND(owo.accepted_amount_original, 2) || ' ' || owt.original_currency_unit, '.', ',') AS accepted_amount_original
    , owo.recommended_sales_price
    , CASE WHEN owo.recommended_sales_price > 0
           THEN REPLACE(ROUND(owo.recommended_sales_price, 2) || ' EUR', '.', ',')
      END AS recommended_sales_price_original
    , owo.manufactor_name
    , ows.stock_out_price
    , CASE WHEN ows.stock_out_price > 0
           THEN REPLACE(ROUND(ows.stock_out_price, 2) || ' EUR', '.', ',')
      END AS stock_out_price_original
    , ows.stock_out_channel
    , pc.value AS condition_id
    , owo.application
    , owt.landingpage
    , owu.lastname
    , owo.latest_stock_out
    , ows.stock_out_info
    , owo.offer_state
    , ows.stock_out_shipping_costs
    , CASE WHEN ows.stock_out_shipping_costs > 0
           THEN REPLACE(ROUND(ows.stock_out_shipping_costs, 2) || ' EUR', '.', ',')
      END AS stock_out_shipping_costs_original
    , owo.state
    , ows.color
    , owt.b2b_order_id
    , CASE WHEN owo.paid_amount_original > 0
           THEN REPLACE(TRIM(ROUND(owo.paid_amount_original, 2) || ' ' || owt.original_currency_unit), '.', ',')
      END AS paid_amount_original
    , owo.tax
    , owo.promo_amount
    , owo.paid_amount
    , CASE WHEN owt.application_id IN (61, 62, 63, 64, 65)
           THEN owo.paid_amount - COALESCE(owo.promo_amount, 0)
           ELSE owo.paid_amount
      END AS paid_amount_2
    , ows.stock_out_price - owo.paid_amount + ows.stock_out_shipping_costs AS marge
    , CASE WHEN owt.application_id IN (61, 62, 63, 64, 65)
           THEN ows.stock_out_price - owo.paid_amount + owo.promo_amount + ows.stock_out_shipping_costs
           ELSE ows.stock_out_price - owo.paid_amount + ows.stock_out_shipping_costs
      END AS marge_2
--    , TO_CHAR(ows.stock_out - ows.stock_in, 'dd')::BIGINT AS "stockrange"
    , datediff('day',ows.stock_in,ows.stock_out)::BIGINT AS "stockrange"

    , EXTRACT(WEEK FROM ows.stock_out) AS stock_out_week
    , EXTRACT(MONTH FROM ows.stock_out) AS stock_out_month
    , EXTRACT(YEAR FROM ows.stock_out) AS stock_out_year
    , TO_CHAR(ows.stock_out, 'YYYY-MM-DD') AS stock_out_date
    , COALESCE(ROUND((ows.stock_out_price + ows.stock_out_shipping_costs) / (1 + owo.tax), 2)) AS stock_out_price_net
    , COALESCE(ROUND((ows.stock_out_price - owo.paid_amount + ows.stock_out_shipping_costs) / (1 + owo.tax), 2)) AS marge_net
    , CASE WHEN owt.application_id IN (61, 62, 63, 64, 65)
           THEN COALESCE(ROUND((ows.stock_out_price - owo.paid_amount + owo.promo_amount + ows.stock_out_shipping_costs) / (1 + owo.tax), 2))
           ELSE COALESCE(ROUND((ows.stock_out_price - owo.paid_amount + ows.stock_out_shipping_costs) / (1 + owo.tax), 2))
      END AS marge_net_2
    , CASE WHEN ows.stock_out_price - owo.paid_amount + ows.stock_out_shipping_costs / (1 + owo.tax) > 0
           AND ows.stock_out_price + ows.stock_out_shipping_costs / (1 + owo.tax) > 0
           THEN ROUND(((ows.stock_out_price - owo.paid_amount + ows.stock_out_shipping_costs / (1 + owo.tax)) /
                (ows.stock_out_price + ows.stock_out_shipping_costs / (1 + owo.tax))) * 100, 2)
      END AS marge_net_in_percent
    , CASE WHEN ows.stock_out_price - owo.paid_amount + ows.stock_out_shipping_costs / (1 + owo.tax) > 0
           AND ows.stock_out_price + ows.stock_out_shipping_costs / (1 + owo.tax) > 0
           THEN
                CASE WHEN owt.application_id IN (61, 62, 63, 64, 65)
                     THEN ROUND(((ows.stock_out_price - owo.paid_amount + owo.promo_amount + ows.stock_out_shipping_costs / (1 + owo.tax)) /
                          (ows.stock_out_price + ows.stock_out_shipping_costs / (1 + owo.tax))) * 100, 2)
                     ELSE ROUND(((ows.stock_out_price - owo.paid_amount + ows.stock_out_shipping_costs / (1 + owo.tax)) /
                          (ows.stock_out_price + ows.stock_out_shipping_costs / (1 + owo.tax))) * 100, 2)
                END
      END AS marge_net2_in_percent
    , ows.stock_out_price + ows.stock_out_shipping_costs AS stock_out_price_gross
FROM {{ ref('obj_wkfs_stock') }} AS ows
JOIN {{ ref('obj_wkfs_offer') }} AS owo
    ON ows.offer_id = owo.id
JOIN {{ ref('obj_wkfs_trade') }} AS owt
    ON owt.id = owo.trade_id
JOIN {{ ref('obj_wkfs_user') }} AS owu
    ON owu.id = owt.user_id
JOIN {{ ref('obj_wkfs_product_check_history_condition')}} AS pc
    ON ows.id = pc.stock_id
WHERE ows.stock_out IS NOT NULL
)

SELECT *,
CASE
  WHEN category = 'Apple Displays' THEN 'Apple Displays'
  WHEN category = 'Apple MacBooks' THEN 'Apple MacBooks'
  WHEN category = 'Apple Macs' THEN 'Apple Macs'
  WHEN category = 'Audio & HiFi' THEN 'Audio & HiFi'
  WHEN category = 'Blitzgeräte' THEN 'Blitzgeräte'
  WHEN category = 'Camcorder' THEN 'Camcorder'
  WHEN category = 'Digitalkameras' THEN 'Digitalkameras'
  WHEN category = 'Grafikkarten' THEN 'Grafikkarten'
  WHEN category = 'Handys' THEN 'Handys'
  WHEN category = 'Konsolen' THEN 'Konsolen'
  WHEN category = 'Notebooks' THEN 'Notebooks'
  WHEN category = 'Objektive' THEN 'Objektive'
  WHEN category = 'Smartwatches' THEN 'Smartwatches'
  WHEN category = 'Tablets' THEN 'Tablets'
  WHEN category = 'Virtual Reality' THEN 'Virtual Reality'
  WHEN category = 'Zubehoer' THEN 'Zubehoer'
  ELSE 'Other or new'
END AS category_shortlist
,

CASE
  WHEN stock_out_channel = null THEN null
  WHEN stock_out_channel = 'Amazon' THEN 'Amazon'
  WHEN stock_out_channel = 'B2B Verkauf' THEN 'B2B Verkauf'
  WHEN stock_out_channel = 'Back Market' THEN 'Back Market'
  WHEN stock_out_channel = 'Cdiscount' THEN 'Cdiscount'
  WHEN stock_out_channel = 'eBay' THEN 'eBay'
  WHEN stock_out_channel = 'eBay_ES' THEN 'eBay_ES'
  WHEN stock_out_channel = 'eBay_FR' THEN 'eBay_FR'
  WHEN stock_out_channel = 'eBay_IT' THEN 'eBay_IT'
  WHEN stock_out_channel = 'eBay_Teildefekte' THEN 'eBay_Teildefekte'
  WHEN stock_out_channel = 'Fnac' THEN 'Fnac'
  WHEN stock_out_channel = 'Handydortmund' THEN 'Handydortmund'
  WHEN stock_out_channel = 'Idealo DE' THEN 'Idealo DE'
  WHEN stock_out_channel = 'Mitarbeiter_Verkauf' THEN 'Mitarbeiter_Verkauf'
  WHEN stock_out_channel = 'Notebook12' THEN 'Notebook12'
  WHEN stock_out_channel = 'Rakuten FR' THEN 'Rakuten FR'
  WHEN stock_out_channel = 'Rakuten GmbH' THEN 'Rakuten GmbH'
  WHEN stock_out_channel = 'refurbed' THEN 'refurbed'
  WHEN stock_out_channel = 'Webshop' THEN 'Webshop'
  WHEN stock_out_channel = 'Webshop_ES' THEN 'Webshop_ES'
  WHEN stock_out_channel = 'Webshop_FR' THEN 'Webshop_FR'
  WHEN stock_out_channel = 'Webshop_IT' THEN 'Webshop_IT'
  WHEN stock_out_channel = 'Wish' THEN 'Wish'
  WHEN stock_out_channel = 'eprice' THEN 'Eprice (IT'
  WHEN stock_out_channel = 'Rueducommerce' THEN 'RDC (FR'
  ELSE 'Other Marketplaces: Amazon IT, Amazon FR, Amazon ES, LDLC, Lengow 1, Lengow 2, Lengow 3, Lengow 4'
END AS channel_shortlist
,

CASE
  WHEN manufactor_name = 'Apple' THEN 'Apple'
  WHEN manufactor_name = 'MacBook' THEN 'Apple'
  WHEN manufactor_name = 'Canon' THEN 'Canon'
  WHEN manufactor_name = 'Google' THEN 'Google'
  WHEN manufactor_name = 'Huawei' THEN 'Huawei'
  WHEN manufactor_name = 'Microsoft' THEN 'Microsoft'
  WHEN manufactor_name = 'Nikon' THEN 'Nikon'
  WHEN manufactor_name = 'OnePlus' THEN 'OnePlus'
  WHEN manufactor_name = 'Samsung' THEN 'Samsung'
  WHEN manufactor_name = 'Sony' THEN 'Sony'
  WHEN manufactor_name = 'Xiaomi' THEN 'Xiaomi'
  ELSE 'other'
END AS brand_shortlist

FROM  original