/* 

category,end_of_month,seasonality,target_revenue,target_revenue_growth,target_perc_gross_margin,target_gross_margin_eur,target_share_channels

*/

SELECT distinct
    category 
  , end_of_month 
  , seasonality 
  , target_revenue 
  , target_revenue_growth 
  , target_perc_gross_margin 
  , cast(coalesce(target_gross_margin_eur::float,null) AS float) AS target_gross_margin_eur
  , target_share_channels 
FROM {{ ref('category_targets') }}



