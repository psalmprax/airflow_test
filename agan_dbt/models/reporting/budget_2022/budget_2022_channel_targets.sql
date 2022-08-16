SELECT 
  channel
  ,end_of_month
  ,seasonality
  ,target_revenue
  ,target_revenue_growth
  ,target_perc_gross_margin
  ,target_gross_margin_eur
  ,target_share_channels
FROM {{ ref('channel_targets') }}
