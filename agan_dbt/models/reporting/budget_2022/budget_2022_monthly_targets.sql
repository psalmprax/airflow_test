SELECT 
   end_of_month
   ,year_month
--   ,mm
--   ,rev_growth_yoy
   ,gross_margin
   ,gross_revenue
   ,gross_margin_pp
   ,stock_value
   ,sourcing_value
FROM {{ ref('monthly_targets') }}