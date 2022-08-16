with left_map as (
	SELECT DISTINCT
	    d.year, d.month_name, d.month, d.year_month, d.days_in_month
	    ,d.week_days_in_month, d.week_day_num, d.week_day, d.month_date, d.date
	    ,db.condition
	    ,'' as channel
	    ,'' category
	    ,'' as brand
	    ,coalesce(bt.target_revenue,0.00) as budget_revenue_monthly
	    ,CASE WHEN d.week_day_num IN (0,6) THEN 0 ELSE bt.target_revenue / d.week_days_in_month END as budget_revenue_daily
	    ,coalesce(bt.target_gross_margin_eur,0.00) as budget_margin_monthly
	    ,CASE WHEN d.week_day_num IN (0,6) THEN 0 ELSE bt.target_gross_margin_eur / d.week_days_in_month END as budget_margin_daily
	FROM {{ ref('dim_date_series') }} d
	CROSS JOIN (SELECT condition FROM {{ ref('dim_budget_table') }}) as db
	LEFT JOIN {{ ref('dim_budget_table') }} bt
	    ON d.year_month = bt.year_month AND db.condition = bt.condition

	union all

	SELECT DISTINCT
	    d.year, d.month_name, d.month, d.year_month, d.days_in_month
	    ,d.week_days_in_month, d.week_day_num, d.week_day, d.month_date, d.date
	    ,-10 as condition
	    ,db.channel
	    ,'' category
	    ,'' as brand
	    ,coalesce(bt.target_revenue,0.00) as budget_revenue_monthly
	    ,CASE WHEN d.week_day_num IN (0,6) THEN 0 ELSE bt.target_revenue / d.week_days_in_month END as budget_revenue_daily
	    ,coalesce(bt.target_gross_margin_eur,0.00) as budget_margin_monthly
	    ,CASE WHEN d.week_day_num IN (0,6) THEN 0 ELSE  bt.target_gross_margin_eur / d.week_days_in_month END as budget_margin_daily
	FROM {{ ref('dim_date_series') }} d
	CROSS JOIN
	    (SELECT channel from {{ ref('dim_budget_table') }}) db
	LEFT JOIN {{ ref('dim_budget_table') }} bt
	   ON d.year_month = bt.year_month and db.channel = bt.channel

	union all

	SELECT DISTINCT
	    d.year, d.month_name, d.month, d.year_month, d.days_in_month
	    ,d.week_days_in_month, d.week_day_num, d.week_day, d.month_date, d.date
	    ,-10 as condition
	    ,'' as channel
	    ,db.category
	    ,'' as brand
	    ,coalesce(bt.target_revenue,0.00) as budget_revenue_monthly
	    ,CASE WHEN d.week_day_num IN (0,6) THEN 0 ELSE bt.target_revenue / d.week_days_in_month END as budget_revenue_daily
	    ,coalesce(bt.target_gross_margin_eur,0.00) as budget_margin_monthly
	    ,CASE WHEN d.week_day_num IN (0,6) THEN 0 ELSE bt.target_gross_margin_eur / d.week_days_in_month END as budget_margin_daily
	FROM {{ ref('dim_date_series') }} d
	CROSS JOIN
	    (SELECT category from {{ ref('dim_budget_table') }} ) db
	LEFT JOIN {{ ref('dim_budget_table') }} bt
	   ON d.year_month = bt.year_month and db.category = bt.category

	union all

	SELECT DISTINCT
	    d.year, d.month_name, d.month, d.year_month, d.days_in_month
	    ,d.week_days_in_month, d.week_day_num, d.week_day,d.month_date, d.date
	    ,-10 as condition
	    ,'' as channel
	    ,'' as category
	    ,db.brand
	    ,coalesce(bt.target_revenue,0.00) as budget_revenue_monthly
	    ,CASE WHEN d.week_day_num IN (0,6) THEN 0 ELSE bt.target_revenue / d.week_days_in_month END as budget_revenue_daily
	    ,coalesce(bt.target_gross_margin_eur,0.00) as budget_margin_monthly
	    ,CASE WHEN d.week_day_num IN (0,6) THEN 0 ELSE bt.target_gross_margin_eur / d.week_days_in_month END as budget_margin_daily
	FROM {{ ref('dim_date_series') }} d
	CROSS JOIN (SELECT brand FROM {{ ref('dim_budget_table') }}) db
	LEFT JOIN {{ ref('dim_budget_table') }} bt
	    ON d.year_month = bt.year_month AND db.brand = bt.brand
)

select * from left_map
