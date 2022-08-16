with budget_table_category_targets as (	

    SELECT 	
        EXTRACT(YEAR FROM to_date(end_of_month,'DD/MM/YYYY')) as year	
        ,to_char(to_date(end_of_month,'DD/MM/YYYY'),'MM') as month	
        ,EXTRACT(YEAR FROM to_date(end_of_month,'DD/MM/YYYY'))::int || '-' || to_char(to_date(end_of_month,'DD/MM/YYYY'),'MM') as year_month	
        ,*	
    FROM analytics_reporting.budget_2022_category_targets
),
budget_table_channel_targets as (
    SELECT 
        EXTRACT(YEAR FROM to_date(end_of_month,'DD/MM/YYYY')) as year
        ,to_char(to_date(end_of_month,'DD/MM/YYYY'),'MM') as month
        ,EXTRACT(YEAR FROM to_date(end_of_month,'DD/MM/YYYY'))::int || '-' || to_char(to_date(end_of_month,'DD/MM/YYYY'),'MM') as year_month
        ,*
    FROM analytics_reporting.budget_2022_channel_targets
),
budget_table_brand_targets as (
    SELECT 
        EXTRACT(YEAR FROM to_date(end_of_month,'DD/MM/YYYY')) as year
        ,to_char(to_date(end_of_month,'DD/MM/YYYY'),'MM') as month
        ,EXTRACT(YEAR FROM to_date(end_of_month,'DD/MM/YYYY'))::int || '-' || to_char(to_date(end_of_month,'DD/MM/YYYY'),'MM') as year_month
        ,*
    FROM analytics_reporting.budget_2022_brand_targets
),

budget_table_condition_targets as (
    SELECT 
        EXTRACT(YEAR FROM to_date(end_of_month,'DD/MM/YYYY')) as year
        ,to_char(to_date(end_of_month,'DD/MM/YYYY'),'MM') as month
        ,EXTRACT(YEAR FROM to_date(end_of_month,'DD/MM/YYYY'))::int || '-' || to_char(to_date(end_of_month,'DD/MM/YYYY'),'MM') as year_month
        ,*
    FROM analytics_reporting.budget_2022_condition_targets
),
results as (
	select 
		year
		, month
		, year_month 
		, channel
		, '' as category 
		, '' as brand
		, cast('-10' as numeric) as condition 
		, end_of_month 
		, seasonality
		, target_revenue
		, target_revenue_growth
		, target_perc_gross_margin
		, target_gross_margin_eur 
		, target_share_channels
	from budget_table_channel_targets 
	union
	select 
		year
		, month
		, year_month 
		, '' as channel
		, category 
		, '' as brand
		, cast('-10' as numeric) as condition 
		, end_of_month 
		, seasonality
		, target_revenue
		, target_revenue_growth
		, target_perc_gross_margin
		, target_gross_margin_eur 
		, target_share_channels
	from budget_table_category_targets 
	union
	select 
		year
		, month
		, year_month 
		, '' as channel
		, '' as category 
		, brand
		, cast('-10' as numeric) as condition 
		, end_of_month 
		, seasonality
		, target_revenue
		, target_revenue_growth
		, target_perc_gross_margin
		, target_gross_margin_eur 
		, target_share_channels
	from budget_table_brand_targets 
	union
	select
		year
		, month
		, year_month
		, '' as channel
		, '' as category
		, '' as brand
		, condition
		, end_of_month
		, seasonality
		, target_revenue
		, target_revenue_growth
		, target_perc_gross_margin
		, target_gross_margin_eur
		, target_share_channels
	from budget_table_condition_targets

)

select * from results