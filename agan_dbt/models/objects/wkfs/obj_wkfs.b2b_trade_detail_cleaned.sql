with results as (
select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case trade_id when '' then null else trade_id end) as trade_id,
	case
		tax_id when '' then null
		else tax_id
	end as tax_id,
	case
		order_number when '' then null
		else order_number
	end as order_number,
	case
		required_accessories when '' then null
		else required_accessories
	end as required_accessories,
	case
		"comment" when '' then null
		else "comment"
	end as "comment",
	convert(numeric, case with_taxes when '' then null else with_taxes end) as with_taxes,
	convert(timestamp, case delivery_date when '' then null else delivery_date end) as delivery_date
from
	{{ source("wkfs","b2b_trade_detail") }}
)

select * from results