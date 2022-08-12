with results as (
select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case transaction_id when '' then null else transaction_id end) as transaction_id,
	convert(numeric, case add_userfield_id when '' then null else add_userfield_id end) as add_userfield_id,
	case
		value when '' then null
		else value
	end as value
from
	{{ source("wkfs","add_userfield_value") }}
)

select * from results