with results as (
select
	convert(numeric, case id when '' then null else id end) as id,
	case
		article_number when '' then null
		else article_number
	end as article_number,
	COALESCE(convert(decimal(15,2), case rsp when '' then null else rsp end),0.00) as rsp,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at
from
	{{ source("wkfs","article_numbers_rsp_history") }}
)

select * from results