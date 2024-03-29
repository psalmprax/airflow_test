with results as (
select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case currency_id when '' then null else currency_id end) as currency_id,
	COALESCE(convert(decimal(15,2), case value when '' then null else value end),0.00) as value,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(numeric, case sf_guard_user_id when '' then null else sf_guard_user_id end) as sf_guard_user_id
from
	{{ source("wkfs","currency_rate") }}
)

select * from results
