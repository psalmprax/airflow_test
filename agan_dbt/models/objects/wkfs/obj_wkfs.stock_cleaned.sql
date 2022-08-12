with results as (
select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case offer_id when '' then null else offer_id end) as offer_id,
	convert(timestamp, case stock_in when '' then null else stock_in end) as stock_in,
	convert(timestamp, case stock_out when '' then null else stock_out end) as stock_out,
	convert(numeric, case location_id when '' then null else location_id end) as location_id,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at
from
	{{ source("wkfs","stock") }}
)

select * from results