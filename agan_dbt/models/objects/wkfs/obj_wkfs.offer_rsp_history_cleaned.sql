with results as (
select
	convert(numeric, case offer_id when '' then null else offer_id end) as offer_id,
	convert(numeric, case identifier when '' then null else identifier end) as identifier,
	convert(decimal(15,2), case rsp when '' then null else rsp end) as rsp
from
	{{ source("wkfs","offer_rsp_history") }}
)

select * from results