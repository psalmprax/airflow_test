with results as (
select
	convert(numeric, case offer_id when '' then null else offer_id end) as offer_id,
	convert(numeric, case advertising_channel_id when '' then null else advertising_channel_id end) as advertising_channel_id,
	convert(numeric, case id when '' then null else id end) as id
from
	{{ source("wkfs","advertising_offer") }}
)

select * from results