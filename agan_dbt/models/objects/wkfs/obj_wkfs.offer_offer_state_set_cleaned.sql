with results as (
select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case offer_id when '' then null else offer_id end) as offer_id,
	convert(numeric, case offer_state_id when '' then null else offer_state_id end) as offer_state_id,
	convert(numeric, case sf_guard_user_id when '' then null else sf_guard_user_id end) as sf_guard_user_id,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at
from
	{{ source("wkfs","offer_offer_state_set") }}
)

select * from results