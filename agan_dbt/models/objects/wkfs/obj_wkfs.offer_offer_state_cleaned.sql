select
	convert(numeric, case offer_id when '' then null else offer_id end) as offer_id,
	convert(numeric, case offer_state_id when '' then null else offer_state_id end) as offer_state_id
from
	raw_wkfs.offer_offer_state