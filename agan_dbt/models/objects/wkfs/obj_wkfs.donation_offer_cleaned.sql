select
	convert(numeric, case offer_id when '' then null else offer_id end) as offer_id,
	convert(numeric, case donation_project_id when '' then null else donation_project_id end) as donation_project_id,
	convert(decimal(15,2), case amount when '' then 0.00 else amount end) as amount,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at
from
	{{ source("wkfs","donation_offer") }}

--	raw_wkfs.donation_offer