select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case business_channel_id when '' then null else business_channel_id end) as business_channel_id,
	convert(numeric, case offer_id when '' then null else offer_id end) as offer_id,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at
from
	raw_wkfs.offer_business_channel_history