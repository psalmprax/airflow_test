select
	convert(numeric, case offer_id when '' then null else offer_id end) as offer_id,
	convert(numeric, case business_channel_id when '' then null else business_channel_id end) as business_channel_id,
	convert(numeric, case is_original_business_channel when '' then null else is_original_business_channel end) as is_original_business_channel
from
	raw_wkfs.business_channel_offer