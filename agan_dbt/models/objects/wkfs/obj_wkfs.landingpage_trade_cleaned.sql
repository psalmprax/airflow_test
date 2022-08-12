select
	convert(numeric, case trade_id when '' then null else trade_id end) as trade_id,
	convert(numeric, case landingpage_id when '' then null else landingpage_id end) as landingpage_id
from
	raw_wkfs.landingpage_trade