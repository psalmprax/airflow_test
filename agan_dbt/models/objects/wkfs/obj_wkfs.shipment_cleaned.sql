select
	convert(numeric, case id when '' then null else id end) as id,
	case
		code when '' then null
		else code
	end as code,
	convert(numeric, case carrier_id when '' then null else carrier_id end) as carrier_id,
	convert(numeric, case trade_id when '' then null else trade_id end) as trade_id,
	convert(numeric, case retoure_id when '' then null else retoure_id end) as retoure_id
from
	raw_wkfs.shipment