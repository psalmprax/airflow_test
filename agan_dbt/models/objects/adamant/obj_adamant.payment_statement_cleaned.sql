select
	convert(numeric,case id when '' then null else id end) as id,
	convert(numeric,case gateway_id when '' then null else gateway_id end) as gateway_id,
	case status when '' then null else status end as status,
	convert(decimal(15,2),case charge when '' then null else charge end) as charge,
	convert(decimal(15,2),case paid_amount when '' then null else paid_amount end) as paid_amount,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at, 
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at,
	case reference when '' then null else reference end as reference
from
	raw_adamant.payment