select
	convert(numeric, case payment_id when '' then null else payment_id end) as payment_id,
	case
		paypal_id when '' then null
		else paypal_id
	end as paypal_id,
	case
		paypal_id_type when '' then null
		else paypal_id_type
	end as paypal_id_type,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at
from
	raw_wkfs.payment_paypal