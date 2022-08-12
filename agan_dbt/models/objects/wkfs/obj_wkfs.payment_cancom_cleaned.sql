select
	convert(numeric, case payment_id when '' then null else payment_id end) as payment_id,
	case
		cancom_customer_number when '' then null
		else cancom_customer_number
	end as cancom_customer_number
from
	raw_wkfs.payment_cancom