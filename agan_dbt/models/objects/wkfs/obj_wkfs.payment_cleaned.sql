select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case user_id when '' then null else user_id end) as user_id,
	convert(numeric, case payment_type_id when '' then null else payment_type_id end) as payment_type_id
from
	raw_wkfs.payment