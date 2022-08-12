select
	convert(numeric,case order_id when '' then null else order_id end) as order_id,
	convert(numeric,case voucher_id when '' then null else voucher_id end) as voucher_id
from
	raw_adamant.order_voucher
