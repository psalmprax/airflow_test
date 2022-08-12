select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case product_checks_id_in when '' then null else product_checks_id_in end) as product_checks_id_in,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at,
	convert(numeric, case product_checks_id_out when '' then null else product_checks_id_out end) as product_checks_id_out
from
	raw_wkfs.check_result_action