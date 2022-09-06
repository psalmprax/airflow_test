select
	convert(numeric, case product_check_id when '' then null else product_check_id end) as product_check_id,
	convert(numeric, case question_id when '' then null else question_id end) as question_id,
	convert(numeric, case clarify when '' then null else clarify end) as clarify,
	convert(numeric, case question_answer_id when '' then null else question_answer_id end) as question_answer_id,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at
from
    {{ source("wkfs","product_check_questionair")}}

--	raw_wkfs.product_check_questionair