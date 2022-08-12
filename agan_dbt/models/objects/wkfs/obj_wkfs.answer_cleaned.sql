with results as (
select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case offer_id when '' then null else offer_id end) as offer_id,
	convert(numeric, case question_id when '' then null else question_id end) as question_id,
	convert(numeric, case question_answer_id when '' then null else question_answer_id end) as question_answer_id,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at
from
	{{ source("wkfs","answer") }}
)

select * from results