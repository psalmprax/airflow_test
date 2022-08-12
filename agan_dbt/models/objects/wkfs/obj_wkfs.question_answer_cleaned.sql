select
	convert(numeric, case id when '' then null else id end) as id,
	case
		answer_code when '' then null
		else answer_code
	end as answer_code,
	case
		answer_text when '' then null
		else answer_text
	end as answer_text,
	convert(numeric, case answer_is_title when '' then null else answer_is_title end) as answer_is_title,
	convert(numeric, case visible_in_frontend when '' then null else visible_in_frontend end) as visible_in_frontend
from
	raw_wkfs.question_answer