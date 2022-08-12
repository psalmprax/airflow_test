select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case questionnaire_id when '' then null else questionnaire_id end) as questionnaire_id,
	case
		"name" when '' then null
		else "name"
	end as "name",
	case
		"text" when '' then null
		else "text"
	end as "text",
	case
		title when '' then null
		else title
	end as title,
	case
		meta_description when '' then null
		else meta_description
	end as meta_description,
	case
		"content" when '' then null
		else "content"
	end as "content",
	case
		meta_keywords when '' then null
		else meta_keywords
	end as meta_keywords,
	convert(numeric, case import_tac when '' then null else import_tac end) as import_tac,
	case
		image_path when '' then null
		else image_path
	end as image_path,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at
from
	raw_wkfs.category