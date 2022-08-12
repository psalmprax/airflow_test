select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case manufactor_id when '' then null else manufactor_id end) as manufactor_id,
	case
		"name" when '' then null
		else "name"
	end as "name",
	case
		image_path when '' then null
		else image_path
	end as image_path,
	case
		title when '' then null
		else title
	end as title,
	case
		meta_description when '' then null
		else meta_description
	end as meta_description,
	case
		meta_keywords when '' then null
		else meta_keywords
	end as meta_keywords,
	case
		"content" when '' then null
		else "content"
	end as "content"
from
	raw_wkfs.serie