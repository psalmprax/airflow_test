select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case category_id when '' then null else category_id end) as category_id,
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
		"content" when '' then null
		else "content"
	end as "content",
	case
		meta_keywords when '' then null
		else meta_keywords
	end as meta_keywords,
	convert(numeric, case in_shop when '' then null else in_shop end) as in_shop,
	convert(numeric, case real_manufactor_id when '' then null else real_manufactor_id end) as real_manufactor_id,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at
from
	raw_wkfs.manufactor