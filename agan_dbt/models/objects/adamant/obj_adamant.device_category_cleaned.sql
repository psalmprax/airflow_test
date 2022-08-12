select
	convert(numeric,case device_id when '' then null else device_id end) as device_id,
	convert(numeric,case category_id when '' then null else category_id end) as category_id
from
	raw_adamant.device_category
