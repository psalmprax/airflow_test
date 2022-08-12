select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case serie_id when '' then null else serie_id end) as serie_id,
	convert(numeric, case manufactor_id when '' then null else manufactor_id end) as manufactor_id,
	case
		model when '' then null
		else model
	end as model,
	case
		edition when '' then null
		else edition
	end as edition,
	case
		capacity when '' then null
		else capacity
	end as capacity,
	case
		cpu when '' then null
		else cpu
	end as cpu,
	case
		ram when '' then null
		else ram
	end as ram,
	case
		hdd when '' then null
		else hdd
	end as hdd,
	convert(numeric, case flag_dont_buy when '' then null else flag_dont_buy end) as flag_dont_buy,
	convert(numeric, case in_shop when '' then null else in_shop end) as in_shop,
	convert(numeric, case deleted when '' then null else deleted end) as deleted,
	convert(numeric, case published when '' then null else published end) as published,
	convert(numeric, case import_tac when '' then null else import_tac end) as import_tac,
	convert(numeric, case import_tac_overwrites_category when '' then null else import_tac_overwrites_category end) as import_tac_overwrites_category,
	case
		product_description when '' then null
		else product_description
	end as product_description,
	case
		technical_description when '' then null
		else technical_description
	end as technical_description,
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
		product_page_banner_url when '' then null
		else product_page_banner_url
	end as product_page_banner_url,
	case
		product_page_banner_link when '' then null
		else product_page_banner_link
	end as product_page_banner_link,
	case
		condition_popup_banner_url when '' then null
		else condition_popup_banner_url
	end as condition_popup_banner_url,
	case
		condition_popup_banner_link when '' then null
		else condition_popup_banner_link
	end as condition_popup_banner_link,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at,
	convert(numeric, case release_month when '' then null else release_month end) as release_month,
	convert(numeric, case release_year when '' then null else release_year end) as release_year,
	case
		image_path when '' then null
		else image_path
	end as image_path,
	convert(numeric, case hero_price when '' then null else hero_price end) as hero_price
from
	raw_wkfs.device