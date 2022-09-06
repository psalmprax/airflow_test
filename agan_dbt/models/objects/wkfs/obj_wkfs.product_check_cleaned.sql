select
	convert(numeric, case id when '' then null else id end) as id,
	convert(numeric, case stock_id when '' then null else stock_id end) as stock_id,
	convert(timestamp, case checked when '' then null else checked end) as checked,
	case
		state when '' then null
		else state
	end as state,
	convert(numeric, case sf_guard_user_id when '' then null else sf_guard_user_id end) as sf_guard_user_id,
	case
		serial when '' then null
		else serial
	end as serial,
	convert(numeric, case ovp when '' then null else ovp end) as ovp,
	case
		color when '' then null
		else color
	end as color,
	convert(numeric, case color_id when '' then null else color_id end) as color_id,
	case
		additional_accessories when '' then null
		else additional_accessories
	end as additional_accessories,
	convert(numeric, case "time" when '' then null else "time" end) as "time",
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at,
	case
		imei when '' then null
		else imei
	end as imei,
	case
		box when '' then null
		else box
	end as box,
	convert(timestamp, case warranty when '' then null else warranty end) as warranty,
	convert(numeric, case costs_estimate when '' then null else costs_estimate end) as costs_estimate,
	case
		video when '' then null
		else video
	end as video,
	case
		optical_state when '' then null
		else optical_state
	end as optical_state,
	convert(numeric, case ebay_product_id when '' then null else ebay_product_id end) as ebay_product_id,
	convert(numeric, case quality_control when '' then null else quality_control end) as quality_control,
	convert(numeric, case costs_estimate_optical when '' then null else costs_estimate_optical end) as costs_estimate_optical
from
    {{ source("wkfs","product_check")}}

--	raw_wkfs.product_check