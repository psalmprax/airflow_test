select
	convert(numeric, case id when '' then null else id end) as id,
	case
		"name" when '' then null
		else "name"
	end as "name",
	case
		"label" when '' then null
		else "label"
	end as "label",
	case
		check_return when '' then null
		else check_return
	end as check_return,
	convert(numeric, case field_type when '' then null else field_type end) as field_type,
	convert(numeric, case with_clarification_field when '' then null else with_clarification_field end) as with_clarification_field,
	convert(numeric, case with_defect_field when '' then null else with_defect_field end) as with_defect_field,
	convert(numeric, case with_value_field when '' then null else with_value_field end) as with_value_field,
	convert(numeric, case with_value_field_type when '' then null else with_value_field_type end) as with_value_field_type,
	case
		with_value_field_label when '' then null
		else with_value_field_label
	end as with_value_field_label,
	case
		description when '' then null
		else description
	end as description,
	case
		"data" when '' then null
		else "data"
	end as "data",
	convert(numeric, case has_confirmation when '' then null else has_confirmation end) as has_confirmation,
	convert(numeric, case is_required when '' then null else is_required end) as is_required,
	case
		default_value when '' then null
		else default_value
	end as default_value,
	convert(numeric, case ignore_default when '' then null else ignore_default end) as ignore_default
from
	raw_wkfs.product_check_field