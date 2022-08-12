select
	convert(numeric, case id when '' then null else id end) as id,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at,
	convert(numeric, case created_by when '' then null else created_by end) as created_by,
	convert(numeric, case modified_by when '' then null else modified_by end) as modified_by,
	convert(numeric, case application_id when '' then null else application_id end) as application_id,
	case
		"name" when '' then null
		else "name"
	end as "name",
	case
		image_alt when '' then null
		else image_alt
	end as image_alt,
	case
		image_link when '' then null
		else image_link
	end as image_link,
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
	end as "content",
	convert(numeric, case searchbox when '' then null else searchbox end) as searchbox,
	convert(numeric, case productbox when '' then null else productbox end) as productbox,
	convert(numeric, case pressbox when '' then null else pressbox end) as pressbox,
	convert(numeric, case promotion when '' then null else promotion end) as promotion,
	convert(numeric, case show_tax when '' then null else show_tax end) as show_tax,
	convert(numeric, case overwrite_application_donation when '' then null else overwrite_application_donation end) as overwrite_application_donation,
	convert(numeric, case overwrite_application_payment when '' then null else overwrite_application_payment end) as overwrite_application_payment,
	case
		ga_id when '' then null
		else ga_id
	end as ga_id,
	convert(numeric, case optin when '' then null else optin end) as optin,
	convert(numeric, case show_slider when '' then null else show_slider end) as show_slider,
	convert(numeric, case show_latest_purchases when '' then null else show_latest_purchases end) as show_latest_purchases,
	convert(numeric, case fastlane_trades when '' then null else fastlane_trades end) as fastlane_trades,
	convert(numeric, case skippable_clarification when '' then null else skippable_clarification end) as skippable_clarification,
	convert(numeric, case force_adress_input when '' then null else force_adress_input end) as force_adress_input,
	convert(numeric, case overwrite_application_mail_activation when '' then null else overwrite_application_mail_activation end) as overwrite_application_mail_activation,
	convert(numeric, case permanent_payment_type_display when '' then null else permanent_payment_type_display end) as permanent_payment_type_display,
	case
		max_cart_value_error when '' then null
		else max_cart_value_error
	end as max_cart_value_error,
	convert(numeric, case b2b_b2c_lp_switch_disabled when '' then null else b2b_b2c_lp_switch_disabled end) as b2b_b2c_lp_switch_disabled,
	convert(numeric, case show_press_div when '' then null else show_press_div end) as show_press_div,
	convert(numeric, case show_slider_div when '' then null else show_slider_div end) as show_slider_div,
	convert(numeric, case overwrite_application_theme when '' then null else overwrite_application_theme end) as overwrite_application_theme,
	case
		theme when '' then null
		else theme
	end as theme,
	case
		image_path when '' then null
		else image_path
	end as image_path,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(numeric, case tax when '' then null else tax end) as tax,
	convert(numeric, case sales_tax_bonus when '' then null else sales_tax_bonus end) as sales_tax_bonus,
	case
		signin_image_path when '' then null
		else signin_image_path
	end as signin_image_path,
	case
		logo_image_path when '' then null
		else logo_image_path
	end as logo_image_path
from
	raw_wkfs.landingpage