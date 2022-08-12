with results as (
select
	convert(numeric, case id when '' then null else id end) as id,
	case
		"name" when '' then null
		else "name"
	end as "name",
	case
		country when '' then null
		else country
	end as country,
	case
		general_terms_pdf_path when '' then null
		else general_terms_pdf_path
	end as general_terms_pdf_path,
	convert(numeric, case attach_general_terms_pdf when '' then null else attach_general_terms_pdf end) as attach_general_terms_pdf,
	convert(numeric, case optin when '' then null else optin end) as optin,
	convert(numeric, case hide_dont_buy_flagged_devices when '' then null else hide_dont_buy_flagged_devices end) as hide_dont_buy_flagged_devices,
	convert(numeric, case fastlane_trades when '' then null else fastlane_trades end) as fastlane_trades,
	convert(numeric, case skippable_clarification when '' then null else skippable_clarification end) as skippable_clarification,
	convert(numeric, case force_adress_input when '' then null else force_adress_input end) as force_adress_input,
	convert(numeric, case show_tax when '' then null else show_tax end) as show_tax,
	case
		deletion when '' then null
		else deletion
	end as deletion,
	case
		addition_trade_info when '' then null
		else addition_trade_info
	end as addition_trade_info,
	convert(numeric, case print_pickup when '' then null else print_pickup end) as print_pickup,
	convert(numeric, case send_mail_to_logged_in_user when '' then null else send_mail_to_logged_in_user end) as send_mail_to_logged_in_user,
	convert(numeric, case permanent_payment_type_display when '' then null else permanent_payment_type_display end) as permanent_payment_type_display,
	convert(numeric, case currency_id when '' then null else currency_id end) as currency_id,
	convert(numeric, case default_b2b_landingpage_id when '' then null else default_b2b_landingpage_id end) as default_b2b_landingpage_id,
	convert(numeric, case default_b2c_landingpage_id when '' then null else default_b2c_landingpage_id end) as default_b2c_landingpage_id,
	case
		theme when '' then null
		else theme
	end as theme,
	convert(numeric, case skip_clarifications when '' then null else skip_clarifications end) as skip_clarifications,
	convert(numeric, case payment_delay_basket_default when '' then null else payment_delay_basket_default end) as payment_delay_basket_default,
	convert(numeric, case payment_delay_basket_with_voucher when '' then null else payment_delay_basket_with_voucher end) as payment_delay_basket_with_voucher,
	convert(timestamp, case created_at when '' then null else created_at end) as created_at,
	convert(timestamp, case updated_at when '' then null else updated_at end) as updated_at,
	convert(numeric, case redirect_to when '' then null else redirect_to end) as redirect_to,
	convert(numeric, case enable_seperate_pick_up_adress when '' then null else enable_seperate_pick_up_adress end) as enable_seperate_pick_up_adress,
	convert(numeric, case offer_state_after_product_check when '' then null else offer_state_after_product_check end) as offer_state_after_product_check
from
	{{ source("wkfs","application") }}
)

select * from results