select
	convert(numeric, case check_result_actions_id when '' then null else check_result_actions_id end) as check_result_actions_id,
	convert(timestamp, case "out" when '' then null else "out" end) as "out",
	convert(timestamp, case returned when '' then null else returned end) as returned,
	convert(decimal(15,2), case costs when '' then null else costs end) as costs,
	case
		remarks when '' then null
		else remarks
	end as remarks,
	convert(timestamp, case "start" when '' then null else "start" end) as "start",
	convert(numeric, case contractor_id when '' then null else contractor_id end) as contractor_id,
	convert(numeric, case repaired when '' then null else repaired end) as repaired,
	convert(numeric, case battery_changed when '' then null else battery_changed end) as battery_changed,
	convert(numeric, case new_optical_state when '' then null else new_optical_state end) as new_optical_state
from
	raw_wkfs.repair