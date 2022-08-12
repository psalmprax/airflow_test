with results as (
select
	convert(numeric, case id when '' then null else id end) as id,
	case
		"name" when '' then null
		else "name"
	end as "name",
	convert(numeric, case usage_id when '' then null else usage_id end) as usage_id,
	convert(numeric, case vat when '' then null else vat end) as vat,
	convert(numeric, case handling when '' then null else handling end) as handling,
	convert(numeric, case logistics when '' then null else logistics end) as logistics,
	convert(numeric, case "returns" when '' then null else "returns" end) as "returns",
	convert(numeric, case sales_costs when '' then null else sales_costs end) as sales_costs,
	convert(numeric, case marketing_costs when '' then null else marketing_costs end) as marketing_costs,
	convert(numeric, case marketing_costs_lower_cap when '' then null else marketing_costs_lower_cap end) as marketing_costs_lower_cap,
	convert(numeric, case marketing_costs_upper_cap when '' then null else marketing_costs_upper_cap end) as marketing_costs_upper_cap,
	convert(numeric, case contribution when '' then null else contribution end) as contribution,
	convert(numeric, case contribution_lower_cap when '' then null else contribution_lower_cap end) as contribution_lower_cap,
	convert(numeric, case contribution_upper_cap when '' then null else contribution_upper_cap end) as contribution_upper_cap,
	convert(numeric, case sales_tax_bonus when '' then null else sales_tax_bonus end) as sales_tax_bonus
from
	{{ source("wkfs","calculation") }}
)

select * from results