--obj_wkfs_offer_state
--https://github.com/asgoodasnu/analytics-transformations/blob/develop/models/objects/wkfs/obj_wkfs_offer_state.sql

with state as (
select
	id,
	name as offer_state,
	code,
	coalesce(do_update_customer = 1, false) as do_update_customer,
	coalesce(is_setting_paid = 1, false) as is_setting_paid,
	coalesce(is_syncable = 1, false) as is_syncable
from
	{{ ref ("obj_wkfs.offer_state_cleaned") }} 


)

select
	*
from
	state