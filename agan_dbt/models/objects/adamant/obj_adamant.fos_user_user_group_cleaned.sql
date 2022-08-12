with results as (
select
	convert(numeric,case user_id when '' then null else user_id end) as user_id,
	convert(numeric,case group_id when '' then null else group_id end) as group_id
from
	{{ source("adamant","fos_user_user_group") }}
)

select * from results