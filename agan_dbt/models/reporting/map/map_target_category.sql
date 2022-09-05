select
	"name" as category,
	case
		when category in 
			('Apple Displays',
			'Apple MacBooks',
			'Apple Macs',
			'Audio & HiFi',
			'Blitzgeräte',
			'Camcorder',
			'Digitalkameras',
			'Grafikkarten',
			'Handys',
			'Konsolen',
			'Notebooks',
			'Objektive',
			'Smartwatches',
			'Tablets',
			'Virtual Reality',
			'Zubehoer'
			) then category
		else 'Other or new'
	end as target_category_2022
from
    {{ ref("obj_wkfs.category_cleaned") }} as owcc

--	analytics_objects."obj_wkfs.category_cleaned"
