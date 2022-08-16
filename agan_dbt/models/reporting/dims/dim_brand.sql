SELECT DISTINCT name AS manufactor_name,
	CASE
		WHEN name LIKE '%Apple%' THEN 'Apple'
		WHEN name LIKE '%MacBook%' THEN 'Apple'
        WHEN name LIKE '%A-rival%' THEN 'a-rival'
        WHEN name LIKE '%A-Rival%' THEN 'a-rival'
        WHEN name LIKE '%Asgoodasnew%' THEN 'asgoodasnew'
        WHEN name LIKE '%ASUS%' THEN 'Asus'
        WHEN name LIKE '%Blackberry%' THEN 'BlackBerry'
        WHEN name LIKE '%easypix%' THEN 'Easypix'
        WHEN name LIKE '%Iriver%' THEN 'iRiver'
        WHEN name LIKE '%Trekstor%' THEN 'TrekStor'
        WHEN name LIKE '%Viewsonic%' THEN 'ViewSonic'
        WHEN name LIKE '%Sonstige%' THEN 'other'
        ELSE name END AS brand,

    CASE
        WHEN name LIKE '%A-rival%' THEN 'a-rival'
        WHEN name LIKE '%A-Rival%' THEN 'a-rival'
        WHEN name LIKE '%Asgoodasnew%' THEN 'asgoodasnew'
        WHEN name LIKE '%ASUS%' THEN 'Asus'
        WHEN name LIKE '%Blackberry%' THEN 'BlackBerry'
        WHEN name LIKE '%easypix%' THEN 'Easypix'
        WHEN name LIKE '%Iriver%' THEN 'iRiver'
        WHEN name LIKE '%Trekstor%' THEN 'TrekStor'
        WHEN name LIKE '%Viewsonic%' THEN 'ViewSonic'
        WHEN name LIKE '%Sonstige%' THEN 'other'
        ELSE name END AS brand_cpm,

    CASE
        WHEN name LIKE '%Apple%' THEN 'Apple'
        WHEN name LIKE '%MacBook%' THEN 'Apple'
        WHEN name LIKE '%Canon%' THEN 'Canon'
        WHEN name LIKE '%Google%' THEN 'Google'
        WHEN name LIKE '%Huawei%' THEN 'Huawei'
        WHEN name LIKE '%Microsoft%' THEN 'Microsoft'
        WHEN name LIKE '%Nikon%' THEN 'Nikon'
        WHEN name LIKE '%OnePlus%' THEN 'OnePlus'
        WHEN name LIKE '%Samsung%' THEN 'Samsung'
        WHEN name LIKE '%Sony%' THEN 'Sony'
        WHEN name LIKE '%Xiaomi%' THEN 'Xiaomi'
        ELSE 'other' END as brand_budget_2022

--FROM  {{ source('wkfs', 'manufactor') }}
FROM  {{ ref('obj_wkfs.manufactor_cleaned') }}