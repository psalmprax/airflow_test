import yaml

with open("data_sources.yml") as file_loc:
	source_table = yaml.load_all(file_loc)
	for tbl in source_table:
		for key, val in tbl.items():
			# print(key, " -> ", val['table_name'])
			dag_id = f'Job-{tbl[key]}'
			description = f'A {tbl[key]} DAG'
			print(dag_id)