data_types = {
	'INTEGER': 'BIGINT ENCODE lzo',
	'TINYINT': 'BIGINT ENCODE lzo',
	'SMALLINT': 'BIGINT ENCODE lzo',
	'MEDIUMINT': 'BIGINT ENCODE lzo',
	'INT': 'BIGINT ENCODE lzo',
	'BIGINT': 'BIGINT ENCODE lzo',
	'DECIMAL': 'DOUBLE PRECISION ENCODE ZSTD',
	'FLOAT': 'REAL ENCODE ZSTD',
	'DOUBLE': 'DOUBLE PRECISION ENCODE ZSTD',
	'CHAR': 'CHAR ENCODE lzo',
	'YEAR': 'VARCHAR(MAX) ENCODE lzo',
	'VARCHAR': 'VARCHAR(MAX) ENCODE lzo',
	'BIT': 'VARCHAR(MAX) ENCODE lzo',
	'BINARY': 'VARCHAR(MAX) ENCODE lzo',
	'VARBINARY': 'VARCHAR(MAX) ENCODE lzo',
	'TINYTEXT': 'VARCHAR(MAX) ENCODE TEXT32k',
	'MEDIUMTEXT': 'VARCHAR(MAX) ENCODE TEXT32k',
	'LONGTEXT': 'VARCHAR(MAX) ENCODE TEXT32k',
	# 'MEDIUMTEXTXTR': 'TEXT ENCODE TEXT32k',
	'TEXT': 'VARCHAR(MAX) ENCODE lzo',
	'ENUM': 'VARCHAR(MAX) ENCODE lzo',
	'SET': 'VARCHAR(MAX) ENCODE lzo',
	'SPATIAL': 'VARCHAR(MAX) ENCODE lzo',
	'DATETIME': 'DATETIME ENCODE lzo',
	'DATE': 'DATE ENCODE lzo',
	'TIME': 'TIME ENCODE lzo',
	'TIMESTAMP': 'TIMESTAMP ENCODE lzo',
	'FIXED_LEN_BYTE_ARRAY': 'DECIMAL({})'
	
}

airflow_job_metadata = [
	"dag",
	"data_interval_start",
	"data_interval_end",
	"execution_date",
	"next_execution_date",
	"prev_data_interval_start_success",
	"prev_data_interval_end_success",
	"prev_execution_date",
	"prev_start_date_success",
	"prev_execution_date_success"
]


query = dict()
query['source_or_target_last_record_id'] = """select {} from {}.{} order by cast({} as integer) desc limit 1"""
query['updated_at_source_or_target_last_record_id'] = """select {} from {}.{} order by {} desc limit 1"""

query['last_id_qry'] = """select count(*) from {}.{} where {} > {}"""
query['source_fetch_new_update_data_qry'] = """select * from {}.{} where {} >= '{}' order by {}"""
query['source_or_target_rec_count_query'] = """select count(*) from {}.{}"""
query['query_for_schema'] = """select * from {}.{} limit 10"""
query['last_id_OR_last_id_val'] = """select * from
									(select * from {}.{} where {} > {})  as tbl limit 0, 100000"""

# query['last_id_OR_last_id_val_SPECIAL'] = """select * from {}.{} where {} > {} order by {} asc limit 0, {}"""
query['last_id_OR_last_id_val_SPECIAL'] = """select * from {}.{} where
											{} > {} limit 0, 100000"""

query['last_id_val_full_refresh'] = """select * from (select * from {}.{} order by {} asc)  as tbl limit {}, 100000"""
query['last_id_val_full_refresh_SPECIAL'] = """select * from {}.{} limit {}, 10000"""



