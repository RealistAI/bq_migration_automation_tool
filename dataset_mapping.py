import re

#downloading the dependencies for the uc4 job. Use regular expression to find all create table/views sql statement. Seperate the DDL and the DMLS
list_of_create_tables = []
list_of_create_views = []
list_of_queries = ["SELECT * FROM my_table LIMIT 100;", "CREATE TABLE IF NOT EXISTS my_tables;", "CREATE VIEW IF NOT EXISTS my_view;"]
json = """
[{
	"name": "C_PP_FST_WITH_WHITLST_MAIN",
	"sql_dependencies": [{
			"name": "MD_IDI_FST_WITH_WHITLST_RADD_CNT_CHK",
			"reference_path": "$DW_EXE/execTdSQL.sh  $DW_SQL/faster_withdrawal_whitelist/fst_with_whitlst_cnt_check.sql dbuser=tddw_risk_qh_ops paypal_batch_date=&PAYPAL_BATCH_DATE#",
			"sql_file_path": "test/sql_2.sql",
			"order": "1",
			"lnr": "2",
			"col": "2"
		},
		{
			"name": "LIB_CHECK_ZERO_IDI_FST_WITH_WHITLST_RADD",
			"reference_path": "$DW_EXE/dw_quality/zero_byte_load.ksh $DW_IN/qc_idi_fst_with_whitlst_radd.txt $DW_IN/data_idi_fst_with_whitlst_radd.txt",
			"sql_file_path": "",
			"order": "2",
			"lnr": "3",
			"col": "3"
		},
		{
			"@name": "C_PP_FST_WITH_WHITLST_RADD",
			"order": "3",
			"lnr": "4",
			"col": "4",
			"sql_dependencies": [{
					"name": "MD_W_FAST_WITH_WHITLST_RADD",
					"reference_path": "&RADD_INFA_SCRIPT_PATH#/radd_informatica.sh &RADD_INFA_WF_FOLDER# wf_IDI_FAST_WITH_WHITLST_RADD &Run_date# IDI_ACH_WTHDRAWAL_WL\n!$DW_EXE/radd_informatica.sh vramasamy wf_IDI_FAST_WITH_WHITLST_RADD &Run_date# IDI_ACH_WTHDRAWAL_WL",
					"sql_file_path": "",
					"order": "1",
					"lnr": "2",
					"col": "2"
				},
				{
					"name": "MD_FST_WITH_WHITLST_RADD_IDI_UPD",
					"reference_path": "$DW_EXE/execTdSQL.sh  $DW_SQL/radd_master/radd_master_upd.sql dbuser=tddw_risk_qh_ops paypal_batch_date=&PAYPAL_BATCH_DATE# radd_name=&radd_name#",
					"sql_file_path": "test/sql_3.sql",
					"order": "2",
					"lnr": "3",
					"col": "3"
				},
				{
					"name": "MD_DDM_FST_WITH_WHITLST_RADD_XML_GEN",
					"reference_path": "&RADD_INFA_SCRIPT_PATH#/radd_replicator.sh &Run_date# DDM_ACH_WTHDRAWAL_WL",
					"sql_file_path": "",
					"order": "2",
					"lnr": "4",
					"col": "3"
				},
				{
					"name": "MD_FST_WITH_WHITLST_RADD_SCP_SAS",
					"reference_path": "&RADD_INFA_SCRIPT_PATH#/radd_scp_sas.sh -RADDNAME IDI_ACH_WTHDRAWAL_WL_RADD -RUNDATE &Run_date#",
					"sql_file_path": "",
					"order": "3",
					"lnr": "5",
					"col": "4"
				},
				{
					"name": "MD_DDM_FST_WITH_WHITLST_RADD_XML_COPY",
					"reference_path": "cp &RADD_SRC_PATH#/IDI_ACH_WTHDRAWAL_WL_RADD.txt &RADD_DDM_TGT_SLC_PATH#/DDM_ACH_WTHDRAWAL_WL_RADD.txt\ncp &RADD_SRC_PATH#/DDM_ACH_WTHDRAWAL_WL_RADD.xml &RADD_DDM_TGT_SLC_PATH#/DDM_ACH_WTHDRAWAL_WL_RADD.xml",
					"sql_file_path": "",
					"order": "4",
					"lnr": "6",
					"col": "4"
				},
				{
					"name": "MD_FST_WITH_WHITLST_RADD_DDM_UPD",
					"reference_path": "$DW_EXE/execTdSQL.sh  $DW_SQL/radd_master/radd_master_upd.sql dbuser=tddw_risk_qh_ops paypal_batch_date=&PAYPAL_BATCH_DATE# radd_name=&radd_name#",
					"sql_file_path": "test/sql_4.sql",
					"order": "6",
					"lnr": "7",
					"col": "5"
				}
			]
		},
		{
			"name": "MD_DW_TAB_CURR_FASTER_WITHDRAWAL_WHITELIST_UPD",
			"reference_path": "$DW_EXE/execTdSQL.sh $DW_SQL/dw_table_current/dw_table_current_roe.sql dbuser=tddw_risk_qh_ops paypal_batch_date=&PAYPAL_BATCH_DATE#  roe_db='pp_risk_ops_qh_tables' job_name='MD_DW_TAB_CURR_FASTER_WITHDRAWAL_WHITELIST_UPD'",
			"sql_file_path": "test/sql_5.sql",
			"order": "4",
			"lnr": "5",
			"col": "5"
		}
	]
}]
"""

for queries in list_of_queries:
    find_create_tables = re.search("^CREATE TABLE", queries)
    find_create_views = re.search("^CREATE VIEW", queries)
    if find_create_tables:
        list_of_create_tables.append(find_create_tables.string)
    elif find_create_views:
        list_of_create_views.append(find_create_views.string)

print("list of views: ", list_of_create_views)
print("list of tables: ", list_of_create_tables)



