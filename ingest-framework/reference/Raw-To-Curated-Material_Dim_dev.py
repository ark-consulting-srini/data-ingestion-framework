
# COMMAND ----------

# ----------------------
# -- 2) Inputs 
# ----------------------
# val_schema_raw = "config"
val_schema_raw = "raw"

# val_schema_curated = "config"
val_schema_curated = "curated"
val_schema_temp = "tempjobs"

# val_src_table_name = f"{val_cat}.{val_schema_temp}.sap_mara_test"
val_src_table_name = f"{val_cat}.{val_schema_raw}.sap_mara"

val_last_change_date = dbutils.widgets.get("change_date")
val_data_product_name = dbutils.widgets.get("data_product_name")


val_target_table = f"{val_cat}.{val_schema_curated}.material_dim"
# val_target_table = f"{val_cat}.{val_schema_curated}.material_test_dim"

print(f"""
      
      source_table:     {val_src_table_name}
      target_table:     {val_target_table}
      last_change_date: {val_last_change_date}
      data_product_name: {val_data_product_name}
      """)

# COMMAND ----------

# --------------------------------
# -- 2.1) Write into the audit_dim
# --------------------------------
# schema: (table_name, data_product_name, audit_dim_id, rec_created_date, source_table_name, target_table_name)
# values = [(val_target_table[4:], val_data_product_name, val_src_table_name[4:], val_target_table[4:])]
# columns = ['table_name', 'data_product_name', 'source_table_name', 'target_table_name']
# df = spark.createDataFrame(values, columns)
# val_sql_tbl_name = "dq_audit_dim"
# sql_server_write(df, val_sql_tbl_name, jdbc_url, username, password)
# df.show()


# COMMAND ----------

# --------------------------------
# -- 2.2) Get the audit_dim_id
# --------------------------------
# query = f"(SELECT max(audit_dim_id) audit_dim_id FROM dbo.dq_audit_dim WHERE target_table_name = '{val_target_table[4:]}') AS audit_dim_id"
# # print(query)
# df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
# val_audit_dim_id = df.first()['audit_dim_id']
# val_sla_dim_id = 0
# print(f"val_audit_dim_id: {val_audit_dim_id}, val_sla_dim_id: {val_sla_dim_id}")

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit "pipeline start"
# --------------------------------
sql_server_write_fact_msg("pipeline start")


# COMMAND ----------

# --------------------------------
# -- 2.4) SELECT: Read the data
# --------------------------------
cat = val_cat
sql = get_read_transform_sql(cat, val_schema_raw, "sap_mara")
df = spark.sql(sql)
df.createOrReplaceTempView("v_source")
# display(df)

# COMMAND ----------

# --------------------------------
# -- 2.5) ASSERT query count 
# --------------------------------
from datetime import datetime
import pytz

df_cnt = spark.sql("SELECT COUNT(*) FROM v_source")
cnt_query = df_cnt.first()[0]
# cnt_query = 0

# Get the current timestamp
timestamp_str = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')

# EXIT if no data
if cnt_query == 0:
      sql_server_write_fact_msg("pipeline end")
      dbutils.notebook.exit(
            f"EXISTING DUE TO NO DATA, QUERY-CNT:{cnt_query}, Time: {timestamp_str}, processing_time: {time.time() - val_notebook_start_ts} [sec]!"
            )

df_cnt = spark.sql(f"SELECT COUNT(*) FROM {val_src_table_name} WHERE change_date > '{val_last_change_date}'")
cnt_tbl = df_cnt.first()[0]

print(f"""
      table_cnt: {cnt_tbl}
      query_cnt: {cnt_query}
      """
      )
assert cnt_tbl == cnt_query 

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit fact: source_row_count, target_row_count, 
# --------------------------------
from datetime import datetime


# write audit fact source_row_count
sql_server_write_fact_count("source_row_count", cnt_query)

# write audit fact target_row_count
cnt_target = spark.sql(f"select count(*) cnt from {val_target_table}").first()['cnt']
sql_server_write_fact_count("target_row_count", cnt_target)


# COMMAND ----------

# -------------------------------
# 3) Create the v_type2 view for type-2 MERGE
# -------------------------------
sql = \
    f"""
    -- get the type-2 UPDATE 
    SELECT a.material_number AS merge_key, a.*, b.valid_from_date, b.valid_through_date
    FROM v_source a
    JOIN {val_target_table} b ON (
        (a.material_number = b.material_number AND b.valid_through_date = '9999-12-31') AND 
        (a.net_weight != b.net_weight OR a.gross_weight != b.gross_weight)
    )

    -- get the type-2 INSERT
    UNION ALL
    SELECT '!!!-DO-NOT-MATCH-!!!' as merge_key, a.*, b.valid_from_date, b.valid_through_date
    FROM v_source a
    JOIN {val_target_table} b ON (
        (a.material_number = b.material_number AND b.valid_through_date = '9999-12-31') AND 
        (a.net_weight != b.net_weight OR a.gross_weight != b.gross_weight)
    )
    
    -- get the new ROW INSERT
    UNION ALL
    SELECT a.material_number AS merge_key, a.*, b.valid_from_date, b.valid_through_date
    FROM v_source a
    LEFT JOIN {val_target_table} b ON (a.material_number = b.material_number)
    WHERE b.material_number IS NULL    
    """
# df_deep_copied = spark.createDataFrame(df_original.rdd.map(lambda x: x), schema=df_original.schema)
df_t2 = spark.sql(sql)
df_t2.createOrReplaceTempView("v_type2")
# display(df_t2)

# COMMAND ----------

# -------------------------------
# 4) create a copy of type-2 material_numbers
# -------------------------------

# METHOD-1: Create a new dataframe
df_t2_pks = df_t2.select("material_number").distinct()
# df_t2_pks = spark.createDataFrame(df_t2_pks.rdd.map(lambda x: x), schema=df_t2_pks.schema)
# df_t2_pks.createOrReplaceTempView("v_type2_material_numbers")
# display(df_t2_pks)

# METHOD-2: Persist into a table
temp_tbl_name = f"{val_cat}.{val_schema_temp}.temp_tbl_material_numbers"
df_p = spark.sql(f"DROP TABLE IF EXISTS {temp_tbl_name}")
df_t2_pks.write.format("delta").mode("overwrite").saveAsTable(temp_tbl_name)
df_p = spark.sql(f"SELECT * FROM {temp_tbl_name}")
df_p.createOrReplaceTempView("v_type2_material_numbers")
# print(f"temp_table:{temp_tbl_name}")
# display(df_p)

# COMMAND ----------

# -----------------------------
# -- 5) MERGE - TYPE-2
# -----------------------------
sql = \
    f"""    
    MERGE INTO {val_target_table} AS t
    USING (
    select * from v_type2
    ) AS s
    ON (t.material_number = s.merge_key) 
    WHEN MATCHED AND t.valid_through_date = '9999-12-31' THEN
    UPDATE SET t.valid_through_date = CURRENT_TIMESTAMP() - INTERVAL 1 SECOND
    WHEN NOT MATCHED THEN
    INSERT (
        material_number
        , valid_from_date
        , material_dim_dw_id
        , material_description
        , division_code
        , division_name
        , material_type_code
        , material_type_description
        , international_article_number_code
        , upc_each
        , upc_case
        , upc_pallet
        , upc_5_digit_code
        , ean_number_category_code
        , ean_number_category_description
        , cross_dist_chain_avail_check_code
        , cross_dist_chain_avail_check_desc
        , cross_distrib_chain_is_valid_date
        , industry_standard_description
        , industry_sector_code
        , industry_sector_description
        , material_group_code
        , material_group_description
        , item_category_group_code
        , item_category_group_description
        , packaging_material_group_code
        , packaging_material_group_desc
        , packaging_material_type_indicator
        , external_material_group_code
        , external_material_group_desc
        , transportation_group_code
        , transportation_group_description
        , base_unit_of_measure_code
        , base_unit_of_meas_description
        , gross_weight
        , net_weight
        , unit_of_weight_code
        , unit_of_weigth_desc
        , volume_number
        , unit_of_volume_code
        , unit_of_volume_description
        , length_number
        , width_number
        , height_number
        , unit_of_dimension_code
        , unit_of_dimension_description
        , size_dimension_description
        , units_of_measure_usage_indicator
        , minimum_remaining_shelf_life_days
        , total_shelf_life_days_number
        , shelf_life_expiration_period_ind
        , remaining_storage_life_percentage
        , stackable_units_number
        , variable_po_unit_quanity
        , label_type_code
        , label_type_description
        , label_form_code
        , label_form_description
        , storage_condition_code
        , storage_condition_description
        , temperature_condition_code
        , temperature_condition_description
        , hazardous_material_number_code
        , hazardous_material_description
        , dangerous_goods_ind_profile_code
        , dangerous_goods_ind_profile_desc
        , batch_mgmt_rqmt_indicator
        , quality_mgmt_active_in_procure_code
        , purchasing_value_key
        , first_billing_reminder_expired_number
        , second_billing_reminder_expired_number
        , third_billing_reminder_expired_number
        , under_delivery_total_number
        , over_delivery_total_number
        , unlimited_indicator
        , prod_allocation_method_code
        , prod_allocation_method_description
        , purchase_order_unit_of_measure_code
        , purchase_order_unit_of_measure_desc
        , labor_code
        , labor_description
        , container_required_code
        , container_required_description
        , catalog_profile_code
        , catalog_profile_description
        , product_inspection_memo_description
        , demand_sensing_type_code
        , demand_sensing_export_indicator
        , demand_planning_prod_type_desc
        , planning_abc_category_indicator
        , planning_abc_category_description
        , release_profile_code
        , release_profile_description
        , advanced_planning_optimization_ind
        , apo_item_item_description
        , snp_attribute_1_description
        , snp_attribute_2_description
        , snp_attribute_3_description
        , snp_attribute_4_description
        , snp_attribute_5_description
        , old_material_number
        , low_level_code
        , no_of_goods_receipts_from_wh_qty
        , material_qualifies_for_discount_ind
        , shelf_life_rounding_rule_desc
        , nafs_date_format_description
        , mfg_number_range_desc
        , product_avail_in_store_date
        , material_status_code
        , material_status_description
        , cross_plant_material_is_valid_date
        , consumer_product_code
        , consumer_product_description
        , create_date
        , change_date
        , valid_through_date    
    ) 
    VALUES (
        s.material_number
        , CURRENT_TIMESTAMP()
        --, concat(s.material_number, "_", replace(CAST(CURRENT_TIMESTAMP() AS STRING),' ', 'T'))
        , MD5(CONCAT('sap_', s.material_number, '_', CURRENT_TIMESTAMP()))
        , s.material_description
        , s.division_code
        , s.division_name
        , s.material_type_code
        , s.material_type_description
        , s.international_article_number_code
        , s.upc_each
        , s.upc_case
        , s.upc_pallet
        , s.upc_5_digit_code
        , s.ean_number_category_code
        , s.ean_number_category_description
        , s.cross_dist_chain_avail_check_code
        , s.cross_dist_chain_avail_check_desc
        , s.cross_distrib_chain_is_valid_date
        , s.industry_standard_description
        , s.industry_sector_code
        , s.industry_sector_description
        , s.material_group_code
        , s.material_group_description
        , s.item_category_group_code
        , s.item_category_group_description
        , s.packaging_material_group_code
        , s.packaging_material_group_desc
        , s.packaging_material_type_indicator
        , s.external_material_group_code
        , s.external_material_group_desc
        , s.transportation_group_code
        , s.transportation_group_description
        , s.base_unit_of_measure_code
        , s.base_unit_of_meas_description
        , s.gross_weight
        , s.net_weight
        , s.unit_of_weight_code
        , s.unit_of_weigth_desc
        , s.volume_number
        , s.unit_of_volume_code
        , s.unit_of_volume_description
        , s.length_number
        , s.width_number
        , s.height_number
        , s.unit_of_dimension_code
        , s.unit_of_dimension_description
        , s.size_dimension_description
        , s.units_of_measure_usage_indicator
        , s.minimum_remaining_shelf_life_days
        , s.total_shelf_life_days_number
        , s.shelf_life_expiration_period_ind
        , s.remaining_storage_life_percentage
        , s.stackable_units_number
        , s.variable_po_unit_quanity
        , s.label_type_code
        , s.label_type_description
        , s.label_form_code
        , s.label_form_description
        , s.storage_condition_code
        , s.storage_condition_description
        , s.temperature_condition_code
        , s.temperature_condition_description
        , s.hazardous_material_number_code
        , s.hazardous_material_description
        , s.dangerous_goods_ind_profile_code
        , s.dangerous_goods_ind_profile_desc
        , s.batch_mgmt_rqmt_indicator
        , s.quality_mgmt_active_in_procure_code
        , s.purchasing_value_key
        , s.first_billing_reminder_expired_number
        , s.second_billing_reminder_expired_number
        , s.third_billing_reminder_expired_number
        , s.under_delivery_total_number
        , s.over_delivery_total_number
        , s.unlimited_indicator
        , s.prod_allocation_method_code
        , s.prod_allocation_method_description
        , s.purchase_order_unit_of_measure_code
        , s.purchase_order_unit_of_measure_desc
        , s.labor_code
        , s.labor_description
        , s.container_required_code
        , s.container_required_description
        , s.catalog_profile_code
        , s.catalog_profile_description
        , s.product_inspection_memo_description
        , s.demand_sensing_type_code
        , s.demand_sensing_export_indicator
        , s.demand_planning_prod_type_desc
        , s.planning_abc_category_indicator
        , s.planning_abc_category_description
        , s.release_profile_code
        , s.release_profile_description
        , s.advanced_planning_optimization_ind
        , s.apo_item_item_description
        , s.snp_attribute_1_description
        , s.snp_attribute_2_description
        , s.snp_attribute_3_description
        , s.snp_attribute_4_description
        , s.snp_attribute_5_description
        , s.old_material_number
        , s.low_level_code
        , s.no_of_goods_receipts_from_wh_qty
        , s.material_qualifies_for_discount_ind
        , s.shelf_life_rounding_rule_desc
        , s.nafs_date_format_description
        , s.mfg_number_range_desc
        , s.product_avail_in_store_date
        , s.material_status_code
        , s.material_status_description
        , s.cross_plant_material_is_valid_date
        , s.consumer_product_code
        , s.consumer_product_description
        , CURRENT_TIMESTAMP()
        , CURRENT_TIMESTAMP()
        , '9999-12-31'
    )
    """
df = spark.sql(sql)
df.show()
# display(df)
val_num_rows_updated_t2 = df.first()['num_updated_rows']
val_num_rows_inserted = df.first()['num_inserted_rows']

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit fact: source_row_count, target_row_count, 
# --------------------------------
sql_server_write_fact_count("target_insert_count", val_num_rows_inserted)
sql_server_write_fact_count("target_update_count_type2", val_num_rows_updated_t2)

# COMMAND ----------

# --------------------------------
# -- 6) MERGE for TYPE-1 UPDATE
# --------------------------------
# -- MERGE INTO dev.curated.matl_dim AS t
sql = \
    f"""
    MERGE INTO {val_target_table} AS t
    USING (
        -- get the type-2 UPDATE 
        SELECT a.material_number AS merge_key, a.*
        FROM v_source a
        LEFT JOIN v_type2_material_numbers b ON (a.material_number = b.material_number)
        WHERE b.material_number IS NULL
    ) AS s
    ON (t.material_number = s.merge_key) 
    -- WHEN MATCHED AND t.valid_thru_date IS NULL THEN
    WHEN MATCHED AND t.valid_through_date = '9999-12-31' THEN
    UPDATE SET 
        material_description = s.material_description
        , division_code = s.division_code
        , division_name = s.division_name
        , material_type_code = s.material_type_code
        , material_type_description = s.material_type_description
        , international_article_number_code = s.international_article_number_code
        , upc_each = s.upc_each
        , upc_case = s.upc_case
        , upc_pallet = s.upc_pallet
        , upc_5_digit_code = s.upc_5_digit_code
        , ean_number_category_code = s.ean_number_category_code
        , ean_number_category_description = s.ean_number_category_description
        , cross_dist_chain_avail_check_code = s.cross_dist_chain_avail_check_code
        , cross_dist_chain_avail_check_desc = s.cross_dist_chain_avail_check_desc
        , cross_distrib_chain_is_valid_date = s.cross_distrib_chain_is_valid_date
        , industry_standard_description = s.industry_standard_description
        , industry_sector_code = s.industry_sector_code
        , industry_sector_description = s.industry_sector_description
        , material_group_code = s.material_group_code
        , material_group_description = s.material_group_description
        , item_category_group_code = s.item_category_group_code
        , item_category_group_description = s.item_category_group_description
        , packaging_material_group_code = s.packaging_material_group_code
        , packaging_material_group_desc = s.packaging_material_group_desc
        , packaging_material_type_indicator = s.packaging_material_type_indicator
        , external_material_group_code = s.external_material_group_code
        , external_material_group_desc = s.external_material_group_desc
        , transportation_group_code = s.transportation_group_code
        , transportation_group_description = s.transportation_group_description
        , base_unit_of_measure_code = s.base_unit_of_measure_code
        , base_unit_of_meas_description = s.base_unit_of_meas_description
        , gross_weight = s.gross_weight
        , net_weight = s.net_weight
        , unit_of_weight_code = s.unit_of_weight_code
        , unit_of_weigth_desc = s.unit_of_weigth_desc
        , volume_number = s.volume_number
        , unit_of_volume_code = s.unit_of_volume_code
        , unit_of_volume_description = s.unit_of_volume_description
        , length_number = s.length_number
        , width_number = s.width_number
        , height_number = s.height_number
        , unit_of_dimension_code = s.unit_of_dimension_code
        , unit_of_dimension_description = s.unit_of_dimension_description
        , size_dimension_description = s.size_dimension_description
        , units_of_measure_usage_indicator = s.units_of_measure_usage_indicator
        , minimum_remaining_shelf_life_days = s.minimum_remaining_shelf_life_days
        , total_shelf_life_days_number = s.total_shelf_life_days_number
        , shelf_life_expiration_period_ind = s.shelf_life_expiration_period_ind
        , remaining_storage_life_percentage = s.remaining_storage_life_percentage
        , stackable_units_number = s.stackable_units_number
        , variable_po_unit_quanity = s.variable_po_unit_quanity
        , label_type_code = s.label_type_code
        , label_type_description = s.label_type_description
        , label_form_code = s.label_form_code
        , label_form_description = s.label_form_description
        , storage_condition_code = s.storage_condition_code
        , storage_condition_description = s.storage_condition_description
        , temperature_condition_code = s.temperature_condition_code
        , temperature_condition_description = s.temperature_condition_description
        , hazardous_material_number_code = s.hazardous_material_number_code
        , hazardous_material_description = s.hazardous_material_description
        , dangerous_goods_ind_profile_code = s.dangerous_goods_ind_profile_code
        , dangerous_goods_ind_profile_desc = s.dangerous_goods_ind_profile_desc
        , batch_mgmt_rqmt_indicator = s.batch_mgmt_rqmt_indicator
        , quality_mgmt_active_in_procure_code = s.quality_mgmt_active_in_procure_code
        , purchasing_value_key = s.purchasing_value_key
        , first_billing_reminder_expired_number = s.first_billing_reminder_expired_number
        , second_billing_reminder_expired_number = s.second_billing_reminder_expired_number
        , third_billing_reminder_expired_number = s.third_billing_reminder_expired_number
        , under_delivery_total_number = s.under_delivery_total_number
        , over_delivery_total_number = s.over_delivery_total_number
        , unlimited_indicator = s.unlimited_indicator
        , prod_allocation_method_code = s.prod_allocation_method_code
        , prod_allocation_method_description = s.prod_allocation_method_description
        , purchase_order_unit_of_measure_code = s.purchase_order_unit_of_measure_code
        , purchase_order_unit_of_measure_desc = s.purchase_order_unit_of_measure_desc
        , labor_code = s.labor_code
        , labor_description = s.labor_description
        , container_required_code = s.container_required_code
        , container_required_description = s.container_required_description
        , catalog_profile_code = s.catalog_profile_code
        , catalog_profile_description = s.catalog_profile_description
        , product_inspection_memo_description = s.product_inspection_memo_description
        , demand_sensing_type_code = s.demand_sensing_type_code
        , demand_sensing_export_indicator = s.demand_sensing_export_indicator
        , demand_planning_prod_type_desc = s.demand_planning_prod_type_desc
        , planning_abc_category_indicator = s.planning_abc_category_indicator
        , planning_abc_category_description = s.planning_abc_category_description
        , release_profile_code = s.release_profile_code
        , release_profile_description = s.release_profile_description
        , advanced_planning_optimization_ind = s.advanced_planning_optimization_ind
        , apo_item_item_description = s.apo_item_item_description
        , snp_attribute_1_description = s.snp_attribute_1_description
        , snp_attribute_2_description = s.snp_attribute_2_description
        , snp_attribute_3_description = s.snp_attribute_3_description
        , snp_attribute_4_description = s.snp_attribute_4_description
        , snp_attribute_5_description = s.snp_attribute_5_description
        , old_material_number = s.old_material_number
        , low_level_code = s.low_level_code
        , no_of_goods_receipts_from_wh_qty = s.no_of_goods_receipts_from_wh_qty
        , material_qualifies_for_discount_ind = s.material_qualifies_for_discount_ind
        , shelf_life_rounding_rule_desc = s.shelf_life_rounding_rule_desc
        , nafs_date_format_description = s.nafs_date_format_description
        , mfg_number_range_desc = s.mfg_number_range_desc
        , product_avail_in_store_date = s.product_avail_in_store_date
        , material_status_code = s.material_status_code
        , material_status_description = s.material_status_description
        , cross_plant_material_is_valid_date = s.cross_plant_material_is_valid_date
        , consumer_product_code = s.consumer_product_code
        , consumer_product_description = s.consumer_product_description
        , change_date = CURRENT_TIMESTAMP()
    """
df = spark.sql(sql)
val_num_rows_updated_t1 = df.first()['num_updated_rows']
df.show()

# display(df)

# COMMAND ----------

# --------------------------------
# -- 6.1) Writie into the audit fact: source_row_count, target_row_count, 
# --------------------------------
sql_server_write_fact_count("target_update_count_type1", val_num_rows_updated_t1)

# COMMAND ----------

# --------------------------------
# -- 6.2) write the post process total count  
# --------------------------------
# write audit fact target_row_count
cnt_target = spark.sql(f"select count(*) cnt from {val_target_table}").first()['cnt']
sql_server_write_fact_count("target_row_count", cnt_target)


# COMMAND ----------

# --------------------------------
# -- 6.3) drop the temp table 
# --------------------------------
spark.sql(f"DROP TABLE IF EXISTS {temp_tbl_name}")

# COMMAND ----------

# -------------------------------
# -- 7.1) Writing the new change_date into the SQL-Server, building the dataframe 
# -------------------------------
table_name = f"{val_cat}.{val_schema_raw}.sap_mara"
df = spark.sql(f"select change_date from {table_name} limit 1")
change_date = df.head()[0] 

print(change_date)

data = [(val_data_product_name, 'curated', 'material_dim', 'Raw_2_curated_Material_dim', 'B', change_date)]
schema = "stream_name:string, schema_name:string, table_name:string, job_name:string, manual_or_batch_load_flag:string, change_date:timestamp"


df_sql_server = spark.createDataFrame(data, schema)

# display(df_sql_server)


# COMMAND ----------

# -------------------------------
# -- 7.3) INSERT INTO SQL-Server
# -------------------------------
val_tbl_name = "dbo.control_curated"
df_sql_server.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", val_tbl_name) \
    .option("user", connection_properties["user"]) \
    .option("password", connection_properties["password"]) \
    .mode("append") \
    .save()


# COMMAND ----------

# -------------------------------
# -- 8) Calling the DQ notebook
# -------------------------------
# Calling the DQ notebook ...
# DQ dashboard ...
val_table_name = val_target_table
audit_dim_id_variable = val_audit_dim_id
sla_dim_id_variable = 0
val_schema_name = val_schema_curated
tbl_name = val_table_name.split('.')[2]
print(f"table_name:{tbl_name}, cat:{val_cat}, schema:{val_schema_name}")
# /Workspace/Repos/adm-pirvalbe@csoups.com/DA_ADB_DLH_DE/common/dq/transformations/dq_rule_builder_post_update_immediate_par_sql
dbutils.notebook.run(
"../../../common/dq/transformations/dq_rule_builder_post_update_immediate_par_sql",
6000,
{
    "table_name": tbl_name, 
    "audit_dim_id": audit_dim_id_variable,
    "sla_dim_id": sla_dim_id_variable,
    "database_name": val_schema_name,
    "catalog_name": val_cat 
})

# COMMAND ----------

# --------------------------------
# -- 8.1) Writie into the audit "pipeline end"
# --------------------------------
sql_server_write_fact_msg("pipeline end")


# COMMAND ----------

# -----------------------------------
# -- 9) notebook processing time
# -----------------------------------
# import time
# dbutils.widgets.text("change_date", "2022-03-04", "Change Date")
print(f"processing DONE IN {time.time() - val_notebook_start_ts} !")
