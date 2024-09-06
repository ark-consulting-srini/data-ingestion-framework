# Databricks notebook source
#####Need to add columns from Product dimension####

# COMMAND ----------

dbutils.widgets.text("change_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("catalog_name", "")
val_change_date = dbutils.widgets.get("change_date")
val_end_date = dbutils.widgets.get("end_date")
table_name=dbutils.widgets.get("table_name")
catalog_name=dbutils.widgets.get("catalog_name")
val_target_table=catalog_name+'.curated.'+table_name
val_data_product_name='wmt'
src_table_name='wmt_itemattributes'
val_src_table_name=catalog_name+'.raw.'+src_table_name




# COMMAND ----------

# --------------------------------
# -- 2.1) Write into the audit_dim
# --------------------------------
# schema: (table_name, data_product_name, audit_dim_id, rec_created_date, source_table_name, target_table_name)

values = [(table_name, val_data_product_name, src_table_name, table_name)]
columns = ['table_name', 'data_product_name', 'source_table_name', 'target_table_name']
df = spark.createDataFrame(values, columns)
val_sql_tbl_name = "dq_audit_dim"
sql_server_write(df, val_sql_tbl_name, jdbc_url, username, password)
df.show()

# COMMAND ----------

# --------------------------------
# -- 2.2) Get the audit_dim_id
# --------------------------------
query = f"(SELECT max(audit_dim_id) audit_dim_id FROM dbo.dq_audit_dim WHERE target_table_name = '{table_name}') AS audit_dim_id"
# print(query)
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
val_audit_dim_id = df.first()['audit_dim_id']
val_sla_dim_id = 0
print(f"val_audit_dim_id: {val_audit_dim_id}, val_sla_dim_id: {val_sla_dim_id}")

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit "pipeline start"
# --------------------------------
sql_server_write_fact_msg("pipeline start")

# COMMAND ----------

# DBTITLE 1,Drop the staging table if exist
spark.sql("DROP TABLE IF EXISTS dev.raw.temp_table_luminate_material_external_dim")
#spark.sql("DROP TABLE IF EXISTS dev.raw.temp_table_distrib_center_external_dim_latest")

# COMMAND ----------

# DBTITLE 1,create the staging table based on the curated layer
#  %sql
#  CREATE table dev.raw.temp_table_luminate_material_external_dim as select *,'' as load_type from dev.curated.material_external_dim LIMIT 0;
#  --CREATE table dev.raw.temp_table_distrib_center_external_dim_latest as select * from dev.curated.distrib_center_external_dim_test LIMIT 0;

# COMMAND ----------

# DBTITLE 1,Identify the start and end date of the batch
sourcesystem_code="wmt";
config_date=val_change_date;
target_date =val_end_date

# COMMAND ----------

print(sourcesystem_code);
print(config_date);
print(target_date)
#print("${config_date}")

# COMMAND ----------

#print(curated_key)

# COMMAND ----------

spark.conf.set('process.startdate',str(config_date))
spark.conf.set('process.enddate',str(target_date))
spark.conf.set('process.catalog_name',catalog_name)
#spark.conf.set('process.curated_key',str(curated_key))

# COMMAND ----------

#  %sql
#  select '${process.enddate}','${process.enddate}'- INTERVAL 1 MILLISECOND

# COMMAND ----------

#  %sql
#  select * from dev.raw.temp_table_luminate_material_external_dim

# COMMAND ----------

# EXIT if no data
from datetime import datetime
import pytz
# Get the current timestamp
timestamp_str = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')

df_cnt = spark.sql(f"SELECT COUNT(*) FROM {val_src_table_name} WHERE change_date>='{config_date}' and change_date<='{target_date}'")
cnt_tbl = df_cnt.first()[0]

if cnt_tbl == 0:
      sql_server_write_fact_msg("pipeline end")
      dbutils.notebook.exit(
            f"EXISTING DUE TO NO DATA, QUERY-CNT:{cnt_tbl}, Time: {timestamp_str}"
            )
print(f"""
      table_cnt: {cnt_tbl}
      """
      )

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit fact: source_row_count, target_row_count, 
# --------------------------------
from datetime import datetime


# write audit fact source_row_count
sql_server_write_fact_count("source_row_count", cnt_tbl)

# write audit fact target_row_count
cnt_target = spark.sql(f"select count(*) cnt from {val_target_table}").first()['cnt']
sql_server_write_fact_count("target_row_count_before_load", cnt_target)


# COMMAND ----------

#  %sql
#  --select count(1)
#  --from  dev.curated.distrib_center_external_dim_test as A 
#  --JOIN dev.raw.wmt_dcdimensions as B on  A.dc_no=B.dc_nm and A.geo_region_code=B.geo_region_cd
#  --where A.valid_to_timestamp ='2099-12-31' and 
#  --A.source_system='wmt' 
#  --and B.change_date>='${process.startdate}' and B.change_date<='${process.enddate}'

# COMMAND ----------

#  %sql
#  --select count(1)
#  --from  dev.raw.wmt_dcdimensions as B 
#  --where B.change_date>='${process.startdate}' and B.change_date<='${process.enddate}'

# COMMAND ----------

# DBTITLE 1,Type 2 records - Update
#  %sql
#  insert into $catalog_name.raw.temp_table_luminate_material_external_dim
#  (material_external_dim_dw_id,
#  source_system_description,
#  material_external_code,
#  material_external_description,
#  universal_product_code,
#  material_external_type_code,
#  material_external_type_description,
#  material_external_status_desc,
#  department_description,
#  segment_code,
#  segment_description,
#  sub_brand_code,
#  sub_brand_description,
#  brand_code,
#  brand_description,
#  sub_category_code,
#  sub_category_description,
#  category_code,
#  category_description,
#  vendor_code,
#  vendor_description,
#  material_attribute_array,
#  valid_from_date,
#  valid_through_date,
#  create_date,
#  change_date,
#  data_quality_valid_flag,
#  data_quality_result_array,
#  load_type
#  )
#  select 
#  A.material_external_dim_dw_id,
#  A.source_system_description,
#  A.material_external_code,
#  A.material_external_description,
#  A.universal_product_code,
#  A.material_external_type_code,
#  A.material_external_type_description,
#  A.material_external_status_desc,
#  A.department_description,
#  A.segment_code,
#  A.segment_description,
#  A.sub_brand_code,
#  A.sub_brand_description,
#  A.brand_code,
#  A.brand_description,
#  A.sub_category_code,
#  A.sub_category_description,
#  A.category_code,
#  A.category_description,
#  A.vendor_code,
#  A.vendor_description,
#  A.material_attribute_array,
#  A.valid_from_date,
#  '${process.enddate}'- INTERVAL 1 MILLISECOND as valid_through_date,
#  A.create_date,
#  current_timestamp() as change_date,
#  A.data_quality_valid_flag,
#  A.data_quality_result_array,
#  'U2' as load_type
#  from  $catalog_name.curated.material_external_dim as A JOIN $catalog_name.raw.wmt_itemattributes as B on  A.material_external_code=B.wm_item_nbr
#  where A.valid_through_date ='2099-12-31' and A.source_system_description='wmt' 
#  and B.change_date>='${process.startdate}' and B.change_date<='${process.enddate}'
#  and (A.material_external_description <> B.item_name OR 
#       A.universal_product_code<> B.wm_upc_nbr OR 
#       A.material_external_type_code<>B.item_type_cd OR 
#       A.segment_description<>B.omni_seg_desc OR 
#       A.sub_brand_description<>B.omni_subcatg_desc OR 
#       A.brand_description<>B.brand_nm OR 
#       A.sub_category_description<>B.prod_desc OR 
#       A.category_description<>B.fineline_desc OR 
#       A.vendor_description<>B.vendor_nm)

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit fact: source_row_count, target_row_count, 
# --------------------------------
val_num_rows_inserted = _sqldf.first()[1]
print(val_num_rows_inserted)
sql_server_write_fact_count("stage_update_count_type2", val_num_rows_inserted)

# COMMAND ----------

#  %sql
#  select * from dev.raw.temp_table_luminate_material_external_dim

# COMMAND ----------

# DBTITLE 1,Incremental data - Type 2 inserts
#  %sql
#  insert into $catalog_name.raw.temp_table_luminate_material_external_dim
#  (material_external_dim_dw_id,
#  source_system_description,
#  material_external_code,
#  material_external_description,
#  universal_product_code,
#  material_external_type_code,
#  material_external_type_description,
#  material_external_status_desc,
#  department_description,
#  segment_code,
#  segment_description,
#  sub_brand_code,
#  sub_brand_description,
#  brand_code,
#  brand_description,
#  sub_category_code,
#  sub_category_description,
#  category_code,
#  category_description,
#  vendor_code,
#  vendor_description,
#  material_attribute_array,
#  valid_from_date,
#  valid_through_date,
#  create_date,
#  change_date,
#  data_quality_valid_flag,
#  data_quality_result_array,
#  load_type)
#  select md5(concat('wmt',A.material_external_code,A.change_date,current_timestamp())) as material_external_dim_dw_id,* from
#  (select 'wmt' as source_system_description,
#  wm_item_nbr as material_external_code,
#  max(item_name) as material_external_description,
#  max(wm_upc_nbr) as universal_product_code,
#  max(item_type_cd) as material_external_type_code,
#  max(item_type_desc) as material_external_type_description,
#  max(item_status_cd) as material_external_status_desc,
#  max(omni_dept_desc) as department_description,
#  max(omni_seg_nbr) as segment_code,
#  max(omni_seg_desc) as segment_description,
#  max(omni_subcatg_nbr) as sub_brand_code,
#  max(omni_subcatg_desc) as sub_brand_description,
#  max(brand_id) as brand_code,
#  max(brand_nm) as brand_description,
#  max(prod_nbr) as sub_category_code,
#  max(prod_desc) as sub_category_description,
#  max(fineline_desc) as category_code,
#  max(fineline_desc) as category_description,
#  max(vendor_nbr) as vendor_code,
#  max(vendor_nm) as vendor_description,
#  to_json(collect_list(struct(op_cmpny_cd as operational_company_name,
#  mds_fam_id as store_material_code,
#  catlg_item_id as external_catalog_material_code,
#  acctg_dept_nbr as accounting_department_number,
#  item_create_dt as material_create_date,
#  obsolete_dt as obsolete_date,
#  wm_upc_nbr as external_upc_number,
#  wm_upc_desc as external_upc_description,
#  all_links_item_desc_1 as all_links_material_description,
#  all_links_item_nbr as all_links_material_code,
#  all_links_mds_fam_id as all_links_store_material_code,
#  base_unit_rtl_amt as base_unit_retail_amount,
#  never_out_ind as never_out_indicator,
#  whse_min_order_qty as warehouse_minimum_order_quantity,
#  pallet_hi_qty as pallet_hi_quantity,
#  pallet_upc_nbr as pallet_upc_number,
#  whpk_qty as warehouse_quantity,
#  whse_algn_type_desc as wh_alignment_type_description,
#  whpk_len_qty as warehouse_length_quantity,
#  whpk_wdth_qty as warehouse_width_quantity,
#  whpk_cube_qty as warehouse_cube_quantity,
#  whpk_ht_qty as warehouse_height_quantity,
#  vendor_dept_nbr as vendor_department_quantity,
#  vndr_min_order_qty as vendor_minimum_order_quantity,
#  vnpk_qty as vendor_quantity,
#  vnpk_wt_qty as vendor_weight_quanity,
#  vnpk_len_qty as vendor_length_quanity,
#  vnpk_wdth_qty as vendor_width_quanity,
#  vnpk_ht_qty as vendor_height_quanity,
#  vendor_seq_nbr as vendor_sequence_number,
#  omni_dept_nbr as omni_department_number,
#  omni_catg_nbr as omni_category_number,
#  omni_catg_desc as omni_category_description,
#  omni_catg_grp_nbr as omni_category_group_number,
#  omni_catg_grp_desc as omni_category_group_description,
#  vendor_stock_id as vendor_stock_number,
#  vendor_nm as vendor_name,
#  brand_ownr_nm as brand_owner_name,
#  brand_fam_nm as brand_family_name,
#  brand_fam_id as brand_family_number,
#  brand_ownr_id as brand_owner_number,
#  vnpk_cost_amt as vendor_cost_amount,
#  whpk_unit_cost as warehouse_unit_cost,
#  item_desc_2 as material_description,
#  color_desc as color_description,
#  size_desc as size_description,
#  repl_subtype_cd as replenishment_subtype_code,
#  repl_subtype_desc as replenishment_subtype_description,
#  item_len_qty as material_length_quantity,
#  item_ht_qty as material_height_quantity,
#  item_wdth_qty as material_width_quantity,
#  item_cube_qty as material_cube_quantity,
#  item_cube_uom_cd as material_cube_uom_code,
#  item_cube_uom_desc as material_cube_uom_description,
#  item_order_eff_dt as material_order_effective_date,
#  sell_qty as sell_quantity,
#  sell_uom_cd as sell_uom_code,
#  sell_uom_desc as sell_uom_description,
#  pallet_ti_qty as pallet_ti_quantity,
#  vnpk_cube_qty as vendor_cube_quantity,
#  vnpk_cube_uom_cd as vendor_cube_uom_code,
#  vnpk_cube_uom_desc as vendor_cube_uom_description,
#  vnpk_cspk_cd as vendor_case_pack_code,
#  vnpk_cspk_desc as vendor_case_pack_description,
#  whpk_upc_nbr as warehouse_upc_number,
#  whpk_wt_qty as warehouse_weight_quantity,
#  whpk_wt_uom_cd as warehouse_weight_uom_code,
#  whpk_wt_uom_desc as warehouse_weight_uom_description,
#  order_sizng_fctr_qty as order_sizing_factory_quantity,
#  repl_unit_ind as replenishment_unit_indicator,
#  repl_group_nbr as replenishment_group_number,
#  fineline_nbr as fineline_number,
#  geo_region_cd as geographic_region_code,
#  base_div_nbr as base_division_number,
#  buyr_rpt_postn_id as buyer_report_position_number,
#  whpk_calc_mthd_desc as wh_calculation_method_description,
#  plnr_rpt_postn_id as planner_report_position_number,
#  acctg_dept_desc as accounting_department_description,
#  cntry_nm as country_name,
#  item_status_desc as material_status_description,
#  prime_xref_item_nbr as prime_cross_reference_material_code,
#  prime_xref_mds_fam_id as prime_xref_store_material_code,
#  prime_lia_item_nbr as prime_local_inv_ads_material_code,
#  prime_lia_mds_fam_id as prime_local_inv_ads_store_matl_code,
#  whse_algn_type_cd as warehouse_alignment_type_code,
#  whse_area_cd as warehouse_area_code,
#  whse_area_desc as warehouse_area_description,
#  whse_rotation_cd as warehouse_rotation_code,
#  whse_rotation_desc as warehouse_rotation_description,
#  whpk_calc_mthd_cd as warehouse_calculation_method_code,
#  whpk_sell_amt as warehouse_sell_amount,
#  plu_nbr as price_lookup_number,
#  cons_item_nbr as consumer_material_code,
#  cons_item_desc as consumer_material_description,
#  cust_base_rtl_amt as customer_base_unit_retail_amount,
#  base_rtl_uom_cd as base_retail_uom_code,
#  base_rtl_uom_desc as base_retail_uom_description,
#  cncl_when_out_ind as cancel_when_out_indicator,
#  item_import_ind as material_import_indicator,
#  item_repl_ind as material_replenishment_indicator,
#  guar_sales_ind as guaranteed_sales_indicator,
#  master_carton_ind as master_carton_indicator,
#  vnpk_wt_uom_cd as vendor_weight_uom_code,
#  vnpk_wt_uom_desc as vendor_weight_uom_description,
#  vnpk_dim_uom_cd as vendor_dimension_uom_code,
#  vnpk_dim_uom_desc as vendor_dimension_uom_description,
#  whse_max_order_qty as warehouse_maximum_order_quantity,
#  whpk_cube_uom_cd as warehouse_cube_uom_code,
#  whpk_cube_uom_desc as warehouse_cube_uom_description,
#  whpk_dim_uom_cd as warehouse_dimension_uom_code,
#  whpk_dim_uom_desc as warehouse_dimension_uom_description,
#  buyg_region_cd as buying_region_code,
#  buyg_region_desc as buying_region_description,
#  shop_desc as shop_description,
#  signing_desc as signing_description,
#  asort_type_cd as asortment_type_code,
#  asort_type_desc as asortment_type_description,
#  seasn_cd as season_code,
#  seasn_desc as season_description,
#  seasn_yr as season_year,
#  item_dim_uom_cd as material_dimension_uom_code,
#  item_dim_uom_desc as material_dimension_uom_description,
#  item_wt_qty as material_weight_quantity,
#  item_wt_uom_cd as material_weight_uom_code,
#  item_wt_uom_desc as material_weight_uom_description,
#  item_expire_dt as material_expire_date,
#  est_out_of_stock_dt as estimated_out_of_stock_date,
#  pallet_size_cd as pallet_size_code,
#  pallet_rnd_pct as pallet_rounding_percent,
#  case_upc_nbr as case_upc_number,
#  vnpk_wt_fmt_cd as vendor_weight_format_code,
#  vnpk_wt_fmt_desc as vendor_weight_format_description,
#  vnpk_net_wt_qty as vendor_net_weight_quantity,
#  vnpk_net_wt_uom_cd as vendor_net_weight_uom_code,
#  modlr_base_mdse_cd as modular_base_merchandise_code,
#  modlr_base_mdse_desc as modular_base_merch_description,
#  item_status_chng_dt as material_status_change_date,
#  item_cost_amt as usd_material_cost_amount,
#  acct_nbr as account_number,
#  acct_nbr_type_cd as account_number_type_code,
#  acct_nbr_type_desc as account_number_type_description,
#  actv_cd as activity_code,
#  actv_desc as activity_description,
#  alcohol_pct as alcohol_percent,
#  alt_chnl_src_ind as alternate_channel_source_indicator,
#  backrm_scale_ind as backroom_scale_indicator,
#  canned_order_ind as canned_order_indicator,
#  case_upc_fmt_cd as case_upc_format_code,
#  case_upc_fmt_desc as case_upc_format_description,
#  catch_wt_ind as catch_weight_indicator,
#  cnsumable_div_nbr as consumable_division_number,
#  cntrl_substance_ind as controlled_substance_indicator,
#  commodity_id as commodity_number,
#  convey_ind as conveyable_indicator,
#  crush_fctr_cd as crush_factor_code,
#  crush_fctr_desc as crush_factor_description,
#  dc_dea_rpt_ind as distrib_cntr_drug_enf_rpt_indicator,
#  dest_cd as destination_code,
#  dest_desc as destination_description,
#  diet_type_cd as diet_type_code,
#  diet_type_desc as diet_type_description,
#  assoc_disc_ind as associate_discount_indicator,
#  fhs_dc_slot_cd as fire_safety_distrib_cntr_slot_code,
#  fhs_dc_slot_desc as fire_safety_distrib_cntr_slot_desc,
#  fsa_ind as flexible_spending_account_indicator,
#  gift_card_face_amt as gift_card_face_amount,
#  gift_card_fee_amt as gift_card_fee_amount,
#  gift_card_fee_pct as gift_card_fee_percent,
#  gift_card_type_cd as gift_card_type_code,
#  gift_card_type_desc as gift_card_type_description,
#  ideal_tempr_hi_qty as ideal_temperature_hi_quantity,
#  ideal_tempr_low_qty as ideal_temperature_low_quantity,
#  infrm_reord_type_cd as inforem_record_type_code,
#  infrm_reord_type_desc as inforem_record_type_description,
#  item_scannable_ind as material_scannable_indicator,
#  item_sync_status_cd as material_synchronized_status_code,
#  item_sync_status_desc as material_sync_status_description,
#  itemfile_src_nm as material_file_source_name,
#  last_order_dt as last_order_date,
#  lic_cd as license_code,
#  lic_desc as license_description,
#  mdse_pgm_id as merchandise_program_number,
#  mfgr_pre_price_amt as manufacturer_pre_price_amount,
#  mfgr_sugd_rtl_amt as manufacturer_suggest_retail_amount,
#  min_price_ind as minimum_price_indicator,
#  min_rcvng_days_qty as minimum_receiving_days_quantity,
#  min_whse_life_qty as minimum_warehouse_life_quantity,
#  non_mbr_upchrg_ind as non_member_upcharge_indicator,
#  osa_supplier_id as on_shelf_avail_supplier_number,
#  pallet_upc_fmt_cd as pallet_upc_format_code,
#  pallet_upc_fmt_desc as pallet_upc_format_description,
#  perfm_ratg_cd as performance_rating_code,
#  perfm_ratg_desc as performance_rating_description,
#  price_comp_qty as price_component_quantity,
#  price_comp_uom_cd as price_component_uom_code,
#  prime_upc_item_nbr as prime_upc_material_code,
#  prime_upc_mds_fam_id as prime_upc_store_material_code,
#  proj_yr_sale_qty as projected_fin_year_sale_quantity,
#  promo_order_book_cd as promo_order_book_code,
#  promo_order_book_desc as promo_order_book_description,
#  prmpt_price_ind as prompt_price_indicator,
#  qlty_cntrl_cd as quality_control_code,
#  qlty_cntrl_desc as quality_contro_description,
#  rsrv_mdse_cd as reserve_merchandise_code,
#  rsrv_mdse_desc as reserve_merchandise_description,
#  rsrv_mdse_ind as reserve_merchandise_indicator,
#  rtl_incl_vat_ind as retail_including_vat_indicator,
#  rtn_resale_ind as return_resale_indicator,
#  rfid_ind as radio_freq_identification_indicator,
#  rppc_ind as rigid_pkg_plastic_cntr_indicator,
#  rtl_notif_store_ind as retail_notification_store_indicator,
#  secur_tag_ind as security_tag_indicator,
#  segregation_cd as segregation_code,
#  segregation_desc as segregation_description,
#  send_store_dt as send_store_date,
#  shelf_lbl_reqmt_ind as shelf_label_requirement_indicator,
#  shelf_rotation_ind as shelf_rotation_indicator,
#  shelf_life_dys_qty as shelf_life_days_quantity,
#  supplier_disp_ind as supplier_display_indicator,
#  tempr_sensitive_ind as temperature_sensitive_indicator,
#  tempr_uom_cd as temperature_uom_code,
#  upc_fmt_cd as upc_format_code,
#  upc_fmt_desc as upc_format_description,
#  var_comp_ind as variable_component_indicator,
#  var_wt_ind as variable_weight_indicator,
#  vndr_first_order_dt as vendor_first_order_date,
#  vndr_first_ship_dt as vendor_first_ship_date,
#  vndr_first_avlbl_dt as vendor_first_available_date,
#  vndr_incrm_order_qty as vendor_incremental_order_quantity,
#  vndr_last_ship_dt as vendor_last_ship_date,
#  vndr_ld_tm_qty as vendor_lead_time_quantity,
#  vndr_min_order_uom_cd as vendor_minimum_order_uom_code,
#  whpk_net_wt_qty as warehouse_net_weight_quantity,
#  whpk_upc_fmt_cd as warehouse_upc_format_code,
#  whpk_upc_fmt_desc as warehouse_upc_format_description
#  ))) as material_attribute_array,
#  '${process.enddate}' as valid_from_date,
#  '9999-12-31' as valid_through_date,
#  current_timestamp() as create_date,
#  current_timestamp() as change_date,
#  null as data_quality_valid_flag,
#  null as data_quality_result_array,
#  'N2' as load_type
#  from $catalog_name.raw.wmt_itemattributes as A 
#  Inner join 
#  (select * from $catalog_name.raw.temp_table_luminate_material_external_dim where load_type='U2') as B
#  on A.wm_item_nbr=B.material_external_code
#  where A.change_date>='${process.startdate}' and A.change_date<='${process.enddate}'
#  group by wm_item_nbr
#  ) as A 

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit fact: source_row_count, target_row_count, 
# --------------------------------
val_num_rows_inserted = _sqldf.first()[1]
print(val_num_rows_inserted)
sql_server_write_fact_count("stage_insert_count_type2", val_num_rows_inserted)

# COMMAND ----------

# DBTITLE 1,Incremental Data - New Records
#  %sql
#  insert into $catalog_name.raw.temp_table_luminate_material_external_dim
#  (material_external_dim_dw_id,
#  source_system_description,
#  material_external_code,
#  material_external_description,
#  universal_product_code,
#  material_external_type_code,
#  material_external_type_description,
#  material_external_status_desc,
#  department_description,
#  segment_code,
#  segment_description,
#  sub_brand_code,
#  sub_brand_description,
#  brand_code,
#  brand_description,
#  sub_category_code,
#  sub_category_description,
#  category_code,
#  category_description,
#  vendor_code,
#  vendor_description,
#  material_attribute_array,
#  valid_from_date,
#  valid_through_date,
#  create_date,
#  change_date,
#  data_quality_valid_flag,
#  data_quality_result_array,
#  load_type)
#  select md5(concat('wmt',A.material_external_code,A.change_date,current_timestamp())) as material_external_dim_dw_id,* from
#  (select 'wmt' as source_system_description,
#  wm_item_nbr as material_external_code,
#  max(item_name) as material_external_description,
#  max(wm_upc_nbr) as universal_product_code,
#  max(item_type_cd) as material_external_type_code,
#  max(item_type_desc) as material_external_type_description,
#  max(item_status_cd) as material_external_status_desc,
#  max(omni_dept_desc) as department_description,
#  max(omni_seg_nbr) as segment_code,
#  max(omni_seg_desc) as segment_description,
#  max(omni_subcatg_nbr) as sub_brand_code,
#  max(omni_subcatg_desc) as sub_brand_description,
#  max(brand_id) as brand_code,
#  max(brand_nm) as brand_description,
#  max(prod_nbr) as sub_category_code,
#  max(prod_desc) as sub_category_description,
#  max(fineline_desc) as category_code,
#  max(fineline_desc) as category_description,
#  max(vendor_nbr) as vendor_code,
#  max(vendor_nm) as vendor_description,
#  to_json(collect_list(struct(op_cmpny_cd as operational_company_name,
#  mds_fam_id as store_material_code,
#  catlg_item_id as external_catalog_material_code,
#  acctg_dept_nbr as accounting_department_number,
#  item_create_dt as material_create_date,
#  obsolete_dt as obsolete_date,
#  wm_upc_nbr as external_upc_number,
#  wm_upc_desc as external_upc_description,
#  all_links_item_desc_1 as all_links_material_description,
#  all_links_item_nbr as all_links_material_code,
#  all_links_mds_fam_id as all_links_store_material_code,
#  base_unit_rtl_amt as base_unit_retail_amount,
#  never_out_ind as never_out_indicator,
#  whse_min_order_qty as warehouse_minimum_order_quantity,
#  pallet_hi_qty as pallet_hi_quantity,
#  pallet_upc_nbr as pallet_upc_number,
#  whpk_qty as warehouse_quantity,
#  whse_algn_type_desc as wh_alignment_type_description,
#  whpk_len_qty as warehouse_length_quantity,
#  whpk_wdth_qty as warehouse_width_quantity,
#  whpk_cube_qty as warehouse_cube_quantity,
#  whpk_ht_qty as warehouse_height_quantity,
#  vendor_dept_nbr as vendor_department_quantity,
#  vndr_min_order_qty as vendor_minimum_order_quantity,
#  vnpk_qty as vendor_quantity,
#  vnpk_wt_qty as vendor_weight_quanity,
#  vnpk_len_qty as vendor_length_quanity,
#  vnpk_wdth_qty as vendor_width_quanity,
#  vnpk_ht_qty as vendor_height_quanity,
#  vendor_seq_nbr as vendor_sequence_number,
#  omni_dept_nbr as omni_department_number,
#  omni_catg_nbr as omni_category_number,
#  omni_catg_desc as omni_category_description,
#  omni_catg_grp_nbr as omni_category_group_number,
#  omni_catg_grp_desc as omni_category_group_description,
#  vendor_stock_id as vendor_stock_number,
#  vendor_nm as vendor_name,
#  brand_ownr_nm as brand_owner_name,
#  brand_fam_nm as brand_family_name,
#  brand_fam_id as brand_family_number,
#  brand_ownr_id as brand_owner_number,
#  vnpk_cost_amt as vendor_cost_amount,
#  whpk_unit_cost as warehouse_unit_cost,
#  item_desc_2 as material_description,
#  color_desc as color_description,
#  size_desc as size_description,
#  repl_subtype_cd as replenishment_subtype_code,
#  repl_subtype_desc as replenishment_subtype_description,
#  item_len_qty as material_length_quantity,
#  item_ht_qty as material_height_quantity,
#  item_wdth_qty as material_width_quantity,
#  item_cube_qty as material_cube_quantity,
#  item_cube_uom_cd as material_cube_uom_code,
#  item_cube_uom_desc as material_cube_uom_description,
#  item_order_eff_dt as material_order_effective_date,
#  sell_qty as sell_quantity,
#  sell_uom_cd as sell_uom_code,
#  sell_uom_desc as sell_uom_description,
#  pallet_ti_qty as pallet_ti_quantity,
#  vnpk_cube_qty as vendor_cube_quantity,
#  vnpk_cube_uom_cd as vendor_cube_uom_code,
#  vnpk_cube_uom_desc as vendor_cube_uom_description,
#  vnpk_cspk_cd as vendor_case_pack_code,
#  vnpk_cspk_desc as vendor_case_pack_description,
#  whpk_upc_nbr as warehouse_upc_number,
#  whpk_wt_qty as warehouse_weight_quantity,
#  whpk_wt_uom_cd as warehouse_weight_uom_code,
#  whpk_wt_uom_desc as warehouse_weight_uom_description,
#  order_sizng_fctr_qty as order_sizing_factory_quantity,
#  repl_unit_ind as replenishment_unit_indicator,
#  repl_group_nbr as replenishment_group_number,
#  fineline_nbr as fineline_number,
#  geo_region_cd as geographic_region_code,
#  base_div_nbr as base_division_number,
#  buyr_rpt_postn_id as buyer_report_position_number,
#  whpk_calc_mthd_desc as wh_calculation_method_description,
#  plnr_rpt_postn_id as planner_report_position_number,
#  acctg_dept_desc as accounting_department_description,
#  cntry_nm as country_name,
#  item_status_desc as material_status_description,
#  prime_xref_item_nbr as prime_cross_reference_material_code,
#  prime_xref_mds_fam_id as prime_xref_store_material_code,
#  prime_lia_item_nbr as prime_local_inv_ads_material_code,
#  prime_lia_mds_fam_id as prime_local_inv_ads_store_matl_code,
#  whse_algn_type_cd as warehouse_alignment_type_code,
#  whse_area_cd as warehouse_area_code,
#  whse_area_desc as warehouse_area_description,
#  whse_rotation_cd as warehouse_rotation_code,
#  whse_rotation_desc as warehouse_rotation_description,
#  whpk_calc_mthd_cd as warehouse_calculation_method_code,
#  whpk_sell_amt as warehouse_sell_amount,
#  plu_nbr as price_lookup_number,
#  cons_item_nbr as consumer_material_code,
#  cons_item_desc as consumer_material_description,
#  cust_base_rtl_amt as customer_base_unit_retail_amount,
#  base_rtl_uom_cd as base_retail_uom_code,
#  base_rtl_uom_desc as base_retail_uom_description,
#  cncl_when_out_ind as cancel_when_out_indicator,
#  item_import_ind as material_import_indicator,
#  item_repl_ind as material_replenishment_indicator,
#  guar_sales_ind as guaranteed_sales_indicator,
#  master_carton_ind as master_carton_indicator,
#  vnpk_wt_uom_cd as vendor_weight_uom_code,
#  vnpk_wt_uom_desc as vendor_weight_uom_description,
#  vnpk_dim_uom_cd as vendor_dimension_uom_code,
#  vnpk_dim_uom_desc as vendor_dimension_uom_description,
#  whse_max_order_qty as warehouse_maximum_order_quantity,
#  whpk_cube_uom_cd as warehouse_cube_uom_code,
#  whpk_cube_uom_desc as warehouse_cube_uom_description,
#  whpk_dim_uom_cd as warehouse_dimension_uom_code,
#  whpk_dim_uom_desc as warehouse_dimension_uom_description,
#  buyg_region_cd as buying_region_code,
#  buyg_region_desc as buying_region_description,
#  shop_desc as shop_description,
#  signing_desc as signing_description,
#  asort_type_cd as asortment_type_code,
#  asort_type_desc as asortment_type_description,
#  seasn_cd as season_code,
#  seasn_desc as season_description,
#  seasn_yr as season_year,
#  item_dim_uom_cd as material_dimension_uom_code,
#  item_dim_uom_desc as material_dimension_uom_description,
#  item_wt_qty as material_weight_quantity,
#  item_wt_uom_cd as material_weight_uom_code,
#  item_wt_uom_desc as material_weight_uom_description,
#  item_expire_dt as material_expire_date,
#  est_out_of_stock_dt as estimated_out_of_stock_date,
#  pallet_size_cd as pallet_size_code,
#  pallet_rnd_pct as pallet_rounding_percent,
#  case_upc_nbr as case_upc_number,
#  vnpk_wt_fmt_cd as vendor_weight_format_code,
#  vnpk_wt_fmt_desc as vendor_weight_format_description,
#  vnpk_net_wt_qty as vendor_net_weight_quantity,
#  vnpk_net_wt_uom_cd as vendor_net_weight_uom_code,
#  modlr_base_mdse_cd as modular_base_merchandise_code,
#  modlr_base_mdse_desc as modular_base_merch_description,
#  item_status_chng_dt as material_status_change_date,
#  item_cost_amt as usd_material_cost_amount,
#  acct_nbr as account_number,
#  acct_nbr_type_cd as account_number_type_code,
#  acct_nbr_type_desc as account_number_type_description,
#  actv_cd as activity_code,
#  actv_desc as activity_description,
#  alcohol_pct as alcohol_percent,
#  alt_chnl_src_ind as alternate_channel_source_indicator,
#  backrm_scale_ind as backroom_scale_indicator,
#  canned_order_ind as canned_order_indicator,
#  case_upc_fmt_cd as case_upc_format_code,
#  case_upc_fmt_desc as case_upc_format_description,
#  catch_wt_ind as catch_weight_indicator,
#  cnsumable_div_nbr as consumable_division_number,
#  cntrl_substance_ind as controlled_substance_indicator,
#  commodity_id as commodity_number,
#  convey_ind as conveyable_indicator,
#  crush_fctr_cd as crush_factor_code,
#  crush_fctr_desc as crush_factor_description,
#  dc_dea_rpt_ind as distrib_cntr_drug_enf_rpt_indicator,
#  dest_cd as destination_code,
#  dest_desc as destination_description,
#  diet_type_cd as diet_type_code,
#  diet_type_desc as diet_type_description,
#  assoc_disc_ind as associate_discount_indicator,
#  fhs_dc_slot_cd as fire_safety_distrib_cntr_slot_code,
#  fhs_dc_slot_desc as fire_safety_distrib_cntr_slot_desc,
#  fsa_ind as flexible_spending_account_indicator,
#  gift_card_face_amt as gift_card_face_amount,
#  gift_card_fee_amt as gift_card_fee_amount,
#  gift_card_fee_pct as gift_card_fee_percent,
#  gift_card_type_cd as gift_card_type_code,
#  gift_card_type_desc as gift_card_type_description,
#  ideal_tempr_hi_qty as ideal_temperature_hi_quantity,
#  ideal_tempr_low_qty as ideal_temperature_low_quantity,
#  infrm_reord_type_cd as inforem_record_type_code,
#  infrm_reord_type_desc as inforem_record_type_description,
#  item_scannable_ind as material_scannable_indicator,
#  item_sync_status_cd as material_synchronized_status_code,
#  item_sync_status_desc as material_sync_status_description,
#  itemfile_src_nm as material_file_source_name,
#  last_order_dt as last_order_date,
#  lic_cd as license_code,
#  lic_desc as license_description,
#  mdse_pgm_id as merchandise_program_number,
#  mfgr_pre_price_amt as manufacturer_pre_price_amount,
#  mfgr_sugd_rtl_amt as manufacturer_suggest_retail_amount,
#  min_price_ind as minimum_price_indicator,
#  min_rcvng_days_qty as minimum_receiving_days_quantity,
#  min_whse_life_qty as minimum_warehouse_life_quantity,
#  non_mbr_upchrg_ind as non_member_upcharge_indicator,
#  osa_supplier_id as on_shelf_avail_supplier_number,
#  pallet_upc_fmt_cd as pallet_upc_format_code,
#  pallet_upc_fmt_desc as pallet_upc_format_description,
#  perfm_ratg_cd as performance_rating_code,
#  perfm_ratg_desc as performance_rating_description,
#  price_comp_qty as price_component_quantity,
#  price_comp_uom_cd as price_component_uom_code,
#  prime_upc_item_nbr as prime_upc_material_code,
#  prime_upc_mds_fam_id as prime_upc_store_material_code,
#  proj_yr_sale_qty as projected_fin_year_sale_quantity,
#  promo_order_book_cd as promo_order_book_code,
#  promo_order_book_desc as promo_order_book_description,
#  prmpt_price_ind as prompt_price_indicator,
#  qlty_cntrl_cd as quality_control_code,
#  qlty_cntrl_desc as quality_contro_description,
#  rsrv_mdse_cd as reserve_merchandise_code,
#  rsrv_mdse_desc as reserve_merchandise_description,
#  rsrv_mdse_ind as reserve_merchandise_indicator,
#  rtl_incl_vat_ind as retail_including_vat_indicator,
#  rtn_resale_ind as return_resale_indicator,
#  rfid_ind as radio_freq_identification_indicator,
#  rppc_ind as rigid_pkg_plastic_cntr_indicator,
#  rtl_notif_store_ind as retail_notification_store_indicator,
#  secur_tag_ind as security_tag_indicator,
#  segregation_cd as segregation_code,
#  segregation_desc as segregation_description,
#  send_store_dt as send_store_date,
#  shelf_lbl_reqmt_ind as shelf_label_requirement_indicator,
#  shelf_rotation_ind as shelf_rotation_indicator,
#  shelf_life_dys_qty as shelf_life_days_quantity,
#  supplier_disp_ind as supplier_display_indicator,
#  tempr_sensitive_ind as temperature_sensitive_indicator,
#  tempr_uom_cd as temperature_uom_code,
#  upc_fmt_cd as upc_format_code,
#  upc_fmt_desc as upc_format_description,
#  var_comp_ind as variable_component_indicator,
#  var_wt_ind as variable_weight_indicator,
#  vndr_first_order_dt as vendor_first_order_date,
#  vndr_first_ship_dt as vendor_first_ship_date,
#  vndr_first_avlbl_dt as vendor_first_available_date,
#  vndr_incrm_order_qty as vendor_incremental_order_quantity,
#  vndr_last_ship_dt as vendor_last_ship_date,
#  vndr_ld_tm_qty as vendor_lead_time_quantity,
#  vndr_min_order_uom_cd as vendor_minimum_order_uom_code,
#  whpk_net_wt_qty as warehouse_net_weight_quantity,
#  whpk_upc_fmt_cd as warehouse_upc_format_code,
#  whpk_upc_fmt_desc as warehouse_upc_format_description
#  ))) as material_attribute_array,
#  '1900-01-01' as valid_from_date,
#  '9999-12-31' as valid_through_date,
#  current_timestamp() as create_date,
#  current_timestamp() as change_date,
#  null as data_quality_valid_flag,
#  null as data_quality_result_array,
#  'N' as load_type
#  from $catalog_name.raw.wmt_itemattributes as A 
#  LEFT join 
#  (select * from $catalog_name.curated.material_external_dim where valid_through_date ='9999-12-31' and source_system_description='wmt') as B
#  on A.wm_item_nbr=B.material_external_code
#  where A.change_date>='${process.startdate}' and A.change_date<='${process.enddate}' and  B.material_external_code is null
#  GROUP BY wm_item_nbr
#  ) as A

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit fact: source_row_count, target_row_count, 
# --------------------------------
val_num_rows_inserted = _sqldf.first()[1]
print(val_num_rows_inserted)
sql_server_write_fact_count("stage_new_record_count", val_num_rows_inserted)

# COMMAND ----------

#  %sql
#  select * from dev.raw.temp_table_luminate_material_external_dim

# COMMAND ----------

# DBTITLE 1,Type 1 - updates ---needs an update for array column
#  %sql
#  insert into $catalog_name.raw.temp_table_luminate_material_external_dim
#  (material_external_dim_dw_id,
#  source_system_description,
#  material_external_code,
#  material_external_description,
#  universal_product_code,
#  material_external_type_code,
#  material_external_type_description,
#  material_external_status_desc,
#  department_description,
#  segment_code,
#  segment_description,
#  sub_brand_code,
#  sub_brand_description,
#  brand_code,
#  brand_description,
#  sub_category_code,
#  sub_category_description,
#  category_code,
#  category_description,
#  vendor_code,
#  vendor_description,
#  material_attribute_array,
#  valid_from_date,
#  valid_through_date,
#  create_date,
#  change_date,
#  data_quality_valid_flag,
#  data_quality_result_array,
#  load_type)
#  select A.material_external_dim_dw_id,
#  A.source_system_description,
#  wm_item_nbr as material_external_code,
#  max(item_name) as material_external_description,
#  max(wm_upc_nbr) as universal_product_code,
#  max(item_type_cd) as material_external_type_code,
#  max(item_type_desc) as material_external_type_description,
#  max(item_status_cd) as material_external_status_desc,
#  max(omni_dept_desc) as department_description,
#  max(omni_seg_nbr) as segment_code,
#  max(omni_seg_desc) as segment_description,
#  max(omni_subcatg_nbr) as sub_brand_code,
#  max(omni_subcatg_desc) as sub_brand_description,
#  max(brand_id) as brand_code,
#  max(brand_nm) as brand_description,
#  max(prod_nbr) as sub_category_code,
#  max(prod_desc) as sub_category_description,
#  max(fineline_desc) as category_code,
#  max(fineline_desc) as category_description,
#  max(vendor_nbr) as vendor_code,
#  max(vendor_nm) as vendor_description,
#  to_json(collect_list(struct(op_cmpny_cd as operational_company_name,
#  mds_fam_id as store_material_code,
#  catlg_item_id as external_catalog_material_code,
#  acctg_dept_nbr as accounting_department_number,
#  item_create_dt as material_create_date,
#  obsolete_dt as obsolete_date,
#  wm_upc_nbr as external_upc_number,
#  wm_upc_desc as external_upc_description,
#  all_links_item_desc_1 as all_links_material_description,
#  all_links_item_nbr as all_links_material_code,
#  all_links_mds_fam_id as all_links_store_material_code,
#  base_unit_rtl_amt as base_unit_retail_amount,
#  never_out_ind as never_out_indicator,
#  whse_min_order_qty as warehouse_minimum_order_quantity,
#  pallet_hi_qty as pallet_hi_quantity,
#  pallet_upc_nbr as pallet_upc_number,
#  whpk_qty as warehouse_quantity,
#  whse_algn_type_desc as wh_alignment_type_description,
#  whpk_len_qty as warehouse_length_quantity,
#  whpk_wdth_qty as warehouse_width_quantity,
#  whpk_cube_qty as warehouse_cube_quantity,
#  whpk_ht_qty as warehouse_height_quantity,
#  vendor_dept_nbr as vendor_department_quantity,
#  vndr_min_order_qty as vendor_minimum_order_quantity,
#  vnpk_qty as vendor_quantity,
#  vnpk_wt_qty as vendor_weight_quanity,
#  vnpk_len_qty as vendor_length_quanity,
#  vnpk_wdth_qty as vendor_width_quanity,
#  vnpk_ht_qty as vendor_height_quanity,
#  vendor_seq_nbr as vendor_sequence_number,
#  omni_dept_nbr as omni_department_number,
#  omni_catg_nbr as omni_category_number,
#  omni_catg_desc as omni_category_description,
#  omni_catg_grp_nbr as omni_category_group_number,
#  omni_catg_grp_desc as omni_category_group_description,
#  vendor_stock_id as vendor_stock_number,
#  vendor_nm as vendor_name,
#  brand_ownr_nm as brand_owner_name,
#  brand_fam_nm as brand_family_name,
#  brand_fam_id as brand_family_number,
#  brand_ownr_id as brand_owner_number,
#  vnpk_cost_amt as vendor_cost_amount,
#  whpk_unit_cost as warehouse_unit_cost,
#  item_desc_2 as material_description,
#  color_desc as color_description,
#  size_desc as size_description,
#  repl_subtype_cd as replenishment_subtype_code,
#  repl_subtype_desc as replenishment_subtype_description,
#  item_len_qty as material_length_quantity,
#  item_ht_qty as material_height_quantity,
#  item_wdth_qty as material_width_quantity,
#  item_cube_qty as material_cube_quantity,
#  item_cube_uom_cd as material_cube_uom_code,
#  item_cube_uom_desc as material_cube_uom_description,
#  item_order_eff_dt as material_order_effective_date,
#  sell_qty as sell_quantity,
#  sell_uom_cd as sell_uom_code,
#  sell_uom_desc as sell_uom_description,
#  pallet_ti_qty as pallet_ti_quantity,
#  vnpk_cube_qty as vendor_cube_quantity,
#  vnpk_cube_uom_cd as vendor_cube_uom_code,
#  vnpk_cube_uom_desc as vendor_cube_uom_description,
#  vnpk_cspk_cd as vendor_case_pack_code,
#  vnpk_cspk_desc as vendor_case_pack_description,
#  whpk_upc_nbr as warehouse_upc_number,
#  whpk_wt_qty as warehouse_weight_quantity,
#  whpk_wt_uom_cd as warehouse_weight_uom_code,
#  whpk_wt_uom_desc as warehouse_weight_uom_description,
#  order_sizng_fctr_qty as order_sizing_factory_quantity,
#  repl_unit_ind as replenishment_unit_indicator,
#  repl_group_nbr as replenishment_group_number,
#  fineline_nbr as fineline_number,
#  geo_region_cd as geographic_region_code,
#  base_div_nbr as base_division_number,
#  buyr_rpt_postn_id as buyer_report_position_number,
#  whpk_calc_mthd_desc as wh_calculation_method_description,
#  plnr_rpt_postn_id as planner_report_position_number,
#  acctg_dept_desc as accounting_department_description,
#  cntry_nm as country_name,
#  item_status_desc as material_status_description,
#  prime_xref_item_nbr as prime_cross_reference_material_code,
#  prime_xref_mds_fam_id as prime_xref_store_material_code,
#  prime_lia_item_nbr as prime_local_inv_ads_material_code,
#  prime_lia_mds_fam_id as prime_local_inv_ads_store_matl_code,
#  whse_algn_type_cd as warehouse_alignment_type_code,
#  whse_area_cd as warehouse_area_code,
#  whse_area_desc as warehouse_area_description,
#  whse_rotation_cd as warehouse_rotation_code,
#  whse_rotation_desc as warehouse_rotation_description,
#  whpk_calc_mthd_cd as warehouse_calculation_method_code,
#  whpk_sell_amt as warehouse_sell_amount,
#  plu_nbr as price_lookup_number,
#  cons_item_nbr as consumer_material_code,
#  cons_item_desc as consumer_material_description,
#  cust_base_rtl_amt as customer_base_unit_retail_amount,
#  base_rtl_uom_cd as base_retail_uom_code,
#  base_rtl_uom_desc as base_retail_uom_description,
#  cncl_when_out_ind as cancel_when_out_indicator,
#  item_import_ind as material_import_indicator,
#  item_repl_ind as material_replenishment_indicator,
#  guar_sales_ind as guaranteed_sales_indicator,
#  master_carton_ind as master_carton_indicator,
#  vnpk_wt_uom_cd as vendor_weight_uom_code,
#  vnpk_wt_uom_desc as vendor_weight_uom_description,
#  vnpk_dim_uom_cd as vendor_dimension_uom_code,
#  vnpk_dim_uom_desc as vendor_dimension_uom_description,
#  whse_max_order_qty as warehouse_maximum_order_quantity,
#  whpk_cube_uom_cd as warehouse_cube_uom_code,
#  whpk_cube_uom_desc as warehouse_cube_uom_description,
#  whpk_dim_uom_cd as warehouse_dimension_uom_code,
#  whpk_dim_uom_desc as warehouse_dimension_uom_description,
#  buyg_region_cd as buying_region_code,
#  buyg_region_desc as buying_region_description,
#  shop_desc as shop_description,
#  signing_desc as signing_description,
#  asort_type_cd as asortment_type_code,
#  asort_type_desc as asortment_type_description,
#  seasn_cd as season_code,
#  seasn_desc as season_description,
#  seasn_yr as season_year,
#  item_dim_uom_cd as material_dimension_uom_code,
#  item_dim_uom_desc as material_dimension_uom_description,
#  item_wt_qty as material_weight_quantity,
#  item_wt_uom_cd as material_weight_uom_code,
#  item_wt_uom_desc as material_weight_uom_description,
#  item_expire_dt as material_expire_date,
#  est_out_of_stock_dt as estimated_out_of_stock_date,
#  pallet_size_cd as pallet_size_code,
#  pallet_rnd_pct as pallet_rounding_percent,
#  case_upc_nbr as case_upc_number,
#  vnpk_wt_fmt_cd as vendor_weight_format_code,
#  vnpk_wt_fmt_desc as vendor_weight_format_description,
#  vnpk_net_wt_qty as vendor_net_weight_quantity,
#  vnpk_net_wt_uom_cd as vendor_net_weight_uom_code,
#  modlr_base_mdse_cd as modular_base_merchandise_code,
#  modlr_base_mdse_desc as modular_base_merch_description,
#  item_status_chng_dt as material_status_change_date,
#  item_cost_amt as usd_material_cost_amount,
#  acct_nbr as account_number,
#  acct_nbr_type_cd as account_number_type_code,
#  acct_nbr_type_desc as account_number_type_description,
#  actv_cd as activity_code,
#  actv_desc as activity_description,
#  alcohol_pct as alcohol_percent,
#  alt_chnl_src_ind as alternate_channel_source_indicator,
#  backrm_scale_ind as backroom_scale_indicator,
#  canned_order_ind as canned_order_indicator,
#  case_upc_fmt_cd as case_upc_format_code,
#  case_upc_fmt_desc as case_upc_format_description,
#  catch_wt_ind as catch_weight_indicator,
#  cnsumable_div_nbr as consumable_division_number,
#  cntrl_substance_ind as controlled_substance_indicator,
#  commodity_id as commodity_number,
#  convey_ind as conveyable_indicator,
#  crush_fctr_cd as crush_factor_code,
#  crush_fctr_desc as crush_factor_description,
#  dc_dea_rpt_ind as distrib_cntr_drug_enf_rpt_indicator,
#  dest_cd as destination_code,
#  dest_desc as destination_description,
#  diet_type_cd as diet_type_code,
#  diet_type_desc as diet_type_description,
#  assoc_disc_ind as associate_discount_indicator,
#  fhs_dc_slot_cd as fire_safety_distrib_cntr_slot_code,
#  fhs_dc_slot_desc as fire_safety_distrib_cntr_slot_desc,
#  fsa_ind as flexible_spending_account_indicator,
#  gift_card_face_amt as gift_card_face_amount,
#  gift_card_fee_amt as gift_card_fee_amount,
#  gift_card_fee_pct as gift_card_fee_percent,
#  gift_card_type_cd as gift_card_type_code,
#  gift_card_type_desc as gift_card_type_description,
#  ideal_tempr_hi_qty as ideal_temperature_hi_quantity,
#  ideal_tempr_low_qty as ideal_temperature_low_quantity,
#  infrm_reord_type_cd as inforem_record_type_code,
#  infrm_reord_type_desc as inforem_record_type_description,
#  item_scannable_ind as material_scannable_indicator,
#  item_sync_status_cd as material_synchronized_status_code,
#  item_sync_status_desc as material_sync_status_description,
#  itemfile_src_nm as material_file_source_name,
#  last_order_dt as last_order_date,
#  lic_cd as license_code,
#  lic_desc as license_description,
#  mdse_pgm_id as merchandise_program_number,
#  mfgr_pre_price_amt as manufacturer_pre_price_amount,
#  mfgr_sugd_rtl_amt as manufacturer_suggest_retail_amount,
#  min_price_ind as minimum_price_indicator,
#  min_rcvng_days_qty as minimum_receiving_days_quantity,
#  min_whse_life_qty as minimum_warehouse_life_quantity,
#  non_mbr_upchrg_ind as non_member_upcharge_indicator,
#  osa_supplier_id as on_shelf_avail_supplier_number,
#  pallet_upc_fmt_cd as pallet_upc_format_code,
#  pallet_upc_fmt_desc as pallet_upc_format_description,
#  perfm_ratg_cd as performance_rating_code,
#  perfm_ratg_desc as performance_rating_description,
#  price_comp_qty as price_component_quantity,
#  price_comp_uom_cd as price_component_uom_code,
#  prime_upc_item_nbr as prime_upc_material_code,
#  prime_upc_mds_fam_id as prime_upc_store_material_code,
#  proj_yr_sale_qty as projected_fin_year_sale_quantity,
#  promo_order_book_cd as promo_order_book_code,
#  promo_order_book_desc as promo_order_book_description,
#  prmpt_price_ind as prompt_price_indicator,
#  qlty_cntrl_cd as quality_control_code,
#  qlty_cntrl_desc as quality_contro_description,
#  rsrv_mdse_cd as reserve_merchandise_code,
#  rsrv_mdse_desc as reserve_merchandise_description,
#  rsrv_mdse_ind as reserve_merchandise_indicator,
#  rtl_incl_vat_ind as retail_including_vat_indicator,
#  rtn_resale_ind as return_resale_indicator,
#  rfid_ind as radio_freq_identification_indicator,
#  rppc_ind as rigid_pkg_plastic_cntr_indicator,
#  rtl_notif_store_ind as retail_notification_store_indicator,
#  secur_tag_ind as security_tag_indicator,
#  segregation_cd as segregation_code,
#  segregation_desc as segregation_description,
#  send_store_dt as send_store_date,
#  shelf_lbl_reqmt_ind as shelf_label_requirement_indicator,
#  shelf_rotation_ind as shelf_rotation_indicator,
#  shelf_life_dys_qty as shelf_life_days_quantity,
#  supplier_disp_ind as supplier_display_indicator,
#  tempr_sensitive_ind as temperature_sensitive_indicator,
#  tempr_uom_cd as temperature_uom_code,
#  upc_fmt_cd as upc_format_code,
#  upc_fmt_desc as upc_format_description,
#  var_comp_ind as variable_component_indicator,
#  var_wt_ind as variable_weight_indicator,
#  vndr_first_order_dt as vendor_first_order_date,
#  vndr_first_ship_dt as vendor_first_ship_date,
#  vndr_first_avlbl_dt as vendor_first_available_date,
#  vndr_incrm_order_qty as vendor_incremental_order_quantity,
#  vndr_last_ship_dt as vendor_last_ship_date,
#  vndr_ld_tm_qty as vendor_lead_time_quantity,
#  vndr_min_order_uom_cd as vendor_minimum_order_uom_code,
#  whpk_net_wt_qty as warehouse_net_weight_quantity,
#  whpk_upc_fmt_cd as warehouse_upc_format_code,
#  whpk_upc_fmt_desc as warehouse_upc_format_description
#  ))) as material_attribute_array,
#  max(A.valid_from_date) as valid_from_date,
#  max(A.valid_through_date) as valid_through_date,
#  max(A.create_date) as create_date,
#  current_timestamp() as change_date,
#  max(A.data_quality_valid_flag) as data_quality_valid_flag,
#  max(A.data_quality_result_array) as data_quality_result_array,
#  'U1' as load_type
#  from  $catalog_name.curated.material_external_dim as A 
#  JOIN $catalog_name.raw.wmt_itemattributes as B on  A.material_external_code=B.wm_item_nbr
#  LEFT JOIN (select * from $catalog_name.raw.temp_table_luminate_material_external_dim where load_type='U2') as C 
#  on A.source_system_description=C.source_system_description and A.material_external_code=C.material_external_code
#  where A.valid_through_date ='9999-12-31' and A.source_system_description='wmt' 
#  and B.change_date>='${process.startdate}' and B.change_date<='${process.enddate}'
#  and C.material_external_code is null
#  GROUP BY A.material_external_dim_dw_id,A.source_system_description,wm_item_nbr

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit fact: source_row_count, target_row_count, 
# --------------------------------
val_num_rows_inserted = _sqldf.first()[1]
print(val_num_rows_inserted)
sql_server_write_fact_count("stage_update_count_type1", val_num_rows_inserted)

# COMMAND ----------

#  %sql
#  --select count(1)
#  --from  dev.curated.distrib_center_external_dim_test as A 
#  --JOIN dev.raw.wmt_dcdimensions as B 
#  --on  A.dc_no=B.dc_nbr and A.geo_region_code=B.geo_region_cd
#  --LEFT JOIN (select * from dev.raw.temp_table_distrib_center_external_dim where load_type='U2') as C 
#  --on A.source_system=C.Source_system and A.dc_no=C.dc_no and A.geo_region_code=C.geo_region_code
#  --where A.valid_to_timestamp ='2099-12-31' and A.source_system='wmt' 
#  --and B.change_date>='${process.startdate}' and B.change_date<='${process.enddate}'
#  --and C.dc_no is null

# COMMAND ----------

#  %sql
#  select * from dev.raw.temp_table_luminate_material_external_dim

# COMMAND ----------

# DBTITLE 1,Merge the data from staging to final table
#  %sql
#  MERGE INTO $catalog_name.curated.material_external_dim as t
#  USING (
#  select material_external_dim_dw_id,
#  source_system_description,
#  material_external_code,
#  valid_from_date,
#  material_external_description,
#  universal_product_code,
#  material_external_type_code,
#  material_external_type_description,
#  material_external_status_desc,
#  department_description,
#  segment_code,
#  segment_description,
#  sub_brand_code,
#  sub_brand_description,
#  brand_code,
#  brand_description,
#  sub_category_code,
#  sub_category_description,
#  category_code,
#  category_description,
#  vendor_code,
#  vendor_description,
#  '' as manufacturer_group_description,
#  material_attribute_array,
#  create_date,
#  change_date,
#  valid_through_date,
#  data_quality_valid_flag,
#  data_quality_result_array
#  from $catalog_name.raw.temp_table_luminate_material_external_dim
#  ) as s
#  on s.material_external_dim_dw_id=t.material_external_dim_dw_id and s.source_system_description=t.source_system_description
#  --WHEN MATCHED AND s.rec_status_flag = 'd' THEN DELETE
#  --WHEN MATCHED AND s.rec_status_flag <> 'd' THEN UPDATE SET *
#  WHEN MATCHED THEN UPDATE SET *
#  --WHEN NOT MATCHED AND s.rec_status_flag <> 'd' THEN INSERT *;
#  WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit fact: source_row_count, target_row_count, 
# --------------------------------
val_num_rows_inserted = _sqldf.first()[3]
val_num_rows_updated = _sqldf.first()[1]
cnt_target = spark.sql(f"select count(*) cnt from {val_target_table}").first()['cnt']
print(val_num_rows_inserted)
print(val_num_rows_updated)
sql_server_write_fact_count("final_target_update_count", val_num_rows_updated)
sql_server_write_fact_count("final_target_insert_count", val_num_rows_inserted)
sql_server_write_fact_count("target_row_count_after_load", cnt_target)

# COMMAND ----------

#  %sql
#  --delete from dev.curated.distribution_center_external_dim
#  --select * from dev.curated.material_external_dim
#  --where current_timestamp() between valid_from_timestamp and valid_to_timestamp

# COMMAND ----------

query = f"(SELECT incr_load_date FROM dbo.control_curated WHERE table_name = 'retailer_distribution_center_inventory_fact') AS incr_load_date"
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
config_date_dcif = df.first()['incr_load_date']
target_date_dcif = spark.sql("select current_timestamp()").collect()[0][0]
spark.conf.set('process.startdate_dcif',str(config_date_dcif))
spark.conf.set('process.enddate_dcif',str(target_date_dcif))

# COMMAND ----------

# DBTITLE 1,Distribution center Inventory Fact - Late Arriving Dimensions check and add default to dimension table
#  %sql
#  insert into $catalog_name.curated.material_external_dim
#  (material_external_dim_dw_id,
#  source_system_description,
#  material_external_code,
#  material_external_description,
#  universal_product_code,
#  material_external_type_code,
#  material_external_type_description,
#  material_external_status_desc,
#  department_description,
#  segment_code,
#  segment_description,
#  sub_brand_code,
#  sub_brand_description,
#  brand_code,
#  brand_description,
#  sub_category_code,
#  sub_category_description,
#  category_code,
#  category_description,
#  vendor_code,
#  vendor_description,
#  material_attribute_array,
#  valid_from_date,
#  valid_through_date,
#  create_date,
#  change_date,
#  data_quality_valid_flag,
#  data_quality_result_array)
#  select md5(concat('wmt',A.wm_item_nbr,current_timestamp())) as material_external_dim_dw_id,
#  'wmt' as source_system_description,
#  A.wm_item_nbr as material_external_code,
#  'NA' as material_external_description,
#  'NA' as universal_product_code,
#  'NA' as material_external_type_code,
#  'NA' as material_external_type_description,
#  'NA' as material_external_status_desc,
#  'NA' as department_description,
#  'NA' as segment_code,
#  'NA' as segment_description,
#  'NA' as sub_brand_code,
#  'NA' as sub_brand_description,
#  'NA' as brand_code,
#  'NA' as brand_description,
#  'NA' as sub_category_code,
#  'NA' as sub_category_description,
#  'NA' as category_code,
#  'NA' as category_description,
#  'NA' as vendor_code,
#  'NA' as vendor_description,
#  'NA' as material_attribute_array,
#  '1900-01-01' as valid_from_date,
#  '9999-12-31' as valid_through_date,
#  current_timestamp() as create_date,
#  current_timestamp() as change_date,
#  null as data_quality_valid_flag,
#  null as data_quality_result_array
#  from 
#  (select distinct A.wm_item_nbr--,A.change_date
#  from
#  $catalog_name.raw.wmt_dcitems as A 
#  LEFT JOIN $catalog_name.curated.material_external_dim as C
#  on A.wm_item_nbr=C.material_external_code and cast(A.invt_dt as timestamp) between C.valid_from_date and C.valid_through_date
#  where A.change_date>='${process.startdate_dcif}' and A.change_date<='${process.enddate_dcif}'
#  and C.material_external_code is null) as A

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit fact: source_row_count, target_row_count, 
# --------------------------------
val_num_rows_inserted = _sqldf.first()[1]
print(val_num_rows_inserted)
sql_server_write_fact_count("final_target_lad(dcitems)_insert_count", val_num_rows_inserted)

# COMMAND ----------

query = f"(SELECT incr_load_date FROM dbo.control_curated WHERE table_name = 'retailer_omni_sales_fact') AS incr_load_date"
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
config_date_odff = df.first()['incr_load_date']
config_date_osf = spark.sql("select current_timestamp()").collect()[0][0]
spark.conf.set('process.startdate_osf',str(config_date_osf))
spark.conf.set('process.enddate_osf',str(target_date_osf))

# COMMAND ----------

# DBTITLE 1,Omni Sales Fact - Late Arriving Dimensions check and add default to dimension table
#  %sql
#  insert into $catalog_name.curated.material_external_dim
#  (material_external_dim_dw_id,
#  source_system_description,
#  material_external_code,
#  material_external_description,
#  universal_product_code,
#  material_external_type_code,
#  material_external_type_description,
#  material_external_status_desc,
#  department_description,
#  segment_code,
#  segment_description,
#  sub_brand_code,
#  sub_brand_description,
#  brand_code,
#  brand_description,
#  sub_category_code,
#  sub_category_description,
#  category_code,
#  category_description,
#  vendor_code,
#  vendor_description,
#  material_attribute_array,
#  valid_from_date,
#  valid_through_date,
#  create_date,
#  change_date,
#  data_quality_valid_flag,
#  data_quality_result_array)
#  select md5(concat('wmt',A.wm_item_nbr,current_timestamp())) as material_external_dim_dw_id,
#  'wmt' as source_system_description,
#  A.wm_item_nbr as material_external_code,
#  'NA' as material_external_description,
#  'NA' as universal_product_code,
#  'NA' as material_external_type_code,
#  'NA' as material_external_type_description,
#  'NA' as material_external_status_desc,
#  'NA' as department_description,
#  'NA' as segment_code,
#  'NA' as segment_description,
#  'NA' as sub_brand_code,
#  'NA' as sub_brand_description,
#  'NA' as brand_code,
#  'NA' as brand_description,
#  'NA' as sub_category_code,
#  'NA' as sub_category_description,
#  'NA' as category_code,
#  'NA' as category_description,
#  'NA' as vendor_code,
#  'NA' as vendor_description,
#  'NA' as material_attribute_array,
#  '1900-01-01' as valid_from_date,
#  '9999-12-31' as valid_through_date,
#  current_timestamp() as create_date,
#  current_timestamp() as change_date,
#  null as data_quality_valid_flag,
#  null as data_quality_result_array
#  from 
#  (select distinct A.wm_item_nbr--,A.change_date
#  from
#  $catalog_name.raw.wmt_omnisales as A 
#  LEFT JOIN $catalog_name.curated.material_external_dim as C
#  on A.wm_item_nbr=C.material_external_code and cast(A.bus_dt as timestamp) between C.valid_from_date and C.valid_through_date
#  where A.change_date>='${process.startdate_osf}' and A.change_date<='${process.enddate_osf}'
#  and C.material_external_code is null) as A

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit fact: source_row_count, target_row_count, 
# --------------------------------
val_num_rows_inserted = _sqldf.first()[1]
print(val_num_rows_inserted)
sql_server_write_fact_count("final_target_lad(omnisales)_insert_count", val_num_rows_inserted)

# COMMAND ----------

query = f"(SELECT incr_load_date FROM dbo.control_curated WHERE table_name = 'retailer_online_inventory_fact') AS incr_load_date"
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
config_date_oif = df.first()['incr_load_date']
target_date_oif = spark.sql("select current_timestamp()").collect()[0][0]
spark.conf.set('process.startdate_oif',str(config_date_oif))
spark.conf.set('process.enddate_oif',str(target_date_oif))

# COMMAND ----------

# DBTITLE 1,Online Inventory Fact - Late Arriving Dimensions check and add default to dimension table(need to be revisited)
#  %sql
#  insert into $catalog_name.curated.material_external_dim
#  (material_external_dim_dw_id,
#  source_system_description,
#  material_external_code,
#  material_external_description,
#  universal_product_code,
#  material_external_type_code,
#  material_external_type_description,
#  material_external_status_desc,
#  department_description,
#  segment_code,
#  segment_description,
#  sub_brand_code,
#  sub_brand_description,
#  brand_code,
#  brand_description,
#  sub_category_code,
#  sub_category_description,
#  category_code,
#  category_description,
#  vendor_code,
#  vendor_description,
#  material_attribute_array,
#  valid_from_date,
#  valid_through_date,
#  create_date,
#  change_date,
#  data_quality_valid_flag,
#  data_quality_result_array)
#  select md5(concat('wmt',A.catlg_item_id,current_timestamp())) as material_external_dim_dw_id,
#  'wmt' as source_system_description,
#  A.catlg_item_id as material_external_code,
#  'NA' as material_external_description,
#  'NA' as universal_product_code,
#  'NA' as material_external_type_code,
#  'NA' as material_external_type_description,
#  'NA' as material_external_status_desc,
#  'NA' as department_description,
#  'NA' as segment_code,
#  'NA' as segment_description,
#  'NA' as sub_brand_code,
#  'NA' as sub_brand_description,
#  'NA' as brand_code,
#  'NA' as brand_description,
#  'NA' as sub_category_code,
#  'NA' as sub_category_description,
#  'NA' as category_code,
#  'NA' as category_description,
#  'NA' as vendor_code,
#  'NA' as vendor_description,
#  'NA' as material_attribute_array,
#  '1900-01-01' as valid_from_date,
#  '9999-12-31' as valid_through_date,
#  current_timestamp() as create_date,
#  current_timestamp() as change_date,
#  null as data_quality_valid_flag,
#  null as data_quality_result_array
#  from 
#  (select distinct A.catlg_item_id--,A.change_date
#  from
#  $catalog_name.raw.wmt_ecomitem as A 
#  left join
#  (
#  select catlg_item_id,wm_item_nbr,row_number() over(partition by catlg_item_id order by item_create_dt desc ) as rnk from $catalog_name.raw.wmt_itemattributes
#  ) as A1
#  on A.catlg_item_id=A1.catlg_item_id and A1.rnk=1
#  LEFT JOIN $catalog_name.curated.material_external_dim as C
#  on nvl(A1.wm_item_nbr,A.catlg_item_id)=C.material_external_code and cast(A.rpt_dt as timestamp) between C.valid_from_date and C.valid_through_date
#  where A.change_date>='${process.startdate_oif}' and A.change_date<='${process.enddate_oif}'
#  and C.material_external_code is null) as A

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit fact: source_row_count, target_row_count, 
# --------------------------------
val_num_rows_inserted = _sqldf.first()[1]
print(val_num_rows_inserted)
sql_server_write_fact_count("final_target_lad(onlineinvenory)_insert_count", val_num_rows_inserted)

# COMMAND ----------

query = f"(SELECT incr_load_date FROM dbo.control_curated WHERE table_name = 'retailer_order_demand_forecast_fact') AS incr_load_date"
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
config_date_odff = df.first()['incr_load_date']
target_date_odff = spark.sql("select current_timestamp()").collect()[0][0]
spark.conf.set('process.startdate_odff',str(config_date_odff))
spark.conf.set('process.enddate_odff',str(target_date_odff))

# COMMAND ----------

# DBTITLE 1,Order Demand Forecast Fact - Late Arriving Dimensions check and add default to dimension table
#  %sql
#  insert into $catalog_name.curated.material_external_dim
#  (material_external_dim_dw_id,
#  source_system_description,
#  material_external_code,
#  material_external_description,
#  universal_product_code,
#  material_external_type_code,
#  material_external_type_description,
#  material_external_status_desc,
#  department_description,
#  segment_code,
#  segment_description,
#  sub_brand_code,
#  sub_brand_description,
#  brand_code,
#  brand_description,
#  sub_category_code,
#  sub_category_description,
#  category_code,
#  category_description,
#  vendor_code,
#  vendor_description,
#  material_attribute_array,
#  valid_from_date,
#  valid_through_date,
#  create_date,
#  change_date,
#  data_quality_valid_flag,
#  data_quality_result_array)
#  select md5(concat('wmt',A.wm_item_nbr,current_timestamp())) as material_external_dim_dw_id,
#  'wmt' as source_system_description,
#  A.wm_item_nbr as material_external_code,
#  'NA' as material_external_description,
#  'NA' as universal_product_code,
#  'NA' as material_external_type_code,
#  'NA' as material_external_type_description,
#  'NA' as material_external_status_desc,
#  'NA' as department_description,
#  'NA' as segment_code,
#  'NA' as segment_description,
#  'NA' as sub_brand_code,
#  'NA' as sub_brand_description,
#  'NA' as brand_code,
#  'NA' as brand_description,
#  'NA' as sub_category_code,
#  'NA' as sub_category_description,
#  'NA' as category_code,
#  'NA' as category_description,
#  'NA' as vendor_code,
#  'NA' as vendor_description,
#  'NA' as material_attribute_array,
#  '1900-01-01' as valid_from_date,
#  '9999-12-31' as valid_through_date,
#  current_timestamp() as create_date,
#  current_timestamp() as change_date,
#  null as data_quality_valid_flag,
#  null as data_quality_result_array
#  from 
#  (select distinct A.wm_item_nbr--,A.change_date
#  from
#  $catalog_name.raw.wmt_orderforecast as A 
#  LEFT JOIN $catalog_name.curated.material_external_dim as C
#  on A.wm_item_nbr=C.material_external_code and cast(A.order_place_dt as timestamp) between C.valid_from_date and C.valid_through_date
#  where A.change_date>='${process.startdate_odff}' and A.change_date<='${process.enddate_odff}'
#  and C.material_external_code is null) as A

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit fact: source_row_count, target_row_count, 
# --------------------------------
val_num_rows_inserted = _sqldf.first()[1]
print(val_num_rows_inserted)
sql_server_write_fact_count("final_target_lad(orderdemandforecast)_insert_count", val_num_rows_inserted)

# COMMAND ----------

query = f"(SELECT incr_load_date FROM dbo.control_curated WHERE table_name = 'retailer_purchase_order_fact') AS incr_load_date"
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
config_date_poff = df.first()['incr_load_date']
target_date_poff = spark.sql("select current_timestamp()").collect()[0][0]
spark.conf.set('process.startdate_poff',str(config_date_odff))
spark.conf.set('process.enddate_poff',str(target_date_odff))

# COMMAND ----------

# DBTITLE 1,Purchase Order Fact - Late Arriving Dimensions check and add default to dimension table
#  %sql
#  insert into $catalog_name.curated.material_external_dim
#  (material_external_dim_dw_id,
#  source_system_description,
#  material_external_code,
#  material_external_description,
#  universal_product_code,
#  material_external_type_code,
#  material_external_type_description,
#  material_external_status_desc,
#  department_description,
#  segment_code,
#  segment_description,
#  sub_brand_code,
#  sub_brand_description,
#  brand_code,
#  brand_description,
#  sub_category_code,
#  sub_category_description,
#  category_code,
#  category_description,
#  vendor_code,
#  vendor_description,
#  material_attribute_array,
#  valid_from_date,
#  valid_through_date,
#  create_date,
#  change_date,
#  data_quality_valid_flag,
#  data_quality_result_array)
#  select md5(concat('wmt',A.wm_item_nbr,current_timestamp())) as material_external_dim_dw_id,
#  'wmt' as source_system_description,
#  A.wm_item_nbr as material_external_code,
#  'NA' as material_external_description,
#  'NA' as universal_product_code,
#  'NA' as material_external_type_code,
#  'NA' as material_external_type_description,
#  'NA' as material_external_status_desc,
#  'NA' as department_description,
#  'NA' as segment_code,
#  'NA' as segment_description,
#  'NA' as sub_brand_code,
#  'NA' as sub_brand_description,
#  'NA' as brand_code,
#  'NA' as brand_description,
#  'NA' as sub_category_code,
#  'NA' as sub_category_description,
#  'NA' as category_code,
#  'NA' as category_description,
#  'NA' as vendor_code,
#  'NA' as vendor_description,
#  'NA' as material_attribute_array,
#  '1900-01-01' as valid_from_date,
#  '9999-12-31' as valid_through_date,
#  current_timestamp() as create_date,
#  current_timestamp() as change_date,
#  null as data_quality_valid_flag,
#  null as data_quality_result_array
#  from 
#  (select distinct poline.wm_item_nbr--,A.change_date
#  from
#  $catalog_name.raw.wmt_purchaseorderline as poline
#  left join $catalog_name.raw.wmt_purchaseorder as po
#  on po.oms_po_nbr=poline.oms_po_nbr and po.order_dt=poline.order_dt
#  LEFT JOIN $catalog_name.curated.material_external_dim as C
#  on poline.wm_item_nbr=C.material_external_code and cast(poline.order_dt as timestamp) between C.valid_from_date and C.valid_through_date
#  where ((poline.change_date>='${process.startdate_poff}' and poline.change_date<='${process.enddate_poff}') OR (po.change_date>='${process.startdate_poff}' and po.change_date<='${process.enddate_poff}'))
#  and C.material_external_code is null) as A

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit fact: source_row_count, target_row_count, 
# --------------------------------
val_num_rows_inserted = _sqldf.first()[1]
print(val_num_rows_inserted)
sql_server_write_fact_count("final_target_lad(purchaseorder)_insert_count", val_num_rows_inserted)

# COMMAND ----------

query = f"(SELECT incr_load_date FROM dbo.control_curated WHERE table_name = 'retailer_store_demand_forecast_fact') AS incr_load_date"
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
config_date_sdff = df.first()['incr_load_date']
target_date_sdff = spark.sql("select current_timestamp()").collect()[0][0]
spark.conf.set('process.startdate_sdff',str(config_date_sdff))
spark.conf.set('process.enddate_sdff',str(target_date_sdff))

# COMMAND ----------

# DBTITLE 1,Store Demand Forecast Fact - Late Arriving Dimensions check and add default to dimension table(need to limit to a day)
#  %sql
#  insert into $catalog_name.curated.material_external_dim
#  (material_external_dim_dw_id,
#  source_system_description,
#  material_external_code,
#  material_external_description,
#  universal_product_code,
#  material_external_type_code,
#  material_external_type_description,
#  material_external_status_desc,
#  department_description,
#  segment_code,
#  segment_description,
#  sub_brand_code,
#  sub_brand_description,
#  brand_code,
#  brand_description,
#  sub_category_code,
#  sub_category_description,
#  category_code,
#  category_description,
#  vendor_code,
#  vendor_description,
#  material_attribute_array,
#  valid_from_date,
#  valid_through_date,
#  create_date,
#  change_date,
#  data_quality_valid_flag,
#  data_quality_result_array)
#  select md5(concat('wmt',A.wm_item_nbr,current_timestamp())) as material_external_dim_dw_id,
#  'wmt' as source_system_description,
#  A.wm_item_nbr as material_external_code,
#  'NA' as material_external_description,
#  'NA' as universal_product_code,
#  'NA' as material_external_type_code,
#  'NA' as material_external_type_description,
#  'NA' as material_external_status_desc,
#  'NA' as department_description,
#  'NA' as segment_code,
#  'NA' as segment_description,
#  'NA' as sub_brand_code,
#  'NA' as sub_brand_description,
#  'NA' as brand_code,
#  'NA' as brand_description,
#  'NA' as sub_category_code,
#  'NA' as sub_category_description,
#  'NA' as category_code,
#  'NA' as category_description,
#  'NA' as vendor_code,
#  'NA' as vendor_description,
#  'NA' as material_attribute_array,
#  '1900-01-01' as valid_from_date,
#  '9999-12-31' as valid_through_date,
#  current_timestamp() as create_date,
#  current_timestamp() as change_date,
#  null as data_quality_valid_flag,
#  null as data_quality_result_array
#  from 
#  (select distinct A.wm_item_nbr--,A.change_date
#  from
#  $catalog_name.raw.wmt_demandforecast as A 
#  inner join $catalog_name.curated.date_dim as A1
#  on A.wm_yr_wk_nbr=A1.walmart_year_week_number -----need to limit to a day
#  LEFT JOIN $catalog_name.curated.material_external_dim as C
#  on A.wm_item_nbr=C.material_external_code and cast(A1.day_date as timestamp) between C.valid_from_date and C.valid_through_date
#  where A.change_date>='${process.startdate_sdff}' and A.change_date<='${process.enddate_sdff}'
#  and C.material_external_code is null) as A

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit fact: source_row_count, target_row_count, 
# --------------------------------
val_num_rows_inserted = _sqldf.first()[1]
print(val_num_rows_inserted)
sql_server_write_fact_count("final_target_lad(storedemandforecast)_insert_count", val_num_rows_inserted)

# COMMAND ----------

query = f"(SELECT incr_load_date FROM dbo.control_curated WHERE table_name = 'retailer_store_inv_adj_tran_fulfil_fact') AS incr_load_date"
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
config_date_iaff = df.first()['incr_load_date']
target_date_iaff = spark.sql("select current_timestamp()").collect()[0][0]
spark.conf.set('process.startdate_iaff',str(config_date_iaff))
spark.conf.set('process.enddate_iaff',str(target_date_iaff))

# COMMAND ----------

# DBTITLE 1,Inventory Adjustment Fact - Late Arriving Dimensions check and add default to dimension table
#  %sql
#  insert into $catalog_name.curated.material_external_dim
#  (material_external_dim_dw_id,
#  source_system_description,
#  material_external_code,
#  material_external_description,
#  universal_product_code,
#  material_external_type_code,
#  material_external_type_description,
#  material_external_status_desc,
#  department_description,
#  segment_code,
#  segment_description,
#  sub_brand_code,
#  sub_brand_description,
#  brand_code,
#  brand_description,
#  sub_category_code,
#  sub_category_description,
#  category_code,
#  category_description,
#  vendor_code,
#  vendor_description,
#  material_attribute_array,
#  valid_from_date,
#  valid_through_date,
#  create_date,
#  change_date,
#  data_quality_valid_flag,
#  data_quality_result_array)
#  select md5(concat('wmt',A.wm_item_nbr,current_timestamp())) as material_external_dim_dw_id,
#  'wmt' as source_system_description,
#  A.wm_item_nbr as material_external_code,
#  'NA' as material_external_description,
#  'NA' as universal_product_code,
#  'NA' as material_external_type_code,
#  'NA' as material_external_type_description,
#  'NA' as material_external_status_desc,
#  'NA' as department_description,
#  'NA' as segment_code,
#  'NA' as segment_description,
#  'NA' as sub_brand_code,
#  'NA' as sub_brand_description,
#  'NA' as brand_code,
#  'NA' as brand_description,
#  'NA' as sub_category_code,
#  'NA' as sub_category_description,
#  'NA' as category_code,
#  'NA' as category_description,
#  'NA' as vendor_code,
#  'NA' as vendor_description,
#  'NA' as material_attribute_array,
#  '1900-01-01' as valid_from_date,
#  '9999-12-31' as valid_through_date,
#  current_timestamp() as create_date,
#  current_timestamp() as change_date,
#  null as data_quality_valid_flag,
#  null as data_quality_result_array
#  from 
#  (select distinct A.wm_item_nbr--,A.change_date
#  from
#  (select distinct wm_item_nbr,bus_dt from $catalog_name.raw.wmt_storeitem where change_date>='${process.startdate_iaff}' and change_date<='${process.enddate_iaff}'
#  union
#  select distinct wm_item_nbr,adj_dt as bus_dt from $catalog_name.raw.wmt_inventoryadjustments where change_date>='${process.startdate_iaff}' and change_date<='${process.enddate_iaff}'
#  union
#  select distinct wm_item_nbr,rpt_dt as bus_dt from $catalog_name.raw.wmt_digitaltransactability where change_date>='${process.startdate_iaff}' and change_date<='${process.enddate_iaff}'
#  union
#  select distinct wm_item_nbr,bus_dt from $catalog_name.raw.wmt_storefulfilment where change_date>='${process.startdate_iaff}' and change_date<='${process.enddate_iaff}') as A 
#  LEFT JOIN $catalog_name.curated.material_external_dim as C
#  on A.wm_item_nbr=C.material_external_code and cast(A.bus_dt as timestamp) between C.valid_from_date and C.valid_through_date
#  where --A.change_date>='${process.startdate_odff}' and A.change_date<='${process.enddate_odff}' and 
#  C.material_external_code is null) as A

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit fact: source_row_count, target_row_count, 
# --------------------------------
val_num_rows_inserted = _sqldf.first()[1]
print(val_num_rows_inserted)
sql_server_write_fact_count("final_target_lad(inventoryadjustment)_insert_count", val_num_rows_inserted)

# COMMAND ----------

query = f"(SELECT incr_load_date FROM dbo.control_curated WHERE table_name = 'retailer_store_returns_fact') AS incr_load_date"
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
config_date_srf = df.first()['incr_load_date']
target_date_srf = spark.sql("select current_timestamp()").collect()[0][0]
spark.conf.set('process.startdate_srf',str(config_date_srf))
spark.conf.set('process.enddate_srf',str(target_date_srf))

# COMMAND ----------

# DBTITLE 1,Sore Returns Fact - Late Arriving Dimensions check and add default to dimension table
#  %sql
#  insert into $catalog_name.curated.material_external_dim
#  (material_external_dim_dw_id,
#  source_system_description,
#  material_external_code,
#  material_external_description,
#  universal_product_code,
#  material_external_type_code,
#  material_external_type_description,
#  material_external_status_desc,
#  department_description,
#  segment_code,
#  segment_description,
#  sub_brand_code,
#  sub_brand_description,
#  brand_code,
#  brand_description,
#  sub_category_code,
#  sub_category_description,
#  category_code,
#  category_description,
#  vendor_code,
#  vendor_description,
#  material_attribute_array,
#  valid_from_date,
#  valid_through_date,
#  create_date,
#  change_date,
#  data_quality_valid_flag,
#  data_quality_result_array)
#  select md5(concat('wmt',A.wm_item_nbr,current_timestamp())) as material_external_dim_dw_id,
#  'wmt' as source_system_description,
#  A.wm_item_nbr as material_external_code,
#  'NA' as material_external_description,
#  'NA' as universal_product_code,
#  'NA' as material_external_type_code,
#  'NA' as material_external_type_description,
#  'NA' as material_external_status_desc,
#  'NA' as department_description,
#  'NA' as segment_code,
#  'NA' as segment_description,
#  'NA' as sub_brand_code,
#  'NA' as sub_brand_description,
#  'NA' as brand_code,
#  'NA' as brand_description,
#  'NA' as sub_category_code,
#  'NA' as sub_category_description,
#  'NA' as category_code,
#  'NA' as category_description,
#  'NA' as vendor_code,
#  'NA' as vendor_description,
#  'NA' as material_attribute_array,
#  '1900-01-01' as valid_from_date,
#  '9999-12-31' as valid_through_date,
#  current_timestamp() as create_date,
#  current_timestamp() as change_date,
#  null as data_quality_valid_flag,
#  null as data_quality_result_array
#  from 
#  (select distinct A.wm_item_nbr--,A.change_date
#  from
#  $catalog_name.raw.wmt_storereturns as A 
#  LEFT JOIN $catalog_name.curated.material_external_dim as C
#  on A.wm_item_nbr=C.material_external_code and cast(A.rtn_dt as timestamp) between C.valid_from_date and C.valid_through_date
#  where A.change_date>='${process.startdate_srf}' and A.change_date<='${process.enddate_srf}'
#  and C.material_external_code is null) as A

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit fact: source_row_count, target_row_count, 
# --------------------------------
val_num_rows_inserted = _sqldf.first()[1]
print(val_num_rows_inserted)
sql_server_write_fact_count("final_target_lad(storereturns)_insert_count", val_num_rows_inserted)

# COMMAND ----------

query = f"(SELECT incr_load_date FROM dbo.control_curated WHERE table_name = 'retailer_store_sales_fact') AS incr_load_date"
df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
config_date_ssf = df.first()['incr_load_date']
target_date_ssf = spark.sql("select current_timestamp()").collect()[0][0]
spark.conf.set('process.startdate_ssf',str(config_date_ssf))
spark.conf.set('process.enddate_ssf',str(target_date_ssf))

# COMMAND ----------

# DBTITLE 1,Store Sales Fact - Late Arriving Dimensions check and add default to dimension table
#  %sql
#  insert into $catalog_name.curated.material_external_dim
#  (material_external_dim_dw_id,
#  source_system_description,
#  material_external_code,
#  material_external_description,
#  universal_product_code,
#  material_external_type_code,
#  material_external_type_description,
#  material_external_status_desc,
#  department_description,
#  segment_code,
#  segment_description,
#  sub_brand_code,
#  sub_brand_description,
#  brand_code,
#  brand_description,
#  sub_category_code,
#  sub_category_description,
#  category_code,
#  category_description,
#  vendor_code,
#  vendor_description,
#  material_attribute_array,
#  valid_from_date,
#  valid_through_date,
#  create_date,
#  change_date,
#  data_quality_valid_flag,
#  data_quality_result_array)
#  select md5(concat('wmt',A.wm_item_nbr,current_timestamp())) as material_external_dim_dw_id,
#  'wmt' as source_system_description,
#  A.wm_item_nbr as material_external_code,
#  'NA' as material_external_description,
#  'NA' as universal_product_code,
#  'NA' as material_external_type_code,
#  'NA' as material_external_type_description,
#  'NA' as material_external_status_desc,
#  'NA' as department_description,
#  'NA' as segment_code,
#  'NA' as segment_description,
#  'NA' as sub_brand_code,
#  'NA' as sub_brand_description,
#  'NA' as brand_code,
#  'NA' as brand_description,
#  'NA' as sub_category_code,
#  'NA' as sub_category_description,
#  'NA' as category_code,
#  'NA' as category_description,
#  'NA' as vendor_code,
#  'NA' as vendor_description,
#  'NA' as material_attribute_array,
#  '1900-01-01' as valid_from_date,
#  '9999-12-31' as valid_through_date,
#  current_timestamp() as create_date,
#  current_timestamp() as change_date,
#  null as data_quality_valid_flag,
#  null as data_quality_result_array
#  from 
#  (select distinct A.wm_item_nbr--,A.change_date
#  from
#  $catalog_name.raw.wmt_storesales as A 
#  LEFT JOIN $catalog_name.curated.material_external_dim as C
#  on A.wm_item_nbr=C.material_external_code and cast(A.bus_dt as timestamp) between C.valid_from_date and C.valid_through_date
#  where A.change_date>='${process.startdate_ssf}' and A.change_date<='${process.enddate_ssf}'
#  and C.material_external_code is null) as A

# COMMAND ----------

# --------------------------------
# -- 2.3) Writie into the audit fact: source_row_count, target_row_count, 
# --------------------------------
val_num_rows_inserted = _sqldf.first()[1]
cnt_target = spark.sql(f"select count(*) cnt from {val_target_table}").first()['cnt']
print(val_num_rows_inserted)
sql_server_write_fact_count("final_target_lad(storesales)_insert_count", val_num_rows_inserted)
sql_server_write_fact_count("target_row_count_after_lad_load", cnt_target)

# COMMAND ----------

# -------------------------------
# -- 8) Calling the DQ notebook
# -------------------------------
# Calling the DQ notebook ...
# DQ dashboard ...
val_table_name = val_target_table
audit_dim_id_variable = val_audit_dim_id
sla_dim_id_variable = 0
val_schema_name = 'curated'
val_cat='dev'
tbl_name = val_table_name.split('.')[2]
print(f"table_name:{tbl_name}, cat:{val_cat}, schema:{val_schema_name}")
# ../../../common/dq/transformations/dq_rule_builder_post_update_immediate_par_sql
dbutils.notebook.run(
"./dq_rule_builder_post_update_immediate_par_sql",
6000,
{
    "table_name": tbl_name, 
    "audit_dim_id": audit_dim_id_variable,
    "sla_dim_id": sla_dim_id_variable,
    "database_name": val_schema_name,
    "catalog_name": val_cat 
})

# COMMAND ----------

sql_server_write_fact_msg("pipeline end")
