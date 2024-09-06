# Databricks notebook source
def main():
    # Get User Arguments
    args = get_user_args()

    audit_write = args.audit_write

    if audit_write is True:
        audit_table_config = {}
        data_product_name = config["data_product_name"]
        target_table = config.get_target_table_name()
        source_table = config.get_source_table_name()

        ath = AuditTableHandler(audit_table_config)

        ath.write_audit_dim_table(target_table, data_product_name, source_table)
        ath.signal_pipeline_start()


    # Process the configuration (either path or dictionary)
    config = ConfigHandler(config_path=args.config_path, config=pipeline_config).get_config()

    pb = PipelineBuilder(spark, config, verbose=args.verbose, fncs=[add_timestamp_cols, display_count])
    
    if args.run_dq_rules is True:
        dq = DataQualityChecks()

    # Read the data
    df = spark.read.
    
    # Perform transformations
    df, _ = pb.run(df)

    # Run Data Quality Checks
    if args.run_dq_rules is True:
        dq_config = get_dq_config(table="dq_database_eval_quality")
        active_valid_df, active_invalid_df, passive_valid_df, passive_invalid_df = dq.run(df, dq_config)

    # Write output
    pb.write(df)

    if audit_write:
        # Write the type 1 updates and the target table count to the audit table
        ath.write_type1_updates(df)
        ath.write_target_table_count(target_table)
        ath.signal_pipeline_end()
