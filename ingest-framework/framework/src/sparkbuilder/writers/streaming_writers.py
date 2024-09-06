#from sparkbuilder.writers.batch_writers import append_write

def streaming_write_table_append(spark,df,write_config):
    
    if  not ("catalog" in write_config[0] and "schema" in write_config[0] and "table" in write_config[0]) :
       
        raise Exception("Exception in the function sparkbuilder.writers.streaming_writers.streaming_write_table_append: catalog/schema/table is not defined in the writes field in the configuration table.")
    
    
    if "mergeSchema" in write_config[0]:
        merge_schema=write_config[0]["mergeSchema"].lower() 
    else:
        #default value
        merge_schema='false'
    
    table_name=f"""{write_config[0]["catalog"]}.{write_config[0]["schema"]}.{write_config[0]["table"]}""".lower()
    target_table_type=write_config[0]["target_table_type"].lower()
    checkpoint_filepath=write_config[0]["checkpoint_filepath"]
    output_mode=write_config[0]["mode"].lower()
    trigger=write_config[0]["trigger"]
   

    try:
        sap_streaming_query = (df
                                .writeStream
                                .format(target_table_type)
                                .option("checkpointLocation",checkpoint_filepath)
                                .outputMode(output_mode)
                                .option("mergeSchema",merge_schema)
                                .trigger(availableNow=True)
                                .toTable(table_name)
                            )
        
        for query in spark.streams.active:
            print(f"Waiting for query {query.id} to complete ...")
            query.awaitTermination()
        
    except Exception as e:
        print(f"Exception in sparkbuilder.writers.streaming_writers.streaming_write_table_append srini => {str(e)}") 
