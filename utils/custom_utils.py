
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import Window

class transforamtion:
        
    def dedup(self,df:DataFrame,dedu_cols:List,cdc:str):
        
        
        df = df.withColumn("dedukey",concat(*dedu_cols))
        df = df.withColumn("deduCounts",row_number()\
            .over(Window.partitionBy("dedukey")\
                .orderBy(desc(cdc))\
                                        ))
        df = df.filter(col("deduCounts")==1)
        df = df.drop('dedukey',"deduCounts")

        return df
    
    def process_timestamp(self,df):

        df = df.withColumn("process_timestamp",current_timestamp())

        return df
    
    def upsart(self,df,key_cols,table,cdc):

        merge_condistion = " AND ".join([f"sr.{i} = trg.{i}" for i in key_cols ])
        dlt_obj = DeltaTable.forName(spark,f"pysparkdbt.silver.{table}")
        dlt_obj.alias("trg").merge(df.alias("src"),merge_condistion)\
            .whenMatchedUpdateAll(condition= "src.{cdc} >= trg.{cdc}")\
            .whenNotMatchedInsertAll()\
            .execute()

        return 1    



