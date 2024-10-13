# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "99133ffd-9859-414d-ab39-3c6ec6bb971f",
# META       "default_lakehouse_name": "Project_Lakehouse",
# META       "default_lakehouse_workspace_id": "800bccfc-9675-4b00-9d8a-088f125584be"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json("Files/Bronze/Source_Data.json")
# df now is a Spark DataFrame containing JSON data from "Files/Bronze/Source_Data.json".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=df.withColumn("Exploded",explode(col("TrainPositions")))
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = StructType([
    StructField("CarCount", StringType(), True),
    StructField("TrainId", StringType(), True),
    StructField("DirectionNum", StringType(), True),
    StructField("SecondsAtLocation", IntegerType(), True),
    StructField("DestinationStationCode",StringType(),True),
    StructField("LineCode",StringType(),True),
    StructField("CircuitId", StringType(), True),
    StructField("TrainNumber", StringType(), True),
    StructField("ServiceType", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn("ExplodedString", to_json(col("Exploded")))

df = df.withColumn("processed", from_json(col("ExplodedString"), schema))

df_cleaned_final = df.select("processed.TrainNumber","processed.TrainId","processed.CircuitId","processed.ServiceType","processed.DestinationStationCode","processed.LineCode","processed.CarCount","processed.DirectionNum","processed.SecondsAtLocation")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


display(spark.sql("SELECT count(*) FROM Project_Lakehouse.real_time_status_fact"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable


try:
    table_name='Project_Lakehouse.real_time_status_fact'
    df_cleaned_final.write.format("delta").saveAsTable(table_name)

except AnalysisException:
    print("Table Already Exists")
    # Assuming 'source_df' is your source DataFrame
    deduplicated_source_df = df.dropDuplicates([
    "TrainNumber", 
    "TrainId", 
    "CircuitId", 
    "ServiceType", 
    "DestinationStationCode", 
    "LineCode", 
    "CarCount", 
    "DirectionNum", 
    "SecondsAtLocation"
])

    deduplicated_source_df.createOrReplaceTempView("cleaned_final")
    # spark. sql(f""" MERGE INTO {table_name} target_table
    #         USING cleaned_final source_view
    #         ON source_view.TrainNumber = target_table.TrainNumber
    #         WHEN MATCHED AND
    #         source_view.TrainNumber <> target_table.TrainNumber OR
    #         source_view.TrainId <> target_table.TrainId OR
    #         source_view.CircuitId <> target_table.CircuitId OR
    #         source_view.ServiceType <> target_table.ServiceType OR
    #         source_view.DestinationStationCode > target_table.DestinationStationCode OR
    #         source_view.LineCode <> target_table.LineCode OR
    #         source_view.CarCount <> target_table.CarCount OR
    #         source_view.DirectionNum <> target_table.DirectionNum OR
    #         source_view.SecondsAtLocation <> target_table.SecondsAtLocation
    #         THEN UPDATE SET *
    #         """)
target_table = DeltaTable.forPath(spark, "{table_name}")
target_table.alias("target_table").merge(
    source_view.alias("source_view"),
    "source_view.TrainNumber = target_table.TrainNumber"
).whenMatchedUpdate(
    condition="""
        source_view.TrainId <> target_table.TrainId OR
        source_view.CircuitId <> target_table.CircuitId OR
        source_view.ServiceType <> target_table.ServiceType OR
        source_view.DestinationStationCode > target_table.DestinationStationCode OR
        source_view.LineCode <> target_table.LineCode OR
        source_view.CarCount <> target_table.CarCount OR
        source_view.DirectionNum <> target_table.DirectionNum OR
        source_view.SecondsAtLocation <> target_table.SecondsAtLocation
    """,
    set={
        "TrainId": "source_view.TrainId",
        "CircuitId": "source_view.CircuitId",
        "ServiceType": "source_view.ServiceType",
        "DestinationStationCode": "source_view.DestinationStationCode",
        "LineCode": "source_view.LineCode",
        "CarCount": "source_view.CarCount",
        "DirectionNum": "source_view.DirectionNum",
        "SecondsAtLocation": "source_view.SecondsAtLocation"
    }
).execute()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("SELECT * FROM cleaned_final"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
