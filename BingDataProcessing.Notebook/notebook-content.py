# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0b9f6b7d-fbe6-447a-896d-4d2d53304621",
# META       "default_lakehouse_name": "BingLakeHouse",
# META       "default_lakehouse_workspace_id": "30d81c44-aba9-47b2-a527-f50e1ff3233a"
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json("Files/bronzelayer/bingnews.json")
# df now is a Spark DataFrame containing JSON data from "Files/bronzelayer/bingnews.json".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.select("value")
df.display()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_exploded=df.select(explode(df["value"]).alias("exploded"))
display(df_exploded)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_processed = df_exploded.select(
    col("exploded.datePublished").alias("datePublished"),
    col("exploded.name").alias("name"),
    col("exploded.description").alias("description"),
    col("exploded.url").alias("url"),
    col("exploded.image.isLicensed").alias("isLicensed"),
    col("exploded.image.thumbnail.contentUrl").alias("imageContentUrl"),
    col("exploded.image.thumbnail.height").alias("height"),
    col("exploded.image.thumbnail.width").alias("width"),
    explode("exploded.provider").alias("provider")
).select(
    "datePublished",
    "name",
    "description",
    "url",
    "isLicensed",
    "imageContentUrl",
    "height",
    "width",
    col("provider._type").alias("providerType"),
    col("provider.image.thumbnail.contentUrl").alias("providerImageContentUrl"),
    col("provider.name").alias("providerName")
)

# Display the processed DataFrame
display(df_processed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_processed.write.format("delta").saveAsTable("BingProcessed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM BingLakeHouse.bingprocessed LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

