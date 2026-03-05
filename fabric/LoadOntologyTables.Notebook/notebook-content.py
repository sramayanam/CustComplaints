# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "",
# META       "default_lakehouse_name": "airlinesLH",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": [
# META         {
# META           "id": ""
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Run in a Fabric Notebook attached to your Lakehouse
df = spark.sql("DESCRIBE DETAIL airlinesLH.dbo.casesnew")
df.select("properties").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "airlinesLH.dbo.complaints_ont")
props = dt.detail().select("properties").collect()[0][0]
print(props)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run once per table — copies Eventhouse shortcut → native Delta table
tables = {
    "passengersnew": "passengers_ont",
    "flightsn":      "flights_ont",
    "casesnew":      "cases_ont",
    "complaintsnew": "complaints_ont",
}

for src, dst in tables.items():
    df = spark.read.table(src)          # reads from Eventhouse shortcut
    df.write.format("delta") \
      .mode("overwrite") \
      .saveAsTable(dst)                 # writes native managed Delta table
    print(f"Done: {src} → {dst}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
