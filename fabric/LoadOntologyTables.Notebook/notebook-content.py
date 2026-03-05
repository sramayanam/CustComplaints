# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "08bf1610-8c6e-407c-be15-44a3d663ca8d",
# META       "default_lakehouse_name": "airlinesLH",
# META       "default_lakehouse_workspace_id": "12204d44-688c-40db-9593-4d23c9ad3e51",
# META       "known_lakehouses": [
# META         {
# META           "id": "08bf1610-8c6e-407c-be15-44a3d663ca8d"
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
