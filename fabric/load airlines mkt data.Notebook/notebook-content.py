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

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM airlinesLH.dbo.dim_airlines LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Apply filter
filtered_df = spark.sql("SELECT * FROM airlinesLH.dbo.mlv_quarterly_summary WHERE business_year in (2013,2014) and origin_airport_code='DFW'")

# Write the result as a *new* managed table in the lakehouse
# (change 'dfw_mlv_quarterly_2013_2014' to your preferred table name)
filtered_df.write.format("delta").mode("overwrite").saveAsTable("dfw_quarterly_mktg")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
