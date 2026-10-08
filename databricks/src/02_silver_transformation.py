# COMMAND ----------
# Notebook cell 1
from pyspark.sql.functions import (
    col,
    concat,
    lit,
    regexp_replace,
    to_date,
    to_timestamp
)

# -----------------------------
# 1. Vehicle registrations
# -----------------------------

vehicle_registrations_df = (
    spark.table(
        "charging_infrastructure.bronze.scb_new_vehicle_registrations"
    )
    .select(
        "region_code",
        "region_name",
        "fuel_code",
        "fuel_type",
        to_date(
            concat(
                regexp_replace(col("period"), "M", "-"),
                lit("-01")
            )
        ).alias("month"),
        col("value").cast("long").alias("registrations")
    )
    .dropDuplicates([
        "region_code",
        "fuel_code",
        "month"
    ])
)

vehicle_registrations_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "charging_infrastructure.silver.vehicle_registrations"
    )


# -----------------------------
# 2. Passenger car stock
# -----------------------------

passenger_car_stock_df = (
    spark.table(
        "charging_infrastructure.bronze.scb_passenger_car_stock"
    )
    .select(
        "region_code",
        "region_name",
        col("year").cast("int").alias("year"),
        col("vehicle_count").cast("long").alias("vehicle_count")
    )
    .dropDuplicates([
        "region_code",
        "year"
    ])
)

passenger_car_stock_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "charging_infrastructure.silver.passenger_car_stock"
    )


# -----------------------------
# 3. Population
# -----------------------------

population_df = (
    spark.table(
        "charging_infrastructure.bronze.scb_population"
    )
    .select(
        "region_code",
        "region_name",
        col("year").cast("int").alias("year"),
        col("population").cast("long").alias("population")
    )
    .dropDuplicates([
        "region_code",
        "year"
    ])
)

population_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "charging_infrastructure.silver.population"
    )


# -----------------------------
# 4. Electricity prices
# -----------------------------

electricity_prices_df = (
    spark.table(
        "charging_infrastructure.bronze.electricity_prices"
    )
    .select(
        "price_area",
        to_date("price_date").alias("price_date"),
        col("sek_per_kwh").cast("double").alias("sek_per_kwh"),
        to_timestamp("time_start").alias("time_start"),
        to_timestamp("time_end").alias("time_end")
    )
    .dropDuplicates([
        "price_area",
        "time_start"
    ])
)

electricity_prices_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "charging_infrastructure.silver.electricity_prices"
    )


print("Silver tables created successfully.")


# COMMAND ----------
# Notebook cell 2
silver_tables = {
    "vehicle_registrations":
        "charging_infrastructure.silver.vehicle_registrations",

    "passenger_car_stock":
        "charging_infrastructure.silver.passenger_car_stock",

    "population":
        "charging_infrastructure.silver.population",

    "electricity_prices":
        "charging_infrastructure.silver.electricity_prices"
}

for table_name, table_path in silver_tables.items():
    row_count = spark.table(table_path).count()
    print(f"{table_name}: {row_count} rows")


# COMMAND ----------
# Notebook cell 3
from pyspark.sql.functions import col

# Vehicle registrations
vehicle_registration_duplicates = (
    vehicle_registrations_df.count()
    - vehicle_registrations_df
        .select("region_code", "fuel_code", "month")
        .distinct()
        .count()
)

vehicle_registration_nulls = (
    vehicle_registrations_df
    .filter(
        col("region_code").isNull()
        | col("fuel_code").isNull()
        | col("month").isNull()
        | col("registrations").isNull()
    )
    .count()
)

# Passenger car stock
vehicle_stock_duplicates = (
    passenger_car_stock_df.count()
    - passenger_car_stock_df
        .select("region_code", "year")
        .distinct()
        .count()
)

vehicle_stock_nulls = (
    passenger_car_stock_df
    .filter(
        col("region_code").isNull()
        | col("year").isNull()
        | col("vehicle_count").isNull()
    )
    .count()
)

# Population
population_duplicates = (
    population_df.count()
    - population_df
        .select("region_code", "year")
        .distinct()
        .count()
)

population_nulls = (
    population_df
    .filter(
        col("region_code").isNull()
        | col("year").isNull()
        | col("population").isNull()
    )
    .count()
)

# Electricity prices
electricity_price_duplicates = (
    electricity_prices_df.count()
    - electricity_prices_df
        .select("price_area", "time_start")
        .distinct()
        .count()
)

electricity_price_nulls = (
    electricity_prices_df
    .filter(
        col("price_area").isNull()
        | col("time_start").isNull()
        | col("sek_per_kwh").isNull()
    )
    .count()
)

print("vehicle_registrations - duplicates:", vehicle_registration_duplicates,
      "nulls:", vehicle_registration_nulls)

print("passenger_car_stock - duplicates:", vehicle_stock_duplicates,
      "nulls:", vehicle_stock_nulls)

print("population - duplicates:", population_duplicates,
      "nulls:", population_nulls)

print("electricity_prices - duplicates:", electricity_price_duplicates,
      "nulls:", electricity_price_nulls)
