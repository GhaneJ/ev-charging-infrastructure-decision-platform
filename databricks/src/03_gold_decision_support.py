# COMMAND ----------
# Notebook cell 1
from pyspark.sql.functions import col, lit, when

SE1_MUNICIPALITIES = [
    "Arjeplog", "Arvidsjaur", "Boden", "Gällivare", "Haparanda",
    "Jokkmokk", "Kalix", "Kiruna", "Luleå", "Malå", "Norsjö",
    "Pajala", "Piteå", "Skellefteå", "Älvsbyn", "Överkalix",
    "Övertorneå"
]

SE2_MUNICIPALITIES = [
    "Berg", "Bjurholm", "Bollnäs", "Bräcke", "Dorotea",
    "Hudiksvall", "Härjedalen", "Härnösand", "Kramfors", "Krokom",
    "Ljusdal", "Lycksele", "Nordanstig", "Nordmaling", "Ockelbo",
    "Ovanåker", "Ragunda", "Robertsfors", "Sollefteå", "Sorsele",
    "Storuman", "Strömsund", "Sundsvall", "Söderhamn", "Timrå",
    "Umeå", "Vilhelmina", "Vindeln", "Vännäs", "Ånge", "Åre",
    "Åsele", "Örnsköldsvik", "Östersund"
]

SE4_MUNICIPALITIES = [
    "Alvesta", "Bjuv", "Borgholm", "Bromölla", "Burlöv", "Båstad",
    "Emmaboda", "Eslöv", "Falkenberg", "Halmstad", "Helsingborg",
    "Hylte", "Hässleholm", "Höganäs", "Högsby", "Hörby", "Höör",
    "Kalmar", "Karlshamn", "Karlskrona", "Klippan", "Kristianstad",
    "Kävlinge", "Laholm", "Landskrona", "Lessebo", "Ljungby",
    "Lomma", "Lund", "Malmö", "Markaryd", "Mönsterås", "Mörbylånga",
    "Nybro", "Olofström", "Osby", "Oskarshamn", "Perstorp", "Ronneby",
    "Simrishamn", "Sjöbo", "Skurup", "Staffanstorp", "Svalöv",
    "Svedala", "Sölvesborg", "Tingsryd", "Tomelilla", "Torsås",
    "Trelleborg", "Uppvidinge", "Vellinge", "Värnamo", "Växjö",
    "Ystad", "Åstorp", "Älmhult", "Ängelholm", "Örkelljunga",
    "Östra Göinge"
]

municipality_df = (
    spark.table("charging_infrastructure.silver.population")
    .select("region_code", "region_name")
    .dropDuplicates()
)

dim_municipality_df = (
    municipality_df
    .withColumn(
        "price_area",
        when(col("region_name").isin(*SE1_MUNICIPALITIES), lit("SE1"))
        .when(col("region_name").isin(*SE2_MUNICIPALITIES), lit("SE2"))
        .when(col("region_name").isin(*SE4_MUNICIPALITIES), lit("SE4"))
        .otherwise(lit("SE3"))
    )
)

area_counts = {
    row["price_area"]: row["count"]
    for row in dim_municipality_df
        .groupBy("price_area")
        .count()
        .collect()
}

EXPECTED_AREA_COUNTS = {
    "SE1": 17,
    "SE2": 34,
    "SE3": 179,
    "SE4": 60
}

assert dim_municipality_df.count() == 290
assert area_counts == EXPECTED_AREA_COUNTS

dim_municipality_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "charging_infrastructure.gold.dim_municipality"
    )

print("Municipalities:", dim_municipality_df.count())
print("Price areas:", area_counts)
print("Gold dimension created successfully.")


# COMMAND ----------
# Notebook cell 2
from pyspark.sql.functions import avg, min, max, count, round

electricity_price_summary_df = (
    spark.table("charging_infrastructure.silver.electricity_prices")
    .groupBy("price_area")
    .agg(
        round(avg("sek_per_kwh"), 4).alias("avg_sek_per_kwh"),
        round(min("sek_per_kwh"), 4).alias("min_sek_per_kwh"),
        round(max("sek_per_kwh"), 4).alias("max_sek_per_kwh"),
        count("*").alias("observations")
    )
)

electricity_price_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "charging_infrastructure.gold.electricity_price_summary"
    )

electricity_price_summary_df.orderBy("price_area").show(truncate=False)


# COMMAND ----------
# Notebook cell 3
from pyspark.sql import functions as F


# ---------------------------------------------------------
# 1. Monthly EV registrations from Silver
# ---------------------------------------------------------

ev_monthly_df = (
    spark.table(
        "charging_infrastructure.silver.vehicle_registrations"
    )
    .filter(F.col("fuel_type") == "el")
)


# ---------------------------------------------------------
# 2. Split the 12-month period into two six-month periods
# ---------------------------------------------------------

registration_months = [
    row["month"]
    for row in (
        ev_monthly_df
        .select("month")
        .distinct()
        .orderBy("month")
        .collect()
    )
]

assert len(registration_months) == 12

previous_6_months = registration_months[:6]
recent_6_months = registration_months[6:]


# ---------------------------------------------------------
# 3. Calculate EV registrations and EV growth
#    for each municipality
# ---------------------------------------------------------

ev_metrics_df = (
    ev_monthly_df
    .groupBy("region_code")
    .agg(
        F.sum("registrations")
        .alias("ev_registrations_12m"),

        F.sum(
            F.when(
                F.col("month").isin(*previous_6_months),
                F.col("registrations")
            ).otherwise(0)
        ).alias("ev_registrations_previous_6m"),

        F.sum(
            F.when(
                F.col("month").isin(*recent_6_months),
                F.col("registrations")
            ).otherwise(0)
        ).alias("ev_registrations_recent_6m")
    )
    .withColumn(
        "ev_registration_change_6m",
        F.col("ev_registrations_recent_6m")
        - F.col("ev_registrations_previous_6m")
    )
)


# ---------------------------------------------------------
# 4. Build one Gold row per municipality
# ---------------------------------------------------------

municipality_metrics_df = (
    spark.table(
        "charging_infrastructure.gold.dim_municipality"
    ).alias("m")

    .join(
        spark.table(
            "charging_infrastructure.silver.population"
        ).alias("p"),
        "region_code"
    )

    .join(
        spark.table(
            "charging_infrastructure.silver.passenger_car_stock"
        ).alias("c"),
        "region_code"
    )

    .join(
        ev_metrics_df.alias("ev"),
        "region_code"
    )

    .join(
        spark.table(
            "charging_infrastructure.gold.electricity_price_summary"
        ).alias("e"),
        F.col("m.price_area") == F.col("e.price_area")
    )

    .select(
        "region_code",

        F.col("m.region_name").alias("municipality"),
        F.col("m.price_area"),

        F.col("p.population"),
        F.col("c.vehicle_count").alias("passenger_cars"),

        F.col("ev.ev_registrations_12m"),
        F.col("ev.ev_registrations_previous_6m"),
        F.col("ev.ev_registrations_recent_6m"),
        F.col("ev.ev_registration_change_6m"),

        F.round(
            F.col("ev.ev_registrations_12m")
            / F.col("c.vehicle_count")
            * 1000,
            1
        ).alias("ev_registrations_per_1000_cars"),

        F.round(
            F.col("ev.ev_registrations_12m")
            / F.col("p.population")
            * 1000,
            1
        ).alias("ev_registrations_per_1000_residents"),

        F.col("e.avg_sek_per_kwh")
    )
)


# ---------------------------------------------------------
# 5. Validate that all 290 municipalities are present
# ---------------------------------------------------------

row_count = municipality_metrics_df.count()

assert row_count == 290


# ---------------------------------------------------------
# 6. Recreate the derived Gold table
#
# The table schema changed from the earlier model, so the
# old derived table is removed before the new one is written.
# ---------------------------------------------------------

spark.sql("""
    DROP TABLE IF EXISTS
    charging_infrastructure.gold.municipality_metrics
""")


municipality_metrics_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "charging_infrastructure.gold.municipality_metrics"
    )


# ---------------------------------------------------------
# 7. Show the result
# ---------------------------------------------------------

print("Municipalities:", row_count)

print(
    "Previous 6 months:",
    previous_6_months[0],
    "to",
    previous_6_months[-1]
)

print(
    "Recent 6 months:",
    recent_6_months[0],
    "to",
    recent_6_months[-1]
)

municipality_metrics_df \
    .orderBy(F.desc("ev_registrations_12m")) \
    .show(10, truncate=False)


# COMMAND ----------
# Notebook cell 4
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)


# ---------------------------------------------------------
# 1. Read municipality metrics
# ---------------------------------------------------------

metrics_df = spark.table(
    "charging_infrastructure.gold.municipality_metrics"
)


# ---------------------------------------------------------
# 2. Score EV volume and EV growth by municipality rank
#
# 0 = lowest municipality
# 1 = highest municipality
# ---------------------------------------------------------

ev_volume_window = Window.orderBy(
    "ev_registrations_12m"
)

ev_growth_window = Window.orderBy(
    "ev_registration_change_6m"
)

scored_df = (
    metrics_df
    .withColumn(
        "ev_volume_score",
        F.percent_rank().over(ev_volume_window)
    )
    .withColumn(
        "ev_growth_score",
        F.percent_rank().over(ev_growth_window)
    )
)


# ---------------------------------------------------------
# 3. Normalize electricity price
#
# Lower electricity price = higher score
# ---------------------------------------------------------

price_bounds = (
    spark.table(
        "charging_infrastructure.gold.electricity_price_summary"
    )
    .agg(
        F.min("avg_sek_per_kwh").alias("min_price"),
        F.max("avg_sek_per_kwh").alias("max_price")
    )
    .first()
)

electricity_price_range = (
    price_bounds["max_price"]
    - price_bounds["min_price"]
)

scored_df = (
    scored_df
    .withColumn(
        "electricity_price_score",
        (
            F.lit(price_bounds["max_price"])
            - F.col("avg_sek_per_kwh")
        )
        / F.lit(electricity_price_range)
    )
)


# ---------------------------------------------------------
# 4. Equal weighting of the three decision criteria
# ---------------------------------------------------------

scored_df = (
    scored_df
    .withColumn(
        "decision_score",
        F.round(
            (
                F.col("ev_volume_score")
                + F.col("ev_growth_score")
                + F.col("electricity_price_score")
            ) / 3,
            4
        )
    )
)


# ---------------------------------------------------------
# 5. Select the 50 strongest candidate municipalities
# ---------------------------------------------------------

top_50_without_rank_df = (
    scored_df
    .orderBy(
        F.desc("decision_score"),
        F.desc("ev_registrations_12m"),
        F.asc("avg_sek_per_kwh"),
        F.asc("region_code")
    )
    .limit(50)
)


# ---------------------------------------------------------
# 6. Add rank 1-50
# ---------------------------------------------------------

rank_rows = [
    (row["region_code"], rank)
    for rank, row in enumerate(
        top_50_without_rank_df
        .select("region_code")
        .collect(),
        start=1
    )
]

rank_schema = StructType([
    StructField("region_code", StringType(), False),
    StructField("rank", IntegerType(), False)
])

rank_df = spark.createDataFrame(
    rank_rows,
    schema=rank_schema
)

top_50_df = (
    top_50_without_rank_df
    .join(rank_df, "region_code")
    .orderBy("rank")
)


# ---------------------------------------------------------
# 7. Save Gold decision-support table
# ---------------------------------------------------------

top_50_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "charging_infrastructure.gold.top_50_charging_candidates"
    )


print("Candidates saved:", top_50_df.count())

top_50_df.select(
    "rank",
    "municipality",
    "price_area",
    "ev_registrations_12m",
    "ev_registration_change_6m",
    "avg_sek_per_kwh",
    "ev_volume_score",
    "ev_growth_score",
    "electricity_price_score",
    "decision_score"
).show(50, truncate=False)


# COMMAND ----------
# Notebook cell 5
from pyspark.sql import functions as F


# ---------------------------------------------------------
# Gold quality check
# ---------------------------------------------------------

candidates_df = spark.table(
    "charging_infrastructure.gold.top_50_charging_candidates"
)


quality_result = (
    candidates_df
    .agg(
        F.count("*").alias("rows"),

        F.countDistinct("region_code")
        .alias("distinct_municipalities"),

        F.countDistinct("rank")
        .alias("distinct_ranks"),

        F.min("rank").alias("min_rank"),
        F.max("rank").alias("max_rank"),

        F.sum(
            F.when(
                F.col("municipality").isNull()
                | F.col("price_area").isNull()
                | F.col("ev_registrations_12m").isNull()
                | F.col("ev_registration_change_6m").isNull()
                | F.col("avg_sek_per_kwh").isNull()
                | F.col("decision_score").isNull(),
                1
            ).otherwise(0)
        ).alias("rows_with_nulls"),

        F.sum(
            F.when(
                ~F.col("ev_volume_score").between(0.0, 1.0)
                | ~F.col("ev_growth_score").between(0.0, 1.0)
                | ~F.col("electricity_price_score").between(0.0, 1.0)
                | ~F.col("decision_score").between(0.0, 1.0),
                1
            ).otherwise(0)
        ).alias("rows_with_invalid_scores")
    )
    .first()
)


# ---------------------------------------------------------
# Validate expected Gold result
# ---------------------------------------------------------

assert quality_result["rows"] == 50
assert quality_result["distinct_municipalities"] == 50
assert quality_result["distinct_ranks"] == 50
assert quality_result["min_rank"] == 1
assert quality_result["max_rank"] == 50
assert quality_result["rows_with_nulls"] == 0
assert quality_result["rows_with_invalid_scores"] == 0


# ---------------------------------------------------------
# Show quality result
# ---------------------------------------------------------

print("Gold quality check: PASSED")
print("Rows:", quality_result["rows"])
print(
    "Distinct municipalities:",
    quality_result["distinct_municipalities"]
)
print(
    "Distinct ranks:",
    quality_result["distinct_ranks"]
)
print(
    "Rank range:",
    quality_result["min_rank"],
    "to",
    quality_result["max_rank"]
)
print(
    "Rows with nulls:",
    quality_result["rows_with_nulls"]
)
print(
    "Rows with invalid scores:",
    quality_result["rows_with_invalid_scores"]
)
