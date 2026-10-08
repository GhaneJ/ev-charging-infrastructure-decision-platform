# COMMAND ----------
# Notebook cell 1
SCB_API_BASE = "https://statistikdatabasen.scb.se/api/v2"

SCB_NEW_REGISTRATIONS_TABLE = "tab3277"
SCB_VEHICLE_STOCK_TABLE = "tab1278"
SCB_POPULATION_TABLE = "tab5557"

ELECTRICITY_PRICE_API_BASE = "https://www.elprisetjustnu.se/api/v1/prices"

ELECTRICITY_AREAS = ["SE1", "SE2", "SE3", "SE4"]
ELECTRICITY_PRICE_DAYS = 30


# COMMAND ----------
# Notebook cell 2
import requests

metadata_url = f"{SCB_API_BASE}/tables/{SCB_NEW_REGISTRATIONS_TABLE}/metadata"

response = requests.get(metadata_url, timeout=30)

print("Status code:", response.status_code)
print(response.text[:2000])


# COMMAND ----------
# Notebook cell 3
metadata = response.json()

print("Table:", metadata["label"])

for dimension_id, size in zip(metadata["id"], metadata["size"]):
    labels = metadata["dimension"][dimension_id]["category"].get("label", {})
    
    print(f"\n{dimension_id}: {size} values")
    print(list(labels.items())[:10])

    if dimension_id == "Tid":
        print("Latest periods:", list(labels.items())[-5:])


# COMMAND ----------
# Notebook cell 4
region_labels = metadata["dimension"]["Region"]["category"]["label"]
time_labels = metadata["dimension"]["Tid"]["category"]["label"]

CURRENT_HEBY_CODE = "0331"
HISTORICAL_HEBY_CODE = "1917"

municipalities = {
    region_code: region_name
    for region_code, region_name in region_labels.items()
    if len(region_code) == 4
    and region_code != HISTORICAL_HEBY_CODE
}

latest_period = list(time_labels.keys())[-1]

print("Municipalities:", len(municipalities))
print("Latest period:", latest_period)
print("Heby:", CURRENT_HEBY_CODE, municipalities[CURRENT_HEBY_CODE])


# COMMAND ----------
# Notebook cell 5
REGISTRATION_MONTHS = 12

fuel_codes = list(
    metadata["dimension"]["Drivmedel"]["category"]["label"].keys()
)

content_codes = list(
    metadata["dimension"]["ContentsCode"]["category"]["label"].keys()
)

selected_periods = list(time_labels.keys())[-REGISTRATION_MONTHS:]

query = {
    "selection": [
        {
            "variableCode": "Region",
            "valueCodes": list(municipalities.keys())
        },
        {
            "variableCode": "Drivmedel",
            "valueCodes": fuel_codes
        },
        {
            "variableCode": "ContentsCode",
            "valueCodes": content_codes
        },
        {
            "variableCode": "Tid",
            "valueCodes": selected_periods
        }
    ]
}

number_of_cells = (
    len(municipalities)
    * len(fuel_codes)
    * len(content_codes)
    * len(selected_periods)
)

print("Municipalities:", len(municipalities))
print("Fuel types:", len(fuel_codes))
print("Periods:", len(selected_periods))
print("From:", selected_periods[0])
print("To:", selected_periods[-1])
print("Data cells:", number_of_cells)


# COMMAND ----------
# Notebook cell 6
data_url = f"{SCB_API_BASE}/tables/{SCB_NEW_REGISTRATIONS_TABLE}/data"

data_response = requests.post(
    data_url,
    params={"outputFormat": "json-stat2"},
    json=query,
    timeout=60
)

data_response.raise_for_status()

data = data_response.json()

print("Status code:", data_response.status_code)
print("Data cells received:", len(data["value"]))


# COMMAND ----------
# Notebook cell 7
from itertools import product
from pyspark.sql.functions import current_timestamp

region_dimension = data["dimension"]["Region"]["category"]
fuel_dimension = data["dimension"]["Drivmedel"]["category"]
measure_dimension = data["dimension"]["ContentsCode"]["category"]
time_dimension = data["dimension"]["Tid"]["category"]

region_codes = sorted(
    region_dimension["index"],
    key=region_dimension["index"].get
)

fuel_codes = sorted(
    fuel_dimension["index"],
    key=fuel_dimension["index"].get
)

measure_codes = sorted(
    measure_dimension["index"],
    key=measure_dimension["index"].get
)

periods = sorted(
    time_dimension["index"],
    key=time_dimension["index"].get
)

rows = []

for dimensions, value in zip(
    product(region_codes, fuel_codes, measure_codes, periods),
    data["value"]
):
    region_code, fuel_code, measure_code, period = dimensions

    rows.append({
        "region_code": region_code,
        "region_name": region_dimension["label"][region_code],
        "fuel_code": fuel_code,
        "fuel_type": fuel_dimension["label"][fuel_code],
        "measure_code": measure_code,
        "measure": measure_dimension["label"][measure_code],
        "period": period,
        "value": value
    })

bronze_df = (
    spark.createDataFrame(rows)
    .withColumn("ingested_at", current_timestamp())
)

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "charging_infrastructure.bronze.scb_new_vehicle_registrations"
    )

print("Rows saved:", len(rows))
print(
    "Table:",
    "charging_infrastructure.bronze.scb_new_vehicle_registrations"
)


# COMMAND ----------
# Notebook cell 8
vehicle_stock_metadata_url = (
    f"{SCB_API_BASE}/tables/{SCB_VEHICLE_STOCK_TABLE}/metadata"
)

population_metadata_url = (
    f"{SCB_API_BASE}/tables/{SCB_POPULATION_TABLE}/metadata"
)

vehicle_stock_response = requests.get(
    vehicle_stock_metadata_url,
    timeout=30
)

population_response = requests.get(
    population_metadata_url,
    timeout=30
)

vehicle_stock_response.raise_for_status()
population_response.raise_for_status()

vehicle_stock_metadata = vehicle_stock_response.json()
population_metadata = population_response.json()

print("VEHICLE STOCK")
print(vehicle_stock_metadata["label"])
print("Dimensions:", vehicle_stock_metadata["id"])
print("Sizes:", vehicle_stock_metadata["size"])

print("\nPOPULATION")
print(population_metadata["label"])
print("Dimensions:", population_metadata["id"])
print("Sizes:", population_metadata["size"])


# COMMAND ----------
# Notebook cell 9
print("VEHICLE TYPES")
for code, name in vehicle_stock_metadata["dimension"]["Fordonsslag"]["category"]["label"].items():
    print(code, "-", name)

print("\nVEHICLE YEARS")
for code, name in vehicle_stock_metadata["dimension"]["Tid"]["category"]["label"].items():
    print(code, "-", name)


print("\nPOPULATION - CIVIL STATUS")
for code, name in population_metadata["dimension"]["Civilstand"]["category"]["label"].items():
    print(code, "-", name)

print("\nPOPULATION - SEX")
for code, name in population_metadata["dimension"]["Kon"]["category"]["label"].items():
    print(code, "-", name)

print("\nPOPULATION - CONTENT")
for code, name in population_metadata["dimension"]["ContentsCode"]["category"]["label"].items():
    print(code, "-", name)

print("\nPOPULATION - YEAR")
for code, name in population_metadata["dimension"]["Tid"]["category"]["label"].items():
    print(code, "-", name)

print("\nPOPULATION - AGE (first and last values)")
age_labels = population_metadata["dimension"]["Alder"]["category"]["label"]

print("First:", list(age_labels.items())[:5])
print("Last:", list(age_labels.items())[-5:])


# COMMAND ----------
# Notebook cell 10
REFERENCE_YEAR = "2025"

PASSENGER_CAR_CODE = "10"

POPULATION_TOTAL_CIVIL_STATUS = "SC"
POPULATION_TOTAL_AGE = "TotSA"
POPULATION_TOTAL_SEX = "TotSa"
POPULATION_COUNT_CODE = "000007ME"


vehicle_region_labels = (
    vehicle_stock_metadata["dimension"]["Region"]["category"]["label"]
)

population_region_labels = (
    population_metadata["dimension"]["Region"]["category"]["label"]
)


vehicle_municipality_codes = [
    code for code in municipalities
    if code in vehicle_region_labels
]

population_municipality_codes = [
    code for code in municipalities
    if code in population_region_labels
]


vehicle_stock_content_code = list(
    vehicle_stock_metadata["dimension"]["ContentsCode"]["category"]["label"].keys()
)[0]


vehicle_stock_query = {
    "selection": [
        {
            "variableCode": "Region",
            "valueCodes": vehicle_municipality_codes
        },
        {
            "variableCode": "Fordonsslag",
            "valueCodes": [PASSENGER_CAR_CODE]
        },
        {
            "variableCode": "ContentsCode",
            "valueCodes": [vehicle_stock_content_code]
        },
        {
            "variableCode": "Tid",
            "valueCodes": [REFERENCE_YEAR]
        }
    ]
}


population_query = {
    "selection": [
        {
            "variableCode": "Region",
            "valueCodes": population_municipality_codes
        },
        {
            "variableCode": "Civilstand",
            "valueCodes": [POPULATION_TOTAL_CIVIL_STATUS]
        },
        {
            "variableCode": "Alder",
            "valueCodes": [POPULATION_TOTAL_AGE]
        },
        {
            "variableCode": "Kon",
            "valueCodes": [POPULATION_TOTAL_SEX]
        },
        {
            "variableCode": "ContentsCode",
            "valueCodes": [POPULATION_COUNT_CODE]
        },
        {
            "variableCode": "Tid",
            "valueCodes": [REFERENCE_YEAR]
        }
    ]
}


print("Vehicle municipalities:", len(vehicle_municipality_codes))
print("Population municipalities:", len(population_municipality_codes))
print("Reference year:", REFERENCE_YEAR)


# COMMAND ----------
# Notebook cell 11
# --- Vehicle stock ---

vehicle_stock_url = (
    f"{SCB_API_BASE}/tables/{SCB_VEHICLE_STOCK_TABLE}/data"
)

vehicle_stock_response = requests.post(
    vehicle_stock_url,
    params={"outputFormat": "json-stat2"},
    json=vehicle_stock_query,
    timeout=60
)

vehicle_stock_response.raise_for_status()
vehicle_stock_data = vehicle_stock_response.json()

vehicle_region = vehicle_stock_data["dimension"]["Region"]["category"]

vehicle_region_codes = sorted(
    vehicle_region["index"],
    key=vehicle_region["index"].get
)

vehicle_stock_rows = []

for region_code, value in zip(
    vehicle_region_codes,
    vehicle_stock_data["value"]
):
    vehicle_stock_rows.append({
        "region_code": region_code,
        "region_name": vehicle_region["label"][region_code],
        "vehicle_type": "personbilar",
        "year": REFERENCE_YEAR,
        "vehicle_count": value
    })

vehicle_stock_df = (
    spark.createDataFrame(vehicle_stock_rows)
    .withColumn("ingested_at", current_timestamp())
)

vehicle_stock_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "charging_infrastructure.bronze.scb_passenger_car_stock"
    )


# --- Population ---

population_url = (
    f"{SCB_API_BASE}/tables/{SCB_POPULATION_TABLE}/data"
)

population_response = requests.post(
    population_url,
    params={"outputFormat": "json-stat2"},
    json=population_query,
    timeout=60
)

population_response.raise_for_status()
population_data = population_response.json()

population_region = population_data["dimension"]["Region"]["category"]

population_region_codes = sorted(
    population_region["index"],
    key=population_region["index"].get
)

population_rows = []

for region_code, value in zip(
    population_region_codes,
    population_data["value"]
):
    population_rows.append({
        "region_code": region_code,
        "region_name": population_region["label"][region_code],
        "year": REFERENCE_YEAR,
        "population": value
    })

population_df = (
    spark.createDataFrame(population_rows)
    .withColumn("ingested_at", current_timestamp())
)

population_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "charging_infrastructure.bronze.scb_population"
    )


print("Vehicle stock rows saved:", len(vehicle_stock_rows))
print("Population rows saved:", len(population_rows))


# COMMAND ----------
# Notebook cell 12
from datetime import date, timedelta

electricity_price_rows = []

end_date = date.today()
start_date = end_date - timedelta(days=ELECTRICITY_PRICE_DAYS - 1)

for day_offset in range(ELECTRICITY_PRICE_DAYS):
    price_date = start_date + timedelta(days=day_offset)

    year = price_date.strftime("%Y")
    month_day = price_date.strftime("%m-%d")

    for price_area in ELECTRICITY_AREAS:
        url = (
            f"{ELECTRICITY_PRICE_API_BASE}/"
            f"{year}/{month_day}_{price_area}.json"
        )

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        for price in response.json():
            electricity_price_rows.append({
                "price_area": price_area,
                "price_date": str(price_date),
                "sek_per_kwh": float(price["SEK_per_kWh"]),
                "eur_per_kwh": float(price["EUR_per_kWh"]),
                "exchange_rate": float(price["EXR"]),
                "time_start": price["time_start"],
                "time_end": price["time_end"]
            })

electricity_prices_df = (
    spark.createDataFrame(electricity_price_rows)
    .withColumn("ingested_at", current_timestamp())
)

electricity_prices_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "charging_infrastructure.bronze.electricity_prices"
    )

print("From:", start_date)
print("To:", end_date)
print("Rows saved:", len(electricity_price_rows))
print("Price areas:", ELECTRICITY_AREAS)
