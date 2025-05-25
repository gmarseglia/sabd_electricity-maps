QUERY_1_COLUMNS = [
    "date",
    "country",
    "carbon-mean",
    "carbon-min",
    "carbon-max",
    "cfe-mean",
    "cfe-min",
    "cfe-max",
]

# QUERY_2_COLUMNS = ["Year", "Month", "CO2 Intensity", "Carbon Free"]
QUERY_2_COLUMNS = [
    "date",
    "carbon-intensity",
    "cfe",
]

COLUMN_NAMES_RAW = [
    "Datetime",
    "Country",
    "Zone_name",
    "Zone_id",
    "CO2_intensity_direct",
    "CO2_intensity_lifecycle",
    "Carbon_free_energy_percent",
    "Renewable_energy_percent",
    "Data_source",
    "Data_estimated",
    "Data_estimation_method",
]

COLUMN_NAMES_DF_1 = [
    "Year",
    "Country",
    "Zone_id",
    "CO2_intensity_direct",
    "Carbon_free_energy_percent",
]

COLUMN_NAMES_DF_2 = [
    "Year",
    "Month",
    "Country",
    "Zone_id",
    "CO2_intensity_direct",
    "Carbon_free_energy_percent",
]


def get_country(x):
    return x.split(",")[1]


def get_year(x):
    return x.split(",")[0].split("-")[0]


def get_month(x):
    return x.split(",")[0].split("-")[1]


def get_co2_intensity(x) -> float:
    return float(x.split(",")[4])


def get_c02_free(x) -> float:
    return float(x.split(",")[6])


def shorten_country(x: str) -> str:
    if x == "Sweden":
        return "SE"
    if x == "Italy":
        return "IT"
    return x
