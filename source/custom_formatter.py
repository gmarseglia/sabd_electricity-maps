QUERY_1_COLUMNS = [
    "Country", "Year",
    "Avg CO2 Intensity", "Min CO2 Intensity", "Max CO2 Intensity",
    "Avg C02 Free", "Min C02 Free", "Max C02 Free"
]

COLUMN_NAMES_RAW = [
    'Datetime', 'Country', 'Zone_name', 'Zone_id',
    'CO2_intensity_direct', 'CO2_intensity_lifecycle',
    'Carbon_free_energy_percent', 'Renewable_energy_percent',
    'Data_source', 'Data_estimated', 'Data_estimation_method'
]

COLUMN_NAMES_DF = [
    'Year', 'Month', 'Country', 'Zone_id',
    'CO2_intensity_direct',
    'Carbon_free_energy_percent'
]


def get_country(x):
    return x.split(",")[1]


def get_year(x):
    return x.split(",")[0].split("-")[0]


def get_month(x):
    return x.split(",")[0].split("-")[1]


def get_co2_intensity(x) -> float:
    return float(x.split(',')[4])


def get_c02_free(x) -> float:
    return float(x.split(',')[6])
