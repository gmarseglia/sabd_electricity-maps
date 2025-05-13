COLUMN_NAMES = [
    'Datetime', 'Country', 'Zone_name', 'Zone_id',
    'CO2_intensity_direct', 'CO2_intensity_lifecycle',
    'Carbon_free_energy_percent', 'Renewable_energy_percent',
    'Data_source', 'Data_estimated', 'Data_estimation_method'
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
