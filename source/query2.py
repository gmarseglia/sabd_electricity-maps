# Query 2.2
from source.formatter import *


def query2(sc, italy_file):
    # by_c02_intensity = hourly_lines \
    #     .filter(lambda x: get_country(x) == "Italy") \
    #     .map(lambda x: (get_year(x), get_month(x), x)) \
    #     .map(lambda x: (get_co2_intensity(x[2]), x[0], x[1], x[2])) \
    #     .cache()
    #
    # best_5 = by_c02_intensity.takeOrdered(5, lambda x: x[0])
    # worst_5 = by_c02_intensity.takeOrdered(5, lambda x: -x[0])

    return
