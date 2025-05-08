x = "2024-01-01 00:00:00,Italy,Italy,IT,210.41,273.86,56.59,48.48,,,"

year = x.split(",")[0].split("-")[0]
month = x.split(",")[0].split("-")[1]
country = x.split(",")[1]
carbon_intensity = x.split(',')[4]
carbon_free = x.split(',')[6]

print(f"Year: {year}, Month: {month}, Country: {country}, C02 intensity: {carbon_intensity}, C02 free: {carbon_free}")

