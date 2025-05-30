# regions = [
#     "IT-CNO", "IT-CSO", "IT-NO", "IT-SAR", "IT-SIC", "IT-SO",
#     "SE-SE1", "SE-SE2", "SE-SE3", "SE-SE4"
# ]

regions = ["IT", "SE"]

years = ["2021", "2022", "2023", "2024"]

combinations = [
    f"https://data.electricitymaps.com/2025-04-03/{s1}_{s2}_hourly.csv"
    for s2 in years
    for s1 in regions
]

print(combinations)

with open("datasets_url.txt", "w") as file:
    for combination in combinations:
        file.write(f"{combination}\n")
