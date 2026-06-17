import csv
import datetime
import random
import math

def generate_weather_data(filename="sample_data.csv"):
    stations = [
        {"id": "72530094846", "name": "CHICAGO OHARE INTERNATIONAL AIRPORT, IL US", "lat": 41.96, "lon": -87.93, "elev": 201.8, "base_temp": 50.0, "seasonal_amp": 30.0},
        {"id": "74486094789", "name": "JFK INTERNATIONAL AIRPORT, NY US", "lat": 40.64, "lon": -73.78, "elev": 3.4, "base_temp": 54.0, "seasonal_amp": 25.0},
        {"id": "72295023174", "name": "LOS ANGELES INTERNATIONAL AIRPORT, CA US", "lat": 33.94, "lon": -118.40, "elev": 29.6, "base_temp": 63.0, "seasonal_amp": 8.0}
    ]

    start_date = datetime.date(2016, 1, 1)
    end_date = datetime.date(2025, 12, 31)
    delta = datetime.date(2026, 1, 1) - start_date
    total_days = delta.days

    headers = [
        "STATION", "DATE", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME",
        "TEMP", "TEMP_ATTRIBUTES", "DEWP", "DEWP_ATTRIBUTES", "SLP", "SLP_ATTRIBUTES",
        "STP", "STP_ATTRIBUTES", "VISIB", "VISIB_ATTRIBUTES", "WDSP", "WDSP_ATTRIBUTES",
        "MXSPD", "GUST", "MAX", "MAX_ATTRIBUTES", "MIN", "MIN_ATTRIBUTES",
        "PRCP", "PRCP_ATTRIBUTES", "SNDP", "FRSHTT"
    ]

    anomalies = [
        # Explicit anomaly dates
        (datetime.date(2020, 7, 15), "72530094846", 115.0), # Extreme heat in Chicago (normal summer is ~75-85)
        (datetime.date(2022, 1, 20), "72530094846", -45.0), # Extreme cold in Chicago (normal winter is ~20-30)
        (datetime.date(2025, 8, 10), "74486094789", 120.0), # Extreme heat at JFK (normal summer is ~75-80)
        (datetime.date(2024, 2, 5), "72295023174", 102.0),  # Extreme heat in LA winter (normal winter is ~55-60)
        (datetime.date(2023, 12, 25), "72295023174", 25.0)   # Extreme cold in LA winter (normal winter is ~55-60)
    ]

    anomaly_map = {(date, stn): temp for date, stn, temp in anomalies}

    print(f"Generating synthetic GSOD weather dataset for {total_days} days across {len(stations)} stations...")

    with open(filename, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for i in range(total_days):
            current_date = start_date + datetime.timedelta(days=i)
            # Seasonal variation using sine wave based on day of the year (July 15 is around peak)
            day_of_year = current_date.timetuple().tm_yday
            angle = 2 * math.pi * (day_of_year - 105) / 365.0 # offset to align peak temp in July

            for station in stations:
                stn_id = station["id"]
                
                # Check if this date & station is an explicit anomaly
                if (current_date, stn_id) in anomaly_map:
                    temp = anomaly_map[(current_date, stn_id)]
                else:
                    # Generate normal temperature with some random Gaussian noise (sigma ~ 5F)
                    expected_temp = station["base_temp"] + station["seasonal_amp"] * math.sin(angle)
                    temp = expected_temp + random.normalvariate(0, 4.5)
                
                # Other variables can be mock values
                row = [
                    stn_id,
                    current_date.strftime("%Y-%m-%d"),
                    station["lat"],
                    station["lon"],
                    station["elev"],
                    station["name"],
                    round(temp, 1),
                    "0", # TEMP_ATTRIBUTES
                    round(temp - 10.0, 1), # DEWP
                    "0",
                    "1013.2", # SLP
                    "0",
                    "1010.5", # STP
                    "0",
                    "10.0", # VISIB
                    "0",
                    "5.4", # WDSP
                    "0",
                    "12.0", # MXSPD
                    "999.9", # GUST
                    round(temp + 5.0, 1), # MAX
                    "0",
                    round(temp - 5.0, 1), # MIN
                    "0",
                    "0.00", # PRCP
                    "I",
                    "999.9", # SNDP
                    "000000" # FRSHTT
                ]
                writer.writerow(row)

    print(f"Data generation complete! Saved to {filename}")

if __name__ == "__main__":
    generate_weather_data()
