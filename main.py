import requests
from pymongo import MongoClient

def fetch_data():
    base_url = "https://earthquake.usgs.gov/fdsnws/event/1/query?"
    start_year = 2018
    end_year = 2023

    data_list = []

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year}-{month:02d}-31"
            params = {
                "format": "geojson",
                "starttime": start_date,
                "endtime": end_date,
                "limit": 20000
            }

            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()

                for feature in data["features"]:
                    properties = feature["properties"]
                    magnitude = properties["mag"]
                    place = properties["place"]
                    date_time = pd.to_datetime(properties["time"], unit='ms')
                    mag_type = properties["magType"]
                    event_type = properties["type"]
                    latitude = feature["geometry"]["coordinates"][1]
                    longitude = feature["geometry"]["coordinates"][0]
                    depth = feature["geometry"]["coordinates"][2]
                    event_id = feature["id"]

                    data_list.append({
                        "magnitude": magnitude,
                        "place": place,
                        "date_time": date_time,
                        "mag_type": mag_type,
                        "event_type": event_type,
                        "latitude": latitude,
                        "longitude": longitude,
                        "depth": depth,
                        "event_id": event_id
                    })
            else:
                print("Error en la solicitud:", response.status_code)

    return data_list

def store_data(data_list):
    # Cadena de conexión a MongoDB Atlas
    client = MongoClient("mongodb+srv://<username>:<password>@cluster1.krfn9qj.mongodb.net/")
    db = client["terremotos"]
    collection = db["coleccion1"]

    # Insertar los datos en la base de datos
    collection.insert_many(data_list)

if __name__ == "__main__":
    data = fetch_data()
    store_data(data)
