import requests
from config import API_URL

def fetch_api_data():
    response = requests.get(API_URL)
    data = response.json()

    final_data = []

    for item in data["products"]:
        final_data.append({
            "id": item.get("id"),
            "title": item.get("title"),
            "price": item.get("price"),
            "brand": item.get("brand"),          # FIXED
            "category": item.get("category"),
            "rating": item.get("rating")
        })

    return final_data

print(fetch_api_data())
