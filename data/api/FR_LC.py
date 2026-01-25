from requests import Session
import json

url = "https://pro-api.coinmarketcap.com/v3/fear-and-greed/latest"

headers = {
    "Accepts": "application/json",
    "X-CMC_PRO_API_KEY": "4fcc7eabd27b4fce8f04e179b2c93eb2"
}

session = Session()
session.headers.update(headers)

def get_fear_greed():
    response = session.get(url)
    data = response.json()

    value = data["data"]["value"]
    classification = data["data"]["value_classification"]

    print("Fear & Greed Index:", value)
    print("Classification:", classification)

    return value, classification

