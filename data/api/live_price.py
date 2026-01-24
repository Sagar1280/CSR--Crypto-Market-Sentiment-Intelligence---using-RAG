from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import os

url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'

parameters = {
    'start': '1',
    'limit': '10',
    'convert': 'USD'
}

headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': "4fcc7eabd27b4fce8f04e179b2c93eb2" # safer way
}

session = Session()
session.headers.update(headers)

live_prices = {}

def get_live_prices():
    response = session.get(url, params=parameters)
    data = response.json()

    for coin in data["data"]:
            symbol = coin["symbol"]
            price = coin["quote"]["USD"]["price"]
            live_prices[symbol] = price
    print(json.dumps(live_prices, indent=4))
    
    return live_prices

