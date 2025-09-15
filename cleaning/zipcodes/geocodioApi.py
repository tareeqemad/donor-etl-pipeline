from geocodio import Client
import requests
import json

# Initialize the client with your API key
client = Client("089262dd8e13cf60a0806980a800812d3c1caa3")

def reverse_geocode(lat, lon):
    """Perform reverse geocoding using the Geocodio API"""
    url = "https://api.geocod.io/v1.6/reverse"
    params = {
        'q': f"{lat},{lon}",
        'api_key': client.api_key
    }
    response = requests.get(url, params=params)
    return response.json()

# Perform reverse geocoding
coordinates = "18.3293,-64.8535"
lat, lon = coordinates.split(',')
result = reverse_geocode(lat, lon)

# Access the first result
if result.get('results'):
    print(result['results'][0]['formatted_address'])
else:
    print("No results found")
