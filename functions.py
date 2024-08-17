import requests
from datetime import datetime
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configuración
api_key = os.getenv('OPENWEATHER_API_KEY')  # Usa la clave de API desde .env
url = 'https://api.openweathermap.org/data/2.5/weather'  # Endpoint para datos del clima

# Lista de ciudades
cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
    'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose',
    'Austin', 'Jacksonville', 'San Francisco', 'Columbus', 'Indianapolis',
    'Fort Worth', 'Charlotte', 'Seattle', 'Denver', 'El Paso',
    'Detroit', 'Boston', 'Memphis', 'Nashville', 'Baltimore',
    'Oklahoma City', 'Las Vegas', 'Louisville', 'Milwaukee', 'Albuquerque',
    'Tucson', 'Fresno', 'Sacramento', 'Kansas City', 'Mesa',
    'Virginia Beach', 'Atlanta', 'Colorado Springs', 'Omaha', 'Raleigh',
    'Miami', 'Cleveland', 'Tulsa', 'Oakland', 'Minneapolis',
    'Wichita', 'Arlington', 'Bakersfield', 'Tampa', 'Aurora',
    'Honolulu', 'Anaheim', 'Santa Ana', 'St. Louis', 'Riverside',
    'Corpus Christi', 'Lexington', 'Stockton', 'Henderson', 'Saint Paul',
    'Cincinnati', 'Pittsburgh', 'Greensboro', 'Anchorage', 'Plano',
    'Newark', 'Lincoln', 'Toledo', 'Orlando', 'Chula Vista',
    'Jersey City', 'Buffalo', 'Durham', 'Madison', 'Lubbock']

def fetch_weather_data():
    """Obtiene datos del clima de OpenWeatherMap para varias ciudades."""
    all_data = []
    for city in cities:
        params = {
            'q': city,
            'appid': api_key,
            'units': 'metric'  # Unidades en grados Celsius
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            all_data.append(response.json())
        else:
            print(f"Error al obtener datos del clima para {city}: {response.status_code}")
    return all_data

def process_weather_data(weather_data_list):
    """Procesa los datos del clima y devuelve una lista de diccionarios."""
    data = []
    for weather_data in weather_data_list:
        if weather_data:
            entry = {
                'city': weather_data['name'],
                'temperature': weather_data['main']['temp'],
                'humidity': weather_data['main']['humidity'],
                'weather_description': weather_data['weather'][0]['description'],
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            data.append(entry)
    return data

def truncate_string(value, length):
    """Trunca una cadena a un tamaño específico."""
    return value[:length] if value and len(value) > length else value
