from flask import Flask, request, jsonify
import requests
import pymysql
from datetime import datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

API_KEY = '7383ac9ab7a320de0f315f45248361a6'

# fonction pour avoir les informations global meteologique des ville à partir de l'api afin d'avoir les informations sur l'UV 
def fetch_uv_index(lat, lon):
    base_url = f'http://api.openweathermap.org/data/2.5/uvi'
    params = {
        'lat': lat,
        'lon': lon,
        'appid': API_KEY
    }
    response = requests.get(base_url, params=params)
    return response.json()


# fonction pour avoir les informations global meteologique des ville  

def fetch_weather_data(city):
    base_url = 'http://api.openweathermap.org/data/2.5/weather'
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric'
    }
    response = requests.get(base_url, params=params)
    return response.json()



@app.route('/weather', methods=['GET'])
def get_weather():
    city = request.args.get('city')
    if not city:
        return jsonify({"error": "City parameter is required"}), 400
    
    weather_data = fetch_weather_data(city)
    
    if weather_data.get('cod') != 200:
        return jsonify({"error": weather_data.get('message')}), weather_data.get('cod', 400)

    data = {
        "city": city,
        "temperature": weather_data['main']['temp'],
        "humidity": weather_data['main']['humidity'],
        "wind_speed": weather_data['wind']['speed'] * 3.6
    }
    return jsonify(data)
# fonction pour avoir les informations sur la temperature 




# fonction pour avoir les informations sur l'UV
@app.route('/weather/UV', methods=['GET'])
@app.route('/weather/UV', methods=['GET'])
def get_UV():
    city = request.args.get('city')
    if not city:
        return jsonify({"error": "City parameter is required"}), 400

    weather_data = fetch_weather_data(city)

    if weather_data.get('cod') != 200:
        return jsonify({"error": weather_data.get('message')}), weather_data.get('cod', 400)

    coord = weather_data['coord']
    lat = coord['lat']
    lon = coord['lon']

    uv_data = fetch_uv_index(lat, lon)

    # Vérifiez si 'value' existe dans la réponse
    if 'value' not in uv_data:
        return jsonify({"error": "Failed to retrieve UV index"}), 400

    uv_index = uv_data['value']

    data = {
        "city": city,
        "uv_index": uv_index
    }
    return jsonify(data)

CITIES = ["Paris", "Douala", "Buea"]
def fetch_and_store_weather_data(city):
    conn = pymysql.connect(host='localhost', user='root', password='@bytes19*#', db='meteo')
    cursor = conn.cursor()
    
    base_url = 'http://api.openweathermap.org/data/2.5/weather'
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric'
    }
    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        temperature = data['main']['temp']
        now = datetime.now()
        timestamp = now.replace(second=0, microsecond=0)

        # Vérifier si les données existent déjà
        cursor.execute('''
            SELECT COUNT(*) FROM temperature_data WHERE city = %s AND timestamp = %s
        ''', (city, timestamp))
        exists = cursor.fetchone()[0] > 0
        
        if not exists:
            cursor.execute('''
                INSERT INTO temperature_data (city, timestamp, temperature)
                VALUES (%s, %s, %s)
            ''', (city, timestamp, temperature))
        else:
            print(f"Data for city '{city}' at timestamp '{timestamp}' already exists.")

    conn.commit()
    conn.close()


   
   


@app.route('/weather/temperature', methods=['GET'])
def get_temperature_data():
    city = request.args.get('city')
    if not city:
        return jsonify({"error": "City parameter is required"}), 400

    conn = pymysql.connect(host='localhost', user='root', password='@bytes19*#', db='meteo')
    cursor = conn.cursor()

    time_threshold = datetime.now() - timedelta(hours=24)
    cursor.execute('''
        SELECT timestamp, temperature FROM temperature_data
        WHERE city = %s AND timestamp >= %s
        ORDER BY timestamp ASC
    ''', (city, time_threshold))

    rows = cursor.fetchall()
    data = {
        "city": city,
        "temperature_data": [{"time": row[0], "temp": row[1]} for row in rows]
    }

    conn.close()
    return jsonify(data)





def cleanup_old_data():
    conn = pymysql.connect(host='localhost', user='root', password='@bytes19*#', db='meteo')
    cursor = conn.cursor()

    time_threshold = datetime.now() - timedelta(hours=24)
    cursor.execute('''
        DELETE FROM temperature_data WHERE timestamp < %s
    ''', (time_threshold,))

    conn.commit()
    conn.close()


    
scheduler = BackgroundScheduler()

def start_scheduler():
    # Ajouter un job pour chaque ville dans CITIES
    for city in CITIES:
        scheduler.add_job(fetch_and_store_weather_data, 'interval', hours=2, args=[city])
    
    # Ajouter un job pour le nettoyage des anciennes données
    scheduler.add_job(cleanup_old_data, 'interval', hours=15)
    
    scheduler.start()

if __name__ == '__main__':
    # Récupérer les données immédiatement pour toutes les villes
    for city in CITIES:
        fetch_and_store_weather_data(city)

    # Démarrer le scheduler
    start_scheduler()

    # Lancer l'application Flask
    app.run(debug=True)