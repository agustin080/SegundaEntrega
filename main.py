import pandas as pd
from functions import fetch_weather_data, process_weather_data, truncate_string
from dotenv import load_dotenv
import os
import psycopg2

# Archivo de salida
output_file = 'weather_data.csv'

dotenv_path = '.env'
load_dotenv(dotenv_path)

redshift_credentials = {
    'user': os.getenv('REDSHIFT_USER'),
    'host': os.getenv('REDSHIFT_HOST'),
    'pass': os.getenv('REDSHIFT_PASS'),
    'db': os.getenv('REDSHIFT_DB'),
    'port': os.getenv('REDSHIFT_PORT')
}

def load_data_to_redshift(df):
    """Carga o actualiza los datos del DataFrame en la base de datos Redshift."""
    try:
        conn = psycopg2.connect(
            dbname=redshift_credentials['db'],
            user=redshift_credentials['user'],
            password=redshift_credentials['pass'],
            host=redshift_credentials['host'],
            port=redshift_credentials['port']
        )
        cursor = conn.cursor()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather (
            city VARCHAR(255),
            temperature FLOAT,
            humidity INTEGER,
            weather_description VARCHAR(255),
            timestamp TIMESTAMP,
            PRIMARY KEY (city, timestamp)
        )
        ''')

        for index, row in df.iterrows():
            # Verificar valores nulos
            if row.isnull().any():
                print(f"Fila {index} omitida debido a valores nulos: {row.to_dict()}")
                continue

            # Truncar los valores de las columnas
            city = truncate_string(row['city'], 255)
            weather_description = truncate_string(row['weather_description'], 255)

            # Eliminar datos existentes con la misma clave primaria
            cursor.execute('''
            DELETE FROM weather WHERE city = %s AND timestamp = %s
            ''', (city, row['timestamp']))

            # Insertar los nuevos datos
            cursor.execute('''
            INSERT INTO weather (city, temperature, humidity, weather_description, timestamp)
            VALUES (%s, %s, %s, %s, %s)
            ''', (city, row['temperature'], row['humidity'], weather_description, row['timestamp']))

        conn.commit()

    except Exception as e:
        print(f"Error al cargar los datos en Redshift: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def main():
    """Función principal para ejecutar el proceso ETL."""
    # Obtener datos del clima
    weather_data_list = fetch_weather_data()

    # Procesar los datos del clima
    data = process_weather_data(weather_data_list)

    # Convertir a DataFrame y guardar en CSV
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)

    print(f"Datos del clima guardados en {output_file} y cargados en REDSHIFT")

    load_data_to_redshift(df)

if __name__ == "__main__":
    main()