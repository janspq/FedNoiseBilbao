import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import pytz
from io import StringIO

# This script downloads sonometer data from the Bilbao City Council's Open Data portal.
# It retrieves data from a specified date range and saves it to a CSV file.
def obtener_datos_sonometros_bilbao(start_date, end_date, output_file="bilbao_sonometers.csv"):
    """
    Downloads the Bilbao sonometer data from 'start_date' to 'end_date'
    and saves it to a CSV file.
    
    Args:
        start_date (str): Start date in YYYYMMDD format.
        end_date (str): End date in YYYYMMDD format.
        output_file (str): Output CSV file name.
    
    Returns:
        pandas.DataFrame: DataFrame with the obtained data.
    """
    print(f"Downloading data from {start_date} to {end_date}...")
    url = "https://www.bilbao.eus/opendata/datos/sonometros-mediciones"
    
    page = 1
    items_per_page = 1000  # API limit for records per page
    complete_data = pd.DataFrame()
    
    while True:
        params = {
            "desde": start_date,
            "hasta": end_date,
            "pagina": page,
            "items": items_per_page
        }
        
        headers = {
            "accept": "text/csv",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        max_retries = 5
        retry_count = 0
        success = False
        
        while retry_count < max_retries and not success:
            try:
                print(f"Requesting page {page} (attempt {retry_count + 1}/{max_retries})...")
                response = requests.get(url, params=params, headers=headers, timeout=30)
                success = True
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 5 * retry_count
                    print(f"Connection error: {e}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"Failed after {max_retries} attempts. Error: {e}")
                    return complete_data
        
        if not success:
            break
            
        if response.status_code == 200:
            content = response.content.decode('utf-8')
            # If the content is empty or only has header, stop
            if len(content.strip().split('\n')) <= 1:
                print("No more data available.")
                break
                
            # Important: use the correct separator, in this case ';'
            df_page = pd.read_csv(StringIO(content), sep=';')
            
            if df_page.empty:
                print("No more data available.")
                break
                
            print(f"{len(df_page)} records obtained on page {page}.")
            complete_data = pd.concat([complete_data, df_page], ignore_index=True)
            
            if len(df_page) < items_per_page:
                print("All available data has been obtained.")
                break
                
            page += 1
            time.sleep(2)
        else:
            print(f"Error retrieving data: {response.status_code} - {response.text}")
            break
    
    if not complete_data.empty:
        complete_data.to_csv(output_file, index=False)
        print(f"{len(complete_data)} records have been saved in '{output_file}'")
    else:
        print("No data was retrieved.")
    
    return complete_data

def agregar_dia_semana_numerico(df):
    """
    Adds two columns:
      - 'week_day_number': number (0 = Monday, ..., 6 = Sunday)
      - 'day_name': day name in English (Monday, Tuesday, ...)
    The function searches for the date column, which can be either 
    'Fecha/Hora medicion' or 'fecha_medicion'.
    
    Args:
        df (pandas.DataFrame): DataFrame containing the data.
    
    Returns:
        pandas.DataFrame: DataFrame with the added columns.
    """
    # Detect the date column based on expected names
    if 'Fecha/Hora medicion' in df.columns:
        date_column = 'Fecha/Hora medicion'
    elif 'fecha_medicion' in df.columns:
        date_column = 'fecha_medicion'
    else:
        print("No date column found ('Fecha/Hora medicion' or 'fecha_medicion').")
        return df
    # Convertir la columna a datetime
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    
    bilbao_tz = pytz.timezone('Europe/Madrid')
    
    # Asignar la zona horaria (manejando ambigüedades con 'NaT' si fuera necesario)
    df[date_column] = df[date_column].dt.tz_localize(bilbao_tz, ambiguous='NaT', nonexistent='NaT')

    # Asignar el día de la semana como un número (0=Monday, 6=Sunday)
    df['week_day'] = df[date_column].dt.weekday


    # Quitar la zona horaria antes de continuar
    df[date_column] = df[date_column].dt.tz_localize(None)
    
    return df


def main():
    # Manually defined dates
    start_date = "20250401"  # Fecha de inicio en formato YYYYMMDD
    end_date = "20250405"    # Fecha final en formato YYYYMMDD

    # Note: Verify that the dates correspond to the desired range;
    # in this example, start_date is later than end_date, which may affect results.
    # Ensure start_date is less than or equal to end_date if required by the API.
    
    # Define output file name for the download
    output_file = f"bilbao_sonometers_data15.csv"
    
    print(f"Downloading Bilbao sonometer data from {start_date} to {end_date}...")
    df = obtener_datos_sonometros_bilbao(start_date, end_date, output_file)
    
    if not df.empty:
        print("\nData information:")
        print(f"Total records: {len(df)}")
        print(f"Available columns: {', '.join(df.columns)}")
        
        # Add the 'week_day_number' and 'day_name' columns
        df = agregar_dia_semana_numerico(df)
        print("\nExample of processed data (with 'day_name'):")
        print(df.head())

        # Rename columns to English for consistency
        df.rename(columns={
            'nombre_dispositivo': 'name',
            'decibelios': 'decibels',
            'fecha_medicion': 'timestamp'
        }, inplace=True)

        # Save the final DataFrame to a new CSV file (optional)
        df.to_csv(output_file, index=False)
        print(f"\nFinal data saved in: {output_file}")
    else:
        print("No data was retrieved for processing.")

if __name__ == "__main__":
    main()
