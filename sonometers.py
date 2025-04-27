import requests
import csv
import sys

# This script downloads sonometer stations from the Bilbao City Council's Open Data portal.
# It retrieves data from a specified date range and saves it to a CSV file.
def obtener_datos_sonometros():
    # Web service URL
    url = "https://www.bilbao.eus/aytoonline/jsp/opendata/movilidad/od_sonometro_ubicacion.jsp?idioma=c&formato=geojson"
    
    try:
        # Make HTTP request
        print("Getting sonometer data from URL...")
        response = requests.get(url)
        
        # Verify if request was successful
        if response.status_code == 200:
            # Parse JSON
            data = response.json()
            return data
        else:
            print(f"Error getting data. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Request error: {e}")
        return None

def guardar_en_csv(datos, nombre_archivo):
    # Check if there's data
    if not datos or "features" not in datos:
        print("No data to save to CSV.")
        return False
    
    try:
        with open(nombre_archivo, 'w', newline='', encoding='utf-8') as archivo_csv:
            # Determine fields from first element
            if datos["features"] and len(datos["features"]) > 0:
                # Get all properties from first element
                propiedades = datos["features"][0]["properties"].keys()
                
                # Add coordinates as separate fields
                campos = list(propiedades) + ["coord_x", "coord_y"]
                
                # Create CSV writer
                writer = csv.DictWriter(archivo_csv, fieldnames=campos)
                
                # Write header
                writer.writeheader()
                
                # Write data for each sonometer
                for feature in datos["features"]:
                    # Copy properties
                    row = feature["properties"].copy()
                    
                    # Add coordinates
                    if "geometry" in feature and "coordinates" in feature["geometry"]:
                        coord = feature["geometry"]["coordinates"]
                        if len(coord) >= 2:
                            row["coord_x"] = coord[0]  # Longitude
                            row["coord_y"] = coord[1]  # Latitude
                    
                    writer.writerow(row)
                
                print(f"Data successfully saved to {nombre_archivo}")
                return True
            else:
                print("No elements to save to CSV.")
                return False
    except Exception as e:
        print(f"Error saving to CSV: {e}")
        return False

def main():
    # Get data
    datos = obtener_datos_sonometros()
    
    if datos:
        print(f"Retrieved {len(datos.get('features', []))} sonometers.")
        
        # Save to CSV
        nombre_archivo = "bilbao_sonometers_new.csv"
        if len(sys.argv) > 1:
            nombre_archivo = sys.argv[1]
        
        guardar_en_csv(datos, nombre_archivo)
    else:
        print("Could not get sonometer data.")

if __name__ == "__main__":
    main()