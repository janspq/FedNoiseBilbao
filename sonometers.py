import requests
import csv
import sys

# This script downloads sonometer stations from the Bilbao City Council's Open Data portal.
# It retrieves data from a specified date range and saves it to a CSV file.
def obtener_datos_sonometros():
    # URL del servicio web
    url = "https://www.bilbao.eus/aytoonline/jsp/opendata/movilidad/od_sonometro_ubicacion.jsp?idioma=c&formato=geojson"
    
    try:
        # Realizar la solicitud HTTP
        print("Obteniendo datos de sonómetros desde la URL...")
        response = requests.get(url)
        
        # Verificar si la solicitud fue exitosa
        if response.status_code == 200:
            # Parsear el JSON
            data = response.json()
            return data
        else:
            print(f"Error al obtener datos. Código de estado: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error en la solicitud: {e}")
        return None

def guardar_en_csv(datos, nombre_archivo):
    # Verificar si hay datos
    if not datos or "features" not in datos:
        print("No hay datos para guardar en CSV.")
        return False
    
    try:
        with open(nombre_archivo, 'w', newline='', encoding='utf-8') as archivo_csv:
            # Determinar los campos a partir del primer elemento
            if datos["features"] and len(datos["features"]) > 0:
                # Obtener todas las propiedades del primer elemento
                propiedades = datos["features"][0]["properties"].keys()
                
                # Añadir coordenadas como campos separados
                campos = list(propiedades) + ["coord_x", "coord_y"]
                
                # Crear el escritor CSV
                writer = csv.DictWriter(archivo_csv, fieldnames=campos)
                
                # Escribir la cabecera
                writer.writeheader()
                
                # Escribir los datos de cada sonómetro
                for feature in datos["features"]:
                    # Copiar propiedades
                    row = feature["properties"].copy()
                    
                    # Añadir coordenadas
                    if "geometry" in feature and "coordinates" in feature["geometry"]:
                        coord = feature["geometry"]["coordinates"]
                        if len(coord) >= 2:
                            row["coord_x"] = coord[0]  # Longitud
                            row["coord_y"] = coord[1]  # Latitud
                    
                    writer.writerow(row)
                
                print(f"Datos guardados exitosamente en {nombre_archivo}")
                return True
            else:
                print("No hay elementos para guardar en el CSV.")
                return False
    except Exception as e:
        print(f"Error al guardar en CSV: {e}")
        return False

def main():
    # Obtener datos
    datos = obtener_datos_sonometros()
    
    if datos:
        print(f"Se han obtenido {len(datos.get('features', []))} sonómetros.")
        
        # Guardar en CSV
        nombre_archivo = "bilbao_sonometers.csv"
        if len(sys.argv) > 1:
            nombre_archivo = sys.argv[1]
        
        guardar_en_csv(datos, nombre_archivo)
    else:
        print("No se pudieron obtener datos de sonómetros.")

if __name__ == "__main__":
    main()