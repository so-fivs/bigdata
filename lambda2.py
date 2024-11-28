import boto3
import csv
from bs4 import BeautifulSoup
from io import StringIO
from urllib.parse import unquote_plus

# Configuración del bucket de S3
S3_BUCKET = "parcialll"  # Cambia esto por tu bucket de S3
FINAL_PATH = "headlines/final/"  # Carpeta destino para los CSV procesados

# Cliente de S3
s3 = boto3.client('s3')


def process_file(bucket_name, key):
    """
    Procesa un archivo HTML descargado desde S3 y guarda un CSV en la estructura especificada.
    
    Args:
        bucket_name (str): Nombre del bucket S3.
        key (str): Clave del archivo HTML en S3.
    """
    print(f"Procesando archivo: {key}")
    try:
        # Descargar el archivo HTML desde S3
        response = s3.get_object(Bucket=bucket_name, Key=key)
        html_content = response['Body'].read().decode('utf-8')
    except Exception as e:
        print(f"Error al descargar o leer el archivo desde S3: {e}")
        return

    # Parsear el HTML
    soup = BeautifulSoup(html_content, 'html.parser')

    # Extraer información de la clave del archivo
    try:
        filename_parts = key.split('/')[-1].replace('.html', '').split('-')
        
        # Validar longitud mínima del nombre del archivo
        if len(filename_parts) < 4:
            print(f"Formato del nombre del archivo no válido: {key}")
            return

        # Obtener partes clave del nombre del archivo
        contenido = filename_parts[0]  # Ejemplo: "contenido"
        date = filename_parts[1] + '-' + filename_parts[2] + '-' + filename_parts[3]  # Ejemplo: "2024-11-27"
        periodico = filename_parts[4]  # Ejemplo: "el_tiempo"

        # Validar formato de la fecha
        try:
            year, month, day = date.split('-')
        except ValueError:
            print(f"Formato de fecha no válido en el archivo: {key}")
            return

    except Exception as e:
        print(f"Error al analizar el nombre del archivo {key}: {e}")
        return

    # Ajustar extracción según el periódico
    articles = []
    try:
        # Lógica para cada periódico
        # Aquí va la lógica específica de cada periódico (ya incluida antes)
        pass

    except Exception as e:
        print(f"Error al procesar el HTML para {periodico}: {e}")
        return

    if not articles:
        print(f"No se encontraron artículos en el archivo: {key}")
        return

    # Generar la clave del CSV en la estructura requerida
    csv_key = f"{FINAL_PATH}periodico={periodico}/year={year}/month={month}/day={day}/headlines.csv"

    # Crear CSV en memoria
    try:
        csv_buffer = StringIO()
        csv_writer = csv.DictWriter(csv_buffer, fieldnames=['category', 'title', 'link'])
        csv_writer.writeheader()
        csv_writer.writerows(articles)
    except Exception as e:
        print(f"Error al generar el CSV en memoria: {e}")
        return

    # Subir el CSV a S3
    try:
        s3.put_object(Bucket=bucket_name, Key=csv_key, Body=csv_buffer.getvalue())
        print(f"Archivo CSV generado y guardado en S3: {csv_key}")
    except Exception as e:
        print(f"Error al subir el CSV a S3: {e}")


def lambda_recive(event, context):
    """
    Handler principal que procesa los eventos de S3.
    
    Args:
        event (dict): Evento que incluye información de S3.
        context: Contexto de la ejecución Lambda.
    """
    print(f"Evento recibido: {event}")
    try:
        # Procesar cada registro del evento
        for record in event['Records']:
            bucket_name = record['s3']['bucket']['name']
            key = unquote_plus(record['s3']['object']['key'])  # Decodificar la clave del archivo
            
            print(f"Archivo encontrado: {key}")  # Registrar todos los archivos

            if key.startswith('headlines/raw/') and key.endswith('.html'):
                process_file(bucket_name, key)
            else:
                print(f"Archivo ignorado (claves no coinciden): {key}")  # Registrar archivos ignorados
    except Exception as e:
        print(f"Error al procesar el evento: {e}")

    return {"statusCode": 200, "body": "Archivos procesados correctamente"}
