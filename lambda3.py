import boto3
import json

# Inicializar cliente de Glue
glue_client = boto3.client('glue')

def lambda_run(event, context):
    """
    Lambda para ejecutar el Crawler de Glue.
    """
    print("Se ejecuto 0")
    try:
        # Definir el nombre del Crawler
        crawler_name = 'headlinesfinal'  # Cambia esto por el nombre de tu Crawler
        print("Se ejecuto 1")

        # Ejecutar el Crawler
        response = glue_client.start_crawler(Name=crawler_name)
        print("Se ejecuto 2")
        
        # Mostrar respuesta para depuración
        print(f"Respuesta del Crawler: {json.dumps(response, indent=2)}")

        return {
            'statusCode': 200,
            'body': json.dumps(f"Crawler '{crawler_name}' ejecutado con éxito.")
        }

    except Exception as e:
        print(f"Error al ejecutar el Crawler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error al ejecutar el Crawler: {str(e)}")
        }
