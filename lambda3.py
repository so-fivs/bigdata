import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def start_crawler(crawler_name: str):
    """Función que inicia el Crawler de Glue."""
    glue_client = boto3.client('glue', region_name='us-east-1')
    
    try:
        # Iniciar el Crawler de Glue
        response = glue_client.start_crawler(Name=crawler_name)
        logger.info(f"Crawler {crawler_name} iniciado con éxito.")
    except Exception as e:
        logger.error(f"Error al iniciar el Crawler: {str(e)}")

def lambda_run(event, context):
    """Función principal que será ejecutada por Lambda."""
    # Inicializa el nombre del Crawler directamente en el código
    crawler_name = 'headlinesfinal'  # Nombre del Crawler de Glue

    if not crawler_name:
        logger.error("El nombre del Crawler no está configurado correctamente.")
        return

    logger.debug(f"Iniciando el Crawler {crawler_name}...")
    
    # Llamar a la función que inicia el Crawler
    start_crawler(crawler_name)
    
    return {
        "statusCode": 200,
        "body": f"Crawler {crawler_name} iniciado correctamente"
    }
