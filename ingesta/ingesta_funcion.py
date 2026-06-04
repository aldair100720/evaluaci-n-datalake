import base64
import json
import boto3
from datetime import datetime

s3_client = boto3.client('s3')
# CAMBIA ESTO por el nombre real de tu bucket
BUCKET_NAME = "evaluacion-datalake" 

def lambda_handler(event, context):
    records_to_write = []
    
    # 1. Leer los registros que vienen desde Kinesis
    for record in event['Records']:
        # Kinesis envía los datos en Base64, hay que decodificarlos
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        try:
            json_data = json.loads(payload)
            records_to_write.append(json_data)
        except Exception as e:
            print(f"Error decodificando registro malformado: {str(e)}")
            # Aquí podrías mandarlo a un SQS DLQ si lo deseas
    
    if not records_to_write:
        return {'statusCode': 200, 'body': 'No hay registros válidos para procesar'}
        
    # 2. Crear una ruta dinámica particionada por fecha (Año/Mes/Día/Hora)
    now = datetime.utcnow()
    s3_key = f"raw/gps/year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}/hour={now.strftime('%H')}/{context.aws_request_id}.json"
    
    # 3. Guardar el lote en S3 (Formato JSON Line, ideal para Big Data)
    json_lines = "\n".join([json.dumps(r) for r in records_to_write])
    
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json_lines
        )
        print(f"Exito: Se guardaron {len(records_to_write)} registros en {s3_key}")
    except Exception as e:
        print(f"Error al escribir en S3: {str(e)}")
        raise e
        
    return {'statusCode': 200, 'body': 'Procesado correctamente'}