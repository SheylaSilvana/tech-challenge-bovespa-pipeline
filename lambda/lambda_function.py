import json
import boto3

def lambda_handler(event, context):
    glue_client = boto3.client('glue')
    glue_job_name = 'etl-bovespa-job'

    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        print(f"Arquivo detectado: s3://{bucket}/{key}")

        response = glue_client.start_job_run(
            JobName=glue_job_name,
            Arguments={
                '--INPUT_PATH': f's3://{bucket}/raw/',
                '--OUTPUT_PATH': f's3://{bucket}/refined/',
                '--DATABASE_NAME': 'bovespa_db'
            }
        )

        print(f"Job iniciado: {response['JobRunId']}")

        return {
            'statusCode': 200,
            'body': json.dumps('Job Glue iniciado com sucesso!')
        }

    except Exception as e:
        print(f"Erro: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Erro: {str(e)}')
        }
