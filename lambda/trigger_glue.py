"""
===============================================================================
AWS LAMBDA: TRIGGER GLUE JOB
===============================================================================
Tech Challenge FIAP - Fase 2 - Machine Learning Engineering

Esta Lambda implementa os Requisitos 3 e 4 do Tech Challenge:
- Requisito 3: O bucket deve acionar uma Lambda que chama o job Glue
- Requisito 4: A Lambda apenas deve iniciar o job Glue (nao faz processamento)

Fluxo de Execucao:
1. Arquivo .parquet eh enviado para s3://bucket/raw/data=YYYY-MM-DD/
2. S3 Event Notification detecta o novo arquivo
3. S3 aciona esta Lambda automaticamente
4. Lambda extrai informacoes do evento (bucket, arquivo)
5. Lambda inicia o Glue Job com os parametros necessarios
6. Glue Job processa os dados (transformacoes)

Configuracao necessaria no AWS:
1. Criar esta Lambda com runtime Python 3.12
2. Usar a role LabRole (AWS Academy)
3. Configurar trigger S3:
   - Bucket: seu-bucket
   - Event type: All object create events
   - Prefix: raw/
   - Suffix: .parquet

Autor: Equipe Going Merry - FIAP PosTech
===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================

# json: Biblioteca para manipulacao de dados JSON
# Usada para formatar a resposta da Lambda
import json

# boto3: SDK da AWS para Python
# Permite interagir com servicos AWS como Glue, S3, etc.
import boto3


# =============================================================================
# FUNCAO HANDLER (PONTO DE ENTRADA DA LAMBDA)
# =============================================================================

def lambda_handler(event, context):
    """
    Handler principal da Lambda - ponto de entrada da funcao.

    Esta funcao eh chamada automaticamente pela AWS quando:
    - Um evento S3 aciona a Lambda (novo arquivo no bucket)
    - A Lambda eh invocada manualmente ou por outro servico

    Implementa o Requisito 4: A Lambda APENAS inicia o Glue Job.
    Nao faz nenhum processamento de dados - isso eh responsabilidade do Glue.

    Args:
        event (dict): Evento que acionou a Lambda.
            Para eventos S3, contem informacoes sobre o arquivo:
            {
                "Records": [{
                    "s3": {
                        "bucket": {"name": "nome-do-bucket"},
                        "object": {"key": "raw/data=2024-01-15/arquivo.parquet"}
                    }
                }]
            }

        context (LambdaContext): Objeto com informacoes sobre a execucao:
            - function_name: nome da funcao
            - memory_limit_in_mb: memoria alocada
            - aws_request_id: ID unico da execucao
            - log_group_name: grupo de logs no CloudWatch

    Returns:
        dict: Resposta HTTP-like com status da operacao
            - statusCode: 200 (sucesso) ou 500 (erro)
            - body: mensagem em formato JSON
    """

    # =========================================================================
    # CONFIGURACAO
    # =========================================================================

    # Nome do Glue Job que sera iniciado
    # Este nome deve corresponder ao job criado no AWS Glue
    # Conforme documentacao das aulas: 'etl-bovespa-job'
    glue_job_name = 'etl-bovespa-job'

    # =========================================================================
    # CRIAR CLIENTE GLUE
    # =========================================================================

    # Cria cliente boto3 para interagir com o servico AWS Glue
    # O cliente usa automaticamente as credenciais da role da Lambda (LabRole)
    glue_client = boto3.client('glue')

    # =========================================================================
    # PROCESSAR EVENTO
    # =========================================================================

    try:
        # ---------------------------------------------------------------------
        # EXTRAIR INFORMACOES DO EVENTO S3
        # ---------------------------------------------------------------------

        # O evento S3 vem em formato de lista de Records
        # Cada Record contem informacoes sobre um arquivo
        # Normalmente ha apenas 1 Record por evento

        # Extrai o nome do bucket do evento
        # event['Records'][0] = primeiro (e geralmente unico) registro
        # ['s3']['bucket']['name'] = navegacao no JSON ate o nome do bucket
        bucket = event['Records'][0]['s3']['bucket']['name']

        # Extrai a chave (path) do objeto no S3
        # Exemplo: "raw/data=2024-01-15/bovespa_2024-01-15.parquet"
        key = event['Records'][0]['s3']['object']['key']

        # Log para debug - aparece no CloudWatch Logs
        print(f"Arquivo detectado: s3://{bucket}/{key}")

        # ---------------------------------------------------------------------
        # INICIAR GLUE JOB
        # ---------------------------------------------------------------------

        # Inicia o Glue Job usando o metodo start_job_run
        # Este metodo retorna imediatamente com um JobRunId
        # O job roda de forma assincrona (em background)

        response = glue_client.start_job_run(
            # Nome do job a ser executado
            JobName=glue_job_name,

            # Argumentos passados para o job
            # Estes argumentos ficam disponiveis no script do Glue via getResolvedOptions
            # IMPORTANTE: Os nomes devem comecar com '--'
            Arguments={
                # Caminho de entrada: pasta raw do bucket
                # O Glue vai ler todos os arquivos .parquet desta pasta
                '--INPUT_PATH': f's3://{bucket}/raw/',

                # Caminho de saida: pasta refined do bucket
                # O Glue vai salvar os dados transformados aqui
                '--OUTPUT_PATH': f's3://{bucket}/refined/',

                # Nome do database no Glue Catalog
                # O Glue vai criar/atualizar a tabela neste database
                '--DATABASE_NAME': 'bovespa_db'
            }
        )

        # ---------------------------------------------------------------------
        # LOG DE SUCESSO
        # ---------------------------------------------------------------------

        # Extrai o ID da execucao do job
        # Este ID pode ser usado para monitorar o progresso do job
        job_run_id = response['JobRunId']

        # Log do ID da execucao
        print(f"Job iniciado: {job_run_id}")

        # ---------------------------------------------------------------------
        # RETORNAR RESPOSTA DE SUCESSO
        # ---------------------------------------------------------------------

        # Retorna resposta no formato API Gateway/HTTP
        # statusCode 200 indica sucesso
        return {
            'statusCode': 200,
            'body': json.dumps('Job Glue iniciado com sucesso!')
        }

    # =========================================================================
    # TRATAMENTO DE ERROS
    # =========================================================================

    except Exception as e:
        # Log do erro para debug no CloudWatch
        print(f"Erro: {str(e)}")

        # Retorna resposta de erro
        # statusCode 500 indica erro interno
        return {
            'statusCode': 500,
            'body': json.dumps(f'Erro: {str(e)}')
        }
