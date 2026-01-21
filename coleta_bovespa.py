"""
===============================================================================
SCRIPT DE COLETA DE DADOS DA BOVESPA
===============================================================================
Tech Challenge FIAP - Fase 2 - Machine Learning Engineering

Este script implementa o Requisito 1 do Tech Challenge:
- Realizar web scraping/coleta de dados de acoes da B3 (Bovespa)
- Granularidade diaria dos dados

Tambem implementa os Requisitos 2 e 3:
- Salvar dados em formato Parquet (Requisito 2)
- Particionar por data de processamento: raw/data=YYYY-MM-DD/ (Requisito 3)

Fluxo do Script:
1. Conecta na API do Yahoo Finance (yfinance)
2. Baixa dados historicos de 10 acoes da B3
3. Processa e consolida os dados em um DataFrame
4. Salva localmente em formato Parquet
5. Envia para o bucket S3 na pasta raw/

Apos o upload para S3, o pipeline automatico eh acionado:
S3 -> Lambda -> Glue Job -> S3 (refined) -> Athena

Dependencias:
    pip install yfinance pandas pyarrow boto3

Uso:
    python coleta_bovespa.py

Autor: Equipe Going Merry - FIAP PosTech
===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================

# yfinance: Biblioteca para acessar dados financeiros do Yahoo Finance
# Permite baixar dados historicos de acoes de forma gratuita
import yfinance as yf

# pandas: Biblioteca para manipulacao e analise de dados
# Usamos para criar e manipular DataFrames (tabelas de dados)
import pandas as pd

# boto3: SDK da AWS para Python
# Permite interagir com servicos AWS como S3, Lambda, Glue, etc.
import boto3

# datetime e timedelta: Bibliotecas para manipulacao de datas
# datetime: trabalha com datas e horas
# timedelta: representa uma duracao (ex: 30 dias)
from datetime import datetime, timedelta

# os: Biblioteca para interacao com o sistema operacional
# Usamos para criar pastas e manipular caminhos de arquivos
import os


# =============================================================================
# CONFIGURACOES
# =============================================================================

# Nome do bucket S3 onde os dados serao armazenados
# IMPORTANTE: Altere para o nome do seu bucket no AWS Academy
BUCKET_NAME = "bovespa-pipeline-fiap-sheyla"

# Lista de acoes da B3 (Bovespa) que serao coletadas
# O sufixo .SA indica que sao acoes da bolsa brasileira (Sao Paulo)
# Estas sao 10 das acoes mais negociadas da B3
ACOES = [
    "PETR4.SA",  # Petrobras - Petroleo e Gas
    "VALE3.SA",  # Vale - Mineracao
    "ITUB4.SA",  # Itau Unibanco - Banco
    "BBDC4.SA",  # Bradesco - Banco
    "ABEV3.SA",  # Ambev - Bebidas
    "B3SA3.SA",  # B3 - Bolsa de Valores
    "WEGE3.SA",  # Weg - Motores Eletricos
    "RENT3.SA",  # Localiza - Aluguel de Veiculos
    "BBAS3.SA",  # Banco do Brasil - Banco
    "MGLU3.SA",  # Magazine Luiza - Varejo
]


# =============================================================================
# FUNCAO: COLETAR DADOS
# =============================================================================

def coletar_dados():
    """
    Coleta dados historicos das acoes usando a API do Yahoo Finance.

    Esta funcao implementa o Requisito 1 do Tech Challenge:
    - Web scraping de dados de acoes da B3
    - Granularidade diaria

    Processo:
    1. Define o periodo de coleta (ultimos 30 dias)
    2. Para cada acao na lista ACOES:
       - Conecta no Yahoo Finance
       - Baixa dados historicos (Open, High, Low, Close, Volume)
       - Adiciona metadados (ticker, nome_acao, data_processamento)
       - Normaliza nomes das colunas para snake_case
    3. Consolida todos os dados em um unico DataFrame

    Returns:
        pd.DataFrame: DataFrame com dados de todas as acoes, ou None se falhar

    Colunas do DataFrame retornado:
        - date: Data do pregao (YYYY-MM-DD)
        - open: Preco de abertura
        - high: Preco maximo do dia
        - low: Preco minimo do dia
        - close: Preco de fechamento
        - volume: Volume negociado
        - dividends: Dividendos pagos
        - stock_splits: Desdobramentos
        - ticker: Codigo da acao (ex: PETR4.SA)
        - nome_acao: Codigo sem sufixo (ex: PETR4)
        - data_processamento: Data em que os dados foram coletados
    """

    # Exibe cabecalho informativo no console
    print("=" * 50)
    print("COLETA DE DADOS DA BOVESPA")
    print("=" * 50)

    # Define o periodo de coleta
    # data_fim: data atual (hoje)
    # data_inicio: 30 dias atras
    data_fim = datetime.now()
    data_inicio = data_fim - timedelta(days=30)

    # Exibe informacoes sobre a coleta
    print(f"\nPeríodo: {data_inicio.strftime('%Y-%m-%d')} até {data_fim.strftime('%Y-%m-%d')}")
    print(f"Ações: {', '.join(ACOES)}")

    # Lista para armazenar os DataFrames de cada acao
    todos_dados = []

    # Loop para coletar dados de cada acao
    for acao in ACOES:
        print(f"\nBaixando {acao}...", end=" ")

        try:
            # Cria objeto Ticker do yfinance para a acao
            # Este objeto permite acessar dados historicos e informacoes da acao
            ticker = yf.Ticker(acao)

            # Baixa dados historicos do periodo especificado
            # Retorna um DataFrame com colunas: Open, High, Low, Close, Volume, Dividends, Stock Splits
            # O indice do DataFrame eh a data (DatetimeIndex)
            df = ticker.history(start=data_inicio, end=data_fim)

            # Verifica se retornou dados
            # Pode estar vazio se a acao nao existir ou nao tiver dados no periodo
            if df.empty:
                print("Sem dados!")
                continue

            # Adiciona coluna com o codigo completo da acao (ex: PETR4.SA)
            df["ticker"] = acao

            # Adiciona coluna com o nome da acao sem o sufixo .SA (ex: PETR4)
            # Isso facilita a leitura e sera usado como particao no S3
            df["nome_acao"] = acao.replace(".SA", "")

            # Adiciona coluna com a data de processamento (quando os dados foram coletados)
            # Formato: YYYY-MM-DD (ex: 2024-01-15)
            df["data_processamento"] = datetime.now().strftime("%Y-%m-%d")

            # Reseta o indice para transformar a data em coluna
            # Antes: indice = Date, colunas = Open, High, Low, Close, etc.
            # Depois: colunas = Date, Open, High, Low, Close, etc.
            df = df.reset_index()

            # Renomeia coluna Date para date (minusculo, padrao snake_case)
            df = df.rename(columns={"Date": "date"})

            # Converte todos os nomes de colunas para snake_case (minusculo, com underline)
            # Ex: "Stock Splits" -> "stock_splits"
            df.columns = [col.lower().replace(" ", "_") for col in df.columns]

            # Adiciona o DataFrame desta acao na lista
            todos_dados.append(df)

            # Exibe quantidade de registros coletados
            print(f"OK! {len(df)} registros")

        except Exception as e:
            # Em caso de erro (conexao, acao invalida, etc.), exibe mensagem e continua
            print(f"Erro: {e}")

    # Verifica se coletou dados de pelo menos uma acao
    if not todos_dados:
        print("\nNenhum dado coletado!")
        return None

    # Concatena todos os DataFrames em um unico DataFrame
    # ignore_index=True: cria um novo indice sequencial (0, 1, 2, ...)
    df_final = pd.concat(todos_dados, ignore_index=True)

    # Exibe total de registros coletados
    print(f"\nTotal de registros: {len(df_final)}")

    return df_final


# =============================================================================
# FUNCAO: SALVAR PARQUET LOCAL
# =============================================================================

def salvar_parquet_local(df):
    """
    Salva o DataFrame em formato Parquet localmente.

    Esta funcao implementa parte do Requisito 2:
    - Salvar dados em formato Parquet

    Parquet eh um formato de arquivo colunar otimizado para Big Data:
    - Compressao eficiente (arquivos menores)
    - Leitura rapida (especialmente para queries analiticas)
    - Suporte a tipos de dados complexos
    - Compativel com Spark, Athena, Glue, etc.

    Args:
        df (pd.DataFrame): DataFrame com os dados coletados

    Returns:
        tuple: (caminho_local, nome_arquivo)
            - caminho_local: caminho completo do arquivo salvo (ex: dados/bovespa_2024-01-15.parquet)
            - nome_arquivo: apenas o nome do arquivo (ex: bovespa_2024-01-15.parquet)
    """

    # Gera nome do arquivo com a data atual
    # Formato: bovespa_YYYY-MM-DD.parquet
    data_atual = datetime.now().strftime("%Y-%m-%d")
    nome_arquivo = f"bovespa_{data_atual}.parquet"

    # Cria a pasta 'dados' se nao existir
    # exist_ok=True: nao gera erro se a pasta ja existir
    os.makedirs("dados", exist_ok=True)

    # Define o caminho completo do arquivo
    caminho_local = f"dados/{nome_arquivo}"

    # Converte coluna de data para string
    # Isso evita erros de timezone ao salvar em Parquet
    # O Parquet tem problemas com timestamps que tem timezone
    if "date" in df.columns:
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    # Salva o DataFrame em formato Parquet
    # index=False: nao salva o indice do DataFrame como coluna
    df.to_parquet(caminho_local, index=False)

    print(f"\nArquivo salvo localmente: {caminho_local}")

    return caminho_local, nome_arquivo


# =============================================================================
# FUNCAO: ENVIAR PARA S3
# =============================================================================

def enviar_para_s3(caminho_local, nome_arquivo):
    """
    Envia o arquivo Parquet para o bucket S3.

    Esta funcao implementa os Requisitos 2 e 3:
    - Requisito 2: Dados ingeridos no S3 em formato Parquet
    - Requisito 3: Particao por data de processamento (raw/data=YYYY-MM-DD/)

    O padrao de particao Hive (data=YYYY-MM-DD) eh usado porque:
    - Athena e Glue reconhecem automaticamente como particao
    - Permite queries eficientes filtrando por data
    - Facilita organizacao e gerenciamento dos dados

    Apos o upload, o S3 Event Notification aciona a Lambda automaticamente,
    que por sua vez inicia o Glue Job (Requisitos 3 e 4).

    Args:
        caminho_local (str): Caminho do arquivo local (ex: dados/bovespa_2024-01-15.parquet)
        nome_arquivo (str): Nome do arquivo (ex: bovespa_2024-01-15.parquet)

    Returns:
        bool: True se upload foi bem sucedido, False caso contrario
    """

    print(f"\nEnviando para S3...")

    try:
        # Cria cliente S3 usando boto3
        # O boto3 usa automaticamente as credenciais configuradas em:
        # - Variaveis de ambiente (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        # - Arquivo ~/.aws/credentials
        # - IAM Role (se rodando em EC2, Lambda, etc.)
        s3_client = boto3.client("s3")

        # Define a data atual para a particao
        data_atual = datetime.now().strftime("%Y-%m-%d")

        # Define a chave (path) do objeto no S3
        # Formato: raw/data=YYYY-MM-DD/nome_arquivo.parquet
        # Exemplo: raw/data=2024-01-15/bovespa_2024-01-15.parquet
        #
        # Estrutura:
        # - raw/: pasta para dados brutos (camada raw do data lake)
        # - data=YYYY-MM-DD/: particao no formato Hive
        # - nome_arquivo.parquet: arquivo de dados
        s3_key = f"raw/data={data_atual}/{nome_arquivo}"

        # Faz upload do arquivo para o S3
        # Parametros:
        # - caminho_local: arquivo de origem
        # - BUCKET_NAME: bucket de destino
        # - s3_key: caminho/chave do objeto no bucket
        s3_client.upload_file(caminho_local, BUCKET_NAME, s3_key)

        # Exibe mensagem de sucesso com a URI do S3
        print(f"Sucesso! Arquivo enviado para:")
        print(f"  s3://{BUCKET_NAME}/{s3_key}")

        return True

    except Exception as e:
        # Em caso de erro (credenciais, permissoes, bucket inexistente, etc.)
        print(f"Erro ao enviar para S3: {e}")
        return False


# =============================================================================
# FUNCAO PRINCIPAL
# =============================================================================

def main():
    """
    Funcao principal que orquestra todo o processo de coleta.

    Fluxo de execucao:
    1. Coleta dados das acoes usando yfinance
    2. Salva os dados localmente em formato Parquet
    3. Envia os dados para o S3
    4. Exibe mensagem de conclusao

    Apos a execucao bem sucedida, o pipeline automatico eh acionado:
    - S3 Event detecta novo arquivo na pasta raw/
    - Lambda eh acionada automaticamente
    - Lambda inicia o Glue Job
    - Glue processa os dados e salva na pasta refined/
    - Dados ficam disponiveis para consulta no Athena
    """

    # Etapa 1: Coletar dados
    df = coletar_dados()

    # Verifica se a coleta foi bem sucedida
    if df is None:
        return

    # Etapa 2: Salvar localmente
    caminho_local, nome_arquivo = salvar_parquet_local(df)

    # Etapa 3: Enviar para S3
    sucesso = enviar_para_s3(caminho_local, nome_arquivo)

    # Etapa 4: Exibir mensagem de conclusao
    if sucesso:
        print("\n" + "=" * 50)
        print("COLETA FINALIZADA COM SUCESSO!")
        print("=" * 50)
        print("\nProximos passos automaticos:")
        print("1. S3 vai acionar a Lambda")
        print("2. Lambda vai iniciar o Job Glue")
        print("3. Glue vai processar e salvar em /refined")
        print("4. Dados disponiveis no Athena!")


# =============================================================================
# PONTO DE ENTRADA
# =============================================================================

# Verifica se o script esta sendo executado diretamente (nao importado)
# Se for executado com: python coleta_bovespa.py
# Entao __name__ == "__main__" e a funcao main() eh chamada
if __name__ == "__main__":
    main()
