import os
from typing import List
import boto3
from dotenv import load_dotenv
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION: str = os.getenv("AWS_REGION")
AWS_BUCKET_NAME: str = os.getenv("AWS_BUCKET_NAME")

# Cria client S3
try:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    logger.info("Cliente S3 inicializado com sucesso.")
except Exception as e:
    logger.error("Erro ao inicializar cliente S3: %s", e)
    raise

# Lista arquivos na pasta
def listar_arquivos(pasta: str) -> List:
    arquivos: List = []  # Inicializa fora do bloco try
    try:
        for arquivo in os.listdir(pasta):
            caminho_completo = os.path.join(pasta, arquivo)
            if os.path.isfile(caminho_completo):
                arquivos.append(caminho_completo)
    except Exception as e:
        logger.error("Erro ao listar arquivos na pasta '%s': %s", pasta, e)
    print(arquivos)
    return arquivos  # Garante que arquivos será retornado, mesmo se ocorrer um erro

# Faz upload de arquivos para o S3
def upload_to_s3(arquivos: List):
    if not arquivos:  # Verifica se a lista está vazia
        logger.warning("Nenhum arquivo para carregar no S3.")
        return

    logger.info("Carregando %d arquivos no S3.", len(arquivos))

    for arquivo in arquivos:
        try:
            if not os.path.isfile(arquivo):  # Verifica se o arquivo realmente existe
                logger.warning("Arquivo '%s' não encontrado. Ignorando.", arquivo)
                continue

            nome_arquivo = os.path.basename(arquivo)
            print(nome_arquivo)
            s3_client.upload_file(arquivo, AWS_BUCKET_NAME, arquivo)
            logger.info("Arquivo '%s' carregado com sucesso no S3.", nome_arquivo)
        except Exception as e:
            logger.error("Erro ao carregar o arquivo '%s' para o S3: %s", arquivo, e)


# Deleta arquivos locais
def deleta_local(arquivos: List):
    for arquivo in arquivos:
        try:
            os.remove(arquivo)
            logger.info("Arquivo '%s' deletado com sucesso.", arquivo)
        except Exception as e:
            logger.error("Erro ao deletar o arquivo '%s': %s", arquivo, e)

# Pipeline principal
def pipeline(pasta: str):
    try:
        arquivos: List = listar_arquivos(pasta)
        if arquivos:
            try:
                upload_to_s3(arquivos)
            except Exception as e:
                logger.error("Erro ao carregar arquivos para o S3: %s", e)
            try:
                deleta_local(arquivos)
            except Exception as e:
                logger.error("Erro ao executar a pipeline: %s", e)
        else:
            logger.info("Nenhum arquivo encontrado na pasta '%s'.", pasta)
    except Exception as e:
        logger.error("Erro na execução da pipeline: %s", e)

if __name__ == '__main__':
    logger.info("Inicializando pipeline.")
    pasta = 'files'
    pipeline(pasta)
