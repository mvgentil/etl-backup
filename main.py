import os
from typing import List
import boto3
from dotenv import load_dotenv

# carrega as variáveis de ambiente do arquivo .env
load_dotenv()

AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION: str = os.getenv("AWS_REGION")
AWS_BUCKET_NAME: str = os.getenv("AWS_BUCKET_NAME")


# cria client s3

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)


# ler o arquivos
def listar_arquivos(pasta: str) -> List[str]:
    arquivos: List[str] = []
    for arquivo in os.listdir(pasta):
        caminho_completo = os.path.join(pasta, arquivo)
        if os.path.isfile(caminho_completo):
            arquivos.append(caminho_completo)
    return arquivos

# joga os arquivos no s3
def upload_to_s3(arquivos: List):
    for arquivo in arquivos:
        nome_arquivo = os.path.basename(arquivo)
        s3_client.upload_file(arquivo, AWS_BUCKET_NAME, arquivo)
        print(f'{nome_arquivo} carregado com sucesso no s3')

# deleta os arquivos na pasta local
def deleta_local(arquivos: List):
    for arquivo in arquivos:
        os.remove(arquivo)
        print(f'{arquivo} deletado com sucesso')

# pipeline
def pipeline(pasta: str):
    arquivos: List = listar_arquivos(pasta)
    if arquivos:
        print(f'Carregando {len(arquivos)} arquivos no s3')
        upload_to_s3(arquivos)
        print('Arquivos carregados com sucesso')
        print('Deletando arquivos locais')
        deleta_local(arquivos)
        print('Arquivos deletados com sucesso')
    else:
        print('Nenhum arquivo encontrado na pasta')
    deleta_local(arquivos)


if __name__ == '__main__':
    print(AWS_ACCESS_KEY_ID)
    print(AWS_SECRET_ACCESS_KEY)
    print(AWS_REGION)
    print(AWS_BUCKET_NAME)
    pasta = 'files'
    pipeline(pasta)