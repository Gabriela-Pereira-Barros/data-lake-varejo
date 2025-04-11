from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

# Iniciar sessão Spark
spark = SparkSession.builder.getOrCreate()

# Data e horário de processamento
dt_proc = datetime.now().strftime('%Y%m%d%H%M%S')

#--------------------------------- Início Processamento Camada Raw ------------------------------------------#

# Lendo tabelas da camada transient
assuntos = ['customers','discounts','employees','products','stores','transactions']

for assunto in assuntos:
    try:
        nome_tabela = f'tb_{assunto}'
        df = spark.read.csv(f's3://data-lake-varejo-905418122144/01_transient/{assunto}.csv', header=True, inferSchema=True)
        df.createOrReplaceTempView(nome_tabela)

        print(f"TempView '{nome_tabela}' criada com sucesso.")

    except Exception as e:
        print(f"Erro ao processar '{assunto}': {str(e)}")

# Salvar as tabelas na camada trusted, em formato parquet e com coluna de processamento
for assunto in assuntos:
    try:
        nome_tabela = f'tb_{assunto}'
        df_resultado = spark.table(f"tb_{assunto}")
        df_resultado_1 = df_resultado.withColumn('dt_proc', lit(dt_proc))
        df_resultado_1.createOrReplaceTempView(nome_tabela)
        df_resultado_1.write.mode('append').partitionBy('dt_proc').parquet(f's3://data-lake-varejo-905418122144/02_raw/tb_{assunto}') 

        print(f"Dados da tabela '{assunto}' salvos com sucesso.")

    except Exception as e:
        print(f"Erro ao processar '{assunto}': {str(e)}")

# Após o processamento, inicializar o cliente S3 com boto3
s3_client = boto3.client('s3')
print("Conexão realizada!")

# Extrair o nome do bucket e o prefixo (pasta) do caminho S3
bucket_name = "data-lake-varejo-905418122144"
prefix = "01_transient/"

try:
    # Listar todos os objetos na pasta do bucket S3
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Verificar se há objetos para deletar
    if 'Contents' in response:
        objects_to_delete = []
        for obj in response['Contents']:
            file_key = obj['Key']
            # Verifica se a chave não é apenas o prefixo da pasta (01_transient/)
            if file_key != prefix:
                print(f"Deletando {file_key} do bucket {bucket_name}")
                objects_to_delete.append({'Key': file_key})

        # Deletar os arquivos em lote (mais eficiente)
        if objects_to_delete:
            s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': objects_to_delete}
            )
            print(f"Deletados {len(objects_to_delete)} arquivos.")
        else:
            print("Nenhum arquivo encontrado para deletar (apenas o prefixo da pasta).")
    else:
        print("Nenhum arquivo encontrado na pasta especificada.")

except ClientError as e:
    print(f"Erro ao interagir com o S3: {e}")


#-------------------------------------- Início Carregamento Base Controle -----------------------------------------------#

# Criando dados de controle das tabelas e salvando
assuntos = ['customers','discounts','employees','products','stores','transactions']

for assunto in assuntos:
    try:
        df = spark.sql(f"""
              select
                'tb_raw_{assunto}' as tb_name
                , {dt_proc} as dt_proc
                , count(*) as qtd_registros
              from tb_{assunto}
        """)
              
        print(f"Dados da tabela '{assunto}' salvos com sucesso.")
        df.show(truncate=False)

    except Exception as e:
        print(f"Erro ao processar '{assunto}': {str(e)}")