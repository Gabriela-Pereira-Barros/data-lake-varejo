from pyspark.sql import SparkSession
from datetime import datetime

# Iniciar sessão Spark
spark = SparkSession.builder.getOrCreate()

# Data e horário de processamento
dt_proc = datetime.now().strftime('%Y%m%d%H%M%S')

#--------------------------------- Início Processamento Camada Trusted ------------------------------------------#

# Lendo dados da camada raw
df_stores = spark.read.parquet('s3://data-lake-varejo-905418122144/02_raw/tb_stores')
df_stores.createOrReplaceTempView('df_stores')

# Retirando duplicatas (linhas inteiras iguais) dos dados
df_stores = spark.sql("select distinct * from df_stores")
df_stores.createOrReplaceTempView('df_stores')

# Armazenando quantidade de registros
qtd_registros = df_stores.count()

# Formatando nomes das colunas e adicionando coluna de data de processamento
df_stores_formated = spark.sql(f"""
  SELECT
    `Store ID` as store_id
    , Country as country
    , City as city
    , `Store Name` as store_name
    , `Number of Employees` as number_of_employees
    , `ZIP Code` as zip_code
    , Latitude as latitude
    , Longitude as longitude
    , '{dt_proc}' as dt_proc
  FROM df_stores
""")

df_stores_formated.createOrReplaceTempView('df_stores_formated')

# Salvando os dados na camada trusted
df_stores_formated.write.mode('append').partitionBy('dt_proc').parquet('s3://data-lake-varejo-905418122144/03_trusted/tb_stores/')

#---------------------------------- Início Carregamento Base Controle -------------------------------------------#

# Criando dados de controle
df = spark.sql(f"""
    select
        'tb_trusted_stores' as tb_name
        , {dt_proc} as dt_proc
        , {qtd_registros} as qtd_registros
""")

# Salvando dados de controle na camada trusted em formato parquet
df.write.mode('append').parquet('s3://data-lake-varejo-905418122144/05_controle/trusted/')