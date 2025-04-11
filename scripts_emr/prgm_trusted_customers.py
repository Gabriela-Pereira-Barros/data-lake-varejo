from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col, current_date, floor, months_between

# Iniciar sessão Spark
spark = SparkSession.builder.getOrCreate()

# Data e horário de processamento
dt_proc = datetime.now().strftime('%Y%m%d%H%M%S')

# Data atual (para cálculo da idade)
data_atual = current_date()

#--------------------------------- Início Processamento Camada Trusted ------------------------------------------#

# Lendo dados da camada raw
df_customers = spark.read.parquet('s3://data-lake-varejo-905418122144/02_raw/tb_customers')
df_customers.createOrReplaceTempView('df_customers')

# Retirando duplicatas (linhas inteiras iguais) dos dados
df_customers = spark.sql("select distinct * from df_customers")
df_customers.createOrReplaceTempView('df_customers')

# Armazenando quantidade de registros
qtd_registros = df_customers.count()

# Calculando a idade
df_customers_age = df_customers.withColumn("age", floor(months_between(data_atual, col("Date Of Birth")) / 12))
df_customers_age.createOrReplaceTempView('df_customers_age')

# Formatando nome das colunas, adicionando coluna de data de processamento e transformando coluna age do tipo long para int
df_customers_formated = spark.sql(f"""
  SELECT
    `Customer ID` as customer_id
    , Name as name
    , Email as email
    , Telephone as telephone
    , City as city
    , Country as country
    , Gender as gender
    , `Job Title` as job_title
    , `Date Of Birth` as birthdate
    , cast(age as int) as age
    , '{dt_proc}' as dt_proc
  FROM df_customers_age
""")

df_customers_formated.createOrReplaceTempView('df_customers_formated')

# Salvando os dados na camada trusted
df_customers_formated.write.mode('append').partitionBy('dt_proc').parquet('s3://data-lake-varejo-905418122144/03_trusted/tb_customers/')

#---------------------------------- Início Carregamento Base Controle -------------------------------------------#

# Criando dados de controle
df = spark.sql(f"""
    select
        'tb_trusted_customers' as tb_name
        , {dt_proc} as dt_proc
        , {qtd_registros} as qtd_registros
""")

# Salvando dados de controle na camada trusted em formato parquet
df.write.mode('append').parquet('s3://data-lake-varejo-905418122144/05_controle/trusted/')