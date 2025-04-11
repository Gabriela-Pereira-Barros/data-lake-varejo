from pyspark.sql import SparkSession
from datetime import datetime

# Iniciar sessão Spark
spark = SparkSession.builder.getOrCreate()

# Data e horário de processamento
dt_proc = datetime.now().strftime('%Y%m%d%H%M%S')

#--------------------------------- Início Processamento Camada Trusted ------------------------------------------#

# Lendo dados da camada raw
df_employees = spark.read.parquet('s3://data-lake-varejo-905418122144/02_raw/tb_employees')
df_employees.createOrReplaceTempView('df_employees')

# Retirando duplicatas (linhas inteiras iguais) dos dados
df_employees = spark.sql("select distinct * from df_employees")
df_employees.createOrReplaceTempView('df_employees')

# Armazenando quantidade de registros
qtd_registros = df_employees.count()

# Formatando nomes das colunas e adicionando coluna de data de processamento
df_employees_formated = spark.sql(f"""
  SELECT
    `Employee ID` as employee_id
    , `Store ID` as store_id
    , Name as name
    , Position as position
    , '{dt_proc}' as dt_proc
  FROM df_employees
""")

df_employees_formated.createOrReplaceTempView('df_employees_formated')

# Salvando os dados na camada trusted
df_employees_formated.write.mode('append').partitionBy('dt_proc').parquet('s3://data-lake-varejo-905418122144/03_trusted/tb_employees/')

#---------------------------------- Início Carregamento Base Controle -------------------------------------------#

# Criando dados de controle
df = spark.sql(f"""
    select
        'tb_trusted_employees' as tb_name
        , {dt_proc} as dt_proc
        , {qtd_registros} as qtd_registros
""")

# Salvando dados de controle na camada trusted em formato parquet
df.write.mode('append').parquet('s3://data-lake-varejo-905418122144/05_controle/trusted/')