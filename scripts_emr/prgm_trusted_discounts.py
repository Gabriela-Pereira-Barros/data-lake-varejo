from pyspark.sql import SparkSession
from datetime import datetime

# Iniciar sessão Spark
spark = SparkSession.builder.getOrCreate()

# Data e horário de processamento
dt_proc = datetime.now().strftime('%Y%m%d%H%M%S')

#--------------------------------- Início Processamento Camada Trusted ------------------------------------------#

# Lendo dados da camada raw
df_discounts = spark.read.parquet('s3://data-lake-varejo-905418122144/02_raw/tb_discounts')
df_discounts.createOrReplaceTempView('df_discounts')

# Retirando duplicatas (linhas inteiras iguais) dos dados
df_discounts = spark.sql("select distinct * from df_discounts")
df_discounts.createOrReplaceTempView('df_discounts')

# Armazenando quantidade de registros
qtd_registros = df_discounts.count()

# Formatando nomes das colunas e adicionando coluna de data de processamento e de referência
df_discounts_formated = spark.sql(f"""
  SELECT
    Start as start
    , End as end
    , substring(replace(Start,'-',''),1,6) as ref
    , Discont as discount
    , Description as description
    , Category as category
    , `Sub category` as subcategory
    , concat(`Description`,' - ',`Category`,' - ',`Sub category`) as discount_id
    , '{dt_proc}' as dt_proc
  FROM df_discounts
""")

df_discounts_formated.createOrReplaceTempView('df_discounts_formated')

# Salvando os dados na camada trusted
df_discounts_formated.write.mode('append').partitionBy('dt_proc').parquet('s3://data-lake-varejo-905418122144/03_trusted/tb_discounts/')

#---------------------------------- Início Carregamento Base Controle -------------------------------------------#

# Criando dados de controle
df = spark.sql(f"""
    select
        'tb_trusted_discounts' as tb_name
        , {dt_proc} as dt_proc
        , {qtd_registros} as qtd_registros
""")

# Salvando dados de controle na camada trusted em formato parquet
df.write.mode('append').parquet('s3://data-lake-varejo-905418122144/05_controle/trusted/')