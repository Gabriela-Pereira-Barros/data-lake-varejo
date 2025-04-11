from pyspark.sql import SparkSession
from datetime import datetime

# Iniciar sessão Spark
spark = SparkSession.builder.getOrCreate()

# Data e horário de processamento
dt_proc = datetime.now().strftime('%Y%m%d%H%M%S')

#--------------------------------- Início Processamento Camada Trusted ------------------------------------------#

# Lendo dados da camada raw
df_products = spark.read.parquet('s3://data-lake-varejo-905418122144/02_raw/tb_products')
df_products.createOrReplaceTempView('df_products')

# Retirando duplicatas (linhas inteiras iguais) dos dados
df_products = spark.sql("select distinct * from df_products")
df_products.createOrReplaceTempView('df_products')

# Armazenando quantidade de registros
qtd_registros = df_products.count()

# Formatando nomes das colunas e adicionando coluna de data de processamento
df_products_formated = spark.sql(f"""
  SELECT
    `Product ID` as product_id
    , `Category` as category
    , `Sub Category` as subcategory
    , `Description PT` as description_pt
    , `Description DE` as description_de
    , `Description FR` as description_fr
    , `Description ES` as description_es
    , `Description EN` as description_en
    , `Description ZH` as description_zh
    , `Color` as color
    , `Sizes` as sizes
    , `Production Cost` as production_cost
    , '{dt_proc}' as dt_proc
  FROM df_products
""")

df_products_formated.createOrReplaceTempView('df_products_formated')

# Salvando os dados na camada trusted
df_products_formated.write.mode('append').partitionBy('dt_proc').parquet('s3://data-lake-varejo-905418122144/03_trusted/tb_products/')


#---------------------------------- Início Carregamento Base Controle -------------------------------------------#

# Criando dados de controle
df = spark.sql(f"""
    select
        'tb_trusted_products' as tb_name
        , {dt_proc} as dt_proc
        , {qtd_registros} as qtd_registros
""")

# Salvando dados de controle na camada trusted em formato parquet
df.write.mode('append').parquet('s3://data-lake-varejo-905418122144/05_controle/trusted/')