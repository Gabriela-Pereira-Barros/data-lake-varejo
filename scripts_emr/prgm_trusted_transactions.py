from pyspark.sql import SparkSession
from datetime import datetime

# Iniciar sessão Spark
spark = SparkSession.builder.getOrCreate()

# Data e horário de processamento
dt_proc = datetime.now().strftime('%Y%m%d%H%M%S')

#--------------------------------- Início Processamento Camada Trusted ------------------------------------------#

# Lendo dados da camada raw
df_transactions = spark.read.parquet('s3://data-lake-varejo-905418122144/02_raw/tb_transactions')
df_transactions.createOrReplaceTempView('df_transactions')

# Retirando duplicatas (linhas inteiras iguais) dos dados
df_transactions = spark.sql("select distinct * from df_transactions")
df_transactions.createOrReplaceTempView('df_transactions')

# Armazenando quantidade de registros
qtd_registros = df_transactions.count()

# Formatando nomes das colunas e adicionando coluna de data de processamento e de referência
df_transactions_formated = spark.sql(f"""
  SELECT
    `Invoice ID` as invoice_id
    , Line as line
    , `Customer ID` as customer_id
    , `Product ID` as product_id
    , Size as size
    , Color as color
    , `Unit Price` as unit_price
    , Quantity as quantity
    , Date as date_hour
    , date(Date) as date
    , hour(Date) as hour
    , substring(replace(Date,'-',''),1,6) as ref
    , Discount as discount
    , `Line Total` as line_total
    , `Store ID` as store_id
    , `Employee ID` as employee_id
    , Currency as currency
    , `Currency Symbol` as currency_symbol
    , SKU as sku
    , `Transaction Type` as transaction_type
    , `Payment Method` as payment_method
    , `Invoice Total` as invoice_total
    , '{dt_proc}' as dt_proc
  FROM df_transactions
""")

df_transactions_formated.createOrReplaceTempView('df_transactions_formated')

# Salvando os dados na camada trusted
df_transactions_formated.write.mode('append').partitionBy('ref').parquet('s3://data-lake-varejo-905418122144/03_trusted/tb_transactions')

#---------------------------------- Início Carregamento Base Controle -------------------------------------------#

# Criando dados de controle
df = spark.sql(f"""
    select
        'tb_trusted_transactions' as tb_name
        , {dt_proc} as dt_proc
        , {qtd_registros} as qtd_registros
""")

# Salvando dados de controle na camada trusted em formato parquet
df.write.mode('append').parquet('s3://data-lake-varejo-905418122144/05_controle/trusted/')