from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime

# Iniciar sessão Spark
spark = SparkSession.builder.getOrCreate()

# Data e horário de processamento
dt_proc = datetime.now().strftime('%Y%m%d%H%M%S')

#--------------------------------- Início Processamento Camada Refined (Data Viz) ------------------------------------------#

# Lendo tabelas da camada trusted
assuntos = ['customers','discounts','employees','products','stores','transactions']

for assunto in assuntos:
  try:
    nome_tabela = f'tb_{assunto}'
    df = spark.read.parquet(f's3://data-lake-varejo-905418122144/03_trusted/tb_{assunto}')
    df.createOrReplaceTempView(nome_tabela)

    print(f"TempView '{nome_tabela}' criada com sucesso.")

  except Exception as e:

    print(f"Erro ao processar '{assunto}': {str(e)}")

#-------------------------------------- Deduplicação das tabelas -----------------------------------------------#

# Pegando linhas com data de processamento mais recente
assuntos = ['customers','discounts','employees','products','stores','transactions']
ids = ['customer_id','discount_id','employee_id','product_id','store_id','invoice_id ']

for assunto, id in zip(assuntos, ids):
    try:
        nome_tabela = f'tb_{assunto}_dedup_01'
        df_resultado = spark.sql(f"""
            select
                {id}
                , max(dt_proc) as max_dedup_key
            from tb_{assunto}
            group by 1
            order by 2 desc
        """)
        df_resultado.createOrReplaceTempView(nome_tabela)

        print(f"TempView '{nome_tabela}' criada com sucesso.")

    except Exception as e:
        print(f"Erro ao processar '{assunto}': {str(e)}")

# Filtrando tabela apenas pelas linhas com processamento mais recente
assuntos = ['customers','discounts','employees','products','stores','transactions']
ids = ['customer_id','discount_id','employee_id','product_id','store_id','invoice_id ']

for assunto, id in zip(assuntos, ids):
    try:
        nome_tabela = f'tb_{assunto}_dedup_02'
        df_resultado = spark.sql(f"""
            select
                a.*
            from tb_{assunto} a
            join tb_{assunto}_dedup_01 b
            on a.{id} = b.{id}
            and a.dt_proc = b.max_dedup_key
        """)
        df_resultado.createOrReplaceTempView(nome_tabela)

        print(f"TempView '{nome_tabela}' criada com sucesso.")

    except Exception as e:
        print(f"Erro ao processar '{assunto}': {str(e)}")

# Retirando coluna de processamento antiga e adicionando atual, e salvando dados na camada refined (Data Viz)
assuntos = ['customers','discounts','employees','products','stores','transactions']

for assunto in assuntos:
    try:
        df_resultado = spark.table(f"tb_{assunto}_dedup_02").drop("dt_proc")
        df_resultado_1 = df_resultado.withColumn('dt_proc', lit(dt_proc))
        df_resultado_1.write.mode('append').partitionBy('dt_proc').parquet(f's3://data-lake-varejo-905418122144/04_refined/data_viz/tb_{assunto}') 

        print(f"Dados da tabela '{assunto}' salvos com sucesso.")

    except Exception as e:
        print(f"Erro ao processar '{assunto}': {str(e)}")


#-------------------------------------- Início Carregamento Base Controle -----------------------------------------------#

# Criando dados de controle das tabelas e salvando
assuntos = ['customers','discounts','employees','products','stores','transactions']

for assunto in assuntos:
    try:
        df = spark.sql(f"""
              select
                'tb_refined_data_viz_{assunto}' as tb_name
                , {dt_proc} as dt_proc
                , count(*) as qtd_registros
              from tb_{assunto}_dedup_02
        """)
              
        df.write.mode('append').parquet('s3://data-lake-varejo-905418122144/05_controle/refined/')      
        print(f"Dados da tabela '{assunto}' salvos com sucesso.")

    except Exception as e:
        print(f"Erro ao processar '{assunto}': {str(e)}")