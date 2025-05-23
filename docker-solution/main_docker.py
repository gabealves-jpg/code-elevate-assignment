from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException
from analysis import analyze_data

file_path = 'resources/access_log.txt'

# Updated SparkSession configuration
spark = SparkSession.builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
            .getOrCreate()
            
def process_data(file_path) -> DataFrame:
    """
    Processa o arquivo de log que já está setado na função. 
    Utiliza regex para fazer o parsing dos dados dentro do arquivo de log.
    Também formata a coluna 'size' para int, que será usado para as próximas questões.
    Além disso, salva o dataframe em...{a definir}

    Args:
        file_path (str): Caminho para o arquivo de log.

    Returns:
        df (pyspark.sql.DataFrame): DataFrame com os dados processados.

    Raises:
        ValueError: Se o arquivo de log estiver vazio.
        FileNotFoundError: Se o arquivo de log não for encontrado.
        AnalysisException: Se houver um erro no processamento Spark.
    """
    try:
        raw_df = spark.read.text(file_path)
        if raw_df.count() == 0:
            raise ValueError("O arquivo de log está vazio")

        log_pattern = r'^(\S+).*\[(.*?)\].*"(\w+)\s+([^\s]+)[^"]*"\s+(\d+)\s+(\S+)'

        df = raw_df.select(
             F.regexp_extract('value', log_pattern, 1).alias('ip'),
             F.regexp_extract('value', log_pattern, 2).alias('timestamp'),
             F.regexp_extract('value', log_pattern, 3).alias('method'),
             F.regexp_extract('value', log_pattern, 4).alias('path'),
             F.regexp_extract('value', log_pattern, 5).alias('status'),
             F.regexp_extract('value', log_pattern, 6).alias('size'))

    except FileNotFoundError as e:
        print(f"Erro ao acessar arquivo: {str(e)}")
        return None
    except AnalysisException as e:
        print(f"Erro no processamento Spark: {str(e)}")
        return None

    #Trocando valores "-" por 0    
    df = df.withColumn('size', F.when(F.col('size') == '-', '0').otherwise(F.col('size')).cast("integer"))
    #Cria coluna de dat_mes_carga para particionamento. Para este case, irei simular que o 'timestamp' será o dat_ref_carga.
    df = df.withColumn('dat_mes_carga', F.date_format(F.to_timestamp(F.col('timestamp'), 'dd/MMM/yyyy:HH:mm:ss Z'),'MM/yyyy'))
    
    df.cache()

    return df

def save_data(df: DataFrame) -> None:
    """
    Salva o dataframe no OpenSearch.
    Args:
        df (pyspark.sql.DataFrame): DataFrame com os dados processados.
    Returns:
        None
    Raises:
        Exception: Ao falhar o save de dados
    """
    try:
        df.write \
            .format("parquet") \
            .mode("overwrite") \
            .partitionBy("dat_mes_carga") \
            .save("s3a://codelevate/bronze/")
        print("Salvo com sucesso")
    except Exception as e:
        print(f"Erro ao salvar: {str(e)}")
        raise

def main():

    #Processing & Saving
    df = process_data(file_path)
    #Saving
    save_data(df)
    #Analysis
    analyze_data(df)
    
if __name__ == "__main__":
    main()
