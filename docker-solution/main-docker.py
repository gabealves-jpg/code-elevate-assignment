from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException
import os
from analysis import analyze_data


spark = SparkSession.builder.appName("CodeElevate").getOrCreate()

def process_and_save_data() -> DataFrame:
    """
    Processa o arquivo de log que já está setado na função. 
    Utiliza regex para fazer o parsing dos dados dentro do arquivo de log.
    Também formata a coluna 'size' para int, que será usado para as próximas questões.
    Além disso, salva o dataframe em...{a definir}

    Args:
        None

    Returns:
        df (pyspark.sql.DataFrame): DataFrame com os dados processados.

    Raises:
        ValueError: Se o arquivo de log estiver vazio.
        FileNotFoundError: Se o arquivo de log não for encontrado.
        AnalysisException: Se houver um erro no processamento Spark.
    """
    try:
        raw_df = spark.read.text('resources/access_log.txt')
        if raw_df.count() == 0:
            raise ValueError("O arquivo de log está vazio")


        df = raw_df.select(
            F.regexp_extract('value', r'^(\S+)', 1).alias('ip'),
            F.regexp_extract('value', r'\[(.*?)\]', 1).alias('time'),
            F.regexp_extract('value', r'"(\w+)\s+([^\s]+)[^"]*"', 1).alias('method'),
            F.regexp_extract('value', r'"(\w+)\s+([^\s]+)[^"]*"', 2).alias('path'),
            F.regexp_extract('value', r'"[^"]*" (\d+)', 1).alias('status'),
            F.regexp_extract('value', r'"[^"]*" \d+ (\S+)', 1).alias('size'))

    except FileNotFoundError as e:
        print(f"Erro ao acessar arquivo: {str(e)}")
        return None
    except AnalysisException as e:
        print(f"Erro no processamento Spark: {str(e)}")
        return None

    #Trocando valores "-" por 0    
    df = df.withColumn('size', F.when(F.col('size') == '-', '0').otherwise(F.col('size')).cast("integer"))


    return df

def main():

    #Processing & Saving
    df = process_and_save_data()
    #Analysis
    analyze_data(df)
    
if __name__ == "__main__":
    main()
