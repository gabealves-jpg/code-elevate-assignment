from pyspark.sql import functions as F
from pyspark.sql import DataFrame

def analyze_data(df: DataFrame) -> None:
    """
    Analisa o DataFrame e imprime os resultados para as seguintes perguntas:

    Para algumas perguntas, há visualizações formatadas.

    Args:
        df (DataFrame): O DataFrame processado pela função anterior.

    Returns:
        None (todas as informações são exibidas)

    Raises:
        ValueError: Se o DataFrame for None.

        
    """
    
    #Alguns tratamentos de erros...
    if df.empty:
        raise ValueError("DataFrame não pode estar vazio")

    required_columns = {'ip', 'path', 'time', 'size', 'status'}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Colunas ausentes no DataFrame: {missing_columns}")

    #10 maiores acessos por IP
    ip_counts = (df
                .groupBy('ip')
                .agg(F.count('*').alias('count'))
                .orderBy('count', ascending=False)
                .limit(10))
    print("Top 10 IPs:")
    ip_counts.show()
    ip_counts = ip_counts.collect()
    
    index = 1
    for row in ip_counts:
         print(f"{index}º - {row['ip']}")
         index += 1

    # Liste os 6 endpoints mais acessados, desconsiderando aqueles que representam arquivos.
    extensions = ["css", "js", "html", "png", "jpg", "jpeg", "gif", "ico", "php", "txt"]
    pattern = r"\.(" + "|".join(extensions) + r")($|\?)"  

    endpoints_counts = (df
                        .filter(~F.col('path').rlike(pattern)) # Use rlike for regex matching
                        .groupBy('path')
                        .agg(F.count('*').alias('count'))
                        .orderBy(F.desc('count'))
                        .limit(6))

    print("\nTop 6 Endpoints (excluindo arquivos):")
    endpoints_counts.show()
    endpoints_counts = endpoints_counts.collect()
    index = 1
    for row in endpoints_counts:
         print(f"{index}º - '{row['path']}', com {row['count']} acessos")
         index += 1

    #Qual a quantidade de Client IPs distintos?
    unique_ips = df.select('ip').distinct().count()
    print("\nQuantidade de Client IPs Distintos:", unique_ips)

    #Quantos dias de dados estão representados no arquivo?
    unique_days = (df
                    .select(F.regexp_extract(F.col('time'), r'(\d{2}/\w{3}/\d{4})', 1).alias('date_str'))
                    .filter(F.col('date_str') != "") # Ensure date_str is not empty
                    .distinct()
                    .count())
    print("\nQuantidade de Dias Representados:", unique_days)
    
    #Perguntas sobre Bytes...

    #Volume total de bytes retornados:
    total_bytes = df.agg(F.sum('size').alias('total_bytes')).collect()[0][0] #TODO Rever isso
    print("\nVolume Total de Bytes Retornados:", total_bytes)

    #O maior volume de dados em uma única resposta
    max_bytes = df.agg(F.max('size').alias('max_bytes')).collect()[0][0]
    print("\nMAIOR Volume de Dados em uma Única Resposta:", max_bytes)

    #O menor volume de dados em uma única resposta excluindo os valores de 0
    min_bytes = df.filter(F.col('size') > 0).agg(F.min('size').alias('min_bytes')).collect()[0][0]
    print("\nMENOR Volume de Dados em uma Única Resposta:", str(min_bytes))

    #O volume médio de dados retornado.
    avg_bytes = df.agg(F.avg('size').alias('avg_bytes')).collect()[0][0]
    print("\nVolume Médio de Dados Retornado (arredondado):", round(avg_bytes))

    #Qual o dia da semana com o maior número de erros do tipo "HTTP Client Error"?
    df = (df
        .filter(F.col("status").cast("integer").between(400, 599)) #Garantir que sejam erros mesmo
        .withColumn(
        "date_parsed",
        F.to_date(F.regexp_extract(F.col("time"), r"(\d{2}/\w{3}/\d{4})", 1), "dd/MMM/yyyy")
    ))

    df = df.withColumn("weekday", F.date_format(F.col("date_parsed"), "EEEE"))

    weekday_errors = (df
                        .groupBy("weekday")
                        .agg(F.count('*').alias("error_count"))
                        .orderBy(F.desc("error_count"))
                        .limit(1))
        
    top_weekday_error_row = weekday_errors.collect()[0]

    print(f"\nDia da semana com o maior número de erros (4xx, 5xx): {top_weekday_error_row['weekday']}, com {top_weekday_error_row['error_count']} erros")
