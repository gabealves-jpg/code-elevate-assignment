from pyspark.sql import functions as F
from pyspark.sql import DataFrame

def get_top_ips(df: DataFrame) -> None:
    """Analisa e mostra os 10 maiores acessos por IP."""

    ip_counts = (df
                .groupBy('ip')
                .agg(F.count('*').alias('count'))
                .orderBy('count', ascending=False)
                .limit(10))
    print("Top 10 IPs:")
    ip_counts.show()
    #Mais formatado
    for idx, row in enumerate(ip_counts.collect(), 1):
        print(f"{idx}º - {row['ip']}, com {row['count']} requests")

def get_top_endpoints(df: DataFrame) -> None:
    """Analisa e mostra os 6 endpoints mais acessados, excluindo arquivos."""

    extensions = ["css", "js", "html", "png", "jpg", "jpeg", "gif", "ico", "php", "txt"]
    pattern = r"(?i)\.(" + "|".join(extensions) + r")($|\?)"  

    endpoints_counts = (df
                    .filter(~F.col('path').rlike(pattern))
                    .groupBy('path')
                    .agg(F.count('*').alias('count'))
                    .orderBy(F.desc('count'))
                    .limit(6))

    print("\nTop 6 Endpoints (excluindo arquivos):")
    endpoints_counts.show()
    
    for idx, row in enumerate(endpoints_counts.collect(), 1):
        print(f"{idx}º - '{row['path']}', com {row['count']} acessos")

def get_unique_stats(df: DataFrame) -> None:
    """Analisa e mostra a quantidade de Client IPs distintos e a quantidade de dias representados no arquivo."""

    unique_ips = df.select('ip').distinct().count()
    print("\nQuantidade de Client IPs Distintos:", unique_ips)

    unique_days = (df
                .select(F.regexp_extract(F.col('timestamp'), r'(\d{2}/\w{3}/\d{4})', 1).alias('date_str'))
                .filter(F.col('date_str') != "")
                .distinct()
                .count())
    print("\nQuantidade de Dias Representados:", unique_days)

def get_bytes_statistics(df: DataFrame) -> None:
    """Analisa alguns dados relacionados aos bytes retornados."""

    total_bytes = df.agg(F.sum('size').alias('total_bytes')).collect()[0][0]
    max_bytes = df.agg(F.max('size').alias('max_bytes')).collect()[0][0]
    min_bytes = df.filter(F.col('size') > 0).agg(F.min('size').alias('min_bytes')).collect()[0][0]
    avg_bytes = df.agg(F.avg('size').alias('avg_bytes')).collect()[0][0]

    print("\nVolume Total de Bytes Retornados:", total_bytes)
    print("\nMAIOR Volume de Dados em uma Única Resposta:", max_bytes)
    print("\nMENOR Volume de Dados em uma Única Resposta:", str(min_bytes))
    print("\nVolume Médio de Dados Retornado (arredondado):", round(avg_bytes))

def get_error_statistics(df: DataFrame) -> None:
    """Analisa e mostra o dia com mais erros (4xx)."""

    error_df = (df
        .filter(F.col("status").cast("integer").between(400, 499))
        #Poderia usar a coluna de dat_ref_carga, mas num cenário ideal, usuaria a o timestamp do arquivo.
        .withColumn(
            "date_parsed",
            F.to_date(F.regexp_extract(F.col("timestamp"), r"(\d{2}/\w{3}/\d{4})", 1), "dd/MMM/yyyy") 
        )
        .withColumn("weekday", F.date_format(F.col("date_parsed"), "EEEE")))

    weekday_errors = (error_df
                    .groupBy("weekday")
                    .agg(F.count('*').alias("error_count"))
                    .orderBy(F.desc("error_count"))
                    .limit(1))
    
    top_weekday_error_row = weekday_errors.collect()[0]
    print(f"\nDia da semana com o maior número de erros (4xx): {top_weekday_error_row['weekday']}, com {top_weekday_error_row['error_count']} erros")

def analyze_data(df: DataFrame) -> None:
    """
    Analisa o dataframe e retorna resultados para as seguintes perguntas:
        - 10 maiores acessos por IP
        - 6 Endpoints mais acessados, excluindo arquivos
        - Quantidade de Client IPs distintos
        - Quantos dias de dados estão representados no arquivo?
        - Volume total de bytes retornados
        - MAIOR volume de dados em uma única resposta
        - MENOR volume de dados em uma única resposta
        - Volume médio de dados retornado.
        - Maior número de erros

    Args:
        df (DataFrame): O DataFrame processado pela função anterior.

    Returns:
        None (a informação é exibida apenas)

    Raises:
        ValueError: Se o DAtaframe estiver vazio ou faltar colunas
    """
    if df is None:
        raise ValueError("DataFrame não pode ser vazio")

    required_columns = {'ip', 'path', 'timestamp', 'size', 'status'}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing columns in DataFrame: {missing_columns}")

    get_top_ips(df)
    get_top_endpoints(df)
    get_unique_stats(df)
    get_bytes_statistics(df)
    get_error_statistics(df)