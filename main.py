import polars as pl  
import re
import datetime

def process_data():
    log_pattern = re.compile(
    r'(?P<ip>\S+) '     
    r'\S+ '                 
    r'(?P<user>\S+) '        
    r'\[(?P<time>.*?)\] '    
    r'"(?P<request>(?P<method>\w+)\s+(?P<path>[^\s]+)[^"]*)" ' 
    r'(?P<status>\d+) '  
    r'(?P<size>\S+)'       
)

    with open('resources/access_log.txt', 'r') as file:
        log_entries = [
            log_pattern.match(line).groupdict()
            for line in file if log_pattern.match(line)
        ]
    
    df = pl.DataFrame(log_entries)

    #Trocando valores "-" por 0
    df = df.with_columns(pl.col('size').str.replace_all('-', '0').cast(pl.Int64).alias('size')) #TODO Rever isso
    
    print(df)

    return df


def analyze_data(df):

    #10 maiores acessos por IP
    ip_counts = (df
                .group_by('ip')
                .count()
                .sort('count', descending=True)
                .limit(10))
    print("Top 10 IPs:")
    print(ip_counts)

    # Liste os 6 endpoints mais acessados, desconsiderando aqueles que representam arquivos.
    extensions = ["css", "js", "html", "png", "jpg", "jpeg", "gif", "ico", "php", "txt"]
    pattern = r"\.(" + "|".join(extensions) + r")($|\?)"  

    endpoints_counts = (df
                    .filter(~pl.col('path').str.contains(pattern))  
                    .group_by('path')
                    .count()
                    .sort('count', descending=True)
                    .limit(6))

    print("\nTop 6 Non-File Endpoints:")
    print(endpoints_counts)

    #Qual a quantidade de Client IPs distintos?
    unique_ips = df['ip'].unique().count()
    print("\nQuantidade de Client IPs Distintos:", unique_ips)

    #Quantos dias de dados estão representados no arquivo?
    unique_days = df['time'].str.extract(r'(\d{2}/\w{3}/\d{4})', 1).unique().count()
    print("\nQuantidade de Dias Representados:", unique_days)
    
    #Perguntas sobre Bytes...

    #Volume total de bytes retornados:
    total_bytes = df["size"].sum() #TODO Rever isso
    print("\nVolume Total de Bytes Retornados:", total_bytes)

    #O maior volume de dados em uma única resposta
    max_bytes = df["size"].max()
    print("\nMAIOR Volume de Dados em uma Única Resposta:", max_bytes)

    #O menor volume de dados em uma única resposta excluindo os valores de 0
    min_bytes = df.select("size").filter(pl.col("size") > 0).min().to_series().item()

    print("\nMENOR Volume de Dados em uma Única Resposta:", str(min_bytes))

    #O volume médio de dados retornado.
    avg_bytes = df["size"].mean()
    print("\nVolume Médio de Dados Retornado (arredondado):", round(avg_bytes))

    #Qual o dia da semana com o maior número de erros do tipo "HTTP Client Error"?
    df = df.with_columns(
        pl.col("time")
        .str.extract(r'(\d{2}/\w{3}/\d{4})')
        .str.strptime(pl.Date, format="%d/%b/%Y")  
        .alias("date_parsed")
    )

    df = df.with_columns(pl.col("date_parsed").dt.strftime("%A").alias("weekday"))

    client_errors = df.filter(pl.col("status").cast(pl.Int32).gt(399).lt(500))
    weekday_errors = client_errors.group_by("weekday").count().sort("count", descending=True).limit(1)
    weekday_errors['weekday'].item()
    print(f"\nDia da Semana com o Maior Número de Erros do Tipo 'HTTP Client Error': {weekday_errors['weekday'].item()}, com {weekday_errors['count'].item()} erros")

def main():
    df = process_data()
    #Save here or after?
    analyze_data(df)
    

if __name__ == "__main__":
    main()
