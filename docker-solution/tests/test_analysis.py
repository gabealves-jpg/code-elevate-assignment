import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from analysis import (get_top_ips, get_top_endpoints, get_unique_stats, 
                     get_bytes_statistics, get_error_statistics, analyze_data)

@pytest.fixture(scope="module")
def spark():
    """
    Cria e retorna uma SparkSession para testes.
        
    Returns:
        SparkSession: A configured SparkSession for testing.
    """
    return SparkSession.builder \
        .master("local[1]") \
        .appName("PySpark-unit-test") \
        .getOrCreate()

@pytest.fixture(scope="module")
def sample_df(spark):
    """
    Cria um DataFrame de exemplo com dados de teste para funções de análise.
    
    Args:
        spark (SparkSession): A fixture SparkSession.
        
    Returns:
        DataFrame: DataFrame de exemplo com dados de teste.
    """
    schema = StructType([
        StructField("ip", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("method", StringType(), True),
        StructField("path", StringType(), True),
        StructField("status", StringType(), True),
        StructField("size", IntegerType(), True),
        StructField("dat_mes_carga", StringType(), True)
    ])
    
    data = [
        ("192.168.1.1", "01/Jan/2023:12:00:00 +0000", "GET", "/home", "200", 1024, "01/2023"),
        ("192.168.1.1", "01/Jan/2023:12:01:00 +0000", "GET", "/about", "200", 2048, "01/2023"),
        ("192.168.1.2", "01/Jan/2023:12:02:00 +0000", "GET", "/home", "200", 1024, "01/2023"),
        ("192.168.1.3", "02/Jan/2023:12:03:00 +0000", "GET", "/contact", "404", 512, "01/2023"),
        ("192.168.1.4", "02/Jan/2023:12:04:00 +0000", "GET", "/image.jpg", "200", 4096, "01/2023"),
        ("192.168.1.5", "03/Jan/2023:12:05:00 +0000", "GET", "/script.js", "200", 2048, "01/2023"),
    ]
    
    return spark.createDataFrame(data, schema)

def test_get_top_ips(sample_df, capfd):
    """
    Tests the get_top_ips function.
    Testa a função get_top_ips.
    
    Args:
        sample_df (DataFrame): sample DataFrame
        capfd (CaptureFixture): Pytests para pegar stdout/stderr.
    """
    get_top_ips(sample_df)
    out, _ = capfd.readouterr()
    assert "192.168.1.1" in out
    assert "2 requests" in out

def test_get_top_endpoints(sample_df, capfd):
    """
    Testa a função get_top_endpoints.
    
    Args:
        sample_df (DataFrame): sample DataFrame
        capfd (CaptureFixture): Pytests para pegar stdout/stderr.
    """
    get_top_endpoints(sample_df)
    out, _ = capfd.readouterr()
    assert "/home" in out
    assert "/about" in out
    assert "/contact" in out
    assert "/image.jpg" not in out  # Should be filtered out as a file

def test_get_unique_stats(sample_df, capfd):
    """
    Testa a função get_unique_stats.
        
    Args:
        sample_df (DataFrame): sample DataFrame
        capfd (CaptureFixture): Pytests para pegar stdout/stderr.
    """
    get_unique_stats(sample_df)
    out, _ = capfd.readouterr()
    assert "Quantidade de Client IPs Distintos: 5" in out
    assert "Quantidade de Dias Representados: 3" in out

def test_get_bytes_statistics(sample_df, capfd):
    """
    Testa a função get_bytes_statistics.

    Args:
        sample_df (DataFrame): sample DataFrame
        capfd (CaptureFixture): Pytests para pegar stdout/stderr.
    """
    get_bytes_statistics(sample_df)
    out, _ = capfd.readouterr()
    assert "Volume Total de Bytes Retornados: 10752" in out
    assert "MAIOR Volume de Dados em uma Única Resposta: 4096" in out
    assert "MENOR Volume de Dados em uma Única Resposta: 512" in out

def test_get_error_statistics(sample_df, capfd):
    """
    Testa a função get_error_statistics.
    
    Args:
        sample_df (DataFrame): sample DataFrame
        capfd (CaptureFixture): Pytests para pegar stdout/stderr.
    """
    get_error_statistics(sample_df)
    out, _ = capfd.readouterr()
    assert "Dia da semana com o maior número de erros (4xx)" in out

def test_analyze_data_with_valid_df(sample_df, capfd):
    """
    Testa a função analyze_data.
    
    Verifies that the function correctly runs all analysis functions
    and produces the expected output when given a valid DataFrame.
    
    Args:
        sample_df (DataFrame): sample DataFrame
        capfd (CaptureFixture): Pytests para pegar stdout/stderr.
    """
    analyze_data(sample_df)
    out, _ = capfd.readouterr()
    assert "Top 10 IPs" in out
    assert "Top 6 Endpoints" in out
    assert "Quantidade de Client IPs Distintos" in out

def test_analyze_data_with_none_df():
    """
    Testa a função analyze_data com nulo de entrada
    """
    with pytest.raises(ValueError, match="DataFrame não pode ser vazio"):
        analyze_data(None)

def test_analyze_data_with_missing_columns(spark):
    """
    Testa a função analyze_data com um DataFrame incompleto
    
    Args:
        spark (SparkSession): Uma fixture SparkSession
    """
    # Create a DataFrame missing required columns
    schema = StructType([
        StructField("ip", StringType(), True),
        # Missing other required columns
    ])
    
    data = [("192.168.1.1",)]
    df = spark.createDataFrame(data, schema)
    
    with pytest.raises(ValueError, match="Missing columns in DataFrame"):
        analyze_data(df)