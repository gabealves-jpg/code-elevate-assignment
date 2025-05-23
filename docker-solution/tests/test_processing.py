import pytest
import os
from pyspark.sql import SparkSession
import main_docker  # Import the module instead of individual functions

@pytest.fixture(scope="module")
def spark():
    """
    Cria e retorna uma SparkSession para testes.
        
    Returns:
        SparkSession: A configured SparkSession for testing.
    """
    spark_session = SparkSession.builder \
        .master("local[1]") \
        .appName("PySpark-integration-test") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    yield spark_session
    
    # Clean up after tests
    spark_session.stop()

@pytest.fixture
def sample_log_file(tmp_path):
    """
    Cria um sample do log    

    Args:
        tmp_path (Path): Fixture do pytest que cria um diretório temporário
        
    Returns:
        Path: Caminho para o local temporário contendo o arquivo sample
    """
    log_content = """192.168.1.1 - - [01/Jan/2025:12:00:00 +0000] "GET /home HTTP/1.1" 200 1024
192.168.1.1 - - [01/Jan/2025:12:01:00 +0000] "GET /about HTTP/1.1" 200 2048
192.168.1.2 - - [01/Jan/2025:12:02:00 +0000] "GET /home HTTP/1.1" 200 1024
192.168.1.3 - - [02/Jan/2025:12:03:00 +0000] "GET /contact HTTP/1.1" 404 512
192.168.1.4 - - [02/Jan/2025:12:04:00 +0000] "GET /image.jpg HTTP/1.1" 200 4096
192.168.1.5 - - [03/Jan/2025:12:05:00 +0000] "GET /script.js HTTP/1.1" 200 2048"""
    
    # Create the test file with the same name as expected in the main code
    log_file = tmp_path / "resources" / "access_log.txt"
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    with open(log_file, "w") as f:
        f.write(log_content)
    
    # Save current directory to restore later
    original_dir = os.getcwd()
    # Change to the temp directory for the test
    os.chdir(tmp_path)
    
    yield tmp_path
    
    # Restore original directory
    os.chdir(original_dir)

def test_process_data(sample_log_file, monkeypatch):
    """
    Testa a função process_data

    Args:
        sample_log_file (Path): Caminho para o diretório temporário com o arquivo de log de teste.
        monkeypatch (MonkeyPatch): Fixture do pytest para modificar objetos durante os testes.
    """
    monkeypatch.setattr("main_docker.spark", SparkSession.builder.getOrCreate())
    
    # Create the test file path
    test_file_path = str(sample_log_file / "resources" / "access_log.txt")
    
    # Run the process_data function with our test file path
    df = main_docker.process_data(test_file_path)  # Use the module reference
    
    # Verify the DataFrame structure and content
    assert df is not None
    assert df.count() == 6
    assert "ip" in df.columns
    assert "timestamp" in df.columns
    assert "method" in df.columns
    assert "path" in df.columns
    assert "status" in df.columns
    assert "size" in df.columns
    assert "dat_mes_carga" in df.columns
    
    # Verify data transformations
    ip_counts = df.groupBy("ip").count().collect()
    assert any(row["ip"] == "192.168.1.1" and row["count"] == 2 for row in ip_counts)
    
    # Verify size column was properly converted to integer
    sizes = df.select("size").collect()
    assert all(isinstance(row["size"], int) for row in sizes)

def test_save_data(sample_log_file, monkeypatch, tmp_path):
    """
    Testa a função save_data
        
    Args:
        sample_log_file (Path): Caminho para o diretório temporário com o arquivo de log de teste.
        monkeypatch (MonkeyPatch): Fixture do pytest para modificar objetos durante os testes.
        tmp_path (Path): Fixture do pytest que fornece um caminho para diretório temporário.
    """
    # Patch the SparkSession to use our test session
    monkeypatch.setattr("main_docker.spark", SparkSession.builder.getOrCreate())
    
    # Create a test output path
    test_output_path = str(tmp_path / "test_output")
    
    # Create the test file path
    test_file_path = str(sample_log_file / "resources" / "access_log.txt")
    
    # Run the process_data function with our test file path
    df = main_docker.process_data(test_file_path)  # Use the module reference
    
    # Mock the save_data function to save to our test location
    def mock_save_data(df):
        df.write.format("parquet").mode("overwrite").partitionBy("dat_mes_carga").save(test_output_path)
        print("Salvo com sucesso")
    
    # Replace the save_data function
    monkeypatch.setattr("main_docker.save_data", mock_save_data)
    
    # Run the save_data function (which is now mocked)
    main_docker.save_data(df)  # Use the module reference
    
    # Verify data was saved correctly
    assert os.path.exists(test_output_path)
    
    # Read back the saved data to verify
    saved_df = SparkSession.builder.getOrCreate().read.parquet(test_output_path)
    assert saved_df.count() == 6