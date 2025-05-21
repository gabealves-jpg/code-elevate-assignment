## **Análise de Logs Web com Apache Spark**
 
Você foi contratado como Data Engineer por uma empresa de tecnologia que oferece serviços online. Sua tarefa é analisar os logs de acesso ao servidor web da empresa, os quais são cruciais para monitorar a performance do sistema, identificar padrões de uso e detectar possíveis problemas de segurança.
 
Como parte do seu trabalho, você recebeu um arquivo de log contendo registros de requisições HTTP feitas ao servidor da empresa ao longo de alguns dias. Sua missão é processar e analisar esses logs para extrair informações relevantes, auxiliando a equipe de operações a compreender melhor o comportamento dos usuários e a performance da infraestrutura. Para isso, você utilizará o **Apache Spark**, um framework amplamente usado para processamento distribuído de grandes volumes de dados.
 
O arquivo de log segue o padrão **Web Server Access Log**, e cada linha representa uma requisição HTTP. Com base nos dados do arquivo, responda às seguintes perguntas:
 
### **Desafio:**
1. **Identifique as 10 maiores origens de acesso (Client IP) por quantidade de acessos.**
2. **Liste os 6 endpoints mais acessados, desconsiderando aqueles que representam arquivos.**
3. **Qual a quantidade de Client IPs distintos?**
4. **Quantos dias de dados estão representados no arquivo?**
5. **Com base no tamanho (em bytes) do conteúdo das respostas, faça a seguinte análise:**
   - O volume total de dados retornado.
   - O maior volume de dados em uma única resposta.
   - O menor volume de dados em uma única resposta.
   - O volume médio de dados retornado.
   - *Dica:* Considere como os dados podem ser categorizados por tipo de resposta para realizar essas análises.
6. **Qual o dia da semana com o maior número de erros do tipo "HTTP Client Error"?**
 
---
 
### **Deployment:**
 
Você tem duas opções para o deployment do seu projeto. Escolha a que preferir para entregar sua solução. Certifique-se de seguir todas as instruções para a opção escolhida.
 
#### **Opção 1: Docker**
 
Se você optar por utilizar **Docker** para o deployment, siga as instruções abaixo:
 
1. **Repositório Github Público:**
   - Submeta seu código em um repositório público no GitHub.
 
2. **Arquivo README.md:**
   - Incluir um arquivo `README.md` que contenha:
      - **Instruções de instalação**: Detalhe as dependências e como configurar o ambiente para rodar o projeto localmente.
      - **Instruções de execução**: Explique como rodar o projeto, descrevendo os comandos necessários e como interpretar a saída do programa.
 
3. **Dockerfile:**
   - Forneça um `Dockerfile` que contenha todas as dependências necessárias para rodar a aplicação.
   - Inclua também um arquivo `docker-compose.yml` que permita rodar o projeto facilmente com um único comando `docker-compose up`.
   - O contêiner deve estar pronto para processar o arquivo de log e fornecer as respostas para as questões do desafio.
 
---
 
#### **Opção 2: Databricks Community Edition**
 
Se preferir, você pode utilizar o **Databricks Community Edition** para desenvolver e testar sua solução. Siga as instruções abaixo:
 
1. **Criação da Conta e Configuração do Ambiente:**
   - Crie uma conta gratuita em [Databricks Community Edition](https://community.cloud.databricks.com/).
   - Importe o arquivo de log para o ambiente do Databricks (você pode fazer upload diretamente ou usar um caminho HTTP para acessá-lo).
 
2. **Notebook:**
   - Desenvolva a solução usando um notebook Databricks com o código em **Python** ou **Scala**.
   - Certifique-se de que o código está bem estruturado e documentado.
 
3. **Entrega:**
   - Inclua um link para o notebook Databricks no seu repositório GitHub ou adicione o código completo diretamente no repositório.
   - No arquivo `README.md`, inclua:
      - **Instruções de como configurar e executar o código no Databricks**.
 
---
 
### **Seção Opcional: Armazenamento dos Logs**
 
Como uma etapa adicional opcional, você pode gravar os logs processados em um sistema de armazenamento persistente. Se optar por realizar esta etapa, você pode escolher entre gravar os dados em um banco de dados de sua preferência ou utilizar um formato de armazenamento otimizado como o **Delta Lake**.
 
#### **Opção 1: Banco de Dados de Sua Escolha**
 
1. **Escolha do Banco de Dados:**
   - Você tem liberdade para escolher o tipo de banco de dados que melhor se adequa à sua solução. Algumas opções incluem:
      - **Banco de dados relacional** (ex: PostgreSQL, MySQL).
      - **Banco de dados NoSQL** (ex: MongoDB, Elasticsearch).
 
2. **Estruturação dos Dados:**
   - Crie uma estrutura de tabelas coerente para armazenar os dados dos logs processados.
      - Como você organizaria esses dados? (Por exemplo, uma tabela de requisições HTTP com colunas para Client IP, Endpoint, Status Code, Response Size, etc.)
 
3. **Justificativa:**
   - Inclua no `README.md` uma breve justificativa sobre por que escolheu determinado banco de dados e como ele facilita futuras análises.
 
4. **Implementação:**
   - Forneça no código a integração com o banco de dados escolhido e mostre como os dados processados são gravados de forma eficiente.
 
#### **Opção 2: Delta Lake (para Databricks)**
 
Se você escolher o **Databricks Community Edition** para o desafio, recomendamos que utilize o **Delta Lake** como sistema de armazenamento, dadas suas vantagens em relação à performance, confiabilidade e versionamento dos dados.
 
1. **Criação da Tabela Delta:**
   - Após processar os dados do log, armazene o resultado em uma tabela **Delta**:
 
     ```python
     df.write.format("delta").mode("overwrite").save("/mnt/delta/logs_delta")
     ```
 
2. **Consulta e Manutenção dos Dados:**
   - Utilize comandos Spark SQL ou DataFrame API para consultar e gerenciar a tabela Delta:
 
     ```python
     df_delta = spark.read.format("delta").load("/mnt/delta/logs_delta")
     df_delta.show()
     ```
 
3. **Entrega:**
   - Inclua no arquivo `README.md`:
      - As instruções para configurar e acessar o Delta Lake.
      - Como executar as operações de escrita e leitura com o Delta Lake.
 
---
 
### **Critérios de Avaliação:**
 
1. **Corretude dos Resultados:**
   - As respostas para o desafio devem estar corretas, com base nos dados fornecidos no arquivo de log.
 
2. **Eficiência do Código:**
   - Avalia-se o uso eficiente do **Apache Spark** para processamento de dados, verificando a capacidade de lidar com grandes volumes de dados e realizar operações de maneira eficiente.
 
3. **Clareza e Organização do Código:**
   - O código deve ser claro, bem organizado, e seguir boas práticas de programação, como modularização, reutilização de funções e comentários explicativos onde necessário.
 
4. **Documentação:**
   - O arquivo `README.md` deve ser completo e bem estruturado, contendo instruções claras sobre:
      - Instalação e configuração do ambiente.
      - Execução do projeto.
      - Explicações sobre a escolha das tecnologias e banco de dados (se aplicável).
 
5. **Deployment (Docker ou Databricks):**
   - **Para Docker:**
      - O `Dockerfile` e o `docker-compose.yml` devem estar corretamente configurados, permitindo que o projeto seja executado de maneira fácil com o comando `docker-compose up`.
   - **Para Databricks:**
      - O notebook deve ser bem estruturado e funcional, com as instruções para execução no Databricks Community Edition claramente descritas no `README.md`.
 
6. **Manutenção e Escalabilidade:**
   - Avaliação da facilidade de manutenção do código e da preparação para cenários de escalabilidade, como aumento no volume de dados ou necessidade de processamento mais complexo.
 
7. **Entrega e Organização do Repositório:**
   - O repositório no GitHub deve ser organizado, com commits descritivos, pastas claras, e arquivos adequadamente nomeados, facilitando a navegação.
 
8. **Testes:**
   - Implementação de testes unitários para as funções principais será um diferencial importante. A cobertura de código em termos de testes será levada em consideração.
 
9. **Seção Opcional de Armazenamento:**
   - **Para Banco de Dados:**
      - Avaliação da estruturação correta das tabelas, justificação da escolha do banco de dados, e eficiência na integração e gravação dos dados processados.
   - **Para Delta Lake (Databricks):**
      - Avaliação da correta implementação e utilização do **Delta Lake**, incluindo a criação da tabela Delta, consultas, e manutenção dos dados.
 
10. **Criatividade e Soluções Alternativas:**
    - Soluções inovadoras ou criativas para o problema, bem como a utilização de técnicas e ferramentas que não foram explicitamente mencionadas, serão consideradas um diferencial.