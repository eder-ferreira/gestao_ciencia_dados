# gestao_ciencia_dados

Carregamento de Dados
Passo 1: Instalar Java e Spark
Este passo instala as dependências necessárias para rodar o Apache Spark, um framework para processamento de dados distribuído.

python
Copiar código
!apt-get update -qq
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop2.7.tgz
!tar xf spark-3.1.2-bin-hadoop2.7.tgz
!pip install -q findspark
Atualiza a lista de pacotes.
Instala o Java 8.
Faz o download e extrai o Apache Spark.
Instala a biblioteca findspark.
Passo 2: Definir Variáveis de Ambiente
Configura as variáveis de ambiente necessárias para o Spark funcionar corretamente.

python
Copiar código
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.2-bin-hadoop2.7"

import findspark
findspark.init()
Define JAVA_HOME e SPARK_HOME para que o Spark encontre o Java e seu próprio diretório.
Inicializa o findspark para facilitar a integração com o PySpark.
Passo 3: Inicializar SparkSession
Cria uma sessão do Spark.

python
Copiar código
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master('local[*]') \
    .appName("Iniciando com Spark") \
    .config('spark.ui.port', '4050') \
    .getOrCreate()
Configura o Spark para rodar localmente usando todos os núcleos disponíveis.
Define o nome da aplicação Spark.
Configura a porta da interface web do Spark UI.
Passo 4: Verificar Instalação do Spark
Verifica a versão do Spark instalada.

python
Copiar código
spark.version
Passo 5: Conexão do SparkUI usando o Ngrok
Instala o Ngrok para acessar o Spark UI no Google Colab.

python
Copiar código
!wget -q https://bin.equinox.io/c/4VmDzA7iaHb/ngrok-stable-linux-amd64.zip
!unzip ngrok-stable-linux-amd64.zip
Faz o download e extrai o Ngrok.
Passo 6: Importar o Ngrok para o projeto
Instala e configura o Ngrok para permitir acesso remoto.

python
Copiar código
!pip install pyngrok
from pyngrok import ngrok

!ngrok authtoken "seu_token"
Instala a biblioteca pyngrok.
Importa o Ngrok e autentica usando um token específico.
Passo 7: Conectar Ngrok
Conecta o Ngrok à porta 4050, permitindo acessar o Spark UI remotamente.

python
Copiar código
ngrok.connect('4050')
Passo 8: Montar o Google Drive
Monta o Google Drive para acessar arquivos de dados.

python
Copiar código
from google.colab import drive
drive.mount('/content/drive')
Passo 9: Descompacta dados
Extrai os dados do Titanic de um arquivo ZIP no Google Drive.

python
Copiar código
import zipfile

zipfile.ZipFile('/content/drive/MyDrive/UFMT/spark_titanic/TitanicDataset.zip','r').extractall('/content/drive/MyDrive/UFMT/spark_titanic/')
Passo 10: Carregamento dos dados
Carrega os dados do Titanic em um DataFrame do Spark.

python
Copiar código
path = '/content/drive/MyDrive/UFMT/spark_titanic/TitanicDataset.csv'

TitanicDataset = spark.read.csv(path, header=True, sep=',', inferSchema=True)
TitanicDataset.show()

cont_dataset = TitanicDataset.count()
print("Total de registros =>", cont_dataset)
Define o caminho do arquivo CSV.
Lê o arquivo CSV no DataFrame Spark e mostra as primeiras linhas.
Conta o número total de registros.
Analisando e Manipulando os Dados
Verificar Schema e Renomear Colunas
Visualiza o esquema dos dados e renomeia as colunas.

python
Copiar código
TitanicDataset.printSchema()

titanicColNames = ["IDpassageiro", "Sobrevivente", "ClassePassagem", "Nome", "Genero", "Idade",
                   "NrIrmaosConjuges", "NrPaisFilhos", "NrTicket", "Tarifa", "Cabine", "PortoEmbarque"]

for old_name, new_name in zip(TitanicDataset.columns, titanicColNames):
    TitanicDataset = TitanicDataset.withColumnRenamed(old_name, new_name)

TitanicDataset.show()
TitanicDataset.limit(5).toPandas()
Mostra o esquema do DataFrame.
Renomeia as colunas do DataFrame para nomes mais descritivos.
Modificando Tipos de Dados
Converte e ajusta os tipos de dados conforme necessário.

python
Copiar código
from pyspark.sql.functions import col, when, round

TitanicDataset = TitanicDataset.withColumn("Idade", col("Idade").cast("integer"))
TitanicDataset.printSchema()

TitanicDataset = TitanicDataset.withColumn("Genero", when(col("Genero") == "male", "masculino")
                                           .when(col("Genero") == "female", "feminino")
                                           .otherwise(col("Genero")))

TitanicDataset = TitanicDataset.withColumn("ClassePassagem", when(col("ClassePassagem") == "1", "1°classe")
                                           .when(col("ClassePassagem") == "2", "2°classe")
                                           .when(col("ClassePassagem") == "3", "3°classe")
                                           .otherwise(col("ClassePassagem")))

TitanicDataset = TitanicDataset.withColumn("PortoEmbarque", when(col("PortoEmbarque") == "S", "southampton")
                                           .when(col("PortoEmbarque") == "C", "cherbourg")
                                           .when(col("PortoEmbarque") == "Q", "queenstown")
                                           .otherwise(col("PortoEmbarque")))

TitanicDataset = TitanicDataset.withColumn("Tarifa", round(col("Tarifa"), 2))

TitanicDataset = TitanicDataset.withColumn("Sobrevivente", when(col("Sobrevivente") == "0", "não_sobreviveu")
                                           .when(col("Sobrevivente") == "1", "sobreviveu")
                                           .otherwise(col("Sobrevivente")))

TitanicDataset.show()
Converte a coluna Idade para inteiro.
Renomeia valores nas colunas Genero, ClassePassagem, PortoEmbarque, Sobrevivente para valores mais descritivos.
Trunca a coluna Tarifa para duas casas decimais.
Identificando Valores Nulos e Estatísticas Descritivas
Conta valores nulos e mostra estatísticas descritivas.

python
Copiar código
from pyspark.sql.functions import col, count, isnan, when

TitanicDataset.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in TitanicDataset.columns]).show()
TitanicDataset.describe().show()
Conta valores nulos em cada coluna.
Mostra estatísticas descritivas para todas as colunas.
Filtrando e Sumarizando Dados
Realiza agregações e filtros para sumarizar dados.

python
Copiar código
from pyspark.sql import functions as F

contagem_sobreviventes = TitanicDataset.groupBy('Genero').agg(
    F.sum(F.when(col('Sobrevivente') == "sobreviveu", 1).otherwise(0)).alias('Sobreviventes'),
    F.sum(F.when(col('Sobrevivente') == "não_sobreviveu", 1).otherwise(0)).alias('Não Sobreviventes')
)

contagem_sobreviventes.show()

contagem_por_classe = TitanicDataset.groupBy('ClassePassagem').count()
contagem_por_classe.show()
Conta o número de sobreviventes e não sobreviventes por gênero.
Conta o número de registros por classe de passagem.
Spark SQL
Cria uma visão temporária e executa consultas SQL.

python
Copiar código
TitanicDataset.createOrReplaceTempView("titanicView")

spark.sql("SELECT * FROM titanicView").show(5)

spark.sql("""
    SELECT *
    FROM titanicView
    WHERE Idade = 0
""").show()
Cria uma visão temporária chamada titanicView.
Executa consultas SQL na visão.
Armazenamento em CSV e Parquet
Salva os dados em formatos CSV e Parquet.

python
Copiar código
TitanicDataset.write.csv(
    path='/content/drive/MyDrive/UFMT/spark_titanic/csv',
    mode='overwrite',
    sep=';',
    header=True                   
)

TitanicDataset2 = spark.read.csv(
    '/content/drive/MyDrive/UFMT/spark_titanic/csv',
    sep=';',
    inferSchema=True,
    header=True                   
)

TitanicDataset2.printSchema()

TitanicDataset.write.parquet(
    path='/content/drive/MyDrive/UFMT/spark_titanic/parquet',
    mode='overwrite',
)

TitanicDataset2 = spark.read.parquet('/content/drive/MyDrive/UFMT/spark_titanic/parquet')
TitanicDataset2.printSchema()
TitanicDataset2.show()
Escreve o DataFrame atualizado em um novo arquivo CSV.
Carrega o arquivo CSV no Spark DataFrame e mostra o esquema.
Escreve o DataFrame atualizado em um novo arquivo Parquet.
Carrega o arquivo Parquet no Spark DataFrame, mostra o esquema e as primeiras linhas.
Particionamento de Dados
Salva e carrega dados particionados por Genero.

python
Copiar código
TitanicDataset.write.parquet(
    path='/content/drive/MyDrive/UFMT/spark_titanic/parquet_particionado',
    mode='overwrite',
    partitionBy='Genero'
)

TitanicDatasetParticionado = spark.read.parquet('/content/drive/MyDrive/UFMT/spark_titanic/parquet_particionado')
TitanicDatasetParticionado.show()

feminino_df = TitanicDatasetParticionado.filter(TitanicDatasetParticionado.Genero == 'feminino')
feminino_df.printSchema()
feminino_df.show()
Salva o DataFrame em um novo arquivo Parquet particionado por Genero.
Carrega os dados particionados e mostra as primeiras linhas.
Filtra os dados para obter apenas as linhas onde Genero é feminino e mostra o esquema e as primeiras linhas do novo DataFrame.
