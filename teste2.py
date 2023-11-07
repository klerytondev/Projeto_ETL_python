from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col

# Inicialize uma sessão Spark
spark = SparkSession.builder.appName("ConversaoTimestamp").getOrCreate()

# Carregue os dados do seu DataFrame
# Substitua "seu_dataframe" pelo nome da variável que contém o seu DataFrame.
seu_dataframe = spark.read.csv("caminho/do/seu/arquivo.csv", header=True, inferSchema=True)

# Converta a coluna de string em timestamp e sobrescreva a mesma coluna
# Substitua "coluna_string" pelo nome da coluna que você deseja converter.
seu_dataframe = seu_dataframe.withColumn("coluna_string", to_timestamp(col("coluna_string"), "yyyyMMddHHmmss"))

# Exiba o DataFrame resultante
seu_dataframe.show()

# Para salvar o DataFrame resultante em um arquivo CSV, você pode usar:
# seu_dataframe.write.mode("overwrite").csv("caminho/do/arquivo_de_saida.csv")

# Encerre a sessão Spark
spark.stop()
