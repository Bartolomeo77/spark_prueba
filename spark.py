from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType
from pyspark.ml.linalg import Vectors

# Configurar la aplicación Spark
conf = SparkConf().setAppName("TuAppSpark").setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Leer el archivo de datos de calificaciones
lines = sc.textFile("work/u.data")

# Definir la función paseline correctamente
def paseline(line):
    try:
        fields = line.split('\t')
        userid = fields[0]
        peliid = fields[1]
        ratingid = fields[2]
        return (userid, peliid, float(ratingid))
    except Exception as e:
        print(f"Error al procesar la línea: {line}")
        return None

# Aplicar la función paseline a cada línea del RDD y filtrar líneas no válidas
data_rdd = lines.map(paseline).filter(lambda x: x is not None)

# Crear un DataFrame
df = spark.createDataFrame(data_rdd, ["userid", "peliid", "ratingid"])

# Procesar en bloques (dividir por userid y peliid)
block_size = 10000  # Ajusta el tamaño del bloque según sea necesario

# Obtener la lista de bloques
blocks = df.select("userid", "peliid").distinct().rdd.flatMap(lambda x: [(x[0], x[1])] * block_size).collect()

# Definir la función para calcular la distancia euclidiana
euclidean_distance_udf = udf(lambda v1, v2: float(Vectors.squared_distance(Vectors.dense(v1), Vectors.dense(v2))), DoubleType())

# Procesar cada bloque
for block in blocks:
    # Filtrar el DataFrame para obtener el bloque actual
    block_df = df.filter((col("userid") == block[0]) & (col("peliid") == block[1]))

    # Muestreo aleatorio para reducir el tamaño del bloque
    sampled_block_df = block_df.sample(False, 0.01)  # Ajusta la fracción de muestreo según sea necesario

    # Calcular la distancia euclidiana entre usuarios dentro del bloque
    distances_df = sampled_block_df.crossJoin(sampled_block_df.withColumnRenamed("userid", "userid2").withColumnRenamed("peliid", "peliid2").withColumnRenamed("ratingid", "ratingid2"))
    distances_df = distances_df.withColumn("euclidean_distance", euclidean_distance_udf(["ratingid", "ratingid2"]))

    # Imprimir o realizar acciones adicionales según sea necesario
    distances_df.show(truncate=False)

# Detener el contexto Spark
sc.stop()
