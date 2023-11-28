# Utiliza una imagen base de Spark
FROM apache/spark:3.1.2

# Copia los archivos necesarios al contenedor
COPY spark.py /opt/spark/work-dir/
COPY u.data /opt/spark/work-dir/

# Configura el entorno
ENV SPARK_APPLICATION_PYTHON_LOCATION /opt/spark/work-dir/spark.py
ENV SPARK_APPLICATION_ARGS ""

# Ejecuta la aplicación Spark
CMD ["/opt/entrypoint.sh"]
