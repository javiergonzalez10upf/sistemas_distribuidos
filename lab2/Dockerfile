FROM apache/spark

# copia el jar al contenedor
COPY /target/spark-test-1.0-SNAPSHOT-fat.jar job.jar

# copia el archivo de entrada al contenedor
COPY eurovision_corto.json /opt/spark/work-dir/

# define un directorio de salida en el contenedor
WORKDIR /opt/spark/work-dir/output_files

# monta un volumen para compartir archivos con el sistema local
VOLUME /opt/spark/work-dir/output_files

# submit (run) el trabajo de Spark y guarda la salida en el directorio de salida compartido
CMD ["/opt/spark/bin/spark-submit", "--class", "edu.upf.TwitterLanguageFilterApp", "job.jar", "es", "./output_files/output_es_sample.txt", "eurovision_corto.json"]
