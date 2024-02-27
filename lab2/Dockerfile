FROM apache/spark

# copy the jar into the container
COPY /target/spark-test-1.0-SNAPSHOT-fat.jar job.jar

# copy input file to container
COPY eurovision9_corto.json /opt/spark/work-dir/

# submit (run) the spark job
CMD ["/opt/spark/bin/spark-submit", "--class", "edu.upf.TwitterLanguageFilterApp", "job.jar", "es", "./output_files/output_es_sample.txt", "hola", "eurovision9_corto.json"]