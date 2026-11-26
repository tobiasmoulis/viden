FROM apache/airflow:2.10.5-python3.10

ARG SPARK_VERSION=3.5.5
ARG DELTA_VERSION=3.3.0
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/java-17-openjdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

USER airflow

RUN pip install --no-cache-dir \
      "pyspark==${SPARK_VERSION}" \
      "delta-spark==${DELTA_VERSION}" \
      "apache-airflow-providers-apache-spark==4.11.2" \
      "pytest>=8.0.0"

# Pre-bake matching Delta Lake & Hadoop-AWS JARs into PySpark's jars/ directory for offline spark-submit.
RUN PYSPARK_DIR="$(python -c 'import pathlib, pyspark; print(pathlib.Path(pyspark.__file__).parent)')" && \
    curl -fsSL -o "${PYSPARK_DIR}/jars/delta-spark_2.12-${DELTA_VERSION}.jar" \
      "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar" && \
    curl -fsSL -o "${PYSPARK_DIR}/jars/delta-storage-${DELTA_VERSION}.jar" \
      "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" && \
    curl -fsSL -o "${PYSPARK_DIR}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    curl -fsSL -o "${PYSPARK_DIR}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" && \
    mkdir -p "${PYSPARK_DIR}/conf" && \
    { \
      echo "spark.sql.extensions                     io.delta.sql.DeltaSparkSessionExtension"; \
      echo "spark.sql.catalog.spark_catalog          org.apache.spark.sql.delta.catalog.DeltaCatalog"; \
      echo "spark.sql.session.timeZone               UTC"; \
      echo "spark.executorEnv.PYTHONPATH             /opt/workspace"; \
      echo "spark.driver.bindAddress                 0.0.0.0"; \
    } > "${PYSPARK_DIR}/conf/spark-defaults.conf" && \
    echo "${PYSPARK_DIR}" > /tmp/pyspark_dir

USER root

RUN ln -s "$(cat /tmp/pyspark_dir)" /opt/spark && rm -f /tmp/pyspark_dir

USER airflow

ENV SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:${PATH}"
ENV PYTHONPATH=/opt/workspace
