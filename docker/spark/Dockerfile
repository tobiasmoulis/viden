FROM apache/spark:3.5.5-scala2.12-java17-python3-ubuntu

USER root

ARG DELTA_VERSION=3.3.0
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Pre-bake Delta Lake and Hadoop-AWS JARs so jobs and pytest run offline without Maven resolution.
RUN curl -fsSL -o /opt/spark/jars/delta-spark_2.12-${DELTA_VERSION}.jar \
      "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar" && \
    curl -fsSL -o /opt/spark/jars/delta-storage-${DELTA_VERSION}.jar \
      "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" && \
    curl -fsSL -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# Install delta-spark (--no-deps to keep /opt/spark as the single Spark/JAR source of truth) + pytest + boto3.
RUN pip3 install --no-cache-dir --no-deps "delta-spark==${DELTA_VERSION}" && \
    pip3 install --no-cache-dir importlib-metadata "pytest>=8.0.0" "boto3>=1.35.0" && \
    python3 -c "import glob, pathlib, site; sp = pathlib.Path(site.getsitepackages()[0]); paths = ['/opt/spark/python'] + sorted(glob.glob('/opt/spark/python/lib/py4j-*.zip')); (sp / 'spark.pth').write_text('\n'.join(paths) + '\n')"

RUN mkdir -p /opt/spark/conf && \
    { \
      echo "spark.sql.extensions                     io.delta.sql.DeltaSparkSessionExtension"; \
      echo "spark.sql.catalog.spark_catalog          org.apache.spark.sql.delta.catalog.DeltaCatalog"; \
      echo "spark.sql.session.timeZone               UTC"; \
      echo "spark.executorEnv.PYTHONPATH             /opt/workspace"; \
      echo "spark.driver.bindAddress                 0.0.0.0"; \
    } > /opt/spark/conf/spark-defaults.conf

RUN mkdir -p /opt/workspace /opt/workspace/lakehouse/spark-events && \
    chown -R spark:root /opt/workspace
WORKDIR /opt/workspace

ENV SPARK_HOME=/opt/spark
ENV PATH="/opt/spark/bin:${PATH}"
ENV PYTHONPATH=/opt/workspace

USER spark
