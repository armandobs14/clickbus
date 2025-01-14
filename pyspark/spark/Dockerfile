FROM bitnami/spark:3.3.2

USER root

ARG SPARK_VER=3.3.2
ARG HADOOP_VER=3

RUN chmod 0777 -R /opt/bitnami/spark/ \
  && apt update && apt install curl -y

RUN curl --location https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$SPARK_VER/hadoop-aws-$SPARK_VER.jar \
  --output /opt/bitnami/spark/jars/hadoop-aws-$SPARK_VER.jar \
  && curl --location https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar \
  --output /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.1026.jar
USER 1001