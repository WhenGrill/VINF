# To run pyspark in this image:
# 1. Start container with: docker run -it apache/spark-py:v3.5.0 /bin/bash
# 2. Inside container run: pyspark --packages com.databricks:spark-xml_2.12:0.18.0
FROM apache/spark-py:v3.4.0

ENV PATH="/opt/spark/bin:${PATH}"
ENV PYSPARK_PYTHON=python3.11

USER root
RUN apt-get update && \
    apt-get install -y python3.11 python3.11-dev python3-pip libsnappy-dev && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 && \
    rm -rf /var/lib/apt/lists/*

# Install native hadoop library dependencies
RUN apt-get update && \
    apt-get install -y build-essential && \
    apt-get install -y libnative-platform-java && \
    apt-get install -y libsnappy1v5 libsnappy-dev && \
    apt-get install -y openjdk-11-jdk && \
    rm -rf /var/lib/apt/lists/*

# The base image apache/spark-py already includes Hadoop libraries
# They are located in $SPARK_HOME/jars
# We just need to ensure Java and native compression libraries are available

RUN pip3 install --no-cache-dir --upgrade pip
RUN pip3 install --no-cache-dir --upgrade setuptools
RUN pip3 install --no-cache-dir --upgrade wheel
RUN pip3 install --no-cache-dir --upgrade pyspark
# Removing spark-xml pip install since it doesn't exist in PyPI
# Use --packages flag with spark-submit/pyspark instead to get spark-xml dependency
