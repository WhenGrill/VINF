Seleng uses Spark 2.12


Running Spark with XML package



```bash
pyspark --packages com.databricks:spark-xml_2.12:0.18.0
```


Set Spark to Python 3 as Spark uses Python 2 by default

```bash
export PYSPARK_PYTHON=python3
```


Run all Spark jobs

```bash
spark-submit --packages com.databricks:spark-xml_2.12:0.18.0 <spark-job>.py
```


Use `.bz2` files for Spark - but there is overhead with decompression


METADATA file - `_SUCCESS` - if this file is not present, job did not run successfully


Remove media-wiki tags, etc. from XML files - INFOBOX - good, delete references, startswith and endswith `ref`, Categories, 



#### RANDOM

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --master local[*] --deploy-mode client --name spark-xml-kafka spark-xml-kafka.py
```


### How to make it faster

Define template - 