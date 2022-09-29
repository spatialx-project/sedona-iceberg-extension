#!/usr/bin/env bash

export SPARK_VERSION="${SPARK_VERSION:-3.3}"

export ICEBERG_SPARK_RUNTIME_JAR="/path/to/iceberg-spark-runtime-jar"
export SEDONA_ICEBERG_EXTENSION_JAR="/path/to/sedona-iceberg-extension-jar"
export GEOTOOLS_JAR="/path/to/geotools-wrapper-geotools-24.0.jar"
export MASTER="${MASTER:-local[*]}"

spark-submit \
    --master $MASTER \
    --jars $ICEBERG_SPARK_RUNTIME_JAR,$SEDONA_ICEBERG_EXTENSION_JAR,$GEOTOOLS_JAR \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryo.registrator=org.apache.sedona.core.serde.SedonaKryoRegistrator \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.iceberg.spark.extensions.SedonaIcebergExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.demo.type=hadoop \
    --conf spark.sql.catalog.demo.warehouse=$(pwd)/iceberg-warehouse \
    --class org.apache.iceberg.sedona.app.App \
    build/libs/sedona-iceberg-example-${SPARK_VERSION}*.jar demo.db
