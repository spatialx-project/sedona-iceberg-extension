# Sedona-Iceberg Extension

This extension module is for using Apache Sedona seamlessly with Apache Iceberg,
where UDT and serializer of geometry values were unified, and spatial
predicates of Apache Sedona will be pushed down to Iceberg tables for partition
pruning and data skipping.

## Usage

Add the sedona-iceberg extension jar to the `--jars` argument of `spark-submit`
command, and append
`org.apache.iceberg.spark.extensions.SedonaIcebergExtensions` to
`spark.sql.extensions` config property.

Typical spark job submission script looks like this:

```
spark-submit \
    --jars /path/to/iceberg-spark-runtime-jar,/path/to/sedona-iceberg-extension-jar,/path/to/geotools-wrapper-geotools-jar \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryo.registrator=org.apache.sedona.core.serde.SedonaKryoRegistrator \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.iceberg.spark.extensions.SedonaIcebergExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hive \
    ...

```

Notices:

1. Don't forget to register kryo serializers provided by Apache Sedona,
   otherwise you'll suffer from poor performance and high memory usage.

2. Since GeoTools was published under GPL license, so we cannot bundle GeoTools
   into our extension jar. You need to obtain and add GeoTools jar
   yourself. Please refer to [sedona documentation on
   GeoTools](https://sedona.apache.org/latest-snapshot/setup/maven-coordinates/#geotools-240)
   for detail.

## Example

`example` directory contains an example spark job processing geometries stored
in iceberg tables using Apache Sedona. Please refer to `example/launch.sh` for
spark-submit command for launching jobs.
