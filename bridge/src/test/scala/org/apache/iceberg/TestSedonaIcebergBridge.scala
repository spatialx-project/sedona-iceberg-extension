package org.apache.iceberg

import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Test
import org.apache.iceberg.spark.extensions.SedonaIcebergBridgeExtensions
import org.apache.spark.sql.internal.SQLConf

class TestSedonaIcebergBridge {
  @Test def basicTest(): Unit = {
    val spark = SparkSession.builder
      .withExtensions(new IcebergSparkSessionExtensions)
      .withExtensions(new SedonaIcebergBridgeExtensions)
      .master("local[2]")
      .config("spark.testing", "true")
      .config(SQLConf.PARTITION_OVERWRITE_MODE.key, "dynamic")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.demo.type", "hadoop")
      .config("spark.sql.catalog.demo.warehouse", "/home/kontinuation/local/iceberg/warehouse")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sql("DROP TABLE IF EXISTS demo.db.test_geom");
    spark.sql("CREATE TABLE demo.db.test_geom (id INT, data STRING, geo GEOMETRY)");
    spark.sql("INSERT INTO demo.db.test_geom VALUES " +
      "(1, 'data_1', IcebergSTGeomFromText('POINT (10 20)'))," +
      "(2, 'data_2', IcebergSTGeomFromText('POINT (20 30)'))")
    spark.sql("SELECT * FROM demo.db.test_geom WHERE ST_Contains(ST_PolygonFromEnvelope(15.0, 20.0, 30.0, 40.0), geo)").show();
  }
}
