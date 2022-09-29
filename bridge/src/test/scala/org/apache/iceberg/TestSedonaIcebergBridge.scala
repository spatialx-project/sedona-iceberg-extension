/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg

import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Test
import org.apache.iceberg.spark.extensions.SedonaIcebergBridgeExtensions
import org.apache.spark.sql.internal.SQLConf
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.io.TempDir

import java.nio.file.Path

class TestSedonaIcebergBridge {
  @TempDir var tempDir: Path = _

  lazy val spark = SparkSession.builder
    .withExtensions(new IcebergSparkSessionExtensions)
    .withExtensions(new SedonaIcebergBridgeExtensions)
    .master("local[2]")
    .config("spark.testing", "true")
    .config(SQLConf.PARTITION_OVERWRITE_MODE.key, "dynamic")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hive")
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.demo.type", "hadoop")
    .config("spark.sql.catalog.demo.warehouse", tempDir.toString)
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()

  @Test def testBasic(): Unit = {
    spark.sql("CREATE TABLE demo.db.test_geom (id INT, data STRING, geo GEOMETRY)")
    spark.sql("INSERT INTO demo.db.test_geom VALUES " +
      "(1, 'data_1', IcebergSTGeomFromText('POINT (10 20)'))," +
      "(2, 'data_2', IcebergSTGeomFromText('POINT (20 30)'))")
    val allRows = spark.sql("SELECT ST_AsText(geo) FROM demo.db.test_geom ORDER BY id").collect()
    Assertions.assertEquals(2, allRows.length)
    Assertions.assertEquals("POINT (10 20)", allRows(0)(0))
    Assertions.assertEquals("POINT (20 30)", allRows(1)(0))

    // Run ST_Contains query, selecting all columns
    var df = spark.sql("SELECT * FROM demo.db.test_geom WHERE ST_Contains(ST_PolygonFromEnvelope(15.0, 20.0, 30.0, 40.0), geo)")
    var executedPlanPattern = ".*BatchScan.*st_within\\(geo.*".r
    var executedPlan = df.queryExecution.executedPlan.toString
    Assertions.assertTrue(executedPlanPattern.findFirstMatchIn(executedPlan).isDefined)
    var partialRows = df.collect()
    Assertions.assertEquals(1, partialRows.length)
    Assertions.assertEquals(2, partialRows(0)(0))
    Assertions.assertEquals("data_2", partialRows(0)(1))
    Assertions.assertEquals("POINT (20 30)", partialRows(0)(2).toString)

    // Run ST_Intersects query, selecting geo column with transformation
    df = spark.sql("SELECT ST_AsText(geo) FROM demo.db.test_geom WHERE ST_Intersects(geo, ST_PolygonFromEnvelope(15.0, 20.0, 30.0, 40.0))")
    executedPlanPattern = ".*BatchScan.*st_intersects\\(geo.*".r
    executedPlan = df.queryExecution.executedPlan.toString
    Assertions.assertTrue(executedPlanPattern.findFirstMatchIn(executedPlan).isDefined)
    partialRows = df.collect()
    Assertions.assertEquals(1, partialRows.length)
    Assertions.assertEquals("POINT (20 30)", partialRows(0)(0))

    // Test UDAF
    partialRows = spark.sql("SELECT ST_AsText(ST_Union_Aggr(geo)) FROM demo.db.test_geom").collect()
    Assertions.assertEquals(1, partialRows.length)
    Assertions.assertEquals("MULTIPOINT ((10 20), (20 30))", partialRows(0)(0))
  }

  @Test def testSpatialJoin(): Unit = {
    spark.sql("CREATE TABLE demo.db.test_left (id INT, data STRING, geo GEOMETRY)")
    spark.sql("INSERT INTO demo.db.test_left VALUES " +
      "(1, 'data_1', IcebergSTGeomFromText('POINT (10 10)'))," +
      "(2, 'data_2', IcebergSTGeomFromText('POINT (20 20)'))")
    spark.sql("CREATE TABLE demo.db.test_right(id INT, geo GEOMETRY)")
    spark.sql("INSERT INTO demo.db.test_right VALUES" +
      "(10, IcebergSTGeomFromText('POLYGON ((0 0, 15 0, 15 15, 0 15, 0 0))'))," +
      "(20, IcebergSTGeomFromText('POLYGON ((15 15, 25 15, 25 25, 15 25, 15 15))'))")

    val df = spark.sql("SELECT l.id id_l, r.id id_r " +
      "FROM demo.db.test_left l JOIN demo.db.test_right r " +
      "ON ST_Contains(r.geo, l.geo) " +
      "ORDER BY id_l")
    Assertions.assertTrue(df.queryExecution.executedPlan.toString contains "RangeJoin")
    val rows = df.collect()
    Assertions.assertEquals(1, rows(0)(0));
    Assertions.assertEquals(10, rows(0)(1));
    Assertions.assertEquals(2, rows(1)(0));
    Assertions.assertEquals(20, rows(1)(1));
  }
}
