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
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.junit.jupiter.api.Test
import org.apache.iceberg.spark.extensions.SedonaIcebergExtensions
import org.apache.spark.sql.internal.SQLConf
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.ClassOrderer.Random
import org.junit.jupiter.api.io.TempDir
import org.locationtech.jts.geom.{Geometry, GeometryFactory}
import org.locationtech.jts.io.WKTReader

import java.nio.file.Path

class TestSedonaIcebergExtension {
  @TempDir var tempDir: Path = _

  lazy val spark = SparkSession.builder
    .withExtensions(new IcebergSparkSessionExtensions)
    .withExtensions(new SedonaIcebergExtensions)
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
    spark.sql("CREATE TABLE demo.db.test_geom (id INT, data STRING, geo GEOMETRY) USING ICEBERG")
    spark.sql("INSERT INTO demo.db.test_geom VALUES (1, 'data_1', IcebergSTGeomFromText('POINT (10 20)'))")
    spark.sql("INSERT INTO demo.db.test_geom SELECT 2, 'data_2', ST_Point(20.0, 30.0)")
    val allRows = spark.sql("SELECT ST_AsText(geo) FROM demo.db.test_geom ORDER BY id").collect()
    Assertions.assertEquals(2, allRows.length)
    Assertions.assertEquals("POINT (10 20)", allRows(0)(0))
    Assertions.assertEquals("POINT (20 30)", allRows(1)(0))

    // Run ST_Contains query, selecting all columns
    var df = spark.sql("SELECT * FROM demo.db.test_geom WHERE ST_Contains(ST_PolygonFromEnvelope(15.0, 20.0, 30.0, 40.0), geo)")
    var executedPlanPattern = ".*BatchScan.*st_coveredBy\\(geo.*".r
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

    // Run UPDATE with ST_Intersects as condition
    df = spark.sql("UPDATE demo.db.test_geom SET data = 'updated' WHERE ST_Intersects(geo, ST_PolygonFromEnvelope(15.0, 20.0, 30.0, 40.0))")
    executedPlan = df.queryExecution.executedPlan.toString
    Assertions.assertTrue(executedPlanPattern.findFirstMatchIn(executedPlan).isDefined)
    df = spark.sql("SELECT * FROM demo.db.test_geom WHERE data = 'updated'")
    Assertions.assertEquals(1, df.count())
    Assertions.assertEquals(2, spark.table("demo.db.test_geom").count())

    // Test UDAF
    partialRows = spark.sql("SELECT ST_AsText(ST_Union_Aggr(geo)) FROM demo.db.test_geom").collect()
    Assertions.assertEquals(1, partialRows.length)
    Assertions.assertEquals("MULTIPOINT ((10 20), (20 30))", partialRows(0)(0))

    // Runtime insert using Sedona UDF
    spark.sql("INSERT INTO demo.db.test_geom SELECT 3, 'data_3', ST_Point(RAND(), RAND())")
    Assertions.assertEquals(3L, spark.sql("SELECT * FROM demo.db.test_geom").count())

    // Test sedona constructor and function
    partialRows = spark.sql("SELECT id + 100, CONCAT('new_  ', data), ST_Point(y, x) " +
      "FROM (SELECT id, data, ST_X(geo) AS x, ST_Y(geo) AS y FROM demo.db.test_geom)").collect()
    Assertions.assertEquals(3, partialRows.length)
    spark.sql("INSERT INTO demo.db.test_geom " +
      "SELECT id + 100, CONCAT('new_  ', data), ST_Point(y, x) " +
      "FROM (SELECT id, data, ST_X(geo) AS x, ST_Y(geo) AS y FROM demo.db.test_geom)")
    Assertions.assertEquals(6L, spark.sql("SELECT * FROM demo.db.test_geom").count())
  }

  @Test def testSpatialJoin(): Unit = {
    spark.sql("CREATE TABLE demo.db.test_left (id INT, data STRING, geo GEOMETRY) USING ICEBERG")
    spark.sql("INSERT INTO demo.db.test_left VALUES " +
      "(1, 'data_1', IcebergSTGeomFromText('POINT (10 10)'))," +
      "(2, 'data_2', IcebergSTGeomFromText('POINT (20 20)'))")
    spark.sql("CREATE TABLE demo.db.test_right(id INT, geo GEOMETRY) USING ICEBERG")
    spark.sql("INSERT INTO demo.db.test_right SELECT 10, ST_PolygonFromEnvelope(0.0, 0.0, 15.0, 15.0)")
    spark.sql("INSERT INTO demo.db.test_right SELECT 20, ST_PolygonFromEnvelope(15.0, 15.0, 25.0, 25.0)")

    var df = spark.sql("SELECT l.id id_l, r.id id_r " +
      "FROM demo.db.test_left l JOIN demo.db.test_right r " +
      "ON ST_Contains(r.geo, l.geo) " +
      "ORDER BY id_l")
    var executedPlanStr = df.queryExecution.executedPlan.toString
    Assertions.assertTrue((executedPlanStr contains "BroadcastIndexJoin") || (executedPlanStr contains "RangeJoin"))
    var rows = df.collect()
    Assertions.assertEquals(1, rows(0)(0))
    Assertions.assertEquals(10, rows(0)(1))
    Assertions.assertEquals(2, rows(1)(0))
    Assertions.assertEquals(20, rows(1)(1))

    // Caching tables before join
    spark.sql("CACHE TABLE demo.db.test_left")
    spark.sql("CACHE TABLE demo.db.test_right")
    df = spark.sql("SELECT l.id id_l, r.id id_r " +
      "FROM demo.db.test_left l JOIN demo.db.test_right r " +
      "ON ST_Contains(r.geo, l.geo) " +
      "ORDER BY id_l")
    executedPlanStr = df.queryExecution.executedPlan.toString
    Assertions.assertTrue((executedPlanStr contains "BroadcastIndexJoin") || (executedPlanStr contains "RangeJoin"))
    rows = df.collect()
    Assertions.assertEquals(1, rows(0)(0))
    Assertions.assertEquals(10, rows(0)(1))
    Assertions.assertEquals(2, rows(1)(0))
    Assertions.assertEquals(20, rows(1)(1))
  }

  @Test def testStCentroidAggr(): Unit = {
    // test data is generated by chatGPT
    val path = "src/test/resources/test.csv"
    val df: Dataset[Row] = spark.read.format("csv").option("header", "true").load(path)
    val rows = df.select("geom").collect()
    val reader = new WKTReader()
    val geoms = rows.map(row => reader.read(row.getAs[String](0)))
    val factory = new GeometryFactory()
    val centroid = factory.createGeometryCollection(geoms).getCentroid

    df.createOrReplaceTempView("df")
    val result = spark.sql("SELECT ST_CENTROID_AGGR(ST_GeomFromText(geom)) FROM df").collect()
    assert(result.length == 1)
    assert(result(0).getAs[Geometry](0).equals(centroid))
  }
}
