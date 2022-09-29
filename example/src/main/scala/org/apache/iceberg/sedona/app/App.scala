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

package org.apache.iceberg.sedona.app

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

object App {
  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      throw new IllegalArgumentException("Please provide a table name prefix in form of <catalog>.<database>")
    }
    val prefix = args(0)
    val spark = SparkSession.builder()
      .config(SQLConf.PARTITION_OVERWRITE_MODE.key, "dynamic")
      .getOrCreate()

    // Call sedona UDF to construct geometry values
    spark.sql("SELECT ST_PolygonFromEnvelope(15.0, 15.0, 30.0, 30.0)").show()

    // Prepare test data
    val table0 = prefix + ".sedona_iceberg_extension_test_0"
    val table1 = prefix + ".sedona_iceberg_extension_test_1"
    val table2 = prefix + ".sedona_iceberg_extension_test_2"
    spark.sql(s"DROP TABLE IF EXISTS $table0")
    spark.sql(s"CREATE TABLE $table0 (id INT, data STRING, geo GEOMETRY) USING ICEBERG")

    // Writing to iceberg table using both geometries constructed using iceberg UDF and sedona UDF
    spark.sql(s"INSERT INTO $table0 VALUES (1, 'data_1', IcebergSTGeomFromText('POINT (10 10)'))")
    spark.sql(s"INSERT INTO $table0 SELECT 2, 'data_2', ST_Point(20.0, 20.0)")
    spark.sql(s"DROP TABLE IF EXISTS $table1")
    spark.sql(s"CREATE TABLE $table1 (id INT, geo GEOMETRY) USING ICEBERG")
    spark.sql(s"INSERT INTO $table1 VALUES " +
      "(10, IcebergSTGeomFromText('POLYGON ((0 0, 15 0, 15 15, 0 15, 0 0))'))," +
      "(20, IcebergSTGeomFromText('POLYGON ((15 15, 25 15, 25 25, 15 25, 15 15))'))")

    // Range query
    spark.sql(s"SELECT id, ST_AsText(geo) FROM $table0 WHERE " +
      "ST_Contains(ST_PolygonFromEnvelope(15.0, 15.0, 30.0, 30.0), geo)").show()

    // Aggregation
    spark.sql(s"SELECT ST_Union_Aggr(geo) FROM $table0").show()

    // Spatial join
    spark.sql(s"SELECT * FROM $table0 l, $table1 r WHERE ST_Contains(r.geo, l.geo)").show()
    spark.sql(s"SELECT * FROM $table0 l, $table1 r WHERE ST_Contains(r.geo, l.geo) AND ST_Contains(ST_PolygonFromEnvelope(15.0, 15.0, 30.0, 30.0), l.geo)").show()

    // Run UPDATE with spatial predicate as condition
    spark.sql(s"UPDATE $table0 SET data = 'updated' WHERE ST_Contains(ST_PolygonFromEnvelope(15.0, 15.0, 30.0, 30.0), geo)")
    spark.sql(s"SELECT * FROM $table0").show()

    // Cache table before running spatial join
    spark.sql(s"CACHE TABLE $table0")
    spark.sql(s"CACHE TABLE $table1")
    spark.sql(s"SELECT * FROM $table0 l, $table1 r WHERE ST_Contains(r.geo, l.geo)").show()
    spark.sql(s"SELECT * FROM $table0 l, $table1 r WHERE ST_Contains(r.geo, l.geo) AND ST_Contains(ST_PolygonFromEnvelope(15.0, 15.0, 30.0, 30.0), l.geo)").show()

    // CTAS + spatial join
    spark.sql(s"DROP TABLE IF EXISTS $table2")
    spark.sql(s"CREATE TABLE $table2 USING ICEBERG AS SELECT l.id id, r.geo geo FROM $table0 l, $table1 r WHERE ST_Contains(r.geo, l.geo)")
    spark.sql(s"SELECT * FROM $table2").show()
  }
}
