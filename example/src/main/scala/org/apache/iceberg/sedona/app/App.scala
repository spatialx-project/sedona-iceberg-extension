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
    val prefix = args(0)
    val spark = SparkSession.builder()
      .config(SQLConf.PARTITION_OVERWRITE_MODE.key, "dynamic")
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions," +
        "org.apache.iceberg.spark.extensions.SedonaIcebergBridgeExtensions")
      .getOrCreate()
    val table0 = prefix + ".sedona_iceberg_bridge_test_0"
    val table1 = prefix + ".sedona_iceberg_bridge_test_1"
    spark.sql(s"DROP TABLE IF EXISTS $table0")
    spark.sql(s"CREATE TABLE $table0 (id INT, data STRING, geo GEOMETRY)")
    spark.sql(s"INSERT INTO $table0 VALUES " +
      "(1, 'data_1', IcebergSTGeomFromText('POINT (10 10)'))," +
      "(2, 'data_2', IcebergSTGeomFromText('POINT (20 20)'))")
    spark.sql(s"DROP TABLE IF EXISTS $table1")
    spark.sql(s"CREATE TABLE $table1 (id INT, geo GEOMETRY)")
    spark.sql(s"INSERT INTO $table1 VALUES " +
      "(10, IcebergSTGeomFromText('POLYGON ((0 0, 15 0, 15 15, 0 15, 0 0))'))," +
      "(20, IcebergSTGeomFromText('POLYGON ((15 15, 25 15, 25 25, 15 25, 15 15))'))")
    spark.sql(s"SELECT id, ST_AsText(geo) FROM $table0 WHERE " +
      "ST_Contains(ST_PolygonFromEnvelope(15.0, 15.0, 30.0, 30.0), geo)").show()
    spark.sql(s"SELECT ST_Union_Aggr(geo) FROM $table0").show()
    spark.sql(s"SELECT * FROM $table0 l, $table1 r WHERE ST_Contains(r.geo, l.geo)").show()
  }
}
