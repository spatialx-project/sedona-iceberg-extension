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

package org.apache.iceberg.spark.extensions

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.iceberg.SedonaPredicatePushDown
import org.apache.spark.sql.sedona_sql.UDF.SedonaExpressionsRegistrator
import org.apache.spark.sql.sedona_sql.UDT.UdtRegistratorWrapper
import org.apache.spark.sql.sedona_sql.strategy.join.JoinQueryDetector

/**
 * Sedona iceberg extension initializes Apache Sedona extension, so user don't
 * need to specify sedona extension class in `spark.sql.extensions` when
 * creating spark session.
 */
class SedonaIcebergExtensions extends (SparkSessionExtensions => Unit)  {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Register UDTs, UDFs and UDAFs provided by Apache Sedona. We uses the post-spark-3 approach
    // instead of calling SedonaSQLRegistrator.registerAll in extensions.injectCheckRule, since
    // the function passed to injectCheckRule may be invoked multiple times and causes redundant
    // registrations.
    UdtRegistratorWrapper.registerAll()
    SedonaExpressionsRegistrator.registerFunctions(extensions)
    extensions.injectCheckRule(spark => {
      if (!spark.experimental.extraOptimizations.contains(SedonaPredicatePushDown)) {
        spark.experimental.extraOptimizations ++= Seq(SedonaPredicatePushDown)
      }
      _ => ()
    })
    extensions.injectPlannerStrategy(spark => new JoinQueryDetector(spark))
  }
}
