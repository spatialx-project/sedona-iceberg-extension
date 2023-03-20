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

package org.apache.spark.sql.sedona_sql.UDF

import org.apache.sedona.sql.UDF.Catalog
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.expressions.UserDefinedAggregator
import org.apache.spark.sql.functions

/**
 * Register sedona UDFs and UDAFs using the post-spark-3 approach.
 */
object SedonaExpressionsRegistrator {
  def registerFunctions(extensions: SparkSessionExtensions): Unit = {
    Catalog.expressions.foreach { case (functionIdentifier, expressionInfo, functionBuilder) =>
      extensions.injectFunction(functionIdentifier, expressionInfo, functionBuilder)
    }
    Catalog.aggregateExpressions.foreach { f =>
      val functionIdentifier = FunctionIdentifier(f.getClass.getSimpleName)
      val expressionInfo = new ExpressionInfo(
        f.getClass.getCanonicalName,
        functionIdentifier.database.orNull,
        functionIdentifier.funcName)
      val udaf = functions.udaf(f).asInstanceOf[UserDefinedAggregator[_, _, _]]
      extensions.injectFunction(functionIdentifier, expressionInfo, udaf.scalaAggregator)
    }
  }
}
