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

package org.apache.spark.sql.iceberg

import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.expressions.{Expression => IcebergExpression}
import org.apache.iceberg.spark.source.SparkBatchQueryScan
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Not
import org.apache.spark.sql.catalyst.expressions.Or
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.utils.PlanUtils.isIcebergRelation
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.parseColumnPath
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.PushableColumn
import org.apache.spark.sql.execution.datasources.PushableColumnBase
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.iceberg.udt.GeometrySerializer
import org.apache.spark.sql.sedona_sql.expressions.ST_Contains
import org.apache.spark.sql.sedona_sql.expressions.ST_Crosses
import org.apache.spark.sql.sedona_sql.expressions.ST_Equals
import org.apache.spark.sql.sedona_sql.expressions.ST_Intersects
import org.apache.spark.sql.sedona_sql.expressions.ST_OrderingEquals
import org.apache.spark.sql.sedona_sql.expressions.ST_Overlaps
import org.apache.spark.sql.sedona_sql.expressions.ST_Touches
import org.apache.spark.sql.sedona_sql.expressions.ST_Within

/**
 * Push down spatial predicates defined by Apache Sedona to Iceberg scan relations. This optimization rules works
 * in almost the same way with [[org.apache.spark.sql.catalyst.optimizer.GeometryPredicatePushDown GeometryPredicatePushDown]].
 */
object SedonaPredicatePushDown extends Rule[LogicalPlan] with PredicateHelper {
  import scala.collection.JavaConverters._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, scanRel: DataSourceV2ScanRelation) if isIcebergRelation(scanRel.relation) =>
      val scan = scanRel.scan.asInstanceOf[SparkBatchQueryScan]
      val filters = splitConjunctivePredicates(condition)
      val normalizedFilters = DataSourceStrategy.normalizeExprs(filters, scanRel.output)
      val (_, normalizedFiltersWithoutSubquery) = normalizedFilters.partition(SubqueryExpression.hasSubquery)
      val icebergSpatialPredicates = filtersToIcebergSpatialPredicates(
        normalizedFiltersWithoutSubquery, nestedPredicatePushdownEnabled = true)
      val newScan = scan.withExpressions(icebergSpatialPredicates.asJava)
      if (newScan != scan) {
        Filter(condition, scanRel.copy(scan = newScan))
      } else {
        filter
      }
  }

  def filtersToIcebergSpatialPredicates(
    predicates: Seq[Expression],
    nestedPredicatePushdownEnabled: Boolean): Seq[IcebergExpression] = {
    val pushableColumn = PushableColumn(nestedPredicatePushdownEnabled)
    predicates.flatMap { predicate => translateToIcebergSpatialPredicate(predicate, pushableColumn) }
  }

  def translateToIcebergSpatialPredicate(
    predicate: Expression,
    pushableColumn: PushableColumnBase): Option[IcebergExpression] = {
    predicate match {
      case And(left, right) =>
        for {
          icebergLeft <- translateToIcebergSpatialPredicate(left, pushableColumn)
          icebergRight <- translateToIcebergSpatialPredicate(right, pushableColumn)
        } yield Expressions.and(icebergLeft, icebergRight)

      case Or(left, right) =>
        for {
          icebergLeft <- translateToIcebergSpatialPredicate(left, pushableColumn)
          icebergRight <- translateToIcebergSpatialPredicate(right, pushableColumn)
        } yield Expressions.or(icebergLeft, icebergRight)

      case Not(innerPredicate) =>
        translateToIcebergSpatialPredicate(innerPredicate, pushableColumn).map(Expressions.not)

      case ST_Contains(Seq(pushableColumn(name), Literal(v, _))) =>
        Some(Expressions.stCovers(unquote(name), GeometrySerializer.deserialize(v)))

      case ST_Contains(Seq(Literal(v, _), pushableColumn(name))) =>
        Some(Expressions.stCoveredBy(unquote(name), GeometrySerializer.deserialize(v)))

      case ST_Within(Seq(pushableColumn(name), Literal(v, _))) =>
        Some(Expressions.stCoveredBy(unquote(name), GeometrySerializer.deserialize(v)))

      case ST_Within(Seq(Literal(v, _), pushableColumn(name))) =>
        Some(Expressions.stCovers(unquote(name), GeometrySerializer.deserialize(v)))

      case ST_Equals(_) | ST_OrderingEquals(_) =>
        for ((name, value) <- resolveNameAndLiteral(predicate.children, pushableColumn))
          yield Expressions.stCovers(unquote(name), GeometrySerializer.deserialize(value))

      case ST_Intersects(_) | ST_Crosses(_) | ST_Overlaps(_) | ST_Touches(_) =>
        for ((name, value) <- resolveNameAndLiteral(predicate.children, pushableColumn))
          yield Expressions.stIntersects(unquote(name), GeometrySerializer.deserialize(value))

      case _ => None
    }
  }

  private def unquote(name: String): String = {
    parseColumnPath(name).mkString(".")
  }

  private def resolveNameAndLiteral(expressions: Seq[Expression], pushableColumn: PushableColumnBase): Option[(String, Any)] = {
    expressions match {
      case Seq(pushableColumn(name), Literal(v, _)) => Some(name, v)
      case Seq(Literal(v, _), pushableColumn(name)) => Some(name, v)
      case _ => None
    }
  }
}
