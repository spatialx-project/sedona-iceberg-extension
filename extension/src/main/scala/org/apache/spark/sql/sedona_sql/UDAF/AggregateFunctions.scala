package org.apache.spark.sql.sedona_sql.UDAF

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}
import org.locationtech.jts.algorithm.Orientation
import org.locationtech.jts.geom._

/**
 * https://raw.githubusercontent.com/locationtech/jts/bd32d40e9f487b33d3d3ce8e60f943d994bc9eb6/modules/core/src/main/java/org/locationtech/jts/algorithm/Centroid.java
 */
case class CentroidBuffer(areaX: Double, areaY: Double, areasum: Double,
                          lineCentSumX: Double, lineCentSumY: Double, totalLength: Double,
                          ptCentSumX: Double, ptCentSumY:Double, ptCount: Int) {
  private var areaBasePt: Coordinate = null

  def add(geom: Geometry): CentroidBuffer = {
    if (geom.isEmpty) return this
    geom match {
      case _: Point =>
        addPoint(geom.getCoordinate)
      case _: LineString =>
        addLineSegments(geom.getCoordinates)
      case polygon: Polygon =>
        addPolygon(polygon)
      case gc: GeometryCollection =>
        var i = 0
        var centroid = this
        while (i < gc.getNumGeometries) {
          centroid = centroid.add(gc.getGeometryN(i))
          i += 1
        }
        centroid
      case _ =>
        throw new IllegalArgumentException("Unsupported geometry type: " + geom.getGeometryType)
    }
  }

  private def addPoint(pt: Coordinate): CentroidBuffer = {
    CentroidBuffer(areaX, areaY, areasum, lineCentSumX, lineCentSumY, totalLength, ptCentSumX + pt.x, ptCentSumY + pt.y, ptCount + 1)
  }

  private def addLineSegments(pts: Array[Coordinate]): CentroidBuffer = {
    var lineLen = 0.0
    var lnCentSumX = 0.0
    var lnCentSumY = 0.0
    var i = 0
    while (i < pts.length - 1) {
      val segmentLen = pts(i).distance(pts(i + 1))
      if (segmentLen != 0.0) {
        lineLen += segmentLen
        val midx = (pts(i).x + pts(i + 1).x) / 2
        lnCentSumX += segmentLen * midx
        val midy = (pts(i).y + pts(i + 1).y) / 2
        lnCentSumY += segmentLen * midy
      }
      i += 1
    }
    if (lineLen == 0.0 && pts.length > 0) addPoint(pts(0)) else
      CentroidBuffer(areaX, areaY, areasum, lineCentSumX + lnCentSumX, lineCentSumY + lnCentSumY, totalLength + lineLen, ptCentSumX, ptCentSumY, ptCount)
  }

  private def addPolygon(poly: Polygon): CentroidBuffer = {
    var centroid = addShell(poly.getExteriorRing.getCoordinates)
    var i = 0
    while (i < poly.getNumInteriorRing) {
      centroid = centroid.addHole(poly.getInteriorRingN(i).getCoordinates)
      i += 1
    }
    centroid
  }

  private def addHole(pts: Array[Coordinate]): CentroidBuffer = {
    val isPositiveArea: Boolean = Orientation.isCCW(pts)
    addTriangles(pts, isPositiveArea)
  }

  private def addShell(pts: Array[Coordinate]): CentroidBuffer = {
    if (pts.length > 0) {
      setAreaBasePoint(pts(0))
    }
    val isPositiveArea: Boolean = !(Orientation.isCCW(pts))
    addTriangles(pts, isPositiveArea)
  }

  private def addTriangles(pts: Array[Coordinate], isPositiveArea: Boolean): CentroidBuffer = {
    var i: Int = 0
    var centroid = this
    while (i < pts.length - 1) {
      centroid = centroid.addTriangle(areaBasePt, pts(i), pts(i + 1), isPositiveArea)
      i += 1
    }
    centroid
  }

  private def setAreaBasePoint(basePt: Coordinate): Unit = {
    this.areaBasePt = basePt
  }

  private def addTriangle(p0: Coordinate, p1: Coordinate, p2: Coordinate, isPositiveArea: Boolean): CentroidBuffer =  {
    val sign = if (isPositiveArea) 1.0 else -1.0;
    val x = p1.x + p2.x + p0.x
    val y = p1.y + p2.y + p0.y
    val area = area2(p0, p1, p2);
    CentroidBuffer(areaX + sign * area * x, areaY + sign * area * y, areasum + sign * area,
      lineCentSumX, lineCentSumY, totalLength, ptCentSumX, ptCentSumY, ptCount)
  }

  private def area2(p1: Coordinate, p2: Coordinate, p3: Coordinate) = (p2.x - p1.x) * (p3.y - p1.y) - (p3.x - p1.x) * (p2.y - p1.y)

  def getCentroid: Coordinate = {
    val cent = new Coordinate()
    if (areasum != 0) {
      cent.x = areaX / areasum / 3
      cent.y = areaY / areasum / 3
    } else if (totalLength != 0) {
      cent.x = lineCentSumX / totalLength
      cent.y = lineCentSumY / totalLength
    } else if (ptCount != 0) {
      cent.x = ptCentSumX / ptCount
      cent.y = ptCentSumY / ptCount
    } else {
      return null
    }
    cent
  }

}


class ST_CENTROID_AGGR extends Aggregator[Geometry, CentroidBuffer, Geometry] {
  private val factory = new GeometryFactory();
  private val serde: ExpressionEncoder[Geometry] = ExpressionEncoder[Geometry]()

  override def zero: CentroidBuffer = CentroidBuffer(0.0, 0.0, 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0)

  override def reduce(buffer: CentroidBuffer, geom: Geometry): CentroidBuffer = {
    buffer.add(geom)
  }

  override def merge(b1: CentroidBuffer, b2: CentroidBuffer): CentroidBuffer = {
    CentroidBuffer(b1.areaX + b2.areaX, b1.areaY + b2.areaY, b1.areasum + b2.areasum,
      b1.lineCentSumX + b2.lineCentSumX, b1.lineCentSumY + b2.lineCentSumY, b1.totalLength + b2.totalLength,
      b1.ptCentSumX + b2.ptCentSumX, b1.ptCentSumY + b2.ptCentSumY, b1.ptCount + b2.ptCount)
  }

  override def finish(reduction: CentroidBuffer): Geometry = {
    val coordinate = reduction.getCentroid
    factory.createPoint(coordinate)
  }

  override def bufferEncoder: Encoder[CentroidBuffer] = Encoders.product[CentroidBuffer]

  override def outputEncoder: ExpressionEncoder[Geometry] = serde
}
