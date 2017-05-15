package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {


  lazy val timeUsage = TimeUsage
  lazy val (columns, initDf) = timeUsage.read("/timeusage/atussum.csv")
  lazy val (primaryNeedsColumns, workColumns, otherColumns) = timeUsage.classifiedColumns(columns)
  lazy val summaryDf:DataFrame = timeUsage.timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
  lazy val finalDf:DataFrame = timeUsage.timeUsageGrouped(summaryDf)

  lazy val sqlDf = timeUsage.timeUsageGroupedSql(summaryDf)
  lazy val summaryDs = timeUsage.timeUsageSummaryTyped(summaryDf)
  lazy val finalDs = timeUsage.timeUsageGroupedTyped(summaryDs)


  test("timeUsage") {
    assert(timeUsage.spark.sparkContext.appName === "Time Usage")
    assert(timeUsage.spark.sparkContext.isStopped === false)
  }

  test("dfSchema"){
    val testSchema = timeUsage.dfSchema(List("fieldA", "fieldB"))

    assert(testSchema.fields(0).name === "fieldA")
    assert(testSchema.fields(0).dataType === StringType)
    assert(testSchema.fields(1).name === "fieldB")
    assert(testSchema.fields(1).dataType === DoubleType)
  }

  test("row"){
    val testRow = timeUsage.row(List("fieldA", "0.3", "1"))

    assert(testRow(0).getClass.getName === "java.lang.String")
    assert(testRow(1).getClass.getName === "java.lang.Double")
    assert(testRow(2).getClass.getName === "java.lang.Double")
  }

  test("read") {
    assert(columns.size === 455)
    assert(initDf.count === 170842)
    initDf.show()
  }

  test("classifiedColumns") {
    val pnC = primaryNeedsColumns.map(_.toString)
    val wC = workColumns.map(_.toString)
    val oC = otherColumns.map(_.toString)


    assert(pnC.contains("t010199"))
    assert(pnC.contains("t030501"))
    assert(pnC.contains("t110101"))
    assert(pnC.contains("t180382"))
    assert(wC.contains("t050103"))
    assert(wC.contains("t180589"))
    assert(oC.contains("t020101"))
    assert(oC.contains("t180699"))

  }

  test("timeUsageSummary"){
    assert(summaryDf.columns.length === 6)
    assert(summaryDf.count === 114997)
    summaryDf.show()
  }


  test("timeUsageGrouped"){
    assert(finalDf.count === 2*2*3)
    assert(finalDf.head.getDouble(3) === 12.4)
    finalDf.show()
  }

  test("timeUsageGroupedSql"){
    assert(sqlDf.count === 2*2*3)
    assert(sqlDf.head.getDouble(3) === 12.4)
    sqlDf.show()
  }

  test("timeUsageSummaryTyped"){
    assert(summaryDs.head.getClass.getName === "timeusage.TimeUsageRow")
    assert(summaryDs.head.other === 8.75)
    assert(summaryDs.count === 114997)
    summaryDs.show()
  }

  test("timeUsageGroupedTyped"){
    assert(finalDs.count === 2*2*3)
    assert(finalDs.head.primaryNeeds === 12.4)
  }


}
