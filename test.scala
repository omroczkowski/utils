import org.scalatest.FeatureSpecLike
import org.scalatest.GivenWhenThen
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.company.testing.{DataFrameTestHelper, SparkSessionTestBase} // adjust package
import your.package.IssVotingReport // replace with actual package

class IssVotingReportTest
  extends DataFrameTestHelper
    with FeatureSpecLike
    with GivenWhenThen
    with Matchers
    with SparkSessionTestBase {

  scenario("black box test: should clean KPI columns and cast to double") {

    Given("sample voting report input with commas and whitespace")
    val result = IssVotingReportTest.runJob(spark)

    Then("we get cleaned and casted output")
    assertDataFramesEquals(IssVotingReportTest.expected, result)
  }
}

object IssVotingReportTest extends SparkSessionTestBase {
  import spark.implicits._

  val date = "2025-08-04"

  val inputDf: DataFrame = List(
    ("entity1", " 1,000.5 ", " 2,500 ", date),
    ("entity2", "3,200.75", " 4,100", date)
  ).toDF("entity_id", "kpi1", "kpi2", "ingestion_date")

  val inputs: Map[String, DataFrame] = Map(
    "voting_report" -> inputDf
  )

  val expected: DataFrame = List(
    ("entity1", 1000.5, 2500.0, date),
    ("entity2", 3200.75, 4100.0, date)
  ).toDF("entity_id", "kpi1", "kpi2", "ingestion_date")

  def runJob(spark: SparkSession): DataFrame = {
    val job = new IssVotingReport(Map.empty)(spark)
    job.run(inputs)("voting_report")
  }
}
