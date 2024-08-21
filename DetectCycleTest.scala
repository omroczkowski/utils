import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession

class DetectCycleTest extends FunSuite {

  // Initialize Spark session
  val spark: SparkSession = SparkSession.builder
    .appName("GraphXCycleDetectionTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Instance of DetectCycle
  val cycleDetector = new DetectCycle(spark)

  test("Test acyclic graph") {
    // Example DataFrame with no cycles
    val data = Seq(
      (1, 0),
      (2, 1),
      (3, 2),
      (4, 3)
    )
    val df = data.toDF("id", "parent_id")

    assert(!cycleDetector.detectCycle(df, "id", "parent_id"))
  }

  test("Test cyclic graph") {
    // Example DataFrame with a cycle
    val data = Seq(
      (1, 0),
      (2, 1),
      (3, 2),
      (4, 3),
      (1, 4)  // Creates a cycle
    )
    val df = data.toDF("id", "parent_id")

    assert(cycleDetector.detectCycle(df, "id", "parent_id"))
  }

  test("Test graph with extra columns but no cycle") {
    // Example DataFrame with extra columns and no cycle
    val data = Seq(
      (1, 0, "A", 100),
      (2, 1, "B", 200),
      (3, 2, "C", 300),
      (4, 3, "D", 400)
    )
    val df = data.toDF("id", "parent_id", "extra1", "extra2")

    assert(!cycleDetector.detectCycle(df, "id", "parent_id"))
  }

  test("Test graph with extra columns and a cycle") {
    // Example DataFrame with extra columns and a cycle
    val data = Seq(
      (1, 0, "A", 100),
      (2, 1, "B", 200),
      (3, 2, "C", 300),
      (4, 3, "D", 400),
      (1, 4, "E", 500)  // Creates a cycle
    )
    val df = data.toDF("id", "parent_id", "extra1", "extra2")

    assert(cycleDetector.detectCycle(df, "id", "parent_id"))
  }

  test("Test graph with custom column names and no cycle") {
    // Example DataFrame with custom column names and no cycle
    val data = Seq(
      (1, 0),
      (2, 1),
      (3, 2),
      (4, 3)
    )
    val df = data.toDF("node_id", "parent_node_id")

    assert(!cycleDetector.detectCycle(df, "node_id", "parent_node_id"))
  }

  test("Test graph with custom column names and a cycle") {
    // Example DataFrame with custom column names and a cycle
    val data = Seq(
      (1, 0),
      (2, 1),
      (3, 2),
      (4, 3),
      (1, 4)  // Creates a cycle
    )
    val df = data.toDF("node_id", "parent_node_id")

    assert(cycleDetector.detectCycle(df, "node_id", "parent_node_id"))
  }
}
