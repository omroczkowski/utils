import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class DetectCycle(spark: SparkSession, df: DataFrame, idColumn: String, parentIdColumn: String) {

  // Create a Graph from a DataFrame with parameterized id and parent_id columns
def createGraphFromDataFrame(df: DataFrame, idColumn: String, parentIdColumn: String): Graph[Int, Int] = {
    val vertices: RDD[(VertexId, Int)] = df
      .select(col(idColumn).cast("long"))
      .distinct()
      .rdd
      .map(row => (row.getLong(0), row.getLong(0).toInt))

    val edges: RDD[Edge[Int]] = df
      .select(col(idColumn).cast("long"), col(parentIdColumn).cast("long"))
      .rdd
      .map(row => Edge(row.getLong(1), row.getLong(0), 1))

    Graph(vertices, edges)
  }

  def getCyclicRows(df: DataFrame, idColumn: String, parentIdColumn: String): DataFrame = {
    val graph = createGraphFromDataFrame(df, idColumn, parentIdColumn)

    // Compute Strongly Connected Components
    val sccGraph: Graph[VertexId, Int] = graph.stronglyConnectedComponents(Int.MaxValue)

    // Filter the SCCs that have more than one vertex (indicating a cycle)
    val cyclicVertices: RDD[VertexId] = sccGraph.vertices
      .map { case (vertexId, componentId) => (componentId, vertexId) }
      .groupByKey()
      .filter { case (_, vertices) => vertices.size > 1 }
      .flatMap { case (_, vertices) => vertices }

    // Convert the cyclicVertices RDD to a DataFrame
    import spark.implicits._
    val cyclicVerticesDF = cyclicVertices.toDF(idColumn)

    // Join the original DataFrame with the cyclic vertices to filter out rows that form cycles
    val cyclicRowsDF = df.join(cyclicVerticesDF, df(idColumn) === cyclicVerticesDF(idColumn))

    cyclicRowsDF
  }
}
