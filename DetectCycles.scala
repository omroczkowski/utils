import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class DetectCycle(spark: SparkSession, df: DataFrame, idColumn: String, parentIdColumn: String) {

  import spark.implicits._

  def createGraph(): Graph[Int, Int] = {
    val edgesRDD: RDD[Edge[Int]] = df.select(idColumn, parentIdColumn)
      .rdd
      .map(row => (row.getLong(0), row.getLong(1)))
      .map { case (src, dst) => Edge(src, dst, 1) }

    val verticesRDD: RDD[(VertexId, Long)] = df.select(idColumn).distinct()
      .rdd
      .map(row => (row.getLong(0), row.getLong(0)))

    Graph(verticesRDD, edgesRDD)
  }

  // Create a Graph from a DataFrame with id and parent_id columns
  def createGraphFromDataFrame(df: DataFrame): Graph[Int, Int] = {
    // Convert DataFrame to RDD[VertexId] and RDD[Edge[Int]]
    val vertices: RDD[(VertexId, Int)] = df.select("id").distinct()
      .map(row => (row.getInt(0).toLong, 0)) // 0 is a placeholder for vertex attribute, adjust as needed

    val edges: RDD[Edge[Int]] = df.select("id", "parent_id")
      .rdd
      .map(row => Edge(row.getInt(1).toLong, row.getInt(0).toLong, 1)) // 1 is a placeholder for edge attribute, adjust as needed

    // Create the graph
    Graph(vertices, edges)
  }

  // Check if the graph is cyclic
  def isCyclic(graph: Graph[Int, Int]): Boolean = {
    // Compute Strongly Connected Components
    val sccGraph = graph.stronglyConnectedComponents(1)
    
    // Extract the SCCs by collecting their vertices
    val sccs: RDD[(VertexId, VertexId)] = sccGraph.vertices

    // Check if any SCC contains more than one node
    val sccSizes: RDD[Long] = sccs.map { case (_, componentId) =>
      (componentId, 1L)
    }.reduceByKey(_ + _)
    
    // If any SCC has more than one node, then there is a cycle
    val hasCycle = sccSizes.map(_._2).filter(_ > 1).count() > 0

    hasCycle
  }
}
}
