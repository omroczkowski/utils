import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

class DetectCycle(spark: SparkSession, df: DataFrame, idColumn: String, parentIdColumn: String) {

  import spark.implicits._

  // Create a Graph from a DataFrame with parameterized id and parent_id columns
  def createGraphFromDataFrame(df: DataFrame, idCol: String, parentIdCol: String): Graph[Int, Int] = {
    // Convert DataFrame to RDD[VertexId] and RDD[Edge[Int]]
    val vertices: RDD[(VertexId, Int)] = df.select(idCol).distinct()
      .map(row => (row.getAs[Int](idCol).toLong, 0)) // 0 is a placeholder for vertex attribute

    val edges: RDD[Edge[Int]] = df.select(idCol, parentIdCol)
      .rdd
      .map(row => Edge(row.getAs[Int](parentIdCol).toLong, row.getAs[Int](idCol).toLong, 1)) // 1 is a placeholder for edge attribute

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
